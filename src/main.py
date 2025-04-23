from concurrent.futures import ThreadPoolExecutor
from json import dumps
from pathlib import Path
from typing import Any, List, Tuple

import click
from bs4 import BeautifulSoup, ResultSet, Tag
from pandas import DataFrame, Series
from progress.bar import Bar
from requests import Response, get

from src.db import DB


def getPage(url: str) -> Response:
    return get(
        url=url,
        timeout=60,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0",
        },
    )


def getTotalNumberOfPages(url: str) -> dict[str, int]:
    resp: Response = getPage(url=url)
    soup: BeautifulSoup = BeautifulSoup(markup=resp.content, features="lxml")

    paginationInformation: Tag = soup.find(
        name="span",
        attrs={"class": "pagy info"},
    )
    totalDocsTag: Tag = paginationInformation.find_all(name="b")[1]
    totalDocs: int = int(totalDocsTag.text)
    totalPages: int = (
        (totalDocs // 20) + 1 if totalDocs % 20 != 0 else totalDocs // 20
    )  # noqa: E501

    return {"docs": totalDocs, "pages": totalPages}


def loadHTMLFrontMatter(resps: List[Response]) -> DataFrame:
    data: dict[str, List[Any]] = {
        "url": [],
        "page": [],
        "status_code": [],
        "html": [],
    }

    resp: Response
    for resp in resps:
        data["url"].append(resp.url)
        data["page"].append(int(resp.url.split("=")[1]))
        data["status_code"].append(resp.status_code)
        data["html"].append(resp.content)

    return DataFrame(data=data).sort_values(by="page", ignore_index=True)


def extractPaperMetadata(df: DataFrame) -> DataFrame:
    data: List[dict[str, Any]] = []

    with Bar(
        "Extracting paper metadata from HTML front matter...", max=df.shape[0]
    ) as bar:
        idx: int
        row: Series
        for idx, row in df.iterrows():
            soup: BeautifulSoup = BeautifulSoup(
                markup=row["html"],
                features="lxml",
            )
            cards: ResultSet = soup.find_all(
                name="div",
                attrs={"class": "paper-card"},
            )

            card: Tag
            for card in cards:
                badgesDatum: List[dict[str, str] | None] = []
                submitterDatum: dict[str, str] = {
                    "submitter": "",
                    "github": "",
                }

                status: str = card.find(
                    name="span", attrs={"class": "badge"}
                ).text.strip()
                time: str = card.find(
                    name="span", attrs={"class": "time"}
                ).text.strip()  # noqa: E501
                title: str = card.find(
                    name="h2", attrs={"class": "paper-title"}
                ).text.strip()

                badges: ResultSet = card.find_all(
                    name="span",
                    attrs={"class": "badge-lang"},
                )
                badge: Tag
                for badge in badges:
                    language: str = badge.text.strip()
                    uri: str = badge.find(name="a").get(key="href").strip()
                    badgesDatum.append(
                        {
                            "language": language,
                            "uri": uri,
                        }
                    )

                submitterTag: Tag = card.find(
                    name="div",
                    attrs={"class": "submitted_by"},
                )
                submitterDatum["submitter"] = submitterTag.text.strip()
                submitterDatum["github"] = (
                    submitterTag.find(name="a").get(key="href").strip()
                )

                doiTag: Tag = card.find(name="div", attrs={"class": "doi"})
                doi: str = doiTag.find(name="a").get("href").strip()

                if doi.__contains__("https://joss.theoj.org"):
                    pass
                else:
                    doi = "https://joss.theoj.org" + doi

                data.append(
                    {
                        "front_matter_id": idx,
                        "status": status,
                        "time": time,
                        "title": title,
                        "badges": dumps(obj=badgesDatum),
                        "submitter": dumps(obj=submitterDatum),
                        "doi": doi,
                    }
                )

            bar.next()

    return DataFrame(data=data)


def extractRepositoryFromMetadata(df: DataFrame) -> DataFrame:
    resps: List[Tuple[int, Response]] = []
    data: List[dict[str, Any]] = []

    dois: List[Tuple[int, str]] = list(df["doi"].to_dict().items())

    with Bar("Downloading paper webpage...", max=len(dois)) as bar:
        with ThreadPoolExecutor() as executor:

            def _run(pairing: Tuple[int, str]) -> None:
                resp: Response = getPage(url=pairing[1])
                datum: Tuple[Response] = (pairing[0], resp)
                resps.append(datum)
                bar.next()

            executor.map(_run, dois)

    with Bar(
        "Extracting repository from paper webpages...", max=len(resps)
    ) as bar:  # noqa: E501
        metadataID: int
        resp: Response
        for metadataID, resp in resps:
            statusCode: int = resp.status_code
            html: str = resp.content

            soup: BeautifulSoup = BeautifulSoup(markup=html, features="lxml")
            buttonsDiv: Tag = soup.find(
                name="div",
                attrs={"class": "btn-group-vertical"},
            )
            repositoryURL: str = buttonsDiv.find(name="a").get(key="href")

            data.append(
                {
                    "metadata_id": metadataID,
                    "status_code": statusCode,
                    "repository_url": repositoryURL,
                    "html": html,
                }
            )
            bar.next()

    return DataFrame(data=data)


@click.command()
@click.option(
    "-o",
    "--output",
    "outputFP",
    help="Path to output SQLite3 database",
    required=False,
    type=click.Path(
        exists=False,
        file_okay=True,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
    default=Path("./joss.db"),
)
def main(outputFP: Path) -> None:
    db: DB = DB(fp=outputFP)
    db.createTables()

    tnop: dict[str, int] = getTotalNumberOfPages(
        url="https://joss.theoj.org/papers",
    )

    htmlFrontMatter: List[Response] = []

    with Bar(
        "Downloading HTML front matter of JOSS...", max=tnop["pages"]
    ) as bar:  # noqa: E501
        with ThreadPoolExecutor() as executor:

            def _run(page: int) -> None:
                resp: Response = getPage(
                    url=f"https://joss.theoj.org/papers?page={page}",
                )
                htmlFrontMatter.append(resp)
                bar.next()

            executor.map(
                _run,
                range(1, tnop["pages"] + 1),
            )

    hfmDF: DataFrame = loadHTMLFrontMatter(resps=htmlFrontMatter)
    pmDF: DataFrame = extractPaperMetadata(df=hfmDF)
    rDF: DataFrame = extractRepositoryFromMetadata(df=pmDF)

    hfmDF.to_sql(
        name="front_matter",
        con=db.engine,
        if_exists="append",
        index=True,
        index_label="id",
    )
    pmDF.to_sql(
        name="metadata",
        con=db.engine,
        if_exists="append",
        index=True,
        index_label="id",
    )
    rDF.to_sql(
        name="software",
        con=db.engine,
        if_exists="append",
        index=True,
        index_label="id",
    )


if __name__ == "__main__":
    main()
