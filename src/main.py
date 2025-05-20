from json import dumps
from pathlib import Path
from typing import Any, List

import click
from bs4 import BeautifulSoup
from pandas import DataFrame
from progress.bar import Bar
from requests import get

from src.db import DB

BASE_URL = "https://openresearchsoftware.metajnl.com"
ARTICLES_URL = f"{BASE_URL}/articles?items=100"


def fetch_jors_article_urls_from_html() -> List[str]:
    """
    Fetches the listing page and parses article links from the static HTML.

    Returns:
        List of article URLs.
    """
    print("Fetching article listing HTML...")
    resp = get(ARTICLES_URL, timeout=60)
    soup = BeautifulSoup(resp.content, features="lxml")

    # Extract article links if present in static content
    article_links = soup.select("a.c-listing__link")
    urls = [link["href"] for link in article_links if link.get("href")]

    # Convert relative paths to full URLs
    full_urls = [url if url.startswith("http") else f"{BASE_URL}{url}" for url in urls]

    print(f"✅ Found {len(full_urls)} article URLs from static HTML")
    return full_urls


def getJORSHTMLFrontMatter() -> DataFrame:
    data: dict[str, List[Any]] = {
        "url": [],
        "status_code": [],
        "html": [],
    }

    urls = fetch_jors_article_urls_from_html()

    with Bar("Downloading article HTML pages...", max=len(urls)) as bar:
        for url in urls:
            try:
                resp = get(url, timeout=60)
                if resp.status_code == 200:
                    data["url"].append(resp.url)
                    data["status_code"].append(resp.status_code)
                    data["html"].append(resp.content)
            except Exception as e:
                print(f"[ERROR] {url}: {e}")
            bar.next()

    return DataFrame(data=data).sort_values(by="url", ignore_index=True)


def extractPaperMetadataFromFrontMatter(df: DataFrame) -> DataFrame:
    data: List[dict[str, Any]] = []

    with Bar("Extracting paper metadata from HTML...", max=df.shape[0]) as bar:
        for idx, row in df.iterrows():
            soup = BeautifulSoup(row["html"], features="lxml")

            title_tag = soup.find("h1", class_="c-article-title")
            doi_tag = soup.find("a", class_="c-bibliographic-information__doi-link")
            abstract_tag = soup.find("div", class_="c-article-section__content")
            authors = [a.text.strip() for a in soup.select("a.c-article-author-list__author")]
            keywords = [k.text.strip() for k in soup.select("span.article-keywords__term")]
            software_section = soup.find("div", id="software-availability")
            software_availability = software_section.text.strip() if software_section else None

            data.append({
                "url": row["url"],
                "title": title_tag.text.strip() if title_tag else "",
                "doi": doi_tag.text.strip() if doi_tag else "",
                "abstract": abstract_tag.text.strip() if abstract_tag else "",
                "authors": dumps(authors),
                "keywords": dumps(keywords),
                "software_availability": software_availability,
                "raw_html": row["html"],
            })

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
    default=Path("./jors.db"),
)
def main(outputFP: Path) -> None:
    db: DB = DB(fp=outputFP)
    db.create_tables()

    hfmDF: DataFrame = getJORSHTMLFrontMatter()
    pmDF: DataFrame = extractPaperMetadataFromFrontMatter(df=hfmDF)

    print("Extracted metadata for", len(pmDF), "articles")
    if not pmDF.empty:
        print(pmDF[["title", "doi"]].head())

    db.df2table(df=pmDF, table="articles")


if __name__ == "__main__":
    main()

