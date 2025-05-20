import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
from pandas import DataFrame
from progress.bar import Bar

from src.db import DB

ARTICLES_URL_TEMPLATE = "https://openresearchsoftware.metajnl.com/articles?items=100&page={}"


def download_listing_pages(total_pages: int = 4) -> DataFrame:
    """
    Downloads the HTML content for each of the listing pages.
    """
    data = {"url": [], "html": [], "page": []}
    print("⚡ Downloading listing pages from JORS...")

    for page in range(1, total_pages + 1):
        url = ARTICLES_URL_TEMPLATE.format(page)
        print(f"📄 Fetching listing page {page}: {url}")
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if response.status_code == 200:
                data["url"].append(url)
                data["html"].append(response.text)
                data["page"].append(page)
        except Exception as e:
            print(f"[ERROR] Failed to fetch page {page}: {e}")

    print(f"✅ Downloaded {len(data['url'])} listing pages successfully.")
    return DataFrame(data)


def get_all_article_urls() -> List[Tuple[str, int]]:
    print("🔎 Scraping article URLs from JORS...")
    all_urls: List[Tuple[str, int]] = []

    for page in range(1, 5):  # 4 pages total
        url = ARTICLES_URL_TEMPLATE.format(page)
        print(f"Fetching {url}...")
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        if response.status_code != 200:
            print(f"[WARN] Failed to load page {page}")
            continue

        soup = BeautifulSoup(response.content, "lxml")
        links = soup.find_all("a", href=True)

        article_links = {
            "https://openresearchsoftware.metajnl.com" + a["href"]
            for a in links
            if a["href"].startswith("/articles/10.") and not a["href"].endswith(".pdf")
        }

        all_urls.extend((url, page) for url in article_links)
        print(f"✅ Found {len(article_links)} unique articles on page {page}")

    unique_urls = list(set(all_urls))
    print(f"✅ Total unique articles found: {len(unique_urls)}")
    return unique_urls


def download_article_pages(urls_with_pages: List[Tuple[str, int]]) -> DataFrame:
    data = {"url": [], "html": [], "page": []}
    print("⚡ Downloading HTML front matter of JORS articles...")

    def fetch(index: int, url: str, page: int) -> Tuple[str, bytes | None, int]:
        try:
            print(f"📄 Fetching {index + 1}/{len(urls_with_pages)}: {url}")
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if response.status_code == 200:
                return url, response.content, page
            else:
                return url, None, page
        except Exception as e:
            print(f"[ERROR] Failed to fetch {url}: {e}")
            return url, None, page

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch, i, url, page) for i, (url, page) in enumerate(urls_with_pages)]
        for future in as_completed(futures):
            url, content, page = future.result()
            if content:
                data["url"].append(url)
                data["html"].append(content)
                data["page"].append(page)

    print(f"✅ Downloaded {len(data['url'])} articles successfully.")
    return DataFrame(data)


def extract_metadata(df: DataFrame) -> DataFrame:
    data: List[dict[str, Any]] = []

    with Bar("Extracting paper metadata from HTML...", max=df.shape[0]) as bar:
        for idx, row in df.iterrows():
            soup = BeautifulSoup(row["html"], "lxml")

            try:
                full_title = soup.title.string.strip() if soup.title else ""
                title = full_title.split(" - Journal of Open Research Software")[0]

                url = row["url"]

                author_tags = soup.find_all("meta", attrs={"name": "dc.creator"})
                if not author_tags:
                    author_tags = soup.find_all("meta", attrs={"name": "citation_author"})
                authors = "; ".join(tag["content"].strip() for tag in author_tags if tag.get("content"))

                pub_date = ""
                for tag in soup.find_all(string=True):
                    text = tag.strip()
                    if text.lower().startswith("published on"):
                        pub_date = text.replace("Published on", "").strip()
                        break

                abstract = ""
                abstract_header = soup.find(lambda tag: tag.name in ["h2", "strong", "b"] and "abstract" in tag.text.lower())
                if abstract_header:
                    next_elem = abstract_header.find_next()
                    while next_elem and next_elem.name not in ["p", "div"]:
                        next_elem = next_elem.find_next()
                    if next_elem:
                        abstract = next_elem.text.strip()

                data.append({
                    "url": url,
                    "title": title,
                    "abstract": abstract,
                    "publication_date": pub_date,
                    "authors": authors,
                })

            except Exception as e:
                print(f"[ERROR] Failed to parse {row['url']}: {e}")

            bar.next()

    print(f"✅ Extracted metadata for {len(data)} articles")
    return DataFrame(data)


@click.command()
@click.option(
    "-o", "--output", "outputFP",
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

    # Store only listing pages in front_matter
    listing_pages_df = download_listing_pages()
    db.df2table(df=listing_pages_df, table="front_matter")

    # Scrape metadata from individual articles
    article_urls = get_all_article_urls()
    article_pages_df = download_article_pages(article_urls)
    metadf = extract_metadata(article_pages_df)
    db.df2table(df=metadf, table="metadata")


if __name__ == "__main__":
    main()

