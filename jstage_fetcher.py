import time
import urllib.parse
import requests
from lxml import etree
import polars as pl

API = "https://api.jstage.jst.go.jp/searchapi/do"
STEP = 1000

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}

def fetch_jstage_data(target_word: str, year: int, field: str, max_records: int, sleep: float = 0.1):
    query = urllib.parse.quote(target_word, safe="")
    all_data = []

    with requests.Session() as session:
        start_idx = 1
        total_results = None

        while True:
            url = (
                f"{API}?service=3&{field}={query}&pubyearfrom={year}"
                f"&start={start_idx}&count={STEP}"
            )

            r = session.get(url, timeout=30)
            r.raise_for_status()
            root = etree.fromstring(r.content)

            if total_results is None:
                tr = root.xpath("//opensearch:totalResults", namespaces=NS)
                total_results = int(tr[0].text) if tr else None

            entries = root.xpath("//atom:entry", namespaces=NS)
            if not entries:
                break

            for entry in entries:
                def get_text(x):
                    res = entry.xpath(x, namespaces=NS)
                    return res[0].text if res else None

                all_data.append({
                    "author": get_text("atom:author/atom:ja/atom:name"),
                    "article_title": get_text("atom:article_title/atom:ja"),
                    "material_title": get_text("atom:material_title/atom:ja"),
                    "article_link": get_text("atom:article_link/atom:ja"),
                    "pubyear": get_text("atom:pubyear"),
                    "doi": get_text("prism:doi"),
                })

                if len(all_data) >= max_records:
                    break

            if len(all_data) >= max_records:
                break

            start_idx += STEP
            if total_results and start_idx > total_results:
                break

            time.sleep(sleep)

    df = pl.DataFrame(all_data)
    if not df.is_empty():
        df = df.with_columns(pl.col("pubyear").cast(pl.Int32, strict=False))
    return df, total_results
