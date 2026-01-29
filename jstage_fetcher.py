# jstage_fetcher.py
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
    "xml": "http://www.w3.org/XML/1998/namespace",
}

def _get_texts(entry, xpath_query: str) -> list[str]:
    nodes = entry.xpath(xpath_query, namespaces=NS)
    return [n.text for n in nodes if getattr(n, "text", None)]

def _get_first(entry, xpath_query: str):
    vals = _get_texts(entry, xpath_query)
    return vals[0] if vals else None

def _pick_ja_or_first(entry, tag_path: str):
    ja = _get_texts(entry, f"{tag_path}[@xml:lang='ja']")
    if ja:
        return ja[0]
    any_ = _get_texts(entry, tag_path)
    return any_[0] if any_ else None

def _texts_local(entry, xpath_expr: str) -> list[str]:
    """namespacesを無視して文字列を回収する（text()/@attr どちらでもOK）"""
    vals = entry.xpath(xpath_expr)
    out = []
    for v in vals:
        if isinstance(v, str):
            t = v.strip()
            if t:
                out.append(t)
        else:
            t = getattr(v, "text", None)
            if t and t.strip():
                out.append(t.strip())
    return out

def _first_local(entry, xpath_expr: str):
    vals = _texts_local(entry, xpath_expr)
    return vals[0] if vals else None

def _pick_ja_or_first_tag_local(entry, tag: str) -> str | None:
    """
    <tag><ja>TEXT</ja></tag> を優先し、なければ <tag> 配下の最初のテキストを返す
    tag例: article_title, material_title, article_link
    """
    ja = _first_local(entry, f"./*[local-name()='{tag}']/*[local-name()='ja']/text()")
    if ja:
        return ja
    any_text = _first_local(entry, f"./*[local-name()='{tag}']//text()")
    return any_text

def _authors_local(entry) -> list[str]:
    """<author><ja><name>..</name></ja></author> を優先し、ダメなら <author><name>..</name> を見る"""
    ja = _texts_local(entry, "./*[local-name()='author']/*[local-name()='ja']/*[local-name()='name']/text()")
    if ja:
        return ja
    return _texts_local(entry, "./*[local-name()='author']/*[local-name()='name']/text()")



def fetch_jstage_data(
    target_word: str,
    year: int,
    field: str,
    max_records: int,
    sleep: float = 0.1,
):
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
                authors = _authors_local(entry)

                all_data.append({
                    "author": authors,
                    "article_title": _pick_ja_or_first_tag_local(entry, "article_title"),
                    "material_title": _pick_ja_or_first_tag_local(entry, "material_title"),
                    "article_link": _pick_ja_or_first_tag_local(entry, "article_link"),

                    "pubyear": _get_first(entry, "atom:pubyear"),
                    "doi": _get_first(entry, "prism:doi"),

                    "volume": _get_first(entry, "prism:volume"),
                    "cdvols": _first_local(entry, "./*[local-name()='cdvols']/text()"),
                    "number": _get_first(entry, "prism:number"),
                    "starting_page": _get_first(entry, "prism:startingPage"),
                    "ending_page": _get_first(entry, "prism:endingPage"),
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
        df = df.with_columns([
            # ★これを追加：authorを必ず list[str] に固定
            pl.col("author").cast(pl.List(pl.Utf8), strict=False),
            pl.col("pubyear").cast(pl.Int32, strict=False),
            pl.col("starting_page").cast(pl.Int32, strict=False),
            pl.col("ending_page").cast(pl.Int32, strict=False),
        ])
    return df, total_results