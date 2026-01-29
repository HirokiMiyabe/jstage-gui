# app.py
import io
import datetime as dt
import polars as pl
import streamlit as st
from jstage_fetcher import fetch_jstage_data

st.set_page_config(page_title="J-STAGE Search GUI", layout="wide")
st.title("J-STAGE Search API GUI（service=3）")

def to_csv_ready(df: pl.DataFrame, sep: str = "; ") -> pl.DataFrame:
    """author(List[str]) -> author(str) へ変換（CSV向け）"""
    if df.is_empty():
        return df
    return df.with_columns(
        pl.when(pl.col("author").is_null())
          .then(None)
          .otherwise(pl.col("author").list.join(sep))
          .alias("author")
    )

with st.sidebar:
    st.header("検索条件")
    target_word = st.text_input("検索語", value="学際")
    year = st.number_input("開始年 (pubyearfrom)", min_value=0, max_value=3000, value=1950, step=1)
    field = st.selectbox("検索フィールド", ["article", "abst", "text"], index=0)
    max_records = st.number_input("最大取得件数（暴走防止）", min_value=1, max_value=500000, value=20000, step=1000)
    sleep = st.slider("リクエスト間隔（秒）", min_value=0.0, max_value=1.0, value=0.1, step=0.05)

    st.divider()
    st.subheader("保存 / 出力")
    autosave_parquet = st.checkbox("data/ に自動保存（parquet）", value=True)
    csv_sep = st.text_input("CSVの著者区切り", value="; ")

    run = st.button("取得する", type="primary")

if run:
    if not target_word.strip():
        st.error("検索語が空です")
        st.stop()

    st.info(f"取得開始：{target_word} / from={int(year)} / field={field}")

    df, total = fetch_jstage_data(
        target_word=target_word,
        year=int(year),
        field=field,
        max_records=int(max_records),
        sleep=float(sleep),
    )

    if df.is_empty():
        st.warning("0件でした。条件を変えて試してください。")
        st.stop()

    # メトリクス
    c1, c2, c3 = st.columns(3)
    c1.metric("取得件数", df.height)
    c2.metric("総件数（API）", total if total is not None else "不明")
    c3.metric("ユニークDOI", df.select(pl.col("doi").n_unique()).item() if "doi" in df.columns else 0)

    # 表示（authorはlistのまま：分析・確認に便利）
    st.caption("表示は author をリストのまま保持（JSON向け）。CSVはダウンロード時に結合します。")
    st.dataframe(df, use_container_width=True, height=520)

    st.divider()
    st.subheader("ダウンロード")

    # CSV（authorを結合して1列）
    df_csv = to_csv_ready(df, sep=csv_sep)
    csv_bytes = df_csv.write_csv().encode("utf-8")
    st.download_button(
        "CSVをダウンロード（author結合）",
        data=csv_bytes,
        file_name="jstage_results.csv",
        mime="text/csv",
    )

    # JSON（authorはlistのまま）
    json_str = df.write_json()
    st.download_button(
        "JSONをダウンロード（authorはlist）",
        data=json_str.encode("utf-8"),
        file_name="jstage_results.json",
        mime="application/json",
    )

    # Parquet（authorはlistのまま：そのまま保持される）
    buf = io.BytesIO()
    df.write_parquet(buf)
    st.download_button(
        "Parquetをダウンロード",
        data=buf.getvalue(),
        file_name="jstage_results.parquet",
        mime="application/octet-stream",
    )

    # =========================
    # 自動保存（ローカル）
    # =========================
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_word = "".join(ch if ch.isalnum() else "_" for ch in target_word)
    base = f"data/jstage_{safe_word}_{field}_{int(year)}_{ts}"

    if autosave_parquet:
        parquet_path = f"{base}.parquet"
        df.write_parquet(parquet_path)
        st.success(f"保存しました: {parquet_path}")

    # CSV（author を結合）
    autosave_csv = st.checkbox("data/ に自動保存（CSV）", value=False)
    if autosave_csv:
        csv_path = f"{base}.csv"
        df_csv = to_csv_ready(df, sep=csv_sep)
        df_csv.write_csv(csv_path)
        st.success(f"保存しました: {csv_path}")

    # JSON（author は list のまま）
    autosave_json = st.checkbox("data/ に自動保存（JSON）", value=False)
    if autosave_json:
        json_path = f"{base}.json"
        df.write_json(json_path)
        st.success(f"保存しました: {json_path}")
