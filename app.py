import io
import datetime as dt
import polars as pl
import streamlit as st
from jstage_fetcher import fetch_jstage_data

st.set_page_config(page_title="J-STAGE Search GUI", layout="wide")
st.title("J-STAGE Search API GUI（service=3）")

with st.sidebar:
    st.header("検索条件")
    target_word = st.text_input("検索語", value="学際")
    year = st.number_input("開始年 (pubyearfrom)", min_value=0, max_value=3000, value=1950, step=1)
    field = st.selectbox("検索フィールド", ["article", "abst", "text"], index=0)
    max_records = st.number_input("最大取得件数（暴走防止）", min_value=1, max_value=500000, value=20000, step=1000)
    sleep = st.slider("リクエスト間隔（秒）", min_value=0.0, max_value=1.0, value=0.1, step=0.05)
    autosave = st.checkbox("data/ に自動保存（parquet）", value=True)
    run = st.button("取得する", type="primary")

if run:
    if not target_word.strip():
        st.error("検索語が空です")
        st.stop()

    st.info(f"取得開始：{target_word} / from={int(year)} / field={field}")
    df, total = fetch_jstage_data(target_word, int(year), field, int(max_records), float(sleep))

    if df.is_empty():
        st.warning("0件でした。条件を変えて試してください。")
        st.stop()

    c1, c2, c3 = st.columns(3)
    c1.metric("取得件数", df.height)
    c2.metric("総件数（API）", total if total is not None else "不明")
    c3.metric("ユニークDOI", df.select(pl.col("doi").n_unique()).item())

    st.dataframe(df, use_container_width=True, height=520)

    # ダウンロード
    csv_bytes = df.write_csv().encode("utf-8")
    st.download_button("CSVをダウンロード", data=csv_bytes, file_name="jstage_results.csv", mime="text/csv")

    buf = io.BytesIO()
    df.write_parquet(buf)
    st.download_button("Parquetをダウンロード", data=buf.getvalue(), file_name="jstage_results.parquet", mime="application/octet-stream")

    # 自動保存（ローカル）
    if autosave:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out = f"data/jstage_{target_word}_{field}_{int(year)}_{ts}.parquet"
        df.write_parquet(out)
        st.success(f"保存しました: {out}")
