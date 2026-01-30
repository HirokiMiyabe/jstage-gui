# app.py
import io
import datetime as dt
import polars as pl
import streamlit as st
from jstage_fetcher import fetch_jstage_data

# ===== 利用規約 同意ゲート =====
if "agreed" not in st.session_state:
    st.session_state.agreed = False

if not st.session_state.agreed:
    st.markdown(
        """
        <div style="max-width: 720px; margin: 2rem auto; padding: 1.5rem;
                    border: 2px solid #f0ad4e; border-radius: 14px;
                    background: rgba(240,173,78,0.08);">
          <h2 style="margin-top: 0;">⚠ 利用前の重要な確認</h2>

          <p style="font-size: 1.05rem;">
            必ず下記の規約・説明ページをよく読んだうえで、各自の責任においてご利用ください。
            本アプリの作成者は、本アプリの利用によって生じたいかなる損害についても責任を負いません。
          </p>

          <ul style="line-height: 1.8;">
            <li>
              <a href="https://www.jstage.jst.go.jp/static/pages/TermsAndPolicies/ForIndividuals/-char/ja"
                 target="_blank" rel="noopener noreferrer">
                j-stage利用規約・ポリシー
              </a>
            </li>
            <li>
              <a href="https://www.jstage.jst.go.jp/static/pages/WebAPI/-char/ja"
                 target="_blank" rel="noopener noreferrer">
                J-STAGE WebAPI 利用規約
              </a>
            </li>
            <li>
              <a href="https://www.jstage.jst.go.jp/static/pages/JstageServices/TAB3/-char/ja"
                 target="_blank" rel="noopener noreferrer">
                J-STAGE WebAPI 利用規約について
              </a>
            </li>
          </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

    agree_read = st.checkbox("上記リンクから規約・説明を読みました")
    agree_responsibility = st.checkbox(
        "本アプリの利用により生じたいかなる損害についても、アプリ作成者ではなく使用者が責任を負うことに同意します"
    )

    if st.button("同意して利用開始", type="primary"):
        if agree_read and agree_responsibility:
            st.session_state.agreed = True
            st.rerun()
        else:
            st.error("すべての項目にチェックを入れてください。")

    # ★ ここで止める：同意するまで先に進めない
    st.stop()



st.set_page_config(page_title="J-STAGE Search GUI", layout="wide")
st.title("J-STAGE Search API GUI（service=3）")

def to_csv_ready(df: pl.DataFrame, sep: str = "; ") -> pl.DataFrame:
    """author(List[str]) -> author(str) へ変換（CSV向け / list[null]対策込み）"""
    if df.is_empty():
        return df
    return df.with_columns(
        pl.coalesce([pl.col("author"), pl.lit([])])
          .cast(pl.List(pl.Utf8), strict=False)
          .list.join(sep)
          .alias("author")
    )

with st.sidebar:
    st.header("検索条件")
    target_word = st.text_input("検索語", value="因果")
    year = st.number_input("開始年 (pubyearfrom)", min_value=0, max_value=3000, value=1950, step=1)
    field = st.selectbox("検索フィールド", ["article", "abst", "text"], index=0)
    max_records = st.number_input(
        "最大取得件数（暴走防止）",
        min_value=1,
        max_value=500000,
        value=20000,
        step=1000,
    )

    # 1) 注意文を追加
    st.caption("※J-STAGE閲覧規約上、登載データの大量ダウンロードは認められていません。")

    sleep = st.slider("リクエスト間隔（秒）", min_value=1.0, max_value=5.0, value=1.0, step=0.25)

    st.divider()
    st.subheader("保存 / 出力")

    autosave_formats = st.multiselect(
        "data/ に自動保存する形式",
        options=["CSV", "JSON", "Parquet"],
        default=["CSV"],
    )

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

    # ファイル名ベース（ダウンロードも自動保存もこれに揃える）
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_word = "".join(ch if ch.isalnum() else "_" for ch in target_word)
    base_name = f"jstage_{safe_word}_{field}_{int(year)}_{ts}"
    base_path = f"data/{base_name}"

    # メトリクス
    c1, c2, c3 = st.columns(3)
    c1.metric("取得件数", df.height)
    c2.metric("総件数（API）", total if total is not None else "不明")
    c3.metric("ユニークDOI", df.select(pl.col("doi").n_unique()).item() if "doi" in df.columns else 0)

    st.caption("表示は author をリストのまま保持（JSON/Parquet向け）。CSVはダウンロード時に結合します。")
    st.dataframe(df, use_container_width=True, height=520)

    st.divider()
    st.subheader("ダウンロード")

    # 2) ダウンロードも base_name を使う

    # CSV（authorを結合して1列）
    df_csv = to_csv_ready(df, sep=csv_sep)
    csv_bytes = df_csv.write_csv().encode("utf-8")
    st.download_button(
        "CSVをダウンロード（author結合）",
        data=csv_bytes,
        file_name=f"{base_name}.csv",
        mime="text/csv",
    )

    # JSON（authorはlistのまま）
    json_str = df.write_json()
    st.download_button(
        "JSONをダウンロード（authorはlist）",
        data=json_str.encode("utf-8"),
        file_name=f"{base_name}.json",
        mime="application/json",
    )

    # Parquet（authorはlistのまま）
    buf = io.BytesIO()
    df.write_parquet(buf)
    st.download_button(
        "Parquetをダウンロード",
        data=buf.getvalue(),
        file_name=f"{base_name}.parquet",
        mime="application/octet-stream",
    )

    # =========================
    # 自動保存（ローカル）
    # =========================
    if "Parquet" in autosave_formats:
        parquet_path = f"{base_path}.parquet"
        df.write_parquet(parquet_path)
        st.success(f"保存しました: {parquet_path}")

    if "CSV" in autosave_formats:
        csv_path = f"{base_path}.csv"
        df_csv.write_csv(csv_path)
        st.success(f"保存しました: {csv_path}")

    if "JSON" in autosave_formats:
        json_path = f"{base_path}.json"
        df.write_json(json_path)
        st.success(f"保存しました: {json_path}")

    st.divider()

# 3) クレジット表示（常にページ下部に表示）
st.markdown(
    """
<div style="font-size: 0.9rem; color: #666; margin-top: 2rem;">
  <div>表示情報提供元：<a href="https://www.jstage.jst.go.jp/browse/-char/ja" target="_blank" rel="noopener noreferrer">J-STAGE</a></div>
  <div>Powered by <a href="https://www.jstage.jst.go.jp/browse/-char/ja" target="_blank" rel="noopener noreferrer">J-STAGE</a></div>
</div>
""",
    unsafe_allow_html=True,
)

