"""
FINE: モダン・ダッシュボード版（タイトル横ロゴ維持版）
抽出エンジン：ハイブリッド（直接抽出 + EasyOCR）
UI：カスタムCSSによるデザイン強化 & タイトル横ポタポタ水道ロゴ
"""

from __future__ import annotations

import time
from typing import Any

import fitz  # PyMuPDF
import pandas as pd
import streamlit as st
import numpy as np
import easyocr
from supabase import Client, create_client

# 安定版の解析ロジックをインポート
from parsing import parse_ocr_text
from sync_supabase import get_max_f18_counter, insert_fine_row, next_unique_f18

# --- 1. ページ設定とデザイン ---
st.set_page_config(
    page_title="FINE Dashboard",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS & アニメーション
st.markdown("""
    <style>
    /* メイン背景とフォント */
    .main {
        background-color: #f8f9fa;
    }
    .stApp {
        font-family: 'Inter', 'Segoe UI', Roboto, sans-serif;
    }
    
    /* タイトル横の水道ロゴ用設定 */
    .header-faucet-wrapper {
        display: inline-flex;
        align-items: center;
        position: relative;
        margin-left: 10px;
    }
    
    .header-faucet-icon {
        font-size: 32px;
    }
    
    .header-water-drop {
        position: absolute;
        top: 25px;
        left: 8px;
        width: 6px;
        height: 10px;
        background-color: #3b82f6;
        border-radius: 50% 50% 50% 50% / 60% 60% 40% 40%;
        animation: drip 1.5s infinite ease-in;
    }
    
    @keyframes drip {
        0% { transform: translateY(0) scale(0.5); opacity: 0; }
        20% { transform: translateY(0) scale(1); opacity: 1; }
        80% { transform: translateY(30px) scale(1); opacity: 0.8; }
        100% { transform: translateY(40px) scale(0.2); opacity: 0; }
    }
    
    /* ヘッダーの装飾 */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1e293b;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
    }
    
    /* アップロードエリアの拡大カスタマイズ */
    [data-testid="stFileUploader"] {
        background-color: white;
        border: 2px dashed #cbd5e1;
        border-radius: 1rem;
        padding: 2rem;
        transition: all 0.3s ease;
        min-height: 250px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: #3b82f6;
        background-color: #f1f5f9;
    }
    /* ドラッグアンドドロップの指示テキストサイズ調整 */
    [data-testid="stFileUploader"] section {
        padding: 3rem !important;
    }
    [data-testid="stFileUploader"] section > label {
        font-size: 1.2rem !important;
        font-weight: 600 !important;
        color: #475569 !important;
    }

    /* カード型のコンテナ */
    div[data-testid="stVerticalBlock"] > div:has(div.stDataEditor) {
        background-color: white;
        padding: 1.5rem;
        border-radius: 0.75rem;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    /* ボタンのカスタマイズ */
    div.stButton > button {
        border-radius: 0.5rem;
        font-weight: 600;
        transition: all 0.2s ease;
    }
    
    /* サイドバーのカスタマイズ */
    section[data-testid="stSidebar"] {
        background-color: #228b22;
        color: white;
    }
    /* 使い方セクションの文字を白にする */
    section[data-testid="stSidebar"] .stMarkdown, 
    section[data-testid="stSidebar"] p, 
    section[data-testid="stSidebar"] li {
        color: #ffffff !important;
    }

    /* サイドバー内のボタン（表示をクリア）の文字色を青にする */
    section[data-testid="stSidebar"] .stButton button p {
        color: #60a5fa !important;
    }
    
    /* 成功・エラーメッセージの丸み */
    div[data-testid="stNotification"] {
        border-radius: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# --- 2. OCRエンジンの初期化 ---
@st.cache_resource
def load_ocr():
    return easyocr.Reader(['ja', 'en'], gpu=False)

ocr_reader = load_ocr()

# --- 3. ユーティリティ ---
def _get_supabase() -> Client | None:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        
        # 修正：supabase-pyの新しいバージョンに対応した接続方法
        from supabase.lib.client_options import ClientOptions
        options = ClientOptions(postgrest_client_timeout=10, storage_client_timeout=10)
        
        return create_client(url, key, options=options)
    except Exception as e:
        st.sidebar.error(f"接続エラー詳細: {e}")
        return None

def extract_pdf_text_local(file_bytes: bytes, filename: str) -> str:
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    full_text_list: list[str] = []
    
    try:
        for i in range(doc.page_count):
            page = doc.load_page(i)
            direct_text = page.get_text("text")
            
            if len(direct_text.strip()) > 50:
                full_text_list.append(direct_text)
            else:
                pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
                img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
                result = ocr_reader.readtext(img, detail=0)
                if result:
                    full_text_list.extend(result)
    finally:
        doc.close()
    
    return "\n".join(full_text_list)

def parsed_to_row(filename: str, parsed: dict[str, Any]) -> dict[str, Any]:
    amt = parsed.get("amount") or 0
    sd = parsed.get("startDate")
    ed = parsed.get("endDate")
    if sd and ed and sd != ed:
        kouki = f"{sd} ~ {ed}"
    else:
        kouki = ed or sd or "-"

    # 変更点：各社の呼び方を併記して分かりやすくしました
    return {
        "送信": False,
        "ファイル名": filename,
        "1. 元請名所": parsed.get("company") or "不明",
        "2. 契約番号(注文/工事)": parsed.get("id") or "-",
        "2-1. 契約枝番号(業者NO)": parsed.get("client_code2") or "",
        "2-2. 発注枝番": parsed.get("client_code3") or "",
        "3. 現場名(事業名)": parsed.get("site_name") or "-",
        "3-1. 工事名(邸名)": parsed.get("koji_name") or parsed.get("site_name") or "-",
        "4. 施工場所(現場住所)": parsed.get("address") or "-",
        "5. 代金(金額)": int(amt) if isinstance(amt, (int, float)) else amt,
        "6. 工事件名(内容/名称)": parsed.get("content") or "不明",
        "7. 注文書年月日(発注日)": parsed.get("date") or "-",
        "8. 工期": kouki,
        "9. 請求日": parsed.get("billing_date") or "",
        "10. 注文書種類": parsed.get("docType") or "注文書",
        "注文No(F18)": "-",
        "ステータス": "未送信",
        "fields_display": parsed.get("fields_display", {})  # 内部保存用のデータも保持
    }

# 変更点：表示列の定義を番号付き・併記のキー名に合わせて修正
EDITOR_COLUMNS = [
    "送信", "ファイル名", "1. 元請名所", "2. 契約番号(注文/工事)", "2-1. 契約枝番号(業者NO)", "2-2. 発注枝番", 
    "3. 現場名(事業名)", "3-1. 工事名(邸名)", "4. 施工場所(現場住所)", "5. 代金(金額)", "6. 工事件名(内容/名称)", "7. 注文書年月日(発注日)", "8. 工期", "9. 請求日", "10. 注文書種類", "注文No(F18)", "ステータス"
]

# --- 4. メインアプリケーション ---
def main() -> None:
    # --- パスワード認証機能 ---
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.markdown("### 🔒 セキュリティ認証")
        password_input = st.text_input("パスワードを入力してください", type="password")
        if st.button("ログイン"):
            if password_input == st.secrets["APP_PASSWORD"]:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("パスワードが正しくありません")
        return  # 認証されるまでこれ以降のコード（OCR画面）は実行しない

    # 状態の初期化
    if "fine_rows" not in st.session_state:
        st.session_state.fine_rows = []
    if "raw_texts" not in st.session_state:
        st.session_state.raw_texts = {}
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0

    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/database.png", width=60)
        # 「FINE Sync」から「同期メニュー」へ変更
        st.title("同期メニュー")
        st.markdown("---")
        
        supabase = _get_supabase()
        if supabase:
            st.success("● クラウドに接続済み")
        else:
            st.error("○ オフライン（保存不可）")
            
        st.markdown("### 使い方")
        st.write("""
        1. PDFをアップロード
        2. 抽出内容を確認・修正
        3. 同期対象にチェック
        4. ボタンを押して送信
        """)
        
        if st.button("表示をクリア"):
            st.session_state.fine_rows = []
            st.session_state.raw_texts = {}
            st.session_state.uploader_key += 1 # アップローダーをリセット
            st.rerun()
            
        # サイドバー下段にシステム名を表示（フォントサイズを大幅に拡大）
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("""
            <div style="font-size: 1.45rem; font-weight: 800; color: #ffffff; line-height: 1.4; margin-top: 10px; letter-spacing: 0.05rem;">
                ㈱FINE<br>
                元請注文書<br>
                読み込みシステム
            </div>
        """, unsafe_allow_html=True)

    # タイトル部分にインラインで水道ロゴを配置、タイトル名を変更
    st.markdown("""
        <div class="main-header">
            元請注文書読み込みシステム
            <div class="header-faucet-wrapper">
                <span class="header-faucet-icon">🚰</span>
                <div class="header-water-drop"></div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.caption("高性能OCR解析 ＆ 基幹システム同期ツール")

    # アップロード場所の改善（uploader_key を使用してリセット可能にする）
    uploaded = st.file_uploader(
        "PDFファイルをここにドラッグ＆ドロップしてください",
        type=["pdf"],
        accept_multiple_files=True,
        label_visibility="visible",
        key=f"pdf_uploader_{st.session_state.uploader_key}"
    )

    if uploaded:
        new_rows: list[dict[str, Any]] = []
        progress_text = st.empty()
        bar = st.progress(0)
        
        for idx, uf in enumerate(uploaded):
            # すでに解析済みのファイルはスキップ（重複防止）
            if any(row["ファイル名"] == uf.name for row in st.session_state.fine_rows):
                continue

            progress_text.text(f"解析中 ({idx+1}/{len(uploaded)}): {uf.name}")
            bar.progress((idx + 1) / len(uploaded))
            
            raw_bytes = uf.read()
            extracted_text = extract_pdf_text_local(raw_bytes, uf.name)
            
            st.session_state.raw_texts[uf.name] = extracted_text
            
            parsed = parse_ocr_text(extracted_text, uf.name)
            new_rows.append(parsed_to_row(uf.name, parsed))
        
        if new_rows:
            st.session_state.fine_rows.extend(new_rows)
            st.toast(f"{len(new_rows)} 件の解析が完了しました！", icon="✅")
        
        progress_text.empty()
        bar.empty()

    if not st.session_state.fine_rows:
        st.markdown("""
        <div style="text-align: center; padding-top: 1rem; color: #94a3b8;">
            <small>※ 複数ファイルを一度にアップロードできます</small>
        </div>
        """, unsafe_allow_html=True)
        return

    df = pd.DataFrame(st.session_state.fine_rows)
    
    m1, m2 = st.columns(2)
    with m1:
        st.metric("解析済み件数", f"{len(df)} 件")
    with m2:
        complete_count = len(df[df["ステータス"] == "完了"])
        st.metric("同期完了", f"{complete_count} / {len(df)}")

    tab1, tab2 = st.tabs(["📋 データ編集・送信", "📄 抽出テキスト確認"])

    with tab1:
        # ★ ここに大きな警告テキストを追加しました ★
        st.markdown("""
            <div style="background-color: #fee2e2; border-left: 6px solid #ef4444; padding: 1rem; margin-bottom: 1rem; border-radius: 0.5rem;">
                <p style="color: #b91c1c; font-size: 1.35rem; font-weight: bold; margin: 0;">
                    ⚠️ ※同期前に、抽出されたデータに誤りがないかご確認ください。
                </p>
            </div>
        """, unsafe_allow_html=True)

        edited_df = st.data_editor(
            df[EDITOR_COLUMNS],
            use_container_width=True,
            num_rows="dynamic",
            # 変更点：代金のキー名を併記フォーマットに変更
            column_config={
                "送信": st.column_config.CheckboxColumn("送信", default=False),
                "5. 代金(金額)": st.column_config.NumberColumn("5. 代金(金額)", format="¥%d"),
                "ステータス": st.column_config.SelectboxColumn(
                    "ステータス", options=["未送信", "完了", "エラー"], disabled=True
                )
            },
            hide_index=True,
            key="main_editor"
        )

        st.markdown("<br/>", unsafe_allow_html=True)
        
        btn_col1, btn_col2, btn_col3 = st.columns(3)
        
        with btn_col1:
            # ボタン名も「クラウドへ送信」など分かりやすく変更可能
            if st.button("🔥 クラウドへデータ送信", type="primary", use_container_width=True):
                if not supabase:
                    st.error("接続設定が見つかりません。")
                else:
                    sync_data(edited_df, supabase)
        
        with btn_col2:
            csv = edited_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 CSV保存",
                data=csv,
                file_name=f"FINE_Export_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )

    with tab2:
        if "raw_texts" in st.session_state:
            for fname, txt in st.session_state.raw_texts.items():
                with st.expander(f"👁️ {fname}"):
                    st.text_area("Raw Text", txt, height=300, key=f"raw_{fname}", label_visibility="collapsed")

def sync_data(edf: pd.DataFrame, supabase: Client):
    to_sync = edf[(edf["送信"] == True) & (edf["ステータス"] != "完了")]
    
    if to_sync.empty:
        st.warning("送信対象が選択されていません（チェックボックスを入れてください）")
        return

    success_count = 0
    with st.spinner("同期中..."):
        current_counter = get_max_f18_counter(supabase)
        
        for idx, row in to_sync.iterrows():
            try:
                f18_code, current_counter = next_unique_f18(supabase, current_counter)
                insert_fine_row(supabase, row.to_dict(), f18_code)
                edf.at[idx, "注文No(F18)"] = f18_code
                edf.at[idx, "ステータス"] = "完了"
                edf.at[idx, "送信"] = False
                success_count += 1
            except Exception as e:
                edf.at[idx, "ステータス"] = "エラー"
                st.error(f"エラー ({row['ファイル名']}): {e}")

    st.session_state.fine_rows = edf.to_dict("records")
    if success_count > 0:
        st.success(f"✅ {success_count} 件のデータを同期しました。")
        time.sleep(2)
        st.rerun()

if __name__ == "__main__":
    main()