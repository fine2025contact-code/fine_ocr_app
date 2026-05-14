"""
FINE: モダン・ダッシュボード版（タイトル横ロゴ維持版）
抽出エンジン：ハイブリッド（直接抽出 + EasyOCR）
UI：カスタムCSSによるデザイン強化 & タイトル横ポタポタ水道ロゴ

【修正 2026-05-06】
- 送信前に担当者選択を必須化（staff_mastersから取得）
- 担当者未選択の場合は送信ボタンを無効化
- 送信済みデータにsent_byカラムを追加
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

from parsing import parse_ocr_text
from sync_supabase import insert_fine_row

st.set_page_config(
    page_title="FINE OCR",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stApp { font-family: 'Inter', 'Segoe UI', Roboto, sans-serif; }
    .header-faucet-wrapper {
        display: inline-flex; align-items: center;
        position: relative; margin-left: 10px;
    }
    .header-faucet-icon { font-size: 32px; }
    .header-water-drop {
        position: absolute; top: 25px; left: 8px;
        width: 6px; height: 10px;
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
    .main-header {
        font-size: 2.5rem; font-weight: 700; color: #1e293b;
        margin-bottom: 0.5rem; display: flex; align-items: center;
    }
    [data-testid="stFileUploader"] {
        background-color: white; border: 2px dashed #cbd5e1;
        border-radius: 1rem; padding: 2rem; transition: all 0.3s ease;
        min-height: 250px; display: flex; align-items: center; justify-content: center;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: #3b82f6; background-color: #f1f5f9;
    }
    [data-testid="stFileUploader"] section { padding: 3rem !important; }
    [data-testid="stFileUploader"] section > label {
        font-size: 1.2rem !important; font-weight: 600 !important; color: #475569 !important;
    }
    div[data-testid="stVerticalBlock"] > div:has(div.stDataEditor) {
        background-color: white; padding: 1.5rem;
        border-radius: 0.75rem; border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    div.stButton > button { border-radius: 0.5rem; font-weight: 600; transition: all 0.2s ease; }
    section[data-testid="stSidebar"] { background-color: #00AEEF; color: white; }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] li { color: #ffffff !important; }
    section[data-testid="stSidebar"] .stButton button p { color: #60a5fa !important; }

    /* ★ 担当者選択エリアのスタイル */
    .staff-selector {
        background: linear-gradient(135deg, #eff6ff, #f0fdf4);
        border: 2px solid #3b82f6; border-radius: 0.75rem;
        padding: 1.2rem 1.5rem; margin-bottom: 1rem;
    }
    .staff-required {
        background: linear-gradient(135deg, #fef2f2, #fff7ed);
        border: 2px solid #ef4444; border-radius: 0.75rem;
        padding: 1.2rem 1.5rem; margin-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_ocr():
    return easyocr.Reader(['ja', 'en'], gpu=False)

ocr_reader = load_ocr()


def _get_supabase() -> Client | None:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        from supabase.lib.client_options import ClientOptions
        options = ClientOptions(postgrest_client_timeout=10, storage_client_timeout=10)
        return create_client(url, key, options=options)
    except Exception as e:
        st.sidebar.error(f"接続エラー詳細: {e}")
        return None


# ★ staff_masters から担当者リストを取得
@st.cache_data(ttl=300)
def fetch_staff_list() -> list[str]:
    try:
        supabase = _get_supabase()
        if not supabase:
            return []
        res = supabase.table("staff_masters").select("name").order("name").execute()
        return [row["name"] for row in (res.data or [])]
    except Exception as e:
        st.sidebar.warning(f"担当者取得エラー: {e}")
        return []


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

    return {
        "送信": False,
        "元請発注書なし": False,      # ★ 手入力時にチェックすると has_client_order=False になる
        "ファイル名": filename,
        "1. 元請名所":              parsed.get("company") or "不明",
        "2. 契約番号(注文/工事)":   parsed.get("id") or "-",
        "2-1. 現場ID/契約枝番号":   parsed.get("client_code2") or "",
        "2-2. 発注枝番":            parsed.get("client_code3") or "",
        "3. 現場名(事業名)":        parsed.get("site_name") or "-",
        "3-1. 工事名(邸名)":        parsed.get("koji_name") or parsed.get("site_name") or "-",
        "4. 施工場所(現場住所)":     parsed.get("address") or "-",
        "5. 代金(金額)":            int(amt) if isinstance(amt, (int, float)) else amt,
        "6. 工事件名(内容/名称)":    parsed.get("content") or "不明",
        "7. 注文書年月日(発注日)":   parsed.get("date") or "-",
        "8. 工期":                  kouki,
        "9. 請求日":                parsed.get("billing_date") or "",
        "10. 注文書種類":           parsed.get("docType") or "注文書",
        "注文No(F18)":             "-",
        "ステータス":               "未送信",
        "fields_display":          parsed.get("fields_display", {}),
    }


EDITOR_COLUMNS = [
    "送信", "元請発注書なし", "ファイル名", "1. 元請名所",
    "2. 契約番号(注文/工事)",
    "2-1. 現場ID/契約枝番号",
    "2-2. 発注枝番",
    "3. 現場名(事業名)", "3-1. 工事名(邸名)",
    "4. 施工場所(現場住所)", "5. 代金(金額)",
    "6. 工事件名(内容/名称)", "7. 注文書年月日(発注日)",
    "8. 工期", "9. 請求日", "10. 注文書種類",
    "注文No(F18)", "ステータス",
]


def main() -> None:
    # パスワード認証
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
        return

    # 状態の初期化
    if "fine_rows" not in st.session_state:
        st.session_state.fine_rows = []
    if "raw_texts" not in st.session_state:
        st.session_state.raw_texts = {}
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0
    # ★ 送信担当者の初期化
    if "selected_staff" not in st.session_state:
        st.session_state.selected_staff = ""
    if "manual_form_key" not in st.session_state:
        st.session_state.manual_form_key = 0

    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/database.png", width=60)
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
        3. **送信担当者を選択**（必須）
        4. 同期対象にチェック
        5. ボタンを押して送信
        """)

        if st.button("表示をクリア"):
            st.session_state.fine_rows = []
            st.session_state.raw_texts = {}
            st.session_state.uploader_key += 1
            # ★ 手入力フォームのstateをリセット（form_keyを変えることで強制リセット）
            st.session_state.manual_form_key = st.session_state.get("manual_form_key", 0) + 1
            st.rerun()

        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("""
            <div style="font-size: 1.45rem; font-weight: 800; color: #ffffff; line-height: 1.4; margin-top: 10px; letter-spacing: 0.05rem;">
                ㈱FINE<br>
                元請注文書<br>
                読み込みシステム
            </div>
        """, unsafe_allow_html=True)

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

    st.markdown("<br>", unsafe_allow_html=True)

    # ★ 起動時から表示される「元請発注書なし」手入力フォーム
    _mfk = st.session_state.get("manual_form_key", 0)
    with st.expander("📝 元請発注書なし（手入力で登録）", expanded=True):
        st.markdown("""
            <div style="background:#fff7ed;border-left:5px solid #f97316;padding:0.7rem 1rem;border-radius:0.5rem;margin-bottom:1rem;">
                <p style="margin:0;color:#c2410c;font-weight:bold;font-size:1rem;">
                    📄 元請から発注書が届いていない場合はここから直接登録できます
                </p>
            </div>
        """, unsafe_allow_html=True)

        staff_list_manual = fetch_staff_list()
        col_a, col_b = st.columns(2)
        with col_a:
            manual_client = st.text_input("元請会社名", key=f"manual_client_{_mfk}", placeholder="例：㈱アイ工務店")
            manual_name = st.text_input("工事名（邸名）", key=f"manual_name_{_mfk}", placeholder="例：田中 様邸 給排水設備工事")
            manual_address = st.text_input("現場住所", key=f"manual_address_{_mfk}", placeholder="例：愛知県名古屋市...")
        with col_b:
            manual_amount = st.number_input("受注額（税込）", key=f"manual_amount_{_mfk}", min_value=0, step=1000, format="%d")
            manual_staff = st.selectbox("送信担当者（必須）", options=[""] + staff_list_manual,
                format_func=lambda x: "担当者を選択..." if x == "" else x, key=f"manual_staff_{_mfk}")
            col_date1, col_date2 = st.columns(2)
            with col_date1:
                manual_start = st.date_input("工期（開始）", key=f"manual_start_{_mfk}", value=None)
            with col_date2:
                manual_end = st.date_input("工期（終了）", key=f"manual_end_{_mfk}", value=None)

        send_ok = bool(manual_staff and manual_client and manual_name)
        if st.button("📤 元請発注書なしで登録・送信", type="primary",
                     use_container_width=True, disabled=not send_ok):
            supabase_client = _get_supabase()
            if not supabase_client:
                st.error("クラウドに接続できません")
            else:
                try:
                    row_dict = {
                        "1. 元請名所": manual_client,
                        "3-1. 工事名(邸名)": manual_name,
                        "3. 現場名(事業名)": manual_name,
                        "4. 施工場所(現場住所)": manual_address,
                        "5. 代金(金額)": manual_amount,
                        "6. 工事件名(内容/名称)": manual_name,
                        "7. 注文書年月日(発注日)": "",
                        "8. 工期": f"{manual_start} ~ {manual_end}" if manual_start and manual_end else "",
                        "2. 契約番号(注文/工事)": "",
                        "2-1. 現場ID/契約枝番号": "",
                        "2-2. 発注枝番": "",
                        "9. 請求日": "",
                        "10. 注文書種類": "注文書",
                        "ファイル名": "手入力",
                        "sent_by": manual_staff,
                        "has_client_order": False,
                    }
                    from sync_supabase import insert_fine_row as _insert
                    _insert(supabase_client, row_dict, "")
                    st.success(f"✅ 登録しました！担当者：{manual_staff}　※F番号は振り分け画面で割り当てられます")
                    st.balloons()
                except Exception as e:
                    st.error(f"エラー: {e}")

    st.markdown("---")

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
        st.markdown("""
            <div style="background-color: #fee2e2; border-left: 6px solid #ef4444; padding: 1rem; margin-bottom: 1rem; border-radius: 0.5rem;">
                <p style="color: #b91c1c; font-size: 1.35rem; font-weight: bold; margin: 0;">
                    ⚠️ ※同期前に、抽出されたデータに誤りがないかご確認ください。
                </p>
            </div>
        """, unsafe_allow_html=True)

        # ★ 送信担当者選択（必須）
        staff_list = fetch_staff_list()
        staff_options = [""] + staff_list

        selected_staff = st.session_state.selected_staff
        is_staff_selected = bool(selected_staff)

        if is_staff_selected:
            st.markdown(f"""
                <div class="staff-selector">
                    <p style="margin:0; font-size:1.1rem; font-weight:800; color:#1d4ed8;">
                        ✅ 送信担当者：{selected_staff}
                    </p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="staff-required">
                    <p style="margin:0; font-size:1.1rem; font-weight:800; color:#dc2626;">
                        ⚠️ 送信担当者を選択してください（必須）
                    </p>
                </div>
            """, unsafe_allow_html=True)

        col_staff, col_spacer = st.columns([2, 3])
        with col_staff:
            new_staff = st.selectbox(
                "📋 送信担当者",
                options=staff_options,
                index=staff_options.index(selected_staff) if selected_staff in staff_options else 0,
                format_func=lambda x: "担当者を選択してください..." if x == "" else x,
                key="staff_selectbox"
            )
            if new_staff != selected_staff:
                st.session_state.selected_staff = new_staff
                st.rerun()

        st.markdown("<br/>", unsafe_allow_html=True)

        edited_df = st.data_editor(
            df[EDITOR_COLUMNS],
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "送信": st.column_config.CheckboxColumn("送信", default=False),
                "元請発注書なし": st.column_config.CheckboxColumn("📄 元請発注書なし", default=False),
                "5. 代金(金額)": st.column_config.NumberColumn("5. 代金(金額)", format="¥%d", min_value=-99000000),
                "注文No(F18)": st.column_config.TextColumn("注文No", disabled=True, help="F番号は振り分け画面で割り当てられます"),
                "ステータス": st.column_config.SelectboxColumn(
                    "ステータス", options=["未送信", "完了", "エラー"], disabled=True
                ),
            },
            hide_index=True,
            key="main_editor"
        )

        st.markdown("<br/>", unsafe_allow_html=True)

        btn_col1, btn_col2, btn_col3 = st.columns(3)

        with btn_col1:
            # ★ 担当者未選択の場合はボタンを無効化
            send_disabled = not is_staff_selected
            if st.button(
                "🔥 クラウドへデータ送信" if is_staff_selected else "🔒 担当者を選択してから送信",
                type="primary",
                use_container_width=True,
                disabled=send_disabled
            ):
                if not supabase:
                    st.error("接続設定が見つかりません。")
                else:
                    sync_data(edited_df, supabase, st.session_state.selected_staff)

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


def sync_data(edf: pd.DataFrame, supabase: Client, sent_by: str):
    to_sync = edf[(edf["送信"] == True) & (edf["ステータス"] != "完了")]

    if to_sync.empty:
        st.warning("送信対象が選択されていません（チェックボックスを入れてください）")
        return

    # ★ 担当者チェック（二重確認）
    if not sent_by:
        st.error("送信担当者が選択されていません。担当者を選択してから送信してください。")
        return

    success_count = 0
    with st.spinner(f"同期中... （送信担当者：{sent_by}）"):
        for idx, row in to_sync.iterrows():
            try:
                row_dict = row.to_dict()
                row_dict["sent_by"] = sent_by
                # ★ 元請発注書なしフラグを渡す
                row_dict["has_client_order"] = not bool(row_dict.get("元請発注書なし", False))
                # ★ F番号はOCR時点では割り当てない（振り分け画面で割り当てる）
                insert_fine_row(supabase, row_dict, "")
                edf.at[idx, "注文No(F18)"] = "振り分け時に割当"
                edf.at[idx, "ステータス"] = "完了"
                edf.at[idx, "送信"] = False
                success_count += 1
            except Exception as e:
                edf.at[idx, "ステータス"] = "エラー"
                st.error(f"エラー ({row['ファイル名']}): {e}")

    st.session_state.fine_rows = edf.to_dict("records")
    if success_count > 0:
        st.success(f"✅ {success_count} 件のデータを同期しました。（送信担当者：{sent_by}）")
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()