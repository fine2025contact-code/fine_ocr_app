"""
新規会社のSupabase登録スクリプト
対象: アーキテックス株式会社 新築事業部、阿部建設株式会社
"""
import sys
import os

# secrets.tomlから直接読み込む
import tomllib

secrets_path = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
with open(secrets_path, "rb") as f:
    secrets = tomllib.load(f)

SUPABASE_URL = secrets["SUPABASE_URL"]
SUPABASE_KEY = secrets["SUPABASE_KEY"]

from supabase import create_client

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# 1. clients テーブルへの登録
# ============================================================
CLIENTS_TO_ADD = [
    {
        "id": "9336c048-c375-4094-8c6b-f6b95fd7a56c",
        "name": "アーキテックス㈱",
    },
    {
        "id": "0cb5c980-9096-4c52-9dd7-7f24bc6b9a03",
        "name": "阿部建設㈱",
    },
]

print("=== clients テーブル確認・登録 ===")
for client in CLIENTS_TO_ADD:
    # 既存チェック
    existing = supabase.table("clients").select("id, name").eq("id", client["id"]).execute()
    if existing.data:
        print(f"  [SKIP] {client['name']} は既に登録済みです (id={client['id']})")
    else:
        res = supabase.table("clients").insert(client).execute()
        if res.data:
            print(f"  [OK]   {client['name']} を登録しました")
        else:
            print(f"  [ERR]  {client['name']} の登録に失敗しました: {res}")

# ============================================================
# 2. client_configs テーブルへの登録
# ============================================================
CLIENT_CONFIGS_TO_ADD = [
    {
        "name": "アーキテックス㈱",
        # 発注書に表示されるラベル名
        "label_no1":         "発注管理ID",       # 管理ID（例: 381936-01V28J-5427673）
        "label_site_name":   "案件名",            # 現場名
        "label_amount":      "発注金額",           # 発注金額（税込）
        "label_content":     "屋内給排水工事",     # 工事内容（案件名の下段）
        "label_date":        "出力日",             # 発注日
        "label_kouki_end":   "工程終了日",         # 工期終了日
    },
    {
        "name": "阿部建設㈱",
        # 発注書に表示されるラベル名
        "label_no1":         "工事コード",         # 工事コード（例: 2460001）
        "label_site_name":   "工事名",             # 工事名
        "label_address":     "工事場所",           # 工事場所（現場住所）
        "label_amount":      "税込金額",           # 税込金額
        "label_content":     "名称",               # 工事内容（名称列）
        "label_date":        "作成日",             # 作成日
        "label_kouki":       "工期",               # 工期
        "label_billing":     "請負日",             # 請負日
    },
]

print("\n=== client_configs テーブル確認・登録 ===")
for config in CLIENT_CONFIGS_TO_ADD:
    # 既存チェック（nameで検索）
    existing = supabase.table("client_configs").select("id, name").ilike("name", f"%{config['name']}%").execute()
    if existing.data:
        print(f"  [SKIP] {config['name']} の設定は既に登録済みです")
        for row in existing.data:
            print(f"         id={row['id']}, name={row['name']}")
    else:
        res = supabase.table("client_configs").insert(config).execute()
        if res.data:
            print(f"  [OK]   {config['name']} のOCR設定を登録しました")
        else:
            print(f"  [ERR]  {config['name']} のOCR設定登録に失敗しました: {res}")

print("\n完了！")
