"""
GAS processSingleRowToSupabase / sendCheckedRows に相当する Supabase 同期。
projects → orders → order_items の順で挿入し、F18 の採番を行う。
※最新のOCR抽出辞書（site_name, billing_date, docType等）対応版
※fields_display（1〜10表示項目）対応版
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import pandas as pd
from supabase import Client

from parsing import resolve_client_id


def _parse_f18_counter(last_no: str | None) -> int:
    if not last_no:
        return 0
    digits = re.sub(r"[^0-9]", "", str(last_no))
    if not digits:
        return 0
    return int(digits) % 10000


def get_max_f18_counter(supabase: Client) -> int:
    """DB上の F18* の最大番号に対応するカウンタ"""
    try:
        res = (
            supabase.table("orders")
            .select("order_custom_no")
            .like("order_custom_no", "F18%")
            .order("order_custom_no", desc=True)
            .limit(1)
            .execute()
        )
        data = getattr(res, "data", None) or []
        if isinstance(data, list) and len(data) > 0:
            return _parse_f18_counter(data.get("order_custom_no"))
    except Exception:
        pass
    return 0


def f18_exists(supabase: Client, f18: str) -> bool:
    res = (
        supabase.table("orders")
        .select("id")
        .eq("order_custom_no", f18)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", None) or []
    return len(data) > 0


def next_unique_f18(supabase: Client, start_counter: int) -> tuple[str, int]:
    """F18 + 下4桁で重複しない番号を返す"""
    c = start_counter
    while True:
        c += 1
        suffix = ("0000" + str(c))[-4:]
        f18 = "F18" + suffix
        if not f18_exists(supabase, f18):
            return f18, c


def _to_float_amount(v: Any) -> float:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("，", "").replace("¥", "").replace("￥", "").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _iso_date_from_cell(val: Any) -> str:
    if val is None or val == "-" or (isinstance(val, float) and pd.isna(val)):
        return date.today().isoformat()
    if isinstance(val, str):
        s = val.strip()
        if s in ("-", ""):
            return date.today().isoformat()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except ValueError:
                continue
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return date.today().isoformat()


def _db_end_date_from_k(val: Any) -> str | None:
    """工期/納期から end_date 用の日付文字列を推定（最後の日付を採用）"""
    t = str(val) if val is not None else ""
    if not t.strip() or t.strip() == "-":
        return None
    ms = list(re.finditer(r"(\d{4})[/\-\.年](\d{1,2})[/\-\.月](\d{1,2})", t))
    if not ms:
        return None
    m = ms[-1]
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def insert_fine_row(
    supabase: Client,
    row: dict[str, Any],
    f18: str,
) -> None:
    """
    1行を projects / orders / order_items に登録。
    UI（画面）で編集された値を最優先で反映するよう列名をマッピングしています。
    """
    # 内部保存用データ（画面に表示されない予備データ等）
    fd = row.get("fields_display", {}) or {}

    # 1. 日付 (UI: 7. 注文書年月日(発注日))
    raw_date = row.get("7. 注文書年月日(発注日)") or fd.get("no7_date") or row.get("date")
    iso_date = _iso_date_from_cell(raw_date)

    # 2. 会社名 (UI: 1. 元請名所)
    moto_name = str(
        row.get("1. 元請名所") or
        fd.get("no1_company") or
        row.get("company") or
        "不明"
    )
    client_id = resolve_client_id(moto_name)

    # 3. 工事名・現場名 (UI: 3. 現場名(事業名), 3-1. 工事名(邸名), 6. 工事件名(内容/名称))
    raw_site    = row.get("3. 現場名(事業名)") or row.get("3-1. 工事名(邸名)") or fd.get("no3_site_name") or ""
    raw_content = row.get("6. 工事件名(内容/名称)") or fd.get("no6_content") or "名称未設定"

    # 現場名と工事件名が別の場合は結合
    if raw_site and raw_site not in raw_content:
        name = f"{raw_site} {raw_content}".strip()
    else:
        name = str(raw_content)

    # 4. 番号類
    code_no1 = str(row.get("2. 契約番号(注文/工事)") or fd.get("no2_id") or "")
    code_no2 = str(row.get("2-1. 契約枝番号(業者NO)") or fd.get("no2_1_code2") or "")
    code_no3 = str(row.get("2-2. 発注枝番") or fd.get("no2_2_code3") or "")

    # 5. 住所・金額・工期
    site_address = str(row.get("4. 施工場所(現場住所)") or fd.get("no4_address") or "")
    budget = _to_float_amount(
        row.get("5. 代金(金額)") or fd.get("no5_amount") or 0
    )

    raw_end = row.get("8. 工期") or fd.get("no8_kouki")
    db_end = _db_end_date_from_k(raw_end) if raw_end else None

    # 6. 書類タイプ
    doc_type  = row.get("10. 注文書種類") or fd.get("no10_doc_type") or ""
    order_desc = f"【{doc_type}】 {name}" if doc_type and doc_type != "注文書" else name

    # --- DB登録 ---
    project_body: dict[str, Any] = {
        "name": name,
        "client_id": client_id,
        "client_name": moto_name,
        "budget": budget,
        "code_no1": code_no1,
        "site_address": site_address,
        "end_date": db_end,
        "status": "active",
    }

    proj_res = supabase.table("projects").insert(project_body).execute()
    proj_data = getattr(proj_res, "data", None) or []
    if not proj_data:
        raise RuntimeError("案件（projects）の作成に失敗しました。")
    new_project_id = proj_data["id"] # リストアクセスエラー修正

    order_body: dict[str, Any] = {
        "project_id": new_project_id,
        "order_date": iso_date,
        "order_custom_no": f18,
        "client_code1": code_no1,
        "client_code2": code_no2,
        "client_code3": code_no3,
        "description": order_desc,
        "status": "active",
    }

    order_res = supabase.table("orders").insert(order_body).execute()
    order_data = getattr(order_res, "data", None) or []
    if not order_data:
        raise RuntimeError("注文（orders）の作成に失敗しました。")
    new_order_id = order_data["id"] # リストアクセスエラー修正

    item_body: dict[str, Any] = {
        "order_id": new_order_id,
        "description": name,
        "unit_price": budget,
        "quantity": 1,
        "unit": "式",
        "tax_type": "税込",
    }

    supabase.table("order_items").insert(item_body).execute()