"""
FINE: Supabase同期モジュール（修正版）

【変更内容】
- insert_fine_row から orders / order_items への書き込みを削除
- projects テーブルへの書き込みのみ行う
- 振り分け画面から F番号で発注書を作成する設計に統一

【列名対応表】
app.py キー                    → DB フィールド
----------------------------------------
1. 元請名所                    → client_name
2. 契約番号(注文/工事)         → code_no1
2-1. 現場ID/契約枝番号         → code_no2
2-2. 発注枝番                  → code_no3
3. 現場名(事業名)              → name
4. 施工場所(現場住所)          → site_address
5. 代金(金額)                  → budget
7. 注文書年月日(発注日)        → start_date（発注日）
8. 工期                        → start_date（工期開始）
8-1. 納期                      → end_date（納期・工期終了）
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import pandas as pd
from supabase import Client

from parsing import resolve_client_id


# ★ F番号の採番ロジック（毎年3/1にプレフィックス更新）
# 2026年度: F190001〜  2027年度: F200001〜  2028年度: F210001〜
# 発注書（F番号）の採番・作成はすべて振り分け画面（OrderDistributor）で行う。
# 採番が必要な場合は fiscal_utils.generate_doc_no() を使用すること。
from fiscal_utils import get_doc_prefix, generate_doc_no  # noqa: F401 (将来利用用)



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
    t = str(val) if val is not None else ""
    if not t.strip() or t.strip() == "-":
        return None
    ms = list(re.finditer(r"(\d{4})[/\-\.年](\d{1,2})[/\-\.月](\d{1,2})", t))
    if not ms:
        return None
    m = ms[-1]
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _db_start_date_from_k(val: Any) -> str | None:
    t = str(val) if val is not None else ""
    if not t.strip() or t.strip() == "-":
        return None
    ms = list(re.finditer(r"(\d{4})[/\-\.年](\d{1,2})[/\-\.月](\d{1,2})", t))
    if not ms:
        return None
    m = ms[0]
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def insert_fine_row(
    supabase: Client,
    row: dict[str, Any],
    f18: str,  # ★ 互換性のため引数は残すが使用しない
) -> None:
    """
    OCR読み取りデータを projects テーブルのみに登録。
    orders / order_items への書き込みは振り分け画面で行う。
    """
    fd = row.get("fields_display", {}) or {}

    # 1. 日付
    raw_date = row.get("7. 注文書年月日(発注日)") or fd.get("no7_date") or row.get("date")
    iso_date = _iso_date_from_cell(raw_date)

    # 2. 会社名
    moto_name = str(
        row.get("1. 元請名所") or
        fd.get("no1_company") or
        row.get("company") or
        "不明"
    )
    client_id = resolve_client_id(moto_name)

    # 3. 工事名・現場名
    # ★ 3-1（邸名・棟番号含む）を優先、なければ3. 現場名を使用
    raw_koji    = row.get("3-1. 工事名(邸名)") or fd.get("no3_1_kojimei") or ""
    raw_site    = row.get("3. 現場名(事業名)") or fd.get("no3_site_name") or ""
    raw_content = row.get("6. 工事件名(内容/名称)") or fd.get("no6_content") or "名称未設定"

    # ★ 3-1に棟番号・号地などが含まれている場合は3-1を優先
    base_name = raw_koji if raw_koji else raw_site

    if base_name and base_name not in raw_content:
        name = f"{base_name} {raw_content}".strip()
    else:
        name = str(raw_content)

    # 4. 番号類（振り分け画面での参照用に保存）
    code_no1 = str(row.get("2. 契約番号(注文/工事)") or fd.get("no2_id") or "")
    code_no2 = str(row.get("2-1. 現場ID/契約枝番号") or fd.get("no2_1_code2") or "")
    code_no3 = str(row.get("2-2. 発注枝番") or fd.get("no2_2_code3") or "")

    # 5. 住所・金額・工期
    site_address = str(row.get("4. 施工場所(現場住所)") or fd.get("no4_address") or "")
    budget = _to_float_amount(
        row.get("5. 代金(金額)") or fd.get("no5_amount") or 0
    )

    raw_kouki = row.get("8. 工期") or fd.get("no8_kouki")
    raw_nouki = row.get("8-1. 納期") or fd.get("no8_1_nouki")
    # 工期・納期が空の場合は None（空欄）のまま。start_date のみ発注日をデフォルトとする
    db_start = _iso_date_from_cell(raw_kouki) if raw_kouki and str(raw_kouki).strip() not in ("-", "") else iso_date
    db_end   = _iso_date_from_cell(raw_nouki) if raw_nouki and str(raw_nouki).strip() not in ("-", "") else None

    # ★ projects テーブルのみに登録（orders/order_itemsは登録しない）
    # ★ 送信担当者（app.pyから渡される）
    sent_by = row.get("sent_by", "")

    project_body: dict[str, Any] = {
        "name": name,
        "client_id": client_id,
        "client_name": moto_name,
        "budget": budget,
        "code_no1": code_no1,
        "code_no2": code_no2,
        "code_no3": code_no3,
        "site_address": site_address,
        "start_date": db_start,
        "end_date": db_end,
        "status": "active",
        "has_client_order": row.get("has_client_order", True),
        "doc_type": str(row.get("10. 注文書種類") or row.get("docType") or "注文書"),  # ★ 枚目情報含む
    }

    proj_res = supabase.table("projects").insert(project_body).execute()
    proj_data = getattr(proj_res, "data", None) or []
    if not proj_data:
        raise RuntimeError("案件（projects）の作成に失敗しました。")
    new_project_id = proj_data[0]["id"]

    # ★ ocr_logs に送信記録を保存
    ocr_log_body: dict[str, Any] = {
        "project_id": new_project_id,
        "sent_by": sent_by,
        "file_name": str(row.get("ファイル名", "")),
        "client_name": moto_name,
        "project_name": name,
        "amount": budget,
    }
    supabase.table("ocr_logs").insert(ocr_log_body).execute()

    # ★ orders / order_items への書き込みはここで終了
    # 発注書（F番号）は振り分け画面（OrderDistributor）で作成する