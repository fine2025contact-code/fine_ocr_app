"""
FINE: 会計年度ユーティリティ（OCRシステム用）

毎年 3/1 に年度が切り替わります。
  2026年度（〜2027/2/28） → プレフィックス 19  → F190001, M190001, S190001
  2027年度（2027/3/1〜）  → プレフィックス 20  → F200001, M200001, S200001
  2028年度（2028/3/1〜）  → プレフィックス 21  → F210001, M210001, S210001

Next.js 側の lib/fiscalUtils.js と同一ロジック。
"""

from __future__ import annotations
from datetime import date


def get_fiscal_prefix() -> int:
    """
    現在の会計年度プレフィックス数値を返す。
    3月以降は当年度、2月以前は前年度。

    Returns:
        int: 例) 19（2026年度）, 20（2027年度）, 21（2028年度）
    """
    today = date.today()
    fiscal_year = today.year if today.month >= 3 else today.year - 1
    return 19 + (fiscal_year - 2026)


def get_doc_prefix(letter: str) -> str:
    """
    書類番号プレフィックス文字列を返す。

    Args:
        letter: 'F', 'M', 'S' のいずれか

    Returns:
        str: 例) 'F19', 'M20', 'S21'
    """
    return f"{letter}{get_fiscal_prefix()}"


def generate_doc_no(supabase_client, table: str, field: str, letter: str) -> str:
    """
    Supabase から現在会計年度の最大連番を取得して次の書類番号を返す。

    Args:
        supabase_client: Supabase クライアント
        table:  テーブル名（例: 'orders', 'estimates', 'invoices'）
        field:  番号フィールド名（例: 'order_custom_no', 'estimate_no', 'invoice_no'）
        letter: 'F', 'M', 'S' のいずれか

    Returns:
        str: 例) 'F190001', 'M200005'
    """
    prefix = get_doc_prefix(letter)  # 例: 'F19'

    res = supabase_client.table(table).select(field) \
        .like(field, f"{prefix}%") \
        .order(field, desc=True) \
        .limit(200) \
        .execute()

    data = res.data or []
    nums = []
    for row in data:
        val = str(row.get(field) or "")
        if val.startswith(prefix):
            seq_str = val[len(prefix):]
            try:
                nums.append(int(seq_str))
            except ValueError:
                pass

    next_seq = (max(nums) if nums else 0) + 1
    return f"{prefix}{next_seq:04d}"
