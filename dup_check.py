"""
FINE: 重複検知ユーティリティ（2026-07-07 新設）

らくしょる(web)側 lib/searchUtils.js の normalizeForSearch と同じ正規化ルールで、
「会社名・工事名・金額・現場住所」の4項目が一致する既存工事を検知する。
- 表記ゆらぎ（渡邊/渡辺・齋/斉・髙/高など異体字、カナ/かな、全角/半角、スペース、
  長音・中点・ハイフン）を吸収してから比較する。
- web側の重複バッジ（赤枠）と判定基準を一致させるためのモジュール。

※ streamlit や supabase に依存しない純粋関数のみ（テスト・再利用しやすくするため）。
"""

from __future__ import annotations

import re
from typing import Any

# ── 人名・社名でよく使われる異体字 → 標準形（searchUtils.js の KANJI_VARIANTS と同一） ──
_KANJI_VARIANTS = {
    "邊": "辺", "邉": "辺",
    "齋": "斉", "齊": "斉", "斎": "斉",
    "髙": "高",
    "﨑": "崎", "嵜": "崎",
    "澤": "沢",
    "濱": "浜", "濵": "浜",
    "嶋": "島", "嶌": "島",
    "國": "国",
    "廣": "広",
    "惠": "恵",
    "榮": "栄",
    "圓": "円",
    "會": "会",
    "眞": "真",
    "淺": "浅",
    "瀨": "瀬",
    "龍": "竜",
    "冨": "富",
    "德": "徳",
    "櫻": "桜",
    "條": "条",
    "曾": "曽",
    "壽": "寿",
    "萬": "万",
    "峯": "峰",
    "舘": "館",
    "塲": "場",
    "藪": "薮", "籔": "薮",
    "𠮷": "吉",
}

# 長音・中点・ハイフン類（searchUtils.js と同一）
_DASH_RE = re.compile(r"[・･ｰー\-‐−–—]")
# スペース（全角含む）
_SPACE_RE = re.compile(r"[\s　]")


def _zenkaku_alnum_to_hankaku(c: str) -> str:
    """全角英数（Ａ-Ｚ ａ-ｚ ０-９）だけを半角へ。記号は変換しない（JS版と同じ範囲）。"""
    o = ord(c)
    if (0xFF21 <= o <= 0xFF3A) or (0xFF41 <= o <= 0xFF5A) or (0xFF10 <= o <= 0xFF19):
        return chr(o - 0xFEE0)
    return c


def _katakana_to_hiragana(c: str) -> str:
    """カタカナ（ァ-ヶ）をひらがなへ。"""
    o = ord(c)
    if 0x30A1 <= o <= 0x30F6:
        return chr(o - 0x60)
    return c


def normalize_for_search(s: Any) -> str:
    """
    検索・重複判定用に文字列を正規化する（searchUtils.js normalizeForSearch と同一動作）。
    小文字化 / 全角英数→半角 / スペース除去 / カナ→かな / 長音・中点・ハイフン除去 / 異体字→標準形。
    """
    if s is None:
        return ""
    t = str(s).lower()
    t = "".join(_zenkaku_alnum_to_hankaku(c) for c in t)
    t = _SPACE_RE.sub("", t)
    t = "".join(_katakana_to_hiragana(c) for c in t)
    t = t.replace("ゖ", "が")  # 「ヶ」→（かな化で「ゖ」）→「が」（霞ヶ関/霞が関）
    t = _DASH_RE.sub("", t)
    t = "".join(_KANJI_VARIANTS.get(c, c) for c in t)
    return t


def _to_amount_int(v: Any) -> int:
    """金額を整数（円）に正規化。カンマ・円記号・全角カンマを除去して比較用に揃える。"""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        try:
            return int(round(float(v)))
        except (ValueError, OverflowError):
            return 0
    s = str(v).replace(",", "").replace("，", "").replace("¥", "").replace("￥", "").replace(" ", "").strip()
    try:
        return int(round(float(s or 0)))
    except (ValueError, TypeError):
        return 0


def make_key(client_name: Any, name: Any, budget: Any, site_address: Any) -> str:
    """
    重複判定用のキー（会社名・工事名・金額・現場住所の4項目）を生成する。
    工事名が空の場合は空文字を返す（空同士の誤検出を防ぐため、呼び出し側は空キーを無視すること）。
    """
    nname = normalize_for_search(name)
    if not nname:
        return ""
    return "|".join([
        normalize_for_search(client_name),
        nname,
        str(_to_amount_int(budget)),
        normalize_for_search(site_address),
    ])
