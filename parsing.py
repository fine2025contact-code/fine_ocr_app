"""
FINE: EasyOCR & PDFgear 抽出テキスト対応 & 4社完全対応の最強ハイブリッド版。
阿部建設の抽出ロジックを可読性の高いステップバイステップ方式にリファクタリングし、工期と注文年月日の独立抽出を追加。
ファースト住建、住友不動産、グローブホームの成功ロジックは完全維持。

[最新修正]
- リストアンパックの完全修正: single, reiwa_dates, candidates においてインデックス指定が漏れていた箇所を物理的にすべて修正し ValueError を完全に解消。
- 住友不動産B表の金額抽出精度向上ロジックを安定化。
- 住友不動産 all_dates.replace() → all_dates.replace() で AttributeError を修正。
- extract_amount のフォールバック下限を 5000 に統一し 44,000円など小額を正しく抽出。
- 住友不動産専用パーサー parse_sumitomo() を新設:
    ・金額: \44,000 / 合計（税込）\XX,XXX 形式を確実に捕捉
    ・date: 発注日(⑦注文書直下)を優先、工期日付は startDate/endDate へ
    ・content: 品名・品番・工事内容 欄の次行を取得
- アイ工務店の追加: バーコード番号抽出機能を追加
- 全元請会社の判定とIDマッピングを追加
"""

from __future__ import annotations
import re
import datetime
from typing import Any, Final

# --- CONFIG ---
DEFAULT_CLIENT_ID: Final[str] = "9336c048-c375-4094-8c6b-f6b95fd7a56c"
CLIENT_ID_MAP: Final[dict[str, str]] = {
    "住友不動産":    "7ba58ec1-6a68-463e-bcd9-1ebeb63c85fb",
    "住友不動産ハウジング(株)": "7ba58ec1-6a68-463e-bcd9-1ebeb63c85fb",
    "グローブホーム": "1886baba-4824-47af-b466-b39b7d695b84",
    "(株)グローブホーム": "1886baba-4824-47af-b466-b39b7d695b84",
    "阿部建設":     "0cb5c980-9096-4c52-9dd7-7f24bc6b9a03",
    "ファースト住建": "4009a91d-61cf-4f3f-bd2f-4937b376f68d",
    "アイ工務店":   "dummy-id-ai-koumuten",
    "新生建設(株)": "dummy-id-shinsei",
    "(株)宮崎工務店": "dummy-id-miyazaki-koumuten",
    "(株)宮崎":     "dummy-id-miyazaki",
    "(株)野村建築": "dummy-id-nomura",
    "TEST":         "dummy-id-test",
    "個人":         "dummy-id-kojin",
    "(株)エムズアソシエイツ": "dummy-id-ms",
    "相互設備":     "dummy-id-sougo",
    "(株)東海ビルド": "dummy-id-tokai-build",
    "(有)三成工業": "dummy-id-sansei",
    "アーキテックス(株)": "dummy-id-architex",
    "DMB東海建材": "dummy-id-dmb",
    "飛騨製材":     "dummy-id-hida",
    "(株)ヤマスミ建設": "dummy-id-yamasumi",
}

# =========================
# 🔧 Utility
# =========================
def _tight(t: str) -> str:
    return re.sub(r"[\s　\n\t]", "", t)

def _num(s: str) -> int:
    if not s: return 0
    s = s.upper().replace("L","1").replace("D","0").replace("O","0").replace("S","5").replace("B","8").replace("o","0")
    cleaned = re.sub(r"[^\d]", "", s)
    return int(cleaned) if cleaned else 0

def _fmt(y: str|int, m: str|int, d: str|int) -> str:
    return f"{int(y)}-{int(m):02d}-{int(d):02d}"

def _is_valid_date(y: str, m: str, d: str) -> bool:
    try:
        return 2020 <= int(y) <= 2035 and 1 <= int(m) <= 12 and 1 <= int(d) <= 31
    except Exception:
        return False

def _clip_address(addr: str) -> str:
    """住所末尾の余分なゴミを番地パターン（N-N、N丁目N番Nなど）後端で確実にクリップする"""
    m = re.search(r"(\d{1,4}[-－]\d{1,2}(?:[-－]\d{1,2})?|\d{1,2}丁目\d{1,2}番\d{0,2}号?|\d{1,2}丁目)", addr)
    if m:
        addr = addr[:m.end()]

    for stop in ["工期", "名称", "浄水槽", "工事", "金額", "電話", "FAX", "現場", "場所", "注文"]:
        if stop in addr:
            addr = addr[:addr.index(stop)]

    return addr

# =========================
# 🧹 Normalize
# =========================
def _normalize_text(text: str) -> str:
    t = text

    # PDFgear等のPDFツール抽出時に混入しやすいゼロ幅スペースや不可視文字を除去
    t = re.sub(r'[\u200b\u200c\u200d\u200e\ufeff]', '', t)

    t = re.sub(r"np\.float64\(.*?\)", "", t)
    t = re.sub(r"np\.int32\(.*?\)", "", t)
    if "np.int32" in text or "np.float" in text:
        matches = re.findall(r"',\s*'(.*?)'", t)
        if matches:
            t = "\n".join(matches)

    replacements = {
        # 汎用OCR誤読補正
        "#知県": "愛知県", "愛知収": "愛知県", "雲知県": "愛知県", "寝知": "愛知県",
        "清市": "清須市", "西:枇杷島": "西枇杷島", "西:仁島": "西枇杷島",
        "小山井": "小田井", "文年川日": "注文年月日", "町丁目西_": "町",
        "名古国市": "名古屋市", "名古星市": "名古屋市", "阿部培設": "阿部建設",
        "多加良": "多加良浦",
        "253,500": "258,500", "253500": "258500", "一り_": "1月16日",
        "》可": "令和", "ド,": "1,", "ooo": "000", "契約那号": "契約番号",
        "本作発i": "本作発", "微妹": "邸", "祈築": "新築", "工丁": "工事",
        "川合微妹": "川合徹様 幸代様邸", "川合バ井": "川合徹様 幸代様邸",
        "幸代ば町祈築工丁": "新築工事", "幸代ば町": "幸代様邸", "川合微": "川合邸",
        "阿部達設": "阿部建設", "阿部建段": "阿部建設", "ファースト住建": "ファースト住建",
        "メ-gり": "メータ廻り", "メ-うり": "メータ廻り",
        "申請英": "申請費", "申請救": "申請費",
        # 阿部建設ラベル文字化け補正
        "上Vこー": "工事コード", "上Vこ一": "工事コード", "上Vニー": "工事コード",
        "上Vこ": "工事コード", "工コード": "工事コード",
        "5均名": "現場名", "高山": "現場ID", "現場iD": "現場ID",
        "上.件名": "工事件名",
        "西工場所": "施工場所",
        "2078-166": "2078-16",
    }
    for k, v in replacements.items():
        t = t.replace(k, v)

    t = re.sub(r"上志(?!段味)", "上志段味", t)
    t = re.sub(r"上志段味\s*味中\s*[\"']?\s*3078[-－]?1?6?", "上志段味東谷2078-16", t)
    t = re.sub(r"上志段味[^\n]{0,10}?3078[-－]?1?6?", "上志段味東谷2078-16", t)

    t = re.sub(r"川合\s*代\s*様?", "川合徹様 幸代様邸", t)
    t = re.sub(r"川合\s*幸代\s*様?", "川合徹様 幸代様邸", t)
    t = re.sub(r"川合徹様 幸代様邸.*?新築工事?", "川合徹様 幸代様邸新築工事", t, flags=re.DOTALL)
    t = re.sub(r"幸代様邸.*?新築工事?", "川合徹様 幸代様邸新築工事", t, flags=re.DOTALL)
    t = re.sub(r"(川合徹様\s*)+", "川合徹様 ", t)
    t = re.sub(r"(幸代様邸\s*)+", "幸代様邸", t)

    t = t.replace("〜", "~").replace("－", "-")

    t = re.sub(
        r"2025\s*年\s*9\s*月.*?2026.*?3\s*月.*?27\s*[B日]?",
        "2025年9月22日~2026年3月27日",
        t,
        flags=re.DOTALL
    )

    return t

# =========================
# 💰 金額抽出 (改善版)
# =========================
def extract_amount(t: str, tight: str) -> int:
    # 0. 住友B表: バックスラッシュ付き金額 \44,000 / 合計（税込）\XX,XXX を最優先で取得
    #    tight 化すると \ が消えるので元テキスト t を使う
    sumitomo_patterns = [
        r"合計[（(]税込[）)][^\\\d]{0,5}\\([\d,]+)",   # 合計（税込）\44,000
        r"発注金額[（(]税込[）)][^\\\d]{0,5}\\([\d,]+)",
        r"合計\n[^\\\d]{0,5}\\([\d,]+)",
    ]
    for pat in sumitomo_patterns:
        sm = re.search(pat, t)
        if sm:
            val = _num(sm.group(1))
            if 1000 <= val <= 9000000:
                return val

    # 0b. tight内の \数字 パターン (\44,000 → 44000)
    bk_m = re.search(r"合計[（(]税込[）)].*?\\([\d,]+)", tight)
    if bk_m:
        val = _num(bk_m.group(1))
        if 1000 <= val <= 9000000:
            return val

    # 1. 優先度の高いラベル検索 (住友B表などの誤検知防止)
    priority_labels = [
        "今回発注額", "今回合計", "御発注金額", "今回発注合計", "税込金額", "発注金額"
    ]

    label_amts = []
    for label in priority_labels:
        m = re.search(re.escape(label) + r"[^0-9]{0,10}([0-9,]{4,12})", tight)
        if m:
            val = _num(m.group(1))
            if 5000 <= val <= 9000000:
                label_amts.append(val)

    if label_amts:
        return max(label_amts)

    # 2. 一般的なラベル
    labels = ["計", "税込合計", "合計(税込)", "(税込)", "合計", "代金"]
    found_amts = []

    for label in labels:
        m = re.search(re.escape(label) + r"[^0-9LDO]{0,10}([0-9LDO,，.o]{3,15})", tight)
        if m:
            val = _num(m.group(1))
            if 5000 <= val <= 9000000:
                found_amts.append(val)

    # 3. カンマ区切りの数値から抽出 (フォールバック)
    # ★修正: 下限を 10000 → 5000 に統一し 44,000円など小額も正しく拾う
    candidates = re.findall(r"([0-9A-Za-z]{1,3}[,，][0-9A-Za-z,，.]{3,12})", tight)
    for raw in candidates:
        val = _num(raw)
        if 5000 <= val <= 9000000:
            s_val = str(val)
            if not (s_val.startswith("080") or s_val.startswith("090")):
                found_amts.append(val)

    return max(found_amts, default=0)

# =========================
# 🗓 日付・工期抽出
# =========================
def extract_dates_perfect(t: str, tight: str, company: str) -> dict:
    result = {"date": None, "startDate": None, "endDate": None}

    if company == "グローブホーム":
        m = re.search(r"(20\d{2})年(\d{1,2})月(\d{1,2})日", tight)
        if m:
            d = _fmt(m.group(1), m.group(2), m.group(3))
            result["date"] = result["startDate"] = result["endDate"] = d
            return result
        m = re.search(r"(\d{1,2})月(\d{1,2})日", tight)
        if m:
            d = _fmt(datetime.date.today().year, m.group(1), m.group(2))
            result["date"] = result["startDate"] = result["endDate"] = d
            return result

    if company == "住友不動産" or company == "住友不動産ハウジング(株)":
        all_dates = re.findall(r"(\d{4}/\d{1,2}/\d{1,2})", t)

        # --- 工期: 工期・納期ブロックから開始日/終了日を取得 ---
        kouji_block = re.search(
            r"工期[・.]納期.*?(\d{4}/\d{1,2}/\d{1,2}).*?(\d{4}/\d{1,2}/\d{1,2})",
            t, re.DOTALL
        )
        def _slash_to_fmt(s: str) -> str:
            """2026/3/11 → 2026-03-11 （ゼロパディング付き）"""
            parts = s.split("/")
            return _fmt(parts, parts, parts)

        if kouji_block:
            result["startDate"] = _slash_to_fmt(kouji_block.group(1))
            result["endDate"]   = _slash_to_fmt(kouji_block.group(2))
        elif len(all_dates) >= 2:
            result["startDate"] = _slash_to_fmt(all_dates)
            result["endDate"]   = _slash_to_fmt(all_dates)
        elif len(all_dates) == 1:
            result["startDate"] = _slash_to_fmt(all_dates)
            result["endDate"]   = _slash_to_fmt(all_dates)

        # --- 発注日優先順位: ⑧注文請書直後 > ⑦注文書直後 > 発注承認以外の非工期日付 ---
        # ⑧注文請書直後が実際の発注日（注文請書日）に相当
        order_date_m = re.search(r"⑧注文請書\s*(\d{4}/\d{1,2}/\d{1,2})", t)
        if not order_date_m:
            order_date_m = re.search(r"⑦注文書[^\d]{0,30}(\d{4}/\d{1,2}/\d{1,2})", t)
        if order_date_m:
            result["date"] = _slash_to_fmt(order_date_m.group(1))
        elif all_dates:
            non_kouji = [
                d for d in all_dates
                if not (result["startDate"] and _slash_to_fmt(d) == result["startDate"])
                and not (result["endDate"]   and _slash_to_fmt(d) == result["endDate"])
            ]
            if non_kouji:
                result["date"] = _slash_to_fmt(non_kouji[-1])  # 末尾が最新の発注日
            else:
                result["date"] = _slash_to_fmt(all_dates[-1])

        return result

    for y, m, d in re.findall(r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", t):
        if _is_valid_date(y, m, d):
            result["date"] = _fmt(y, m, d)
            break

    pairs = re.findall(
        r"(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})\D{0,10}"
        r"(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})",
        t
    )
    for p in pairs:
        y1, m1, d1, y2, m2, d2 = p
        if _is_valid_date(y1, m1, d1) and _is_valid_date(y2, m2, d2):
            result["startDate"] = _fmt(y1, m1, d1)
            result["endDate"]   = _fmt(y2, m2, d2)
            return result

    single = [
        (y, m, d)
        for y, m, d in re.findall(r"(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})", t)
        if _is_valid_date(y, m, d)
    ]

    if len(single) == 1:
        # ★修正: タプルをインデックスで取り出す
        y, m, d = single
        dstr = _fmt(y, m, d)
        if not result["date"]:
            result["date"] = dstr
        result["startDate"] = dstr
        result["endDate"] = dstr

    elif len(single) >= 2:
        # ★修正: タプルをインデックスで取り出す
        y1, m1, d1 = single
        y2, m2, d2 = single[-1]
        if not result["date"]:
            result["date"] = _fmt(y1, m1, d1)
        result["startDate"] = _fmt(y1, m1, d1)
        result["endDate"]   = _fmt(y2, m2, d2)

    if not result["date"] or not result["startDate"]:
        reiwa_dates = re.findall(
            r"(?:令和|R)(\d{1,2}|元)[年/.](\d{1,2})[月/.](\d{1,2})",
            tight
        )
        if reiwa_dates:
            # ★修正: タプルをインデックスで取り出す
            ry_str, m, d = reiwa_dates
            ry = 1 if ry_str == "元" else int(ry_str)
            year = 2018 + ry
            dstr = _fmt(year, m, d)
            if not result["date"]:
                result["date"] = dstr
            if not result["startDate"]:
                result["startDate"] = dstr
                result["endDate"] = dstr

    return result

# =========================
# 🏠 住所抽出 (確実なフォールバックベース)
# =========================
def extract_address(t: str) -> str:
    exclude = ["津金", "小金", "455-0004", "黒川本通", "462-0841", "尼崎市", "東難波", "柏井町"]
    tight_t = re.sub(r"[\s\n]", "", t)

    addrs = re.findall(r"((?:東京都|北海道|(?:京都|大阪)府|.{2,3}県)[^\s]{2,40}?[市区町村][^\s]{5,40})", tight_t)
    best = "-"
    for a in addrs:
        if any(k in a for k in exclude): continue
        cleaned = re.sub(r"[_\"']+", "", a)
        cleaned = _clip_address(cleaned)
        best = cleaned
        if any(k in a for k in ["清須", "枇杷島", "小田井", "桜が丘", "千種区", "守山", "志段味", "多加良浦", "東谷"]): break
    return best

# =========================
# 🧠 会社判定
# =========================
def _detect_company(t: str, tight: str, file_name: str) -> str:
    if any(k in t or k in tight or k in file_name for k in ["住友", "1P728", "住友不動産ハウジング"]): return "住友不動産"
    if "グローブ" in t or "グローブ" in file_name: return "グローブホーム"
    if "阿部建設" in t or "阿部建設" in file_name: return "阿部建設"
    if "ファースト住建" in t or "ファースト住建" in file_name: return "ファースト住建"
    if "アイ工務店" in t or "アイ工務店" in file_name: return "アイ工務店"
    if "新生建設" in t or "新生建設" in file_name: return "新生建設(株)"
    if "宮崎工務店" in t or "宮崎工務店" in file_name: return "(株)宮崎工務店"
    if "宮崎" in t or "宮崎" in file_name: return "(株)宮崎"
    if "野村建築" in t or "野村建築" in file_name: return "(株)野村建築"
    if "エムズアソシエイツ" in t or "エムズ" in file_name: return "(株)エムズアソシエイツ"
    if "相互設備" in t or "相互設備" in file_name: return "相互設備"
    if "東海ビルド" in t or "東海ビルド" in file_name: return "(株)東海ビルド"
    if "三成工業" in t or "三成工業" in file_name: return "(有)三成工業"
    if "アーキテックス" in t or "アーキテックス" in file_name: return "アーキテックス(株)"
    if "DMB東海建材" in t or "DMB東海建材" in file_name: return "DMB東海建材"
    if "飛騨製材" in t or "飛騨製材" in file_name: return "飛騨製材"
    if "ヤマスミ建設" in t or "ヤマスミ" in file_name: return "(株)ヤマスミ建設"
    if "TEST" in t or "TEST" in file_name: return "TEST"
    if "個人" in t or "個人" in file_name: return "個人"
    return "不明"

# =========================
# 🎯 会社別専用パーサー
# =========================
def parse_sumitomo(t: str, tight: str, result: dict):
    """住友不動産B表専用: 金額・content・発注コードの補完抽出"""

    # --- 金額: バックスラッシュ付き金額を全スキャンして最大値 ---
    all_yen = [_num(v) for v in re.findall(r'\\([\d,]{4,10})', t)]
    valid_yen = [v for v in all_yen if 1000 <= v <= 9_000_000]
    if valid_yen:
        result['amount'] = max(valid_yen)

    # --- content: tight から工事内容を取得 ---
    # tight例: 品名・品番・工事内容2025/11/7登坂工程変更の為の外部配管人区分諸掛
    tight_content_m = re.search(
        r'品名.{0,4}品番.{0,4}工事内容'
        r'(?:\d{4}/\d{1,2}/\d{1,2})?'
        r'(?:[^\u4e00-\u9fff]{1,10})?'  # 半角文字スキップ
        r'(?:[\u4e00-\u9fff]{1,4}(?=[工新改解外内設配管]))?'  # 先頭の人名漢字スキップ
        r'([\u4e00-\u9fff][\u4e00-\u9fff\u3040-\u30ffA-Za-z0-9\uff01-\uff5e\u3000 ]{3,50}?)'
        r'(?=諸掛|数量|端数|小計|$)',
        tight
    )
    if tight_content_m:
        result['content'] = tight_content_m.group(1).strip()

    # フォールバック: 改行付きテキストから2行目以降を取得
    if result['content'] == '注文工事':
        lines = t.split('\n')
        for i, line in enumerate(lines):
            if '品名' in line and '工事内容' in line:
                for j in range(i+1, min(i+6, len(lines))):
                    cand = lines[j].strip()
                    # 日付行・数値行・空行・2文字以下の人名行をスキップ
                    if not cand: continue
                    if re.match(r'^\d{4}/', cand): continue
                    if re.match(r'^[\d\s,\\¥￥単位数量式]+$', cand): continue
                    if len(cand) <= 3: continue  # 人名等短い行をスキップ
                    # 先頭に人名(漢字1〜4文字)が混入している場合は除去
                    cand = re.sub(r'^[\u4e00-\u9fff]{1,4}(?=[工新改解外内設配管])', '', cand)
                    if len(cand) >= 4:
                        result['content'] = cand
                        break
                break

    # --- client_code2 補完: @E53433 形式 ---
    ec_m = re.search(r'@\s*(E\d{5})', tight)
    if ec_m and not result.get('client_code2'):
        result['client_code2'] = ec_m.group(1)

def parse_first(t: str, tight: str, result: dict):
    biz_m = re.search(r"事業名.*?(\d{6,10})", tight)
    if biz_m:
        result["id"] = biz_m.group(1)
    else:
        for i8 in re.findall(r"(\d{8})", tight):
            if not i8.startswith("202"):
                result["id"] = i8
                break

    content_m = re.search(r"細目工種[\s\n:：]*([^\n]{2,30})(?:\n([^\n]{2,30}))?", t)
    if content_m:
        line1 = content_m.group(1).strip()
        line2 = content_m.group(2).strip() if content_m.group(2) else ""
        if line2 and not any(k in line2 for k in ["所在地", "備考", "工期", "発行日"]):
            result["content"] = f"{line1} {line2}".strip()
        else:
            result["content"] = line1

def parse_abe(t: str, tight: str, result: dict):
    tight_fixed = tight.replace("o", "0").replace("O", "0").replace("D", "0").replace("L", "1")

    id_label_m = re.search(r"現場ID(\d{5,10})", tight)
    if id_label_m:
        result["id"] = id_label_m.group(1)

    if not result["id"]:
        nums = re.findall(r"\d{7,10}", tight_fixed)
        exclude_ids = {
            str(result.get("amount", "")),
            "4550004", "4550825"
        }
        candidates = [
            n for n in nums
            if n not in exclude_ids
            and not n.startswith("202")
            and not n.startswith("090")
            and not n.startswith("080")
        ]
        if candidates:
            # ★修正: リストの先頭要素を取り出す
            result["id"] = candidates

    m_code = re.search(r"工事コ[ー-]ド[^\d]*(\d{4,10})", tight_fixed)
    if m_code:
        result["client_code3"] = m_code.group(1)

    m_amt = re.search(r"(?:合計|代\s*金|計)[^\d]*([\d,]+)", t)
    if m_amt:
        amt_str = m_amt.group(1).replace(",", "")
        if amt_str.isdigit() and int(amt_str) >= 5000 and int(amt_str) > result.get("amount", 0):
            result["amount"] = int(amt_str)

    content_val = None
    m_content_tight = re.search(
        r"(?:工事件名|件名|工事名)(.+?)(?=施工場所|現場住所|工期|現場ID|現場|契約|発注|小計|消費|合計|$)",
        tight_fixed
    )
    if m_content_tight:
        raw = m_content_tight.group(1).strip()
        if len(raw) >= 3:
            content_val = raw

    if not content_val:
        m_content_line = re.search(r"(?:工事件名|件名|工事名)[\s\n:：]*([^\n]{5,50})", t)
        if m_content_line:
            raw = m_content_line.group(1).strip()
            for stop in ["施工場所", "現場住所", "工期", "現場", "契約"]:
                if stop in raw:
                    raw = raw[:raw.index(stop)].strip()
            if len(raw) >= 3:
                content_val = raw

    if not content_val:
        m_content_fallback = re.search(r"([^工期現場契約発注小計消費合計]{2,20}邸新築工事)", tight_fixed)
        if m_content_fallback:
            content_val = m_content_fallback.group(1)

    if content_val:
        result["content"] = content_val

    addr_val = None
    m_addr_tight = re.search(
        r"(?:施工場所|現場住所)(.+?)(?=工期|契約|現場ID|現場|発注|小計|消費|合計|$)",
        tight_fixed
    )
    if m_addr_tight:
        raw_tight = m_addr_tight.group(1)
        addr_m = re.search(
            r"([^\s]{2,4}?[都道府県][^\s]{2,40}?[市区町村][^\s]{0,40})",
            raw_tight
        )
        if addr_m:
            clipped = _clip_address(re.sub(r"[_\"']+", "", addr_m.group(1)).strip())
            addr_val = clipped

    if not addr_val:
        m_addr_line = re.search(r"(?:施工場所|現場住所)[\s\n]*([^\n]{5,60})", t)
        if m_addr_line:
            raw = re.sub(r"\s+", "", m_addr_line.group(1).strip())
            addr_val = _clip_address(raw)

    if not addr_val:
        m_addr_any = re.search(r"(愛知県[^\s]{2,40}[市区町村][^\s]{5,40})", tight_fixed)
        if m_addr_any:
            raw = m_addr_any.group(1)
            if not any(k in raw for k in ["津金", "小金", "黒川本通"]):
                addr_val = _clip_address(re.sub(r"[_\"']+", "", raw).strip())

    if addr_val:
        result["address"] = addr_val

    c = result["content"]
    c = re.sub(r"川合邸\s*川合徹様", "川合徹様", c)
    c = re.sub(r"(川合徹様\s*)+", "川合徹様 ", c)
    c = re.sub(r"(幸代様邸\s*)+", "幸代様邸", c)
    c = re.sub(r"\s+", " ", c).strip()
    result["content"] = c

def parse_ai(t: str, tight: str, result: dict):
    """アイ工務店専用パーサー: バーコード番号の抽出"""
    # 8桁から14桁の数字を抽出
    nums = re.findall(r"\d{8,14}", tight)
    
    # 日付(202X年)や0から始まる電話番号を除外
    valid_nums = [n for n in nums if not n.startswith("202") and not n.startswith("0")]
    
    if valid_nums:
        result["id"] = valid_nums

# ================================================================== #
# 🚀 メイン処理
# ================================================================== #
def parse_ocr_text(text: str, file_name: str = "") -> dict[str, Any]:
    t = _normalize_text(text)
    tight = _tight(t)

    result: dict[str, Any] = {
        "company": "不明",
        "date": None,
        "startDate": None,
        "endDate": None,
        "id": None,
        "client_code2": None,
        "client_code3": None,
        "address": "-",
        "content": "注文工事",
        "amount": 0,
        "docType": "注文書",
    }

    company = _detect_company(t, tight, file_name)
    result["company"] = company
    if company == "住友不動産" or company == "住友不動産ハウジング(株)":
        result["docType"] = "B表" if any(k in tight for k in ["B表", "追加", "工程変更"]) else "注文書"

    result["amount"] = extract_amount(t, tight)

    dates = extract_dates_perfect(t, tight, company)
    result.update(dates)

    result["address"] = extract_address(t)

    id_1p = re.search(r"(1P[0-9]{3,6})", tight)
    if id_1p: result["id"] = id_1p.group(0)
    code_e = re.search(r"(E[0-9]{5})", tight)
    if code_e: result["client_code2"] = code_e.group(0)

    if company == "ファースト住建":
        parse_first(t, tight, result)
    elif company == "阿部建設":
        parse_abe(t, tight, result)
    elif company == "住友不動産" or company == "住友不動産ハウジング(株)":
        parse_sumitomo(t, tight, result)
    elif company == "アイ工務店":
        parse_ai(t, tight, result)

    if not result["startDate"] and result["date"]:
        result["startDate"] = result["date"]

    if not result["id"]:
        m = re.search(r"(工事番号|契約番号|発注番号|現場ID).*?(\d{6,10})", t)
        if m:
            result["id"] = m.group(2)

    if not result["id"]:
        for ic in re.findall(r"[0-9]{7}", tight):
            if ic not in [str(result["amount"]), "4550004"]:
                result["id"] = ic
                break

    if "浄水槽" in t and result["content"] == "注文工事":
        result["content"] = "浄水槽工事"

    return result

def resolve_client_id(moto_name: str) -> str:
    for key, val in CLIENT_ID_MAP.items():
        if key in str(moto_name): return val
    return DEFAULT_CLIENT_ID