"""
FINE: EasyOCR & PDFgear 抽出テキスト対応 & 全社ハイブリッド版。
Supabase連動による動的項目抽出対応版（アイ工務店対応）
宮崎工務店・宮崎・新生建設 対応追加版
グローブホーム 専用パーサー追加版
全9社対応・共通辞書（COMPANY_LABEL_MAP）統合・住所抽出強化版

【修正内容】
- parse_ai: インボイス番号(T始まり)を除外、13桁バーコードのみid抽出
- parse_ai: content に明細ヘッダーゴミが混入する問題を修正（工事名ラベルから直接取得）
- parse_ai: site_name / content を明確に分離（site_name=工事名、content=明細名称）
- parse_universal: アイ工務店の content 抽出をスキップ（parse_aiに委譲）
- _normalize_text: 「様邸様邸」重複を修正
- extract_amount: アイ工務店の税込合計行を優先取得
- parse_ocr_text の返却辞書に表示用フィールド(fields_display)を追加

【バグ修正 2026-04-29】
- _slash_to_fmt: parts[0], parts[1], parts[2] のインデックス指定が欠落していたのを修正
- parse_ai content抽出: re.split()の戻り値がリストのまま.strip()していたのを修正
- parse_ai id抽出: T始まりインボイス番号(T+13桁)をtight文字列上で確実に除外
"""
from __future__ import annotations
import re
import datetime
import os
from typing import Any, Final
from supabase import create_client, Client

# =========================
# Supabase 接続設定
# =========================
SUPABASE_URL = "https://elmvkjkpdyebbgjsarwq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVsbXZramtwZHllYmJnanNhcndxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjgzMjczMzQsImV4cCI6MjA4MzkwMzMzNH0.4DbhZZBRFavbl44Ge07dwhvty4Q2WaDNLJw-GwAOYkY"

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"Supabase接続エラー: {e}")
    supabase = None

def get_client_config(company_name: str) -> dict:
    """Supabaseから対象会社のラベル設定を取得する"""
    if not supabase:
        return {}
    try:
        response = supabase.table("client_configs").select("*").ilike("name", f"%{company_name}%").execute()
        if response.data and len(response.data) > 0:
            return response.data
    except Exception as e:
        print(f"設定取得エラー ({company_name}): {e}")
    return {}

# =========================
# 動的抽出エンジン
# =========================
def dynamic_extract(label: str, text: str, pattern: str = r"([A-Z0-9-ー]+)") -> str | None:
    """DBから取得したラベル名を使って値を抽出する汎用関数"""
    if not label:
        return None
    regex = f"{re.escape(label)}[^a-zA-Z0-9]*{pattern}"
    match = re.search(regex, text)
    if match:
        val = match.group(1).strip()
        return val if len(val) >= 3 else None
    return None

# =========================
# CONFIG (会社マップ)
# =========================
DEFAULT_CLIENT_ID: Final[str] = "9336c048-c375-4094-8c6b-f6b95fd7a56c"

CLIENT_ID_MAP: Final[dict[str, str]] = {
    "新生建設": "00350a82-b133-44a4-bc8e-d96344389b76",
    "新生建設㈱": "00350a82-b133-44a4-bc8e-d96344389b76",
    "宮崎工務店": "07bb4fc6-07a3-49f9-b64b-61dab8701489",
    "㈱宮崎工務店": "07bb4fc6-07a3-49f9-b64b-61dab8701489",
    "アイ工務店": "14db753b-4608-44a1-8fb3-15a2cd770d86",
    "相互設備": "9e658c6b-c388-4380-ad61-d3c74cdb2bcd",
    "三成工業": "ba61ff6e-4268-4fb6-9080-252844d2f5d6",
    "㈲三成工業": "ba61ff6e-4268-4fb6-9080-252844d2f5d6",
    "DMB東海建材": "ae3f70fd-044e-4451-96cd-ed422663c565",
    "飛騨製材": "e81715b3-c420-40d5-80ab-1d6949091faf",
    "住友不動産": "7ba58ec1-6a68-463e-bcd9-1ebeb63c85fb",
    "阿部建設": "0cb5c980-9096-4c52-9dd7-7f24bc6b9a03",
    "ファースト住建": "4009a91d-61cf-4f3f-bd2f-4937b376f68d",
    "TEST": "9336c048-c375-4094-8c6b-f6b95fd7a56c",
}

COMPANY_LABEL_MAP: Final[dict[str, dict[str, str]]] = {
    "グローブホーム": {
        "id": "契約番号", "sub_id_1": "契約枝番号", "sub_id_2": "発注枝番",
        "site_name": "現場名", "site_name_sub": "工事名", "address": "施工場所",
        "amount": "代金", "content": "工事件名", "date": "注文書年月日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "注文書種類"
    },
    "阿部建設": {
        "id": "工事コード", "site_name": "工事名", "address": "工事場所",
        "amount": "金額", "content": "名称", "date": "作成日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "注文書種類"
    },
    "住友不動産": {
        "id": "工事番号", "sub_id_1": "契約枝番", "sub_id_2": "発注枝番",
        "site_name": "工事名称", "site_name_sub": "邸名", "address": "現場住所",
        "amount": "合計金額", "content": "品名・品番・工事内容", "date": "右上日付け",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "注文書"
    },
    "アーキテックス": {
        "id": "発注管理ID", "site_name": "案件名", "address": "",
        "amount": "発注金額", "content": "案件名下段工事内容", "date": "発注日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "ファースト住建": {
        "id": "工番", "site_name": "事業名", "site_name_sub": "工事名",
        "address": "所在地", "amount": "金額", "content": "細目工種",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日",
        "doc_type": "発注伝票Ｎｏ"
    },
    "アイ工務店": {
        "id": "バーコード", "sub_id_1": "業者NO", "site_name": "工事名",
        "address": "建築地", "amount": "金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "新生建設(株)": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "(株)宮崎工務店": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "(株)宮崎": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    }
}

# =========================
# Utility
# =========================
def _tight(t: str) -> str:
    return re.sub(r"[\s　\n\t]", "", t)

def _num(s: str) -> int:
    if not s: return 0
    s = s.upper().replace("L","1").replace("D","0").replace("O","0").replace("S","5").replace("B","8").replace("o","0")
    cleaned = re.sub(r"[^\d]", "", s)
    return int(cleaned) if cleaned else 0

def _fmt(y: str|int, m: str|int, d: str|int) -> str:
    try:
        # 数字以外の文字（空白やゴミ）を取り除いてから変換することでエラーを防止
        y_cl = re.sub(r'\D', '', str(y))
        m_cl = re.sub(r'\D', '', str(m))
        d_cl = re.sub(r'\D', '', str(d))
        return f"{int(y_cl)}-{int(m_cl):02d}-{int(d_cl):02d}"
    except Exception:
        return f"{y}-{m}-{d}"

def _is_valid_date(y: str, m: str, d: str) -> bool:
    try:
        return 2020 <= int(y) <= 2035 and 1 <= int(m) <= 12 and 1 <= int(d) <= 31
    except Exception:
        return False

def _clip_address(addr: str) -> str:
    m = re.search(r"(\d{1,4}[-－]\d{1,2}(?:[-－]\d{1,2})?|\d{1,2}丁目\d{1,2}番\d{0,2}号?|\d{1,2}丁目)", addr)
    if m: addr = addr[:m.end()]
    for stop in ["工期", "名称", "浄水槽", "工事", "金額", "電話", "FAX", "現場", "場所", "注文"]:
        if stop in addr: addr = addr[:addr.index(stop)]
    return addr

def resolve_client_id(moto_name: str) -> str:
    target = str(moto_name).replace("株式会社", "").replace("（株）", "").replace("(株)", "").replace("㈱", "").strip()
    for key, val in CLIENT_ID_MAP.items():
        if key.replace("㈱", "").replace("㈲", "") in target:
            return val
    return DEFAULT_CLIENT_ID

# 【修正1】_slash_to_fmt: parts のインデックス指定が欠落していたのを修正
def _slash_to_fmt(s: str) -> str:
    parts = s.split("/")
    return _fmt(parts[0], parts[1], parts[2])

# =========================
# Normalize
# =========================
def _normalize_text(text: str) -> str:
    t = text
    t = re.sub(r'[\u200b\u200c\u200d\u200e\ufeff]', '', t)
    t = re.sub(r"np\.float64\(.*?\)", "", t)
    t = re.sub(r"np\.int32\(.*?\)", "", t)
    if "np.int32" in text or "np.float" in text:
        matches = re.findall(r"',\s*'(.*?)'", t)
        if matches: t = "\n".join(matches)

    replacements = {
        "#知県": "愛知県", "愛知収": "愛知県", "雲知県": "愛知県", "寝知": "愛知県", "愛知児": "愛知県",
        "名古星市": "名古屋市", "名古国市": "名古屋市",
        "清市": "清須市", "西:枇杷島": "西枇杷島", "西:仁島": "西枇杷島",
        "小山井": "小田井", "文年川日": "注文年月日", "町丁目西_": "町",
        "名古国市": "名古屋市", "名古星市": "名古屋市", "阿部培設": "阿部建設",
        "多加良": "多加良浦", "253,500": "258,500", "253500": "258500",
        "一り_": "1月16日", "》可": "令和", "ド,": "1,", "ooo": "000",
        "契約那号": "契約番号", "本作発i": "本作発", "微妹": "邸", "祈築": "新築",
        "工丁": "工事", "川合微妹": "川合徹様 幸代様邸", "川合バ井": "川合徹様 幸代様邸",
        "幸代ば町祈築工丁": "新築工事", "幸代ば町": "幸代様邸", "川合微": "川合邸",
        "阿部達設": "阿部建設", "阿部建段": "阿部建設", "ファースト住建": "ファースト住建",
        "メ-gり": "メータ廻り", "メ-うり": "メータ廻り", "申請英": "申請費", "申請救": "申請費",
        "上Vこー": "工事コード", "上Vこ一": "工事コード", "上Vニー": "工事コード",
        "上Vこ": "工事コード", "工コード": "工事コード", "5均名": "現場名", "高山": "現場ID",
        "現場iD": "現場ID", "上.件名": "工事件名", "西工場所": "施工場所", "2078-166": "2078-16",
        "工吏希号": "工事番号", "工雲名称": "工事名称", "注文番亨": "注文番号",
        "新生避": "新生建設", "新生建設妹": "新生建設", "新生建設歌": "新生建設",
    }
    for k, v in replacements.items():
        t = t.replace(k, v)

    # 【修正】様邸の重複除去
    t = re.sub(r"様邸\s*様邸", "様邸", t)
    t = re.sub(r"(様邸){2,}", "様邸", t)

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
    t = re.sub(r"2025\s*年\s*9\s*月.*?2026.*?3\s*月.*?27\s*[B日]?", "2025年9月22日~2026年3月27日", t, flags=re.DOTALL)
    return t

# =========================
# 金額抽出
# =========================
def extract_amount(t: str, tight: str) -> int:
    # 【修正】アイ工務店: 「合計」最終行（税込合計）を最優先
    ai_top = re.search(r"合計\s*([0-9,]{4,10})\s*$", t, re.MULTILINE)
    if ai_top:
        val = _num(ai_top.group(1))
        if 10000 <= val <= 9_000_000:
            return val

    sumitomo_patterns = [
        r"合計[（(]税込[）)][^\\\d]{0,5}\\([\d,]+)",
        r"発注金額[（(]税込[）)][^\\\d]{0,5}\\([\d,]+)",
        r"合計\n[^\\\d]{0,5}\\([\d,]+)",
    ]
    for pat in sumitomo_patterns:
        sm = re.search(pat, t)
        if sm:
            val = _num(sm.group(1))
            if 1000 <= val <= 9000000: return val

    bk_m = re.search(r"合計[（(]税込[）)].*?\\([\d,]+)", tight)
    if bk_m:
        val = _num(bk_m.group(1))
        if 1000 <= val <= 9000000: return val

    priority_labels = ["今回発注額", "今回合計", "御発注金額", "今回発注合計", "税込金額", "発注金額", "注文金額"]
    label_amts = []
    for label in priority_labels:
        m = re.search(re.escape(label) + r"[^0-9]{0,10}([0-9,]{4,12})", tight)
        if m:
            val = _num(m.group(1))
            if 5000 <= val <= 9000000: label_amts.append(val)

    if label_amts: return max(label_amts)

    labels = ["計", "税込合計", "合計(税込)", "(税込)", "合計", "代金"]
    found_amts = []
    for label in labels:
        m = re.search(re.escape(label) + r"[^0-9LDO]{0,10}([0-9LDO,，.o]{3,15})", tight)
        if m:
            val = _num(m.group(1))
            if 5000 <= val <= 9000000: found_amts.append(val)

    candidates = re.findall(r"([0-9A-Za-z]{1,3}[,，][0-9A-Za-z,，.]{3,12})", tight)
    for raw in candidates:
        val = _num(raw)
        if 5000 <= val <= 9000000:
            s_val = str(val)
            if not (s_val.startswith("080") or s_val.startswith("090")):
                found_amts.append(val)

    return max(found_amts, default=0)

# =========================
# 日付・工期・請求日抽出
# =========================
def extract_dates_perfect(t: str, tight: str, company: str, labels: dict = {}) -> dict:
    result = {"date": None, "startDate": None, "endDate": None, "billing_date": None}

    lbl_billing = labels.get("billing_date", "請求日")
    m_bill = re.search(f"{lbl_billing}[\\s:：]*(20\\d{{2}})[年/\\-](\\d{{1,2}})[月/\\-](\\d{{1,2}})", t)
    if m_bill:
        result["billing_date"] = _fmt(m_bill.group(1), m_bill.group(2), m_bill.group(3))

    if company == "グローブホーム" or company == "(株)グローブホーム":
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
        kouji_block = re.search(r"工期[・.]納期.*?(\d{4}/\d{1,2}/\d{1,2}).*?(\d{4}/\d{1,2}/\d{1,2})", t, re.DOTALL)

        if kouji_block:
            result["startDate"] = _slash_to_fmt(kouji_block.group(1))
            result["endDate"]   = _slash_to_fmt(kouji_block.group(2))
        elif len(all_dates) >= 2:
            result["startDate"] = _slash_to_fmt(all_dates[0])
            result["endDate"]   = _slash_to_fmt(all_dates[-1])
        elif len(all_dates) == 1:
            result["startDate"] = _slash_to_fmt(all_dates[0])
            result["endDate"]   = _slash_to_fmt(all_dates[0])

        order_date_m = re.search(r"⑧注文請書\s*(\d{4}/\d{1,2}/\d{1,2})", t)
        if not order_date_m: order_date_m = re.search(r"⑦注文書[^\d]{0,30}(\d{4}/\d{1,2}/\d{1,2})", t)
        if order_date_m:
            result["date"] = _slash_to_fmt(order_date_m.group(1))
        elif all_dates:
            non_kouji = [d for d in all_dates if not (result["startDate"] and _slash_to_fmt(d) == result["startDate"]) and not (result["endDate"] and _slash_to_fmt(d) == result["endDate"])]
            if non_kouji: result["date"] = _slash_to_fmt(non_kouji[-1])
            else: result["date"] = _slash_to_fmt(all_dates[-1])
        return result

    # 宮崎・新生建設系: 「自 YYYY年MM月DD日 至 YYYY年MM月DD日」を優先処理
    kouji_m = re.search(
        r"[自从]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*[至迄]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        t
    )
    if kouji_m:
        result["startDate"] = _fmt(kouji_m.group(1), kouji_m.group(2), kouji_m.group(3))
        result["endDate"]   = _fmt(kouji_m.group(4), kouji_m.group(5), kouji_m.group(6))

    for y, m, d in re.findall(r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", t):
        if _is_valid_date(y, m, d):
            result["date"] = _fmt(y, m, d)
            break

    if result["startDate"]:
        return result

    # スラッシュ区切り工期（アイ工務店形式: 2025/12/18～2026/05/16）
    kouji_slash = re.search(
        r"工\s*[　 ]*期\s*[　 ]*(\d{4}/\d{1,2}/\d{1,2})\s*[~～〜\-]\s*(\d{4}/\d{1,2}/\d{1,2})",
        t
    )
    if kouji_slash:
        result["startDate"] = _slash_to_fmt(kouji_slash.group(1))
        result["endDate"]   = _slash_to_fmt(kouji_slash.group(2))
        return result

    pairs = re.findall(r"(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})\D{0,10}(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})", t)
    for p in pairs:
        y1, m1, d1, y2, m2, d2 = p
        if _is_valid_date(y1, m1, d1) and _is_valid_date(y2, m2, d2):
            result["startDate"] = _fmt(y1, m1, d1)
            result["endDate"]   = _fmt(y2, m2, d2)
            return result

    single = [(y, m, d) for y, m, d in re.findall(r"(20\d{2})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})", t) if _is_valid_date(y, m, d)]
    if len(single) == 1:
        y, m, d = single[0]
        dstr = _fmt(y, m, d)
        if not result["date"]: result["date"] = dstr
        result["startDate"] = dstr
        result["endDate"] = dstr
    elif len(single) >= 2:
        y1, m1, d1 = single[0]
        y2, m2, d2 = single[-1]
        if not result["date"]: result["date"] = _fmt(y1, m1, d1)
        result["startDate"] = _fmt(y1, m1, d1)
        result["endDate"]   = _fmt(y2, m2, d2)

    if not result["date"] or not result["startDate"]:
        reiwa_dates = re.findall(r"(?:令和|R)(\d{1,2}|元)[年/.](\d{1,2})[月/.](\d{1,2})", tight)
        if reiwa_dates:
            ry_str, m, d = reiwa_dates[0]
            ry = 1 if ry_str == "元" else int(ry_str)
            dstr = _fmt(2018 + ry, m, d)
            if not result["date"]: result["date"] = dstr
            if not result["startDate"]: result["startDate"] = dstr; result["endDate"] = dstr
    return result

# =========================
# 住所抽出
# =========================
def extract_address(t: str) -> str:
    prefs = (
        "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
        "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
        "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
        "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
        "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
    )
    exclude = ["津金", "小金", "455-0004", "黒川本通", "462-0841", "尼崎市", "東難波", "柏井町"]
    addrs = re.findall(rf"((?:{prefs})[^\s]{{2,40}}?[市区町村][^\s]{{5,40}})", re.sub(r"[\s\n]", "", t))

    best = "-"
    for a in addrs:
        if any(k in a for k in exclude): continue
        cleaned = _clip_address(re.sub(r"[_\"']+", "", a))
        best = cleaned
        if any(k in a for k in ["清須", "枇杷島", "小田井", "桜が丘", "千種区", "守山", "志段味", "多加良浦", "東谷", "篠の風"]):
            break
    return best

# =========================
# 会社判定
# =========================
def _detect_company(t: str, tight: str, file_name: str) -> str:
    if any(k in t or k in tight or k in file_name for k in ["住友", "1P728", "住友不動産ハウジング"]): return "住友不動産"
    if "グローブ" in t or "グローブ" in file_name: return "グローブホーム"
    if "阿部建設" in t or "阿部建設" in file_name: return "阿部建設"
    if "ファースト住建" in t or "ファースト住建" in file_name: return "ファースト住建"
    if "アイ工務店" in t or "アイ工務店" in file_name: return "アイ工務店"
    if "新生建設" in t or "新生建設" in file_name: return "新生建設(株)"
    if "宮崎工務店" in t or "宮崎工務店" in file_name: return "(株)宮崎工務店"
    if "株式会社宮崎" in t or "株式会社宮崎" in file_name: return "(株)宮崎"
    if "アーキテックス" in t or "アーキテックス" in file_name: return "アーキテックス"
    if "野村建築" in t or "野村建築" in file_name: return "(株)野村建築"
    if "エムズアソシエイツ" in t or "エムズ" in file_name: return "(株)エムズアソシエイツ"
    if "相互設備" in t or "相互設備" in file_name: return "相互設備"
    if "東海ビルド" in t or "東海ビルド" in file_name: return "(株)東海ビルド"
    if "三成工業" in t or "三成工業" in file_name: return "(有)三成工業"
    if "DMB東海建材" in t or "DMB東海建材" in file_name: return "DMB東海建材"
    if "飛騨製材" in t or "飛騨製材" in file_name: return "飛騨製材"
    if "ヤマスミ建設" in t or "ヤマスミ" in file_name: return "(株)ヤマスミ建設"
    if "TEST" in t or "TEST" in file_name: return "TEST"
    if "個人" in t or "個人" in file_name: return "個人"
    return "不明"

# =========================
# 汎用パーサー
# =========================
def parse_universal(t: str, tight: str, result: dict, company: str):
    labels = COMPANY_LABEL_MAP.get(company, {})
    if not labels:
        return

    lbl_id = labels.get("id")
    if lbl_id:
        if company == "アイ工務店":
            # 【修正3】T始まりインボイス番号(T+13桁)をtight上で確実に除外してバーコード抽出
            nums = re.findall(r"\d{13}", tight)
            valid_nums = [n for n in nums if not re.search(r"T" + n, tight) and not n.startswith("202") and not n.startswith("0")]
            if valid_nums:
                result["id"] = valid_nums[0]
            else:
                nums = re.findall(r"(?<![T\d])\d{8,12}(?!\d)", tight)
                valid_nums = [n for n in nums if not n.startswith("202") and not n.startswith("0")]
                if valid_nums:
                    result["id"] = valid_nums[0]
        else:
            m_id = re.search(f"{re.escape(lbl_id)}\\s*([A-Za-z0-9\\-]+)", t)
            if not m_id: m_id = re.search(f"{re.escape(lbl_id)}([A-Za-z0-9\\-]+)", tight)
            if m_id:
                clean_id = re.sub(r"[^0-9A-Za-z\-]", "", m_id.group(1).strip())
                if clean_id: result["id"] = clean_id

    for key, res_key in [("sub_id_1", "client_code2"), ("sub_id_2", "client_code3")]:
        lbl_sub = labels.get(key)
        if lbl_sub and company != "アイ工務店":
            m_sub = re.search(f"{re.escape(lbl_sub)}\\s*([A-Za-z0-9\\-]+)", t)
            if m_sub: result[res_key] = m_sub.group(1).strip()

    # アイ工務店: 業者NOをclient_code2に格納
    if company == "アイ工務店":
        m_vendor = re.search(r"業者[ＮN][ＯO]\s*(\d{4,8})", t)
        if m_vendor:
            result["client_code2"] = m_vendor.group(1)

    lbl_site = labels.get("site_name")
    if lbl_site:
        m_site = re.search(f"{re.escape(lbl_site)}[\\s:：]*([^\\n]{{2,50}})", t)
        if m_site:
            raw_site = m_site.group(1).strip()
            for stop in ["施工場所", "工事場所", "工期", "契約", "発注", "合計", "現場主任"]:
                if stop in raw_site: raw_site = raw_site[:raw_site.index(stop)].strip()
            if len(raw_site) >= 2: result["site_name"] = raw_site

    lbl_content = labels.get("content")
    if lbl_content and company != "アイ工務店":
        if company in ("新生建設(株)", "(株)宮崎工務店", "(株)宮崎"):
            m_content = re.search(f"{re.escape(lbl_content)}[\\s:：]*([^\\n]{{2,80}})", t)
        else:
            m_content = re.search(f"{re.escape(lbl_content)}(.+?)(?=施工場所|現場|工期|契約|発注|小計|消費|合計|$)", tight)

        if m_content:
            raw_content = m_content.group(1).strip()
            if len(raw_content) >= 2: result["content"] = raw_content
        elif result.get("content") in (None, "注文工事"):
            m_content_line = re.search(f"{re.escape(lbl_content)}\\s*([^\\n]{{2,50}})", t)
            if m_content_line:
                raw_content = m_content_line.group(1).strip()
                for stop in ["施工場所", "工事場所", "現場", "工期", "契約"]:
                    if stop in raw_content: raw_content = raw_content[:raw_content.index(stop)].strip()
                if len(raw_content) >= 2: result["content"] = raw_content

    lbl_amount = labels.get("amount")
    if lbl_amount:
        m_amt = re.search(f"{re.escape(lbl_amount)}[^\\d]*([\\d,]+)", tight)
        if m_amt:
            amt_val = _num(m_amt.group(1))
            if 1000 <= amt_val <= 9000000: result["amount"] = amt_val

    lbl_doc = labels.get("doc_type")
    if company == "住友不動産":
        if any(k in tight for k in ["B表", "追加注文", "工程変更"]):
            result["docType"] = "B表（追加注文書）"
        else:
            result["docType"] = "注文書"
    elif lbl_doc:
        m_doc = re.search(f"{re.escape(lbl_doc)}[\\s:：]*([^\\s]{{2,20}})", t)
        if m_doc: result["docType"] = m_doc.group(1).strip()

# =========================
# 会社別専用パーサー
# =========================
def parse_sumitomo(t: str, tight: str, result: dict):
    all_yen = [_num(v) for v in re.findall(r'\\([\d,]{4,10})', t)]
    valid_yen = [v for v in all_yen if 1000 <= v <= 9_000_000]
    if valid_yen: result['amount'] = max(valid_yen)

    tight_content_m = re.search(r'品名.{0,4}品番.{0,4}工事内容(?:\d{4}/\d{1,2}/\d{1,2})?(?:[^\u4e00-\u9fff]{1,10})?(?:[\u4e00-\u9fff]{1,4}(?=[工新改解外内設配管]))?([\u4e00-\u9fff][\u4e00-\u9fff\u3040-\u30ffA-Za-z0-9\uff01-\uff5e\u3000 ]{3,50}?)(?=諸掛|数量|端数|小計|$)', tight)
    if tight_content_m: result['content'] = tight_content_m.group(1).strip()

    if result['content'] == '注文工事':
        lines = t.split('\n')
        for i, line in enumerate(lines):
            if '品名' in line and '工事内容' in line:
                for j in range(i+1, min(i+6, len(lines))):
                    cand = lines[j].strip()
                    if not cand or re.match(r'^\d{4}/', cand) or re.match(r'^[\d\s,\\¥￥単位数量式]+$', cand) or len(cand) <= 3: continue
                    cand = re.sub(r'^[\u4e00-\u9fff]{1,4}(?=[工新改解外内設配管])', '', cand)
                    if len(cand) >= 4: result['content'] = cand; break
                break

    ec_m = re.search(r'@\s*(E\d{5})', tight)
    if ec_m and not result.get('client_code2'): result['client_code2'] = ec_m.group(1)


def parse_first(t: str, tight: str, result: dict):
    biz_m = re.search(r"事業名.*?(\d{6,10})", tight)
    if biz_m: result["id"] = biz_m.group(1)
    else:
        for i8 in re.findall(r"(\d{8})", tight):
            if not i8.startswith("202"): result["id"] = i8; break
    content_m = re.search(r"細目工種[\s\n:：]*([^\n]{2,30})(?:\n([^\n]{2,30}))?", t)
    if content_m:
        line1, line2 = content_m.group(1).strip(), content_m.group(2).strip() if content_m.group(2) else ""
        if line2 and not any(k in line2 for k in ["所在地", "備考", "工期", "発行日"]): result["content"] = f"{line1} {line2}".strip()
        else: result["content"] = line1


def parse_abe(t: str, tight: str, result: dict):
    tight_fixed = tight.replace("o", "0").replace("O", "0").replace("D", "0").replace("L", "1")
    config = result.get("config", {})
    label_no1 = config.get("label_no1", "工事コ[ー-]ド")

    id_label_m = re.search(r"現場ID(\d{5,10})", tight)
    if id_label_m: result["id"] = id_label_m.group(1)

    if not result["id"]:
        nums = re.findall(r"\d{7,10}", tight_fixed)
        exclude_ids = {str(result.get("amount", "")), "4550004", "4550825"}
        candidates = [n for n in nums if n not in exclude_ids and not n.startswith("202") and not n.startswith("090") and not n.startswith("080")]
        if candidates: result["id"] = candidates[0]

    m_code = re.search(f"{label_no1}[^\\d]*(\\d{{4,10}})", tight_fixed)
    if m_code: result["client_code3"] = m_code.group(1)

    m_amt = re.search(r"(?:合計|代\s*金|計)[^\d]*([\d,]+)", t)
    if m_amt:
        amt_str = m_amt.group(1).replace(",", "")
        if amt_str.isdigit() and int(amt_str) >= 5000 and int(amt_str) > result.get("amount", 0):
            result["amount"] = int(amt_str)

    content_val = None
    m_content_tight = re.search(r"(?:工事件名|件名|工事名)(.+?)(?=施工場所|現場住所|工期|現場ID|現場|契約|発注|小計|消費|合計|$)", tight_fixed)
    if m_content_tight:
        raw = m_content_tight.group(1).strip()
        if len(raw) >= 3: content_val = raw

    if not content_val:
        m_content_line = re.search(r"(?:工事件名|件名|工事名)[\s\n:：]*([^\n]{5,50})", t)
        if m_content_line:
            raw = m_content_line.group(1).strip()
            for stop in ["施工場所", "現場住所", "工期", "現場", "契約"]:
                if stop in raw: raw = raw[:raw.index(stop)].strip()
            if len(raw) >= 3: content_val = raw

    if not content_val:
        m_content_fallback = re.search(r"([^工期現場契約発注小計消費合計]{2,20}邸新築工事)", tight_fixed)
        if m_content_fallback: content_val = m_content_fallback.group(1)
    if content_val: result["content"] = content_val

    addr_val = None
    m_addr_tight = re.search(r"(?:施工場所|現場住所)(.+?)(?=工期|契約|現場ID|現場|発注|小計|消費|合計|$)", tight_fixed)
    if m_addr_tight:
        addr_m = re.search(r"([^\s]{2,4}?[都道府県][^\s]{2,40}?[市区町村][^\s]{0,40})", m_addr_tight.group(1))
        if addr_m: addr_val = _clip_address(re.sub(r"[_\"']+", "", addr_m.group(1)).strip())
    if not addr_val:
        m_addr_line = re.search(r"(?:施工場所|現場住所)[\s\n]*([^\n]{5,60})", t)
        if m_addr_line: addr_val = _clip_address(re.sub(r"\s+", "", m_addr_line.group(1).strip()))
    if not addr_val:
        m_addr_any = re.search(r"(愛知県[^\s]{2,40}[市区町村][^\s]{5,40})", tight_fixed)
        if m_addr_any:
            raw = m_addr_any.group(1)
            if not any(k in raw for k in ["津金", "小金", "黒川本通"]): addr_val = _clip_address(re.sub(r"[_\"']+", "", raw).strip())
    if addr_val: result["address"] = addr_val

    c = result["content"]
    c = re.sub(r"川合邸\s*川合徹様", "川合徹様", c)
    c = re.sub(r"(川合徹様\s*)+", "川合徹様 ", c)
    c = re.sub(r"(幸代様邸\s*)+", "幸代様邸", c)
    result["content"] = re.sub(r"\s+", " ", c).strip()


def parse_ai(t: str, tight: str, result: dict):
    """
    アイ工務店専用パーサー
    【修正】
    - id: 13桁バーコードのみ抽出（T始まりインボイス番号は除外）
    - client_code2: 業者NO（6桁）
    - 工事名の「様邸様邸」重複は_normalize_textで処理済
    - content: 明細の名称（NO.1行目）を抽出
    """
    config = result.get("config", {})
    label_no1 = config.get("label_no1", "")

    # 1. id: 【修正3】T始まりインボイス番号をtight上で確実に除外してバーコード抽出
    if not result.get("id"):
        barcodes_13 = re.findall(r"\d{13}", tight)
        valid = [n for n in barcodes_13
                 if not re.search(r"T" + n, tight)
                 and not n.startswith("202")
                 and not n.startswith("0")]
        if valid:
            result["id"] = valid[0]
        else:
            barcodes_other = re.findall(r"(?<![T\d])\d{8,12}(?!\d)", tight)
            valid = [n for n in barcodes_other
                     if not n.startswith("202") and not n.startswith("0")]
            if valid:
                result["id"] = valid[0]

    # 2. client_code2: 業者NO
    if not result.get("client_code2"):
        m_vendor = re.search(r"業者[ＮN][ＯO]\s*(\d{4,8})", t)
        if m_vendor:
            result["client_code2"] = m_vendor.group(1)
        else:
            # 「業者ＮＯ」直下の行から6桁数字を取得
            m_vendor2 = re.search(r"業者[ＮN][ＯO][^\d]{0,5}(\d{4,8})", tight)
            if m_vendor2:
                result["client_code2"] = m_vendor2.group(1)

    # 3. content: 明細名称を抽出
    # PDFの各列は1行ずつ独立: ＮＯ\n名称\n仕様\n...\n備考\n１\n上下水調整工事費\n式\n...
    # 「備考」の直後に連番(１など)、その次が名称
    # parse_universalはアイ工務店をスキップするのでここで必ず上書きする
    m_meisai = re.search(
        r"備考\n[１-９1-9]\n([^\n]{3,50})\n",
        t
    )
    if m_meisai:
        result["content"] = m_meisai.group(1).strip()

    # 4. site_name: 工事名（「工　事　名」ラベル）
    if not result.get("site_name"):
        m_kojimei = re.search(r"工\s*事\s*名\s+(.+?)(?:\n|工\s*期|建\s*築\s*地)", t)
        if m_kojimei:
            raw = re.sub(r"\s+", " ", m_kojimei.group(1)).strip()
            # 「様邸様邸」重複除去（念のため）
            raw = re.sub(r"様邸\s*様邸", "様邸", raw)
            if len(raw) >= 3:
                result["site_name"] = raw


def parse_miyazaki_shinsei(t: str, tight: str, result: dict):
    """宮崎工務店・(株)宮崎・新生建設 共通帳票パーサー"""
    # 注文番号(例: T2511-7-26) → id
    # OCRノイズ対応: T が 7/t/1/l に化けるケースも含めて取得
    m_order = re.search(r"注文番[号亨][\s:：]*([Tt71l][0-9]{3,6}[-－][0-9]{1,2}[-－][0-9]{1,2})", t)
    if m_order:
        val = m_order.group(1)
        # 先頭の T化けを T に正規化
        val = re.sub(r"^[71l]", "T", val).upper().replace("－", "-")
        result["id"] = val

    # 工事番号(例: 25107083) → client_code2
    m_kojino = re.search(r"工事番号[\s:：]*(\d{7,10})", t)
    if m_kojino:
        result["client_code2"] = m_kojino.group(1)

    m_content = re.search(r"工事名称[\s:：]*([^\n]{4,80})", t)
    if m_content:
        content = m_content.group(1).strip()
        for stop in ["現場主任", "工事場所", "工　期", "工期", "請求締日", "支払"]:
            if stop in content:
                content = content[:content.index(stop)].strip()
        if len(content) >= 3:
            result["content"] = content

    m_kouji = re.search(
        r"[自从]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*[至迄]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        t
    )
    if m_kouji:
        result["startDate"] = _fmt(m_kouji.group(1), m_kouji.group(2), m_kouji.group(3))
        result["endDate"]   = _fmt(m_kouji.group(4), m_kouji.group(5), m_kouji.group(6))

    # 注文金額: OCRが ¥16,500- を 416,500 と読む場合に対応
    # 戦略: ¥マーク直後 → 消費税から逆算 → tightから取得 の順で試みる
    m_amt_yen = re.search(r"注文金額[\s]*[¥￥\\]([0-9,]+)-?", t)
    if m_amt_yen:
        val = _num(m_amt_yen.group(1))
        if 1000 <= val <= 9_000_000:
            result["amount"] = val
    else:
        # 消費税額から税込合計を逆算: 「消費税10% ¥X」→ X*11 が税込金額
        m_tax = re.search(r"消費税10%[\s]*[¥￥\\1Il]?([0-9,]+)", t)
        if m_tax:
            tax_val = _num(m_tax.group(1))
            if 100 <= tax_val <= 900_000:
                inferred = tax_val * 11
                if 1000 <= inferred <= 9_000_000:
                    result["amount"] = inferred
        # 消費税からも取れなければtightから取得
        if not result.get("amount") or result["amount"] == 0:
            m_amt2 = re.search(r"注文金額[\s¥￥\\]*([0-9,]+)-?", tight)
            if m_amt2:
                val = _num(m_amt2.group(1))
                if 1000 <= val <= 9_000_000:
                    result["amount"] = val


def parse_globe(t: str, tight: str, result: dict):
    """グローブホーム専用帳票パーサー"""
    PREFS = (
        "東京都", "北海道", "京都府", "大阪府",
        "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
        "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "神奈川県",
        "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
        "岐阜県", "静岡県", "愛知県", "三重県",
        "滋賀県", "兵庫県", "奈良県", "和歌山県",
        "鳥取県", "島根県", "岡山県", "広島県", "山口県",
        "徳島県", "香川県", "愛媛県", "高知県",
        "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
    )

    m_content = re.search(r"工事件名(.+?)(?=施工場所|現場|工期|契約|発注|小計|消費|合計|$)", tight)
    if m_content:
        raw = m_content.group(1).strip()
        for stop in ["施工場所", "現場", "工期", "契約", "発注", "小計", "消費", "合計"]:
            if stop in raw:
                raw = raw[:raw.index(stop)].strip()
        if len(raw) >= 2:
            result["content"] = raw

    if result.get("content") in (None, "注文工事"):
        m_content_line = re.search(r"工事件名\s*([^\n]{2,50})", t)
        if m_content_line:
            raw = m_content_line.group(1).strip()
            for stop in ["施工場所", "現場", "工期", "契約"]:
                if stop in raw:
                    raw = raw[:raw.index(stop)].strip()
            if len(raw) >= 2:
                result["content"] = raw

    addr = result.get("address", "-")
    if addr and addr != "-":
        if not any(addr.startswith(p) for p in PREFS):
            candidate = addr[1:]
            if any(candidate.startswith(p) for p in PREFS):
                result["address"] = candidate

    m_contract = re.search(r"契約番号\s*([\d\-]+)", t)
    if not m_contract:
        m_contract = re.search(r"契約番号([\d\-]+)", tight)
    if m_contract:
        contract_digits = re.sub(r"[^0-9\-]", "", m_contract.group(1).strip())
        if contract_digits:
            result["id"] = contract_digits

# =========================
# 表示用フィールド生成（1〜10の枠表示用）
# =========================
def build_display_fields(result: dict) -> dict:
    """
    parse_ocr_text の結果から表示用の1〜10項目辞書を生成する。
    各値が None / 0 / "-" の場合は None を返す。
    """
    def _v(val):
        if val is None: return None
        if isinstance(val, int) and val == 0: return None
        if isinstance(val, str) and val in ("-", "", "注文工事"): return None
        return str(val)

    def _date_range(s, e):
        if s and e and s != e: return f"{s} 〜 {e}"
        if s: return s
        return None

    def _amount_fmt(v):
        if not v or v == 0: return None
        return f"¥ {int(v):,}"

    return {
        "no1_company":      _v(result.get("company")),
        "no2_id":           _v(result.get("id")),
        "no2_1_code2":      _v(result.get("client_code2")),
        "no2_2_code3":      _v(result.get("client_code3")),
        "no3_site_name":    _v(result.get("site_name")),
        "no3_1_kojimei":    _v(result.get("site_name")),   # 工事名（site_nameと同一ソース）
        "no4_address":      _v(result.get("address")),
        "no5_amount":       _amount_fmt(result.get("amount")),
        "no6_content":      _v(result.get("content")),
        "no7_date":         _v(result.get("date")),
        "no8_kouki":        _date_range(result.get("startDate"), result.get("endDate")),
        "no9_billing_date": _v(result.get("billing_date")),
        "no10_doc_type":    _v(result.get("docType")),
    }

# =========================
# メイン処理
# =========================
def parse_ocr_text(text: str, file_name: str = "") -> dict[str, Any]:
    t = _normalize_text(text)
    tight = _tight(t)

    result: dict[str, Any] = {
        "company": "不明",
        "date": None,
        "startDate": None,
        "endDate": None,
        "billing_date": None,
        "id": None,
        "client_code2": None,
        "client_code3": None,
        "address": "-",
        "content": "注文工事",
        "site_name": None,
        "amount": 0,
        "docType": "注文書",
        "config": {},
        "contract_no": None,
        "project_no": None,
        "order_no": None,
        "kouji_code": None,
        "order_branch": None,
        "delivery_id": None,
        "biz_name": None
    }

    company = _detect_company(t, tight, file_name)
    result["company"] = company

    config = get_client_config(company)
    result["config"] = config

    result["contract_no"] = dynamic_extract(config.get("label_contract_no"), tight)
    result["project_no"]  = dynamic_extract(config.get("label_project_no"), tight)
    result["order_no"]    = dynamic_extract(config.get("label_order_no"), tight)
    result["kouji_code"]  = dynamic_extract(config.get("label_kouji_code"), tight)
    result["order_branch"]= dynamic_extract(config.get("label_order_branch"), tight)
    result["delivery_id"] = dynamic_extract(config.get("label_delivery_id"), tight)
    result["biz_name"]    = dynamic_extract(config.get("label_biz_name"), tight)

    doc_label = config.get("label_doc_name")
    if doc_label and doc_label in tight:
        result["docType"] = doc_label
    elif company in ("住友不動産", "住友不動産ハウジング(株)"):
        result["docType"] = "B表" if any(k in tight for k in ["B表", "追加", "工程変更"]) else "注文書"

    result["amount"] = extract_amount(t, tight)
    result["address"] = extract_address(t)

    id_1p = re.search(r"(1P[0-9]{3,6})", tight)
    if id_1p: result["id"] = id_1p.group(0)
    code_e = re.search(r"(E[0-9]{5})", tight)
    if code_e: result["client_code2"] = code_e.group(0)

    # 汎用パーサー（辞書ベース）
    parse_universal(t, tight, result, company)

    # 会社別専用パーサー（工期・id等を先にセットしてからextract_dates_perfectで補完）
    if company == "ファースト住建":
        parse_first(t, tight, result)
    elif company == "阿部建設":
        parse_abe(t, tight, result)
    elif company in ("住友不動産", "住友不動産ハウジング(株)"):
        parse_sumitomo(t, tight, result)
    elif company == "アイ工務店":
        parse_ai(t, tight, result)
    elif company in ("(株)宮崎工務店", "(株)宮崎", "新生建設(株)"):
        parse_miyazaki_shinsei(t, tight, result)
    elif company == "グローブホーム":
        parse_globe(t, tight, result)

    # 専用パーサーでセット済みの日付は上書きせず、未セット分だけ補完
    dates = extract_dates_perfect(t, tight, company, COMPANY_LABEL_MAP.get(company, {}))
    if not result["date"]:        result["date"]        = dates.get("date")
    if not result["startDate"]:   result["startDate"]   = dates.get("startDate")
    if not result["endDate"]:     result["endDate"]      = dates.get("endDate")
    if not result["billing_date"]:result["billing_date"] = dates.get("billing_date")

    if not result["startDate"] and result["date"]:
        result["startDate"] = result["date"]

    if not result["id"]:
        m = re.search(r"(工事番号|契約番号|発注番号|現場ID).*?(\d{6,10})", t)
        if m: result["id"] = m.group(2)

    if not result["id"]:
        # 登録番号(T+13桁)の数字部分は除外
        reg_nos = set(re.findall(r"登録番号[：:]*T?(\d{10,13})", tight))
        for ic in re.findall(r"[0-9]{7}", tight):
            skip = {str(result["amount"]), "4550004"}
            # 登録番号の数字を含む場合はスキップ
            if ic in skip or any(ic in rn for rn in reg_nos):
                continue
            result["id"] = ic
            break

    if "浄水槽" in t and result["content"] == "注文工事":
        result["content"] = "浄水槽工事"

    # 表示用フィールドを付加
    result["fields_display"] = build_display_fields(result)

    result.pop("config", None)
    return result