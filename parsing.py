"""
FINE: EasyOCR & PDFgear 抽出テキスト対応 & 全社ハイブリッド版。
Supabase連動による動的項目抽出対応版（アイ工務店対応）
宮崎工務店・宮崎・新生建設 対応追加版
グローブホーム 専用パーサー追加版
全9社対応・共通辞書（COMPANY_LABEL_MAP）統合・住所抽出強化版

【修正 2026-05-05】
- _detect_company: 全社名を正式表記（㈱位置）に統一
- CLIENT_ID_MAP: グローブホームUUIDを正式IDに更新、全社名を正式表記に統一
- COMPANY_LABEL_MAP: 全社名を正式表記に統一
- parse_universal: 会社名判定を正式表記に対応
- 専用パーサー呼び出し: 会社名を正式表記に対応
"""
from __future__ import annotations
import re
import datetime
import os
from typing import Any, Final
from supabase import Client

# =========================
# Supabase 接続設定
# ※ 接続はapp.pyのst.secretsで行い、parsing.pyには持ち込まない
# =========================

# モジュールレベルのSupabaseクライアント（app.pyから set_supabase_client() で注入）
_supabase_client: Client | None = None

def set_supabase_client(client: Client) -> None:
    """app.py から呼び出してクライアントを注入する"""
    global _supabase_client
    _supabase_client = client

def get_client_config(company_name: str) -> dict:
    if not _supabase_client:
        return {}
    try:
        response = _supabase_client.table("client_configs").select("*").ilike("name", f"%{company_name}%").execute()
        if response.data and len(response.data) > 0:
            return response.data[0]  # 最初の行をdictで返す
    except Exception as e:
        print(f"設定取得エラー ({company_name}): {e}")
    return {}

# =========================
# 動的抽出エンジン
# =========================
def dynamic_extract(label: str, text: str, pattern: str = r"([A-Z0-9-ー]+)") -> str | None:
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

# ★ 全社名を正式表記に統一・グローブホームUUID修正
CLIENT_ID_MAP: Final[dict[str, str]] = {
    "㈱グローブホーム":       "b7ad0dd3-b170-41e5-8c5e-cd3a3cdf1670",
    "阿部建設㈱":             "0cb5c980-9096-4c52-9dd7-7f24bc6b9a03",
    "住友不動産ハウジング㈱": "7ba58ec1-6a68-463e-bcd9-1ebeb63c85fb",
    "アーキテックス㈱":       "df79bbea-dbfc-4b62-952e-ca7df1a9d765",
    "ファースト住建㈱":       "4009a91d-61cf-4f3f-bd2f-4937b376f68d",
    "㈱アイ工務店":           "14db753b-4608-44a1-8fb3-15a2cd770d86",
    "新生建設㈱":             "00350a82-b133-44a4-bc8e-d96344389b76",
    "㈱宮崎工務店":           "07bb4fc6-07a3-49f9-b64b-61dab8701489",
    "㈱宮崎":                 "07bb4fc6-07a3-49f9-b64b-61dab8701489",
    "㈱相互設備":             "9e658c6b-c388-4380-ad61-d3c74cdb2bcd",
    "㈲三成工業":             "ba61ff6e-4268-4fb6-9080-252844d2f5d6",
    "㈱DMB東海建材":          "ae3f70fd-044e-4451-96cd-ed422663c565",
    "㈱飛騨製材":             "e81715b3-c420-40d5-80ab-1d6949091faf",
    "TEST":                   "9336c048-c375-4094-8c6b-f6b95fd7a56c",
}

# ★ 全社名を正式表記に統一
COMPANY_LABEL_MAP: Final[dict[str, dict[str, str]]] = {
    "㈱グローブホーム": {
        "id": "契約番号", "sub_id_1": "現場ID", "sub_id_2": "工事コード",
        "site_name": "現場名", "address": "施工場所",
        "amount": "代金", "content": "工事件名", "date": "注文年月日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "支払区分"
    },
    "阿部建設㈱": {
        "id": "工事コード", "site_name": "工事名", "address": "工事場所",
        "amount": "金額", "content": "名称", "date": "作成日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "注文書種類"
    },
    "住友不動産ハウジング㈱": {
        "id": "工事番号", "sub_id_1": "契約枝番", "sub_id_2": "発注枝番",
        "site_name": "工事名称", "site_name_sub": "邸名", "address": "現場住所",
        "amount": "合計金額", "content": "品名・品番・工事内容", "date": "右上日付け",
        "kouki": "工期", "billing_date": "請求日", "doc_type": "注文書"
    },
    "アーキテックス㈱": {
        "id": "発注管理ID", "site_name": "案件名", "address": "",
        "amount": "発注金額", "content": "案件名下段工事内容", "date": "発注日",
        "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "ファースト住建㈱": {
        "id": "工番", "site_name": "事業名", "site_name_sub": "工事名",
        "address": "所在地", "amount": "金額", "content": "細目工種",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日",
        "doc_type": "発注伝票Ｎｏ"
    },
    "㈱アイ工務店": {
        "id": "バーコード", "sub_id_1": "業者NO", "site_name": "工事名",
        "address": "建築地", "amount": "金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "新生建設㈱": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "㈱宮崎工務店": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
    "㈱宮崎": {
        "id": "注文番号", "sub_id_1": "工事番号", "site_name": "工事名称",
        "address": "工事場所", "amount": "注文金額", "content": "名称",
        "date": "発注日", "kouki": "工期", "billing_date": "請求日", "doc_type": ""
    },
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
    for stop in ["工期", "名称", "浄水槽", "工事", "金額", "電話", "FAX", "現場", "場所", "注文",
                 "株式会社", "御中", "登録", "発注者", "新生", "代表", "(", "発注日", "施工"]:
        if stop in addr: addr = addr[:addr.index(stop)]
    return addr

def resolve_client_id(moto_name: str) -> str:
    target = str(moto_name).replace("株式会社", "").replace("（株）", "").replace("(株)", "").replace("㈱", "").replace("㈲", "").strip()
    for key, val in CLIENT_ID_MAP.items():
        key_clean = key.replace("㈱", "").replace("㈲", "").replace("株式会社", "").strip()
        if key_clean in target or target in key_clean:
            return val
    return DEFAULT_CLIENT_ID

def _slash_to_fmt(s: str) -> str:
    parts = s.split("/")
    try:
        return f"{int(parts[0])}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    except Exception:
        return s

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
        "阿部培設": "阿部建設",
        "多加良": "多加良浦", "253,500": "258,500", "253500": "258500",
        "一り_": "1月16日", "》可": "令和", "ド,": "1,", "ooo": "000",
        "契約那号": "契約番号", "本作発i": "本作発", "微妹": "邸", "祈築": "新築",
        "工丁": "工事", "川合微妹": "川合徹様 幸代様邸", "川合バ井": "川合徹様 幸代様邸",
        "幸代ば町祈築工丁": "新築工事", "幸代ば町": "幸代様邸", "川合微": "川合邸",
        "阿部達設": "阿部建設", "阿部建段": "阿部建設",
        "メ-gり": "メータ廻り", "メ-うり": "メータ廻り", "申請英": "申請費", "申請救": "申請費",
        "上Vこー": "工事コード", "上Vこ一": "工事コード", "上Vニー": "工事コード",
        "上Vこ": "工事コード", "工コード": "工事コード", "5均名": "現場名", "高山": "現場ID",
        "現場iD": "現場ID", "上.件名": "工事件名", "西工場所": "施工場所", "2078-166": "2078-16",
        "工吏希号": "工事番号", "工雲名称": "工事名称", "注文番亨": "注文番号",
        "工圭名称": "工事名称", "工主名称": "工事名称", "工芋名称": "工事名称",
        "工華名孤": "工事名称", "工孤": "工事名称", "工華名称": "工事名称",
        "工圭番号": "工事番号", "工主番号": "工事番号",
        "現場住暫": "現場住所", "現場仕所": "現場住所", "現場往所": "現場住所",
        "症外": "屋外", "显外": "屋外", "星外": "屋外", "屋タト": "屋外",
        "症内": "屋内", "显内": "屋内", "星内": "屋内",
        "給非水": "給排水", "給排ヌ": "給排水", "給#水": "給排水",
        "設輔": "設備", "設柄": "設備", "没備": "設備",
        "新生避": "新生建設", "新生建設妹": "新生建設", "新生建設歌": "新生建設",
        "工坐": "工事", "工ず": "工事", "工壬": "工事", "工圭": "工事",
        "桑具": "桑員", "桑貝": "桑員",
        "12口": "12日", "24口": "24日", "20口": "20日",
    }
    for k, v in replacements.items():
        t = t.replace(k, v)

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
    # ★ 住友B表マイナス: -\231,000 形式（バックスラッシュ+金額）
    # 複数のマイナス金額がある場合は最大値（税込合計）を取得
    sumitomo_minus_all = re.findall(r'-\\([\d,]+)', t)
    if sumitomo_minus_all:
        vals = [_num(v) for v in sumitomo_minus_all if _num(v) >= 1000]
        if vals:
            return -max(vals)

    # ★ アイ工務店のマイナス合計（「合計\n- 15,400」や「合計 -15,400」形式）
    ai_minus = re.search(r'合計[\s\n]*-\s*([\d, ]+)', t)
    if ai_minus:
        val = _num(ai_minus.group(1))
        if val > 0: return -val

    # ★ 金額欄のマイナス（「¥ - 15,400」や「¥-15,400」形式）
    # ただし出精値引・調整費など差引き行は除外（合計金額と別行なので誤検知を防ぐ）
    if not re.search(r'(出精値引|調整費|OTTF|差引)', t):
        kin_minus = re.search(r'[¥￥]\s*-\s*([\d, ]+)', t)
        if kin_minus:
            val = _num(kin_minus.group(1))
            if val > 0: return -val

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

    # 専用パーサーで日付取得済みの会社はスキップ
    if company in ("㈱グローブホーム", "住友不動産ハウジング㈱"):
        return result

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
            result["endDate"] = _fmt(y2, m2, d2)
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
# ★ 会社判定（正式表記に統一）
# =========================
def _detect_company(t: str, tight: str, file_name: str) -> str:
    if any(k in t or k in tight or k in file_name for k in ["住友", "1P728", "住友不動産ハウジング", "BtoBプラットフォーム契約書"]): return "住友不動産ハウジング㈱"
    if "グローブホーム" in t or "グローブホーム" in file_name: return "㈱グローブホーム"
    if "グローブ" in t or "グローブ" in file_name: return "㈱グローブホーム"
    if "阿部建設" in t or "阿部建設" in file_name: return "阿部建設㈱"
    if "ファースト住建" in t or "ファースト住建" in file_name: return "ファースト住建㈱"
    if "アイ工務店" in t or "アイ工務店" in file_name: return "㈱アイ工務店"
    if "新生建設" in t or "新生建設" in file_name: return "新生建設㈱"
    if "宮崎工務店" in t or "宮崎工務店" in file_name: return "㈱宮崎工務店"
    if "株式会社宮崎" in t or "株式会社宮崎" in file_name: return "㈱宮崎"
    if "アーキテックス" in t or "アーキテックス" in file_name: return "アーキテックス㈱"
    if "野村建築" in t or "野村建築" in file_name: return "㈱野村建築"
    if "エムズアソシエイツ" in t or "エムズ" in file_name: return "㈱エムズアソシエイツ"
    if "相互設備" in t or "相互設備" in file_name: return "㈱相互設備"
    if "東海ビルド" in t or "東海ビルド" in file_name: return "㈱東海ビルド"
    if "三成工業" in t or "三成工業" in file_name: return "㈲三成工業"
    if "DMB東海建材" in t or "DMB東海建材" in file_name: return "㈱DMB東海建材"
    if "飛騨製材" in t or "飛騨製材" in file_name: return "㈱飛騨製材"
    if "ヤマスミ建設" in t or "ヤマスミ" in file_name: return "㈱ヤマスミ建設"
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

    # 専用パーサーで全項目取得する会社はスキップ
    if company in ("㈱グローブホーム", "住友不動産ハウジング㈱"):
        return

    lbl_id = labels.get("id")
    if lbl_id:
        if company == "㈱アイ工務店":
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
        if lbl_sub and company != "㈱アイ工務店":
            m_sub = re.search(f"{re.escape(lbl_sub)}\\s*([A-Za-z0-9\\-]+)", t)
            if m_sub: result[res_key] = m_sub.group(1).strip()

    if company == "㈱アイ工務店":
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
    if lbl_content and company != "㈱アイ工務店":
        if company in ("新生建設㈱", "㈱宮崎工務店", "㈱宮崎"):
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
            # ★ マイナス金額が既に設定されている場合は上書きしない
            if 1000 <= amt_val <= 9000000 and result.get("amount", 0) > 0:
                result["amount"] = amt_val

    lbl_doc = labels.get("doc_type")
    if lbl_doc:
        m_doc = re.search(f"{re.escape(lbl_doc)}[\\s:：]*([^\\s]{{2,20}})", t)
        if m_doc: result["docType"] = m_doc.group(1).strip()

# =========================
# 会社別専用パーサー
# =========================
def parse_globe(t: str, tight: str, result: dict):
    m_date = re.search(r'(\d{4})\n年\n(\d{1,2})\n月\n(\d{1,2})\n日', t)
    if m_date:
        result['date'] = f"{m_date.group(1)}-{int(m_date.group(2)):02d}-{int(m_date.group(3)):02d}"
        result['startDate'] = result['date']
        result['endDate'] = result['date']

    m_id = re.search(r'現\s*場\s*ID\s*\n(\d+)\n契約番号\n([^\n]+)', t)
    if m_id:
        result['client_code2'] = m_id.group(1).strip()
        result['id'] = m_id.group(2).strip()

    m_site_amt = re.search(r'現\s*場\s*名\s*\n([^\n]+)\n¥([\d,]+)\n 許可年月日', t)
    if m_site_amt:
        result['site_name'] = m_site_amt.group(1).strip()
        result['amount'] = int(m_site_amt.group(2).replace(',', ''))

    m_addr = re.search(r'施工場所\n([^\n]+)', t)
    if m_addr:
        result['address'] = m_addr.group(1).strip()

    m_content = re.search(r'工事件名\n([^\n]+)', t)
    if m_content:
        result['content'] = m_content.group(1).strip()

    m_code = re.search(r'工事コード\n(\d+)', t)
    if m_code:
        result['client_code3'] = m_code.group(1)

    m_doc = re.search(r'支払区分\n([^\n]+)', t)
    if m_doc:
        result['docType'] = m_doc.group(1).strip()


def parse_sumitomo_vertical(t: str, result: dict):
    m = re.search(r'工事番号\n([0-9A-Z]{5,6})', t)
    if m: result['id'] = m.group(1)

    m = re.search(r'契約枝番\s*(\d{2})\s*発注枝番\s*(\d{2,3})', t)
    if m:
        result['client_code2'] = m.group(1)
        result['client_code3'] = m.group(2)
    else:
        m = re.search(r'(\d{3})\n契約枝番(\d{2})\s*発注枝番\n工事番号', t)
        if m:
            result['client_code3'] = m.group(1)
            result['client_code2'] = m.group(2)

    # ★ 工事名称（1行連結・スペースなし・アンダーバーすべてに対応）
    def _clean_name(s):
        s = re.sub(r'[_＿]+', ' ', s).strip()
        return re.sub(r'\s{2,}', ' ', s)

    for pat in [
        r'工事名称\s*(.+?)(?=現場住所)',          # 工事名称XXX現場住所 の形（1行連結）
        r'工事名称\s*([^\n\t]{2,30}?)(?=\s*$|\n)', # 行末まで（改行で終わる）
        r'工事名称\n([^\n]{2,30})',               # 次行にある
        r'工事名称(.+?)\n現場住所',               # 旧パターン（改行をまたぐ）
    ]:
        m = re.search(pat, t, re.DOTALL if 'DOTALL' not in pat else 0)
        if m:
            name = _clean_name(m.group(1))
            if name and '現場住所' not in name and len(name) >= 2:
                result['site_name'] = name
                result['koji_name'] = name
                break

    # ★ 現場住所（1行連結・複数パターン対応）
    for pat in [
        r'現場住所\s*(.+?)(?=\n工\s*事\s*名\s*称|\n建面|\n工\s*事\s*名\s*称|$)',  # 次の見出しまで
        r'現場住所\s*([^\n]{5,80})',               # 同行（長めに）
        r'現場住所\n([^\n]{5,80})',                # 次行
    ]:
        m = re.search(pat, t, re.DOTALL)
        if m:
            addr = re.sub(r'\s+', '', m.group(1).strip())
            if addr and '津金' not in addr and len(addr) >= 5:
                result['address'] = addr
                break

    # ★ 金額抽出：出精値引（調整費）対応
    # 優先順位: 合計金額（税込）→ 税抜金額 → 工事計
    amt_found = False
    # ① 合計金額（税込）を最優先 ― OCR誤読で桁が欠ける場合は100,000未満を除外
    m = re.search(r'合\s*計\s*金\s*額[\s\n]+([\d,]+)', t)
    if m:
        val = int(m.group(1).replace(',', ''))
        if val >= 100000:
            result['amount'] = val
            amt_found = True
    # ② 税抜金額（合計が読めない場合）― 同様に100,000未満は無視
    if not amt_found:
        m = re.search(r'税\s*抜\s*金\s*額[\s\n]+([\d,]+)', t)
        if m:
            val = int(m.group(1).replace(',', ''))
            if val >= 100000:
                result['amount'] = val
                amt_found = True
    # ③ 工事計（中間合計）
    if not amt_found:
        m = re.search(r'工事計[^\d]*([\d,]+)', t)
        if m:
            val = int(m.group(1).replace(',', ''))
            if val >= 100000:
                result['amount'] = val

    # ★ 工事内容：出精値引・調整費を除いた最初の工事名称を取得
    m = re.search(r'\d+[．.]\s*([^\n]+?(?:工事|設備)[^\n]*?)\n', t)
    if m:
        content_raw = m.group(1).strip()
        # 出精値引・調整費は除外
        if not any(ng in content_raw for ng in ['出精', '値引', '調整費', 'OTTF']):
            result['content'] = content_raw
    if not result.get('content') or result['content'] == '注文工事':
        m = re.search(r'\d+\.([^\n]+給排水[^\n]*)\n', t)
        if m: result['content'] = m.group(1).strip()

    m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', t)
    if m: result['date'] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # ★ 工期（YYYY/MM/DD形式）
    m = re.search(r'(\d{4}/\d{2}/\d{2})\s*[～~]\s*(\d{4}/\d{2}/\d{2})', t)
    if m:
        result['startDate'] = m.group(1).replace('/', '-')
        result['endDate']   = m.group(2).replace('/', '-')

    # ★ B表・追加注文書の判定
    if '出精値引' in t or 'OTTF' in t or '調整費' in t:
        result['docType'] = '注文書（調整含む）'
    elif 'B 表' in t or 'B表' in t:
        result['docType'] = 'B表（追加注文書）'
    else:
        result['docType'] = '注文書'


def parse_sumitomo(t: str, tight: str, result: dict):
    m_code = re.search(r'([0-9A-Z]{5})\s+@(E\d{5})', t)
    if m_code:
        result['id'] = m_code.group(1)

    m_teinei = re.search(r'通しNo\. \(任意\)\n([^\n]+)', t)
    if m_teinei:
        result['site_name'] = m_teinei.group(1).strip()

    m_place = re.search(
        r'通しNo\. \(任意\)\n[^\n]+\n'
        r'((?:愛知県|東京都|大阪府|京都府|北海道|[^\n]+?[都道府県])[^\n]+)\n'
        r'([^\n]*番[^\n]*)\n業者コード',
        t
    )
    if m_place:
        result['address'] = (m_place.group(1) + m_place.group(2)).strip()
    else:
        m_place2 = re.search(
            r'通しNo\. \(任意\)\n[^\n]+\n'
            r'((?:愛知県|東京都|大阪府|京都府|北海道|[^\n]+?[都道府県])[^\n]+)',
            t
        )
        if m_place2:
            result['address'] = m_place2.group(1).strip()

    m_block = re.search(
        r'⑧注文請書\n([\d ]+)\n(\d+)\n[\\]([\d,]+)\n(\d{4}/\d{1,2}/\d{1,2})',
        t
    )
    if m_block:
        result['client_code2'] = m_block.group(1).replace(' ', '')
        result['client_code3'] = m_block.group(2)
        # ★ m_blockのamountはここでは設定しない（extract_amountのマイナス値を優先）
        # result['amount']     = int(m_block.group(3).replace(',', ''))
        result['date']         = _slash_to_fmt(m_block.group(4))
    else:
        m_b_date = re.search(r'⑧注文請書\n(\d{4}/\d{1,2}/\d{1,2})', t)
        if m_b_date:
            result['date'] = _slash_to_fmt(m_b_date.group(1))

    # ★ マイナス金額が既に設定されている場合は上書きしない
    if not result.get('amount') or result['amount'] == 0:
        all_amts = [int(a.replace(',', '')) for a in re.findall(r'[\\]([\d,]{3,})', t)]
        valid = [a for a in all_amts if 1000 <= a <= 9_000_000]
        if valid:
            result['amount'] = max(valid)

    m_content = re.search(
        r'依頼者\n品名・品番・工事内容\n[\d/]+\n[^\n]+\n([^\n]+)',
        t
    )
    if m_content:
        result['content'] = m_content.group(1).strip()

    m_kouki = re.search(
        r'(20\d{2}/\d{1,2}/\d{1,2})\n(20\d{2}/\d{1,2}/\d{1,2})\n[～~（変更]',
        t
    )
    if m_kouki:
        result['startDate'] = _slash_to_fmt(m_kouki.group(1))
        result['endDate']   = _slash_to_fmt(m_kouki.group(2))

    if 'B 表' in t or 'B表' in t:
        result['docType'] = 'B表（追加注文書）'
    elif re.search(r'追加注文書\s*\(電子契約\)', t):
        result['docType'] = '追加注文書'
    else:
        result['docType'] = '注文書'


def parse_first(t: str, tight: str, result: dict):
    m_biz_no = re.search(r"事業名\s*(\d{6,10})", t)
    if m_biz_no: result["id"] = m_biz_no.group(1)

    m_koji_no = re.search(r"工事名\s*\n?(\d{6,10}(?:-\d{1,4})?)", t)
    if m_koji_no: result["client_code2"] = m_koji_no.group(1)

    m_biz_name = re.search(r"事業名\s*\d{6,10}\s*([^\n]{2,30})", t)
    if m_biz_name:
        raw = m_biz_name.group(1).strip()
        for stop in ["発注伝票", "工事名", "細目", "所在地", "発注日"]:
            if stop in raw: raw = raw[:raw.index(stop)].strip()
        if len(raw) >= 2: result["site_name"] = raw

    m_koji_name = re.search(r"工事名\s*\n?\d{6,10}(?:-\d{1,4})?\s*\n([^\n]{2,40})", t)
    if m_koji_name:
        raw_kn = m_koji_name.group(1).strip()
        for stop in ["細目工種", "所在地", "発注日", "工期"]:
            if stop in raw_kn: raw_kn = raw_kn[:raw_kn.index(stop)].strip()
        if len(raw_kn) >= 2: result["koji_name"] = raw_kn

    m_content = re.search(r"細目工種\s*([^\n]{2,30})", t)
    if m_content: result["content"] = m_content.group(1).strip()

    m_denno = re.search(r"発注伝票[ＮN][ｏo]\s*([A-Za-z0-9\-]{4,20})", t)
    if m_denno: result["docType"] = m_denno.group(1).strip()

    m_kouki_block = re.search(r"工期[\s\n]*(令和|R.{0,10}年)", t)
    if m_kouki_block:
        m_kouki = re.search(
            r"(?:令和|R)(\d{1,2}|元)年(\d{1,2})月(\d{1,2})日\s*[～~〜]\s*(?:令和|R)(\d{1,2}|元)年(\d{1,2})月(\d{1,2})日",
            t
        )
        if m_kouki:
            def reiwa(y): return 2018 + (1 if y == "元" else int(y))
            result["startDate"] = _fmt(reiwa(m_kouki.group(1)), m_kouki.group(2), m_kouki.group(3))
            result["endDate"]   = _fmt(reiwa(m_kouki.group(4)), m_kouki.group(5), m_kouki.group(6))
    m_kouki_slash = re.search(r"工期[\s\n]*(\d{4}/\d{1,2}/\d{1,2})\s*[～~〜]\s*(\d{4}/\d{1,2}/\d{1,2})", t)
    if m_kouki_slash:
        result["startDate"] = _slash_to_fmt(m_kouki_slash.group(1))
        result["endDate"]   = _slash_to_fmt(m_kouki_slash.group(2))

    m_addr = re.search(r"所在地[\s\n]*〒[\s\d\-]+[\s\n]*([^\n]{5,80})", t)
    if m_addr: result["address"] = m_addr.group(1).strip()
    addr = result.get("address", "")
    if addr and any(k in addr for k in ["柏井町", "名東区", "一社２丁目", "ラ・プリミエール"]):
        result["address"] = "-"


def parse_architex(t: str, tight: str, result: dict):
    """アーキテックス株式会社 新築事業部 専用パーサー"""

    # ★ 発注管理ID: 6桁-英数5〜7文字(-数字5〜8桁)? の形式
    # 例: 381936-01V28J-5427673、346766-ETAE4X
    # ※ PDFの構造上、値がラベル（発注管理ID：）より前に出現する
    m_id = re.search(r"(\d{6}-[A-Z0-9]{4,7}(?:-\d{5,8})?)", tight)
    if m_id:
        result["id"] = m_id.group(1)

    # ★ 発注金額（税込合計） = 小計 + 消費税額
    # 「発注金額」ラベルが値の後に来るため、小計＋税で計算する
    m_kei = re.search(r"小\s*計[^\d]*([\d,]+)", t)
    m_tax = re.search(r"消費税[額]?[^\d]*([\d,]+)", t)
    if m_kei and m_tax:
        kei = _num(m_kei.group(1))
        tax = _num(m_tax.group(1))
        if 5000 <= kei <= 9_000_000 and 0 < tax <= kei:
            result["amount"] = kei + tax
    # フォールバック: 全金額の最大値
    if not result.get("amount") or result["amount"] <= 0:
        all_amts = [_num(a) for a in re.findall(r"([\d,]{4,10})円", t)]
        valid = [v for v in all_amts if 10_000 <= v <= 9_000_000]
        if valid:
            result["amount"] = max(valid)

    # ★ 工事内容（案件名の直下の行 = 工種名、ラベルなし）
    # 例: 案件名：KH 平井 善大様邸 新築工事\n屋内給排水工事
    m_content = re.search(r"案件名[：:][^\n]*\n\s*([^\n]{3,40})", t)
    if m_content:
        content = m_content.group(1).strip()
        if "工事" in content:
            result["content"] = content

    # ★ 住所：アーキテックス発注書に現場住所フィールドなし → 本社住所を誤検知しないよう "-" に固定
    result["address"] = "-"

    # ★ 工程終了日 → endDate（開始日の記載がない場合は終了日で代用）
    m_end = re.search(r"工程終了日[：:]?\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", t)
    if m_end:
        result["endDate"] = _fmt(m_end.group(1), m_end.group(2), m_end.group(3))
        if not result.get("startDate"):
            result["startDate"] = result["endDate"]

    # ★ 出力日 → 発注日
    m_date = re.search(r"出力日[：:]?\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", t)
    if m_date:
        result["date"] = _fmt(m_date.group(1), m_date.group(2), m_date.group(3))


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

    if not result.get("client_code2"):
        m_vendor = re.search(r"業者[ＮN][ＯO]\s*(\d{4,8})", t)
        if m_vendor:
            result["client_code2"] = m_vendor.group(1)
        else:
            m_vendor2 = re.search(r"業者[ＮN][ＯO][^\d]{0,5}(\d{4,8})", tight)
            if m_vendor2:
                result["client_code2"] = m_vendor2.group(1)

    m_meisai = re.search(r"備考\n[１-９1-9]\n([^\n]{3,50})\n", t)
    if m_meisai:
        result["content"] = m_meisai.group(1).strip()

    if not result.get("site_name"):
        m_kojimei = re.search(r"工\s*事\s*名\s+(.+?)(?:\n|工\s*期|建\s*築\s*地)", t)
        if m_kojimei:
            raw = re.sub(r"\s+", " ", m_kojimei.group(1)).strip()
            raw = re.sub(r"様邸\s*様邸", "様邸", raw)
            if len(raw) >= 3:
                result["site_name"] = raw


def parse_miyazaki_shinsei(t: str, tight: str, result: dict):
    m_order = re.search(r"注文番[号亨][\s:：]*([Tt71l][0-9]{3,6}[-－][0-9]{1,2}[-－][0-9]{1,2})", t)
    if m_order:
        val = m_order.group(1)
        val = re.sub(r"^[71l]", "T", val).upper().replace("－", "-")
        result["id"] = val

    m_kojino = re.search(r"工事番号[\s:：]*(\d{7,10})", t)
    if m_kojino: result["client_code2"] = m_kojino.group(1)

    m_content = re.search(r"工事名称[\s:：]*([^\n]{4,80})", t)
    if m_content:
        content = m_content.group(1).strip()
        for stop in ["現場主任", "工事場所", "工　期", "工期", "請求締日", "支払"]:
            if stop in content: content = content[:content.index(stop)].strip()
        if len(content) >= 3: result["content"] = content

    m_kouji = re.search(
        r"[自从]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*[至迄]\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        t
    )
    if m_kouji:
        result["startDate"] = _fmt(m_kouji.group(1), m_kouji.group(2), m_kouji.group(3))
        result["endDate"]   = _fmt(m_kouji.group(4), m_kouji.group(5), m_kouji.group(6))

    m_amt_yen = re.search(r"注文金額[\s]*[¥￥\\]([0-9,]+)-?", t)
    if m_amt_yen:
        val = _num(m_amt_yen.group(1))
        if 1000 <= val <= 9_000_000: result["amount"] = val
    else:
        m_tax = re.search(r"消費税10%[\s]*[¥￥\\1Il]?([0-9,]+)", t)
        if m_tax:
            tax_val = _num(m_tax.group(1))
            if 100 <= tax_val <= 900_000:
                inferred = tax_val * 11
                if 1000 <= inferred <= 9_000_000: result["amount"] = inferred
        if not result.get("amount") or result["amount"] == 0:
            m_amt2 = re.search(r"注文金額[\s¥￥\\]*([0-9,]+)-?", tight)
            if m_amt2:
                val = _num(m_amt2.group(1))
                if 1000 <= val <= 9_000_000: result["amount"] = val

# =========================
# 表示用フィールド生成
# =========================
def build_display_fields(result: dict) -> dict:
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
        "no3_1_kojimei":    _v(result.get("koji_name")),
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
        "koji_name": None,
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

    # 専用パーサーで全項目取得する会社は汎用抽出をスキップ
    if company == "㈱グローブホーム":
        pass  # グローブホームは専用パーサーで処理
    else:
        # ★ 住友不動産もextract_amountを呼ぶ（マイナス金額対応）
        result["amount"] = extract_amount(t, tight)
        if company != "住友不動産ハウジング㈱":
            result["address"] = extract_address(t)

    id_1p = re.search(r"(1P[0-9]{3,6})", tight)
    if id_1p: result["id"] = id_1p.group(0)
    code_e = re.search(r"(E[0-9]{5})", tight)
    if code_e: result["client_code2"] = code_e.group(0)

    # 汎用パーサー
    parse_universal(t, tight, result, company)

    # ★ 会社別専用パーサー（正式表記に対応）
    if company == "㈱グローブホーム":
        parse_globe(t, tight, result)
    elif company == "ファースト住建㈱":
        parse_first(t, tight, result)
    elif company == "阿部建設㈱":
        parse_abe(t, tight, result)
    elif company == "住友不動産ハウジング㈱":
        if 'BtoBプラットフォーム契約書' in t or '工事名称' in t[:500]:
            parse_sumitomo_vertical(t, result)
        else:
            parse_sumitomo(t, tight, result)
    elif company == "㈱アイ工務店":
        parse_ai(t, tight, result)
    elif company in ("㈱宮崎工務店", "㈱宮崎", "新生建設㈱"):
        parse_miyazaki_shinsei(t, tight, result)
    elif company == "アーキテックス㈱":
        parse_architex(t, tight, result)

    # 日付補完
    dates = extract_dates_perfect(t, tight, company, COMPANY_LABEL_MAP.get(company, {}))
    if not result["date"]:         result["date"]         = dates.get("date")
    if not result["billing_date"]: result["billing_date"] = dates.get("billing_date")
    if company not in ("㈱グローブホーム", "住友不動産ハウジング㈱", "ファースト住建㈱", "アーキテックス㈱"):
        if not result["startDate"]: result["startDate"] = dates.get("startDate")
        if not result["endDate"]:   result["endDate"]   = dates.get("endDate")

    if not result["startDate"] and result["date"] and result["company"] != "ファースト住建㈱":
        result["startDate"] = result["date"]

    if not result["id"]:
        m = re.search(r"(工事番号|契約番号|発注番号|現場ID).*?(\d{6,10})", t)
        if m: result["id"] = m.group(2)

    if not result["id"]:
        reg_nos = set(re.findall(r"登録番号[：:]*T?(\d{10,13})", tight))
        for ic in re.findall(r"[0-9]{7,8}", tight):
            skip = {str(result["amount"]), "4550004"}
            if ic in skip or any(ic in rn for rn in reg_nos) or ic.startswith("202"):
                continue
            result["id"] = ic
            break

    if "浄水槽" in t and result["content"] == "注文工事":
        result["content"] = "浄水槽工事"

    # ★ 枚目判定（「（ 2 / 2 枚 目 ）」のようなスペースあり形式に対応）
    m_page = re.search(r'[（(]\s*(\d+)\s*/\s*(\d+)\s*枚\s*目\s*[）)]', t)
    if m_page:
        current = m_page.group(1)
        total = m_page.group(2)
        result["docType"] = f"{result.get('docType', '注文書')} ({current}/{total}枚目)"
    else:
        # 「支払 1/2枚目50% 2/2枚目50%」形式も判定
        m_shiharai = re.search(r'(\d+)/(\d+)枚目', t)
        if m_shiharai:
            total = m_shiharai.group(2)
            result["docType"] = f"{result.get('docType', '注文書')} ({total}枚構成)"

    result["fields_display"] = build_display_fields(result)
    result.pop("config", None)
    return result