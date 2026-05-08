from pathlib import Path

p = Path("main.py")
s = p.read_text(encoding="utf-8")

# 1) 중복 브리핑 제거: 7:00 daily_digest, 8:00 briefing_scheduler 끄기
s = s.replace("        asyncio.create_task(briefing_scheduler(bot, state)),\n", "")
s = s.replace("        asyncio.create_task(daily_digest_scheduler(bot, state)),\n", "")

# 2) live news 2개씩 전송 방지
s = s.replace("LIVE_NEWS_MAX_PER_SCAN = 2", "LIVE_NEWS_MAX_PER_SCAN = 1")
s = s.replace("LIVE_NEWS_MIN_INTERVAL = timedelta(minutes=7)", "LIVE_NEWS_MIN_INTERVAL = timedelta(minutes=25)")

# 3) HTML 엔티티 정리 강화
s = s.replace("import hashlib\n", "import hashlib\nimport html\n")

old = '''def html_clean(value: str, limit: int = 500) -> str:
    value = value or ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace(" ", " ").replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
    value = re.sub(r"https?://\\S+", "", value)
    value = re.sub(r"\\s+", " ", value).strip()
    return value[:limit].strip()
'''

new = '''def html_clean(value: str, limit: int = 500) -> str:
    value = value or ""
    value = html.unescape(value)
    value = value.replace("\\xa0", " ").replace("&nbsp;", " ")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"https?://\\S+", "", value)
    value = re.sub(r"\\s+", " ", value).strip()
    return value[:limit].strip()
'''

if old in s:
    s = s.replace(old, new)

# 4) 저품질 뉴스 차단 강화
s = s.replace(
'''LIVE_HARD_BLOCK_TERMS = (
    "migrant worker",''',
'''LIVE_HARD_BLOCK_TERMS = (
    "coinmarketcap", "price chart", "market cap", "가격, 차트", "시가총액",
    "swift student challenge", "google for korea", "구글 포 코리아",
    "맛집", "학생", "challenge", "행사", "성과급 논란",
    "migrant worker",'''
)

# 5) 관련 키워드 GOOGL 고정 방지
old_related = '''def related_assets_for_news(title: str, summary: str = "") -> str:
    txt = f"{title} {summary}".lower()
'''

new_related = '''def related_assets_for_news(title: str, summary: str = "") -> str:
    txt = f"{title} {summary}".lower()
    found = []

    if any(k in txt for k in ("nvidia","엔비디아","hbm","반도체","semiconductor","sk하이닉스","삼성전자")):
        found.append("AI · 반도체 · 나스닥")
    if any(k in txt for k in ("oil","wti","brent","유가","원유","호르무즈","해운")):
        found.append("유가 · 달러 · 인플레")
    if any(k in txt for k in ("fed","fomc","cpi","pce","금리","연준","파월","국채")):
        found.append("금리 · 달러 · 나스닥")
    if any(k in txt for k in ("bitcoin","btc","ethereum","eth","solana","sol","비트코인","이더리움","솔라나","청산","etf")):
        found.append("BTC · ETH · SOL")
    if any(k in txt for k in ("kospi","코스피","환율","외국인","국민연금")):
        found.append("KOSPI · 환율 · 외국인")
    if any(k in txt for k in ("tesla","테슬라","apple","애플","meta","메타","amazon","아마존","microsoft","마이크로소프트")):
        found.append("빅테크 · 나스닥")
    if any(k in txt for k in ("ionq","아이온큐","quantum","양자")):
        found.append("양자 · 성장주")

    if found:
        return " / ".join(dict.fromkeys(found[:3]))

'''
if old_related in s:
    s = s.replace(old_related, new_related)

# 6) BTC 80K 회복/이탈 과민반응 완화
s = s.replace(
'''                        side = "above" if price >= level else "below"''',
'''                        buffer = max(80, level * 0.001)
                        if price >= level + buffer:
                            side = "above"
                        elif price <= level - buffer:
                            side = "below"
                        else:
                            continue'''
)

p.write_text(s, encoding="utf-8")
print("PATCH OK")
