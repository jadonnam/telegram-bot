# final_ops_patch.py
# 최종 운영 패치
# - 브리핑 중복 전송 차단: 08:00 / 15:30 / 21:30 / 05:00 정각 1회
# - 뉴스 도배 차단: 하루 3개, 스캔당 1개, 일반 60분, 속보 30분
# - 유사 뉴스 반복 방지 강화
# - 뉴스/가격 알림 문구를 수익형 시그널 구조로 변경
# - 브리핑 가독성 개선
# - 80K 근처 돌파/이탈 알림 도배 방지

from pathlib import Path
from datetime import datetime
import re

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. main.py 있는 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_ops_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

# =========================
# 상수 강화
# =========================
text = re.sub(r"NEWS_DAILY_LIMIT\s*=\s*\d+", "NEWS_DAILY_LIMIT = 3", text)
text = re.sub(r"NEWS_MIN_INTERVAL\s*=\s*timedelta\(minutes=\d+\)", "NEWS_MIN_INTERVAL = timedelta(minutes=60)", text)
text = re.sub(r"NEWS_URGENT_MIN_INTERVAL\s*=\s*timedelta\(minutes=\d+\)", "NEWS_URGENT_MIN_INTERVAL = timedelta(minutes=30)", text)
text = re.sub(r"NEWS_NORMAL_SCORE\s*=\s*\d+", "NEWS_NORMAL_SCORE = 8", text)
text = re.sub(r"NEWS_URGENT_SCORE\s*=\s*\d+", "NEWS_URGENT_SCORE = 9", text)
text = re.sub(r"PRICE_MILESTONE_COOLDOWN\s*=\s*timedelta\(hours=\d+\)", "PRICE_MILESTONE_COOLDOWN = timedelta(hours=12)", text)

if "PRICE_MILESTONE_BUFFER_PCT" not in text:
    m = re.search(r"BTC_PRICE_MILESTONES\s*=.*\n", text)
    if m:
        text = text[:m.end()] + "\n# 80K 같은 심리 가격대에서 위아래로 흔들릴 때 알림 도배 방지용 완충폭\nPRICE_MILESTONE_BUFFER_PCT = 0.15\n" + text[m.end():]

if "NEWS_TITLE_SIMILARITY_BLOCK_HOURS" not in text:
    m = re.search(r"NEWS_URGENT_SCORE\s*=.*\n", text)
    if m:
        text = text[:m.end()] + "\n# 유사 뉴스 반복 방지\nNEWS_TITLE_SIMILARITY_BLOCK_HOURS = 24\nNEWS_RECENT_TITLE_LIMIT = 200\n" + text[m.end():]

# =========================
# State 강화
# =========================
if "self.news_recent_titles" not in text:
    text = text.replace(
        "self.last_news_sent_at: Optional[datetime] = None",
        "self.last_news_sent_at: Optional[datetime] = None\n        self.news_recent_titles: Deque[Tuple[datetime, str]] = deque(maxlen=NEWS_RECENT_TITLE_LIMIT)"
    )

# =========================
# 스타일 헬퍼 추가
# =========================
helper_anchor = "def fmt_pct(value: float) -> str:"
helper_insert = '''
def move_icon(value: float) -> str:
    if value > 0.15:
        return "🟢"
    if value < -0.15:
        return "🔴"
    return "⚪"


def fmt_market_value(name: str, snap, digits: int = 2) -> str:
    if not snap:
        return ""
    price, pct = snap
    return f"{move_icon(pct)} {name}: {price:,.{digits}f} ({fmt_pct(pct)})"


def fmt_btc_line(price: float, pct: float) -> str:
    return f"{move_icon(pct)} BTC: {price:,.0f} USDT ({fmt_pct(pct)})"


def section_bar(title: str) -> str:
    return f"━━━━━━━━━━━━━━\\n{title}\\n━━━━━━━━━━━━━━"


def sentiment_label(*pcts: float) -> str:
    vals = [p for p in pcts if p is not None]
    if not vals:
        return "⚪ 데이터 확인 중"
    avg = sum(vals) / len(vals)
    if avg >= 0.35:
        return "🟢 위험선호 우위"
    if avg <= -0.35:
        return "🔴 리스크오프 우위"
    return "🟡 혼조"
'''
if "def move_icon(value: float)" not in text:
    text = text.replace(helper_anchor, helper_insert + "\n" + helper_anchor)

# =========================
# 뉴스 dedup 함수 추가
# =========================
dedup_helpers = '''
def normalize_news_url(link: str) -> str:
    link = (link or "").strip()
    link = re.sub(r"[?&](utm_[^=]+|utm_source|utm_medium|utm_campaign|utm_term|utm_content)=[^&]+", "", link)
    link = re.sub(r"[?&]output=amp", "", link)
    link = link.split("#")[0]
    return link.rstrip("/")


def normalize_title_for_dedup(title: str) -> str:
    title = clean_text(title, limit=300).lower()
    title = re.sub(r"[^a-z0-9가-힣\\s]", " ", title)
    stop = {
        "the", "a", "an", "to", "for", "of", "and", "in", "on", "with", "as",
        "is", "are", "was", "were", "report", "says", "said", "amid",
        "news", "crypto", "cryptocurrency"
    }
    words = [w for w in title.split() if len(w) >= 2 and w not in stop]
    return " ".join(words[:18])


def title_similarity(a: str, b: str) -> float:
    sa = set(normalize_title_for_dedup(a).split())
    sb = set(normalize_title_for_dedup(b).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def is_duplicate_or_similar_news(state: State, title: str, link: str, now: datetime) -> bool:
    normalized_url_hash = hashlib.sha256(normalize_news_url(link).encode("utf-8")).hexdigest()
    if state.has_news(normalized_url_hash):
        return True

    cutoff = now - timedelta(hours=NEWS_TITLE_SIMILARITY_BLOCK_HOURS)
    while state.news_recent_titles and state.news_recent_titles[0][0] < cutoff:
        state.news_recent_titles.popleft()

    for _, old_title in state.news_recent_titles:
        if title_similarity(title, old_title) >= 0.55:
            return True

    return False


def mark_news_sent_strict(state: State, title: str, link: str, published: str, now: datetime) -> None:
    state.mark_news(news_id(title, link, published))
    state.mark_news(hashlib.sha256(normalize_news_url(link).encode("utf-8")).hexdigest())
    state.news_recent_titles.append((now, title))
'''
if "def normalize_news_url" not in text:
    text = text.replace("# ============================================================\n# MONITORS", dedup_helpers + "\n\n# ============================================================\n# MONITORS")

# =========================
# 가격대 알림 교체
# =========================
new_milestone = '''
async def maybe_send_price_milestone_alert(bot: Bot, state: State, symbol: str, prev_price: Optional[float], price: float, now: datetime) -> None:
    if symbol != "BTCUSDT" or prev_price is None or prev_price <= 0:
        state.last_market_price[symbol] = price
        return

    for level in BTC_PRICE_MILESTONES:
        buffer = level * (PRICE_MILESTONE_BUFFER_PCT / 100)

        direction = None
        if prev_price < level and price >= level + buffer:
            direction = "breakout"
        elif prev_price > level and price <= level - buffer:
            direction = "breakdown"

        if not direction:
            continue

        key = f"milestone:{symbol}:{level}:{direction}"
        if is_price_milestone_on_cooldown(state, key, now):
            continue

        level_text = format_price_level(level)

        if direction == "breakout":
            msg = (
                f"🚨 [BTC 핵심 구간 돌파]\\n"
                f"기준가: {level_text} 달러\\n"
                f"현재가: {price:,.0f} USDT\\n\\n"
                f"관찰: 심리 저항선을 위로 넘긴 구간.\\n"
                f"리스크: 돌파 직후 위꼬리/휩쏘 가능성.\\n"
                f"대응: {level_text} 위에서 15~30분 버티면 추세 유지로 판단."
            )
        else:
            msg = (
                f"⚠️ [BTC 핵심 구간 이탈]\\n"
                f"기준가: {level_text} 달러\\n"
                f"현재가: {price:,.0f} USDT\\n\\n"
                f"관찰: 심리 지지선이 깨진 구간.\\n"
                f"리스크: 단기 청산 물량과 변동성 확대.\\n"
                f"대응: {level_text} 빠른 회복 실패 시 추가 눌림 주의."
            )

        await safe_send(bot, msg)
        touch_price_milestone(state, key, now)

    state.last_market_price[symbol] = price
'''
text = re.sub(
    r"async def maybe_send_price_milestone_alert\(bot: Bot, state: State, symbol: str, prev_price: Optional\[float\], price: float, now: datetime\) -> None:\n.*?\n\ndef futures_signal_comment",
    new_milestone + "\n\ndef futures_signal_comment",
    text,
    flags=re.S,
)

# =========================
# 뉴스 점수/필터 교체
# =========================
new_news_score = '''
def news_importance_score(title: str, summary: str) -> int:
    text = f"{title}\\n{summary}".lower()
    score = 0

    tier_critical = (
        "spot bitcoin etf", "bitcoin etf", "btc etf", "etf inflow", "etf outflow",
        "sec approves", "sec rejects", "fomc", "cpi", "rate cut", "interest rate",
        "liquidation", "sell-off", "crash", "hacked", "exploit",
        "iran", "israel", "hormuz", "missile", "strike", "warship", "tanker", "oil",
        "비트코인 etf", "승인", "거절", "금리", "연준", "청산", "급락",
        "해킹", "이란", "이스라엘", "호르무즈", "미사일", "피격", "유가",
    )
    tier_good = (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "fed", "inflation", "regulation", "sec", "dollar", "market",
        "비트코인", "이더리움", "솔라나", "규제", "달러", "시장",
    )

    score += sum(4 for k in tier_critical if k in text)
    score += sum(1 for k in tier_good if k in text)
    score -= sum(5 for k in NEWS_BLOCK_KEYWORDS if k in text)

    low_value = (
        "lawsuit", "sues", "sue", "custody fraud", "parent company", "ceo",
        "withdrawal lock", "aml proposal", "reporting rule",
        "소송", "고소", "보관 사기", "대표", "출금 잠금", "aml", "보고 규칙",
    )
    if any(k in text for k in low_value):
        score -= 8

    if any(k in text for k in ("exchange", "binance", "coinbase", "kraken", "거래소", "바이낸스", "코인베이스")):
        if not any(k in text for k in ("hack", "exploit", "freeze", "frozen", "liquidation", "sec", "regulation", "해킹", "동결", "청산", "규제", "승인", "거절")):
            score -= 7

    return score
'''
text = re.sub(
    r"def news_importance_score\(title: str, summary: str\) -> int:\n.*?\n\ndef is_forced_breaking_news",
    new_news_score + "\n\ndef is_forced_breaking_news",
    text,
    flags=re.S,
)

new_high_quality = '''
def is_high_quality_news(title: str, summary: str) -> bool:
    text = f"{title}\\n{summary}".lower()

    if is_forced_breaking_news(title, summary):
        return True

    if any(k in text for k in NEWS_BLOCK_KEYWORDS):
        return False

    if not any(k in text for k in NEWS_KEYWORDS):
        return False

    low_value = (
        "lawsuit", "sues", "sue", "custody fraud", "parent company", "ceo",
        "withdrawal lock", "aml proposal", "reporting rule",
        "소송", "고소", "보관 사기", "대표", "출금 잠금", "aml", "보고 규칙",
    )
    if any(k in text for k in low_value):
        if not any(k in text for k in ("bitcoin etf", "sec approves", "sec rejects", "hack", "exploit", "liquidation", "해킹", "청산", "승인", "거절")):
            return False

    return news_importance_score(title, summary) >= NEWS_NORMAL_SCORE
'''
text = re.sub(
    r"def is_high_quality_news\(title: str, summary: str\) -> bool:\n.*?\n\ndef is_urgent_news",
    new_high_quality + "\n\ndef is_urgent_news",
    text,
    flags=re.S,
)

new_urgent = '''
def is_urgent_news(title: str, summary: str) -> bool:
    if is_forced_breaking_news(title, summary):
        return True

    text = f"{title}\\n{summary}".lower()

    true_urgent = (
        "sec approves", "sec rejects", "fomc", "cpi",
        "hacked", "exploit", "freeze", "frozen", "seized",
        "liquidation", "sell-off", "crash",
        "missile", "strike", "warship", "hormuz", "tanker", "iran", "israel",
        "승인", "거절", "해킹", "압수", "동결", "청산", "급락",
        "미사일", "피격", "군함", "호르무즈", "이란", "이스라엘", "유조선",
    )

    if not any(k in text for k in true_urgent):
        return False

    return news_importance_score(title, summary) >= NEWS_URGENT_SCORE
'''
text = re.sub(
    r"def is_urgent_news\(title: str, summary: str\) -> bool:\n.*?\n\ndef news_importance_line",
    new_urgent + "\n\ndef news_importance_line",
    text,
    flags=re.S,
)

# =========================
# 뉴스 메시지 교체
# =========================
new_build_msg = '''
async def build_korean_news_message(session: aiohttp.ClientSession, title: str, summary: str, link: str) -> Tuple[str, str]:
    title_ko = await translate_to_korean(session, title)
    source = source_name_from_link(link)
    score = normalized_news_score(title, summary)
    line = news_importance_line(title, summary)

    btc_line = ""
    btc = await get_market_ticker(session, "BTCUSDT")
    if btc:
        try:
            btc_price = float(btc["lastPrice"])
            btc_pct = float(btc["priceChangePercent"])
            flow = "상승 흐름" if btc_pct > 0.15 else "하락 압력" if btc_pct < -0.15 else "보합권"
            btc_line = f"\\n\\n📊 현재 BTC: {btc_price:,.0f} USDT ({fmt_pct(btc_pct)}, {flow})"
        except Exception:
            btc_line = ""

    if is_urgent_news(title, summary):
        tag = f"🚨 [속보 · 중요도 {score}/10]"
    elif score >= 8:
        tag = f"🔥 [핵심뉴스 · 중요도 {score}/10]"
    else:
        tag = f"📰 [뉴스 · 중요도 {score}/10]"

    telegram_msg = (
        f"{tag}\\n"
        f"{title_ko}\\n\\n"
        f"관찰: {line.replace('시장 해석: ', '')}\\n"
        f"리스크: 뉴스 직후 과한 추격은 변동성에 휘말릴 수 있음.\\n"
        f"대응: BTC 가격 반응과 거래량 동반 여부 확인."
        f"{btc_line}\\n\\n"
        f"출처: {source}\\n"
        f"{link}"
    )

    return telegram_msg, build_threads_text(title_ko, title, summary)
'''
text = re.sub(
    r"async def build_korean_news_message\(session: aiohttp.ClientSession, title: str, summary: str, link: str\) -> Tuple\[str, str\]:\n.*?\n\nasync def fetch_feed_entries",
    new_build_msg + "\n\nasync def fetch_feed_entries",
    text,
    flags=re.S,
)

# =========================
# 뉴스 모니터 교체
# =========================
new_news_monitor = '''
async def news_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            sent_this_scan = 0
            try:
                kst = now_kst()
                if state.news_daily_date != kst.date():
                    state.news_daily_date = kst.date()
                    state.news_daily_count = 0

                if state.news_daily_count < NEWS_DAILY_LIMIT:
                    for feed in RSS_FEEDS:
                        if state.news_daily_count >= NEWS_DAILY_LIMIT or sent_this_scan >= NEWS_MAX_PER_SCAN:
                            break

                        entries = await fetch_feed_entries(session, feed)
                        candidates = []

                        for e in entries[:20]:
                            title = (e.get("title") or "").strip()
                            link = (e.get("link") or "").strip()
                            summary = (e.get("summary") or "").strip()
                            published = (e.get("published") or "").strip()
                            if not title or not link:
                                continue

                            nid = news_id(title, link, published)
                            now = utc_now()

                            if state.has_news(nid) or is_duplicate_or_similar_news(state, title, link, now):
                                logging.info("뉴스 중복/유사 스킵 title=%s", clean_text(title, 80))
                                continue

                            if not is_high_quality_news(title, summary):
                                logging.info("뉴스 스킵 score=%s title=%s", news_importance_score(title, summary), clean_text(title, 80))
                                continue

                            score = normalized_news_score(title, summary)
                            urgent = is_urgent_news(title, summary)
                            candidates.append((urgent, score, title, summary, link, published, nid))

                        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

                        for urgent, score, title, summary, link, published, nid in candidates:
                            if state.news_daily_count >= NEWS_DAILY_LIMIT or sent_this_scan >= NEWS_MAX_PER_SCAN:
                                break

                            now = utc_now()
                            min_interval = NEWS_URGENT_MIN_INTERVAL if urgent else NEWS_MIN_INTERVAL
                            if state.last_news_sent_at and (now - state.last_news_sent_at) < min_interval:
                                logging.info("뉴스 간격 제한 스킵 urgent=%s title=%s", urgent, clean_text(title, 80))
                                continue

                            msg, threads_text = await build_korean_news_message(session, title, summary, link)
                            await safe_send(bot, msg, disable_preview=False)
                            await publish_to_threads(session, threads_text)

                            logging.info("뉴스 전송 완료 score=%s urgent=%s title=%s", score, urgent, clean_text(title, 80))
                            mark_news_sent_strict(state, title, link, published, now)
                            state.last_news_sent_at = now
                            state.news_daily_count += 1
                            sent_this_scan += 1
                            break

            except Exception:
                logging.exception("news_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, NEWS_CHECK_SECONDS - int(elapsed)))
'''
text = re.sub(
    r"async def news_monitor\(bot: Bot, state: State\) -> None:\n.*?\n\n# ============================================================\n# BRIEFINGS",
    new_news_monitor + "\n\n# ============================================================\n# BRIEFINGS",
    text,
    flags=re.S,
)

# =========================
# 브리핑 표시 함수 교체
# =========================
new_append = '''
def append_if_value(lines: list[str], name: str, snap: Optional[Tuple[float, float]], digits: int = 2) -> None:
    if snap:
        line = fmt_market_value(name, snap, digits)
        if line:
            lines.append(line)
'''
text = re.sub(
    r"def append_if_value\(lines: list\[str\], name: str, snap: Optional\[Tuple\[float, float\]\], digits: int = 2\) -> None:\n.*?\n\ndef session_data_note",
    new_append + "\n\ndef session_data_note",
    text,
    flags=re.S,
)

new_dir = '''
def market_direction_label(*pcts: float) -> str:
    return sentiment_label(*pcts)
'''
text = re.sub(
    r"def market_direction_label\(\*pcts: float\) -> str:\n.*?\n\ndef append_if_value",
    new_dir + "\n\ndef append_if_value",
    text,
    flags=re.S,
)

# =========================
# 브리핑 스케줄러 교체
# =========================
new_market_session = '''
async def market_session_scheduler(bot: Bot, state: State) -> None:
    def is_exact_time(now: datetime, hour: int, minute: int) -> bool:
        return now.hour == hour and now.minute == minute

    def pct_or_zero(snap: Optional[Tuple[float, float]]) -> float:
        return float(snap[1]) if snap else 0.0

    async def btc_line(session: aiohttp.ClientSession) -> Tuple[str, float]:
        btc = await get_market_ticker(session, "BTCUSDT")
        if not btc:
            return "⚪ BTC: 데이터 지연", 0.0
        price = float(btc["lastPrice"])
        pct = float(btc["priceChangePercent"])
        return fmt_btc_line(price, pct), pct

    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()

                if is_korean_market_weekday(now) and is_exact_time(now, 8, 0):
                    key = "kr_pre_0800"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        btc_text, _ = await btc_line(session)

                        lines = [section_bar("🇰🇷 한국장 1시간 전")]
                        append_if_value(lines, "코스피", kospi)
                        append_if_value(lines, "코스닥", kosdaq)
                        if usd_krw:
                            lines.append(f"💵 달러/원: {usd_krw:,.2f}원")
                        append_if_value(lines, "S&P500 선물", sp_fut)
                        append_if_value(lines, "나스닥 선물", nq_fut)
                        lines.append(btc_text)

                        mood = market_direction_label(pct_or_zero(sp_fut), pct_or_zero(nq_fut), pct_or_zero(kospi), pct_or_zero(kosdaq))
                        conclusion = kr_open_conclusion(pct_or_zero(kospi), pct_or_zero(kosdaq), pct_or_zero(sp_fut), pct_or_zero(nq_fut), usd_krw or 0)
                        msg = "\\n".join(lines) + session_data_note(lines) + f"\\n\\n🧭 시장 분위기: {mood}\\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

                if is_korean_market_weekday(now) and is_exact_time(now, 15, 30):
                    key = "kr_close_1530"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        btc_text, _ = await btc_line(session)

                        kp, kq = pct_or_zero(kospi), pct_or_zero(kosdaq)
                        if kospi and kosdaq:
                            if kp >= 0 and kq >= 0:
                                feature = "🟢 지수 동반 강세"
                            elif kp >= 0 > kq:
                                feature = "🟡 대형주 상대강세, 중소형주 약세"
                            elif kp < 0 <= kq:
                                feature = "🟡 종목장 성격 강화"
                            else:
                                feature = "🔴 지수 전반 약세"
                        else:
                            feature = "⚪ 데이터 지연. 환율·미국 선물 흐름 확인 필요"

                        lines = [section_bar("🔔 한국장 마감 정리")]
                        append_if_value(lines, "코스피", kospi)
                        append_if_value(lines, "코스닥", kosdaq)
                        if usd_krw:
                            lines.append(f"💵 달러/원: {usd_krw:,.0f}원")
                        lines.append(btc_text)

                        conclusion = kr_close_conclusion(kp, kq, usd_krw or 0)
                        msg = "\\n".join(lines) + session_data_note(lines) + f"\\n\\n📍 오늘 특징: {feature}\\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

                if is_us_market_premarket_day(now) and is_exact_time(now, 21, 30):
                    key = "us_pre_2130"
                    if state.market_session_sent_dates.get(key) != now.date():
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)

                        lines = [section_bar("🇺🇸 미국장 1시간 전")]
                        append_if_value(lines, "S&P500 선물", sp_fut)
                        append_if_value(lines, "나스닥 선물", nq_fut)
                        append_if_value(lines, "달러인덱스", dxy)
                        append_if_value(lines, "10년물 금리", tnx)
                        lines.append(btc_text)

                        mood = market_direction_label(pct_or_zero(sp_fut), pct_or_zero(nq_fut))
                        conclusion = us_open_conclusion(pct_or_zero(sp_fut), pct_or_zero(nq_fut), pct_or_zero(dxy), pct_or_zero(tnx))
                        msg = "\\n".join(lines) + session_data_note(lines) + f"\\n\\n🧭 시장 분위기: {mood}\\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

                if is_us_market_close_day(now) and is_exact_time(now, 5, 0):
                    key = "us_close_0500"
                    if state.market_session_sent_dates.get(key) != now.date():
                        spx = await get_yahoo_snapshot(session, "%5EGSPC")
                        ixic = await get_yahoo_snapshot(session, "%5EIXIC")
                        dji = await get_yahoo_snapshot(session, "%5EDJI")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)

                        lines = [section_bar("🌙 미국장 마감 정리")]
                        append_if_value(lines, "S&P500", spx)
                        append_if_value(lines, "나스닥", ixic)
                        append_if_value(lines, "다우", dji)
                        append_if_value(lines, "달러인덱스", dxy)
                        append_if_value(lines, "10년물 금리", tnx)
                        lines.append(btc_text)

                        mood = market_direction_label(pct_or_zero(spx), pct_or_zero(ixic), pct_or_zero(dji))
                        conclusion = us_close_conclusion(pct_or_zero(spx), pct_or_zero(ixic), pct_or_zero(dji))
                        msg = "\\n".join(lines) + session_data_note(lines) + f"\\n\\n🧭 마감 분위기: {mood}\\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

            except Exception:
                logging.exception("market_session_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_SESSION_CHECK_SECONDS - int(elapsed)))
'''
text = re.sub(
    r"async def market_session_scheduler\(bot: Bot, state: State\) -> None:\n.*?\n\nasync def briefing_scheduler",
    new_market_session + "\n\nasync def briefing_scheduler",
    text,
    flags=re.S,
)

p.write_text(text, encoding="utf-8")

print("최종 운영 패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
