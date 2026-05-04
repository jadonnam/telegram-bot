import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Deque, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
import feedparser
from telegram import Bot


CHANNEL_ID = "@jadonnam"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
KST = ZoneInfo("Asia/Seoul")

MARKET_CHECK_SECONDS = 5 * 60
NEWS_CHECK_SECONDS = 15 * 60
FUTURES_FLOW_CHECK_SECONDS = 15 * 60
PREDICTION_CHECK_SECONDS = 60 * 60
FNG_CHECK_SECONDS = 5 * 60
KIMCHI_CHECK_SECONDS = 10 * 60
WHALE_CHECK_SECONDS = 60
BRIEFING_CHECK_SECONDS = 30
REFERRAL_CHECK_SECONDS = 60
MARKET_SESSION_CHECK_SECONDS = 30

PRICE_CHANGE_THRESHOLD = 1.5
VOLUME_SURGE_THRESHOLD = 4.0
WHALE_NOTIONAL_THRESHOLD = 3_000_000
SIGNAL_COOLDOWN = timedelta(minutes=45)
FUTURES_SIGNAL_COOLDOWN = timedelta(minutes=90)

# --- 핵심 가격대 돌파/이탈 알림 ---
# 10만명 방 기준: 단순 변동률보다 80K, 90K 같은 심리적 가격대가 더 중요함.
BTC_PRICE_MILESTONES = (60000, 70000, 75000, 80000, 85000, 90000, 100000)
PRICE_MILESTONE_COOLDOWN = timedelta(hours=18)

NEWS_DAILY_LIMIT = 4
NEWS_MIN_INTERVAL = timedelta(minutes=60)
NEWS_URGENT_SCORE = 9
NEWS_NORMAL_SCORE = 8
NEWS_BLOCK_HOURS = None  # 미국장 시간대도 뉴스 전송: 새벽 1~7시 차단 해제

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    # 지정학 속보 보강: RSS 원문이 느릴 때 Google News RSS로 보완
    "https://news.google.com/rss/search?q=(Iran%20OR%20Hormuz%20OR%20UAE%20OR%20Israel%20OR%20missile%20OR%20warship%20OR%20tanker)%20(US%20Navy%20OR%20oil%20OR%20attack%20OR%20strike)&hl=ko&gl=KR&ceid=KR:ko",
)
# 뉴스는 너무 기술적인 기사보다 가격/정책/거시/규제/거래소 이슈 위주로 보냄
NEWS_KEYWORDS = (
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "etf",
    "fed",
    "fomc",
    "cpi",
    "inflation",
    "interest rate",
    "rate cut",
    "sec",
    "regulation",
    "lawsuit",
    "tariff",
    "trump",
    "dollar",
    "oil",
    "hack",
    "exploit",
    "exchange",
    "binance",
    "coinbase",
    "금리",
    "유가",
    "달러",
    "연준",
    "규제",
    "해킹",
    "거래소",
)

# 일반 구독자에게 덜 중요한 개발자/기술 논쟁성 뉴스는 차단
NEWS_BLOCK_KEYWORDS = (
    "airdrop",
    "fork",
    "ecash",
    "developer",
    "developers",
    "github",
    "testnet",
    "protocol upgrade",
    "whitepaper",
    "podcast",
    "interview",
    "opinion",
    "guide",
    "how to",
    "recap",
    "what happened in crypto today",
    "today in crypto",
    "daily crypto news",
    "market wrap",
    "price prediction",
    "sponsored",
    "press release",
    "newsletter",
    "magazine",
)
POLY_KEYWORDS = ("btc", "eth", "금리", "전쟁", "trump")


class State:
    def __init__(self) -> None:
        self.price_history: Dict[str, Deque[Tuple[datetime, float]]] = defaultdict(deque)
        self.cooldowns: Dict[str, datetime] = {}

        self.news_seen_ids: Deque[str] = deque(maxlen=3000)
        self.news_seen_set = set()
        self.news_daily_date: Optional[date] = None
        self.news_daily_count = 0
        self.last_news_sent_at: Optional[datetime] = None

        self.briefing_sent_dates: Dict[str, date] = {}
        self.referral_sent_dates: Dict[str, date] = {}
        self.market_session_sent_dates: Dict[str, date] = {}

        self.last_fng_zone = "normal"
        self.last_kimchi_zone = "normal"
        self.polymarket_prob_cache: Dict[str, float] = {}
        self.futures_oi_cache: Dict[str, float] = {}
        self.futures_last_signal: Dict[str, datetime] = {}

        # 가격대 돌파/이탈 감지용
        self.last_market_price: Dict[str, float] = {}
        self.price_milestone_cooldowns: Dict[str, datetime] = {}

        self.whale_seen_ids: Deque[int] = deque(maxlen=8000)
        self.whale_seen_set = set()

    def is_on_cooldown(self, signal_key: str, now: datetime) -> bool:
        expires_at = self.cooldowns.get(signal_key)
        return bool(expires_at and expires_at > now)

    def touch_cooldown(self, signal_key: str, now: datetime) -> None:
        self.cooldowns[signal_key] = now + SIGNAL_COOLDOWN

    def has_news(self, news_id: str) -> bool:
        return news_id in self.news_seen_set

    def mark_news(self, news_id: str) -> None:
        if news_id in self.news_seen_set:
            return
        if len(self.news_seen_ids) == self.news_seen_ids.maxlen:
            old = self.news_seen_ids.popleft()
            self.news_seen_set.discard(old)
        self.news_seen_ids.append(news_id)
        self.news_seen_set.add(news_id)

    def has_whale_trade(self, trade_id: int) -> bool:
        return trade_id in self.whale_seen_set

    def mark_whale_trade(self, trade_id: int) -> None:
        if trade_id in self.whale_seen_set:
            return
        if len(self.whale_seen_ids) == self.whale_seen_ids.maxlen:
            old = self.whale_seen_ids.popleft()
            self.whale_seen_set.discard(old)
        self.whale_seen_ids.append(trade_id)
        self.whale_seen_set.add(trade_id)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_kst() -> datetime:
    return datetime.now(KST)


def fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def format_price_level(level: float) -> str:
    if level >= 1000:
        return f"{level / 1000:.0f}K"
    return f"{level:,.0f}"


def is_price_milestone_on_cooldown(state: State, key: str, now: datetime) -> bool:
    expires_at = state.price_milestone_cooldowns.get(key)
    return bool(expires_at and expires_at > now)


def touch_price_milestone(state: State, key: str, now: datetime) -> None:
    state.price_milestone_cooldowns[key] = now + PRICE_MILESTONE_COOLDOWN


async def maybe_send_price_milestone_alert(bot: Bot, state: State, symbol: str, prev_price: Optional[float], price: float, now: datetime) -> None:
    # 현재는 BTC 핵심 구간만 보냄. ETH/SOL까지 켜면 알림 피로가 커짐.
    if symbol != "BTCUSDT" or prev_price is None or prev_price <= 0:
        state.last_market_price[symbol] = price
        return

    for level in BTC_PRICE_MILESTONES:
        direction = None
        if prev_price < level <= price:
            direction = "breakout"
        elif prev_price > level >= price:
            direction = "breakdown"

        if not direction:
            continue

        key = f"milestone:{symbol}:{level}:{direction}"
        if is_price_milestone_on_cooldown(state, key, now):
            continue

        level_text = format_price_level(level)
        if direction == "breakout":
            msg = (
                f"🚨 [핵심 돌파]\n"
                f"BTC {level_text} 달러 돌파\n\n"
                f"심리 저항 구간을 넘은 자리.\n"
                f"신규 매수세가 붙을 수 있는 구간.\n\n"
                f"📌 지금 핵심은 {level_text} 위에서 버티는지 확인."
            )
        else:
            msg = (
                f"⚠️ [핵심 이탈]\n"
                f"BTC {level_text} 달러 이탈\n\n"
                f"심리 지지선이 깨진 자리.\n"
                f"단기 손절/청산 물량이 나올 수 있는 구간.\n\n"
                f"📌 지금 핵심은 {level_text} 회복 여부."
            )

        await safe_send(bot, msg)
        touch_price_milestone(state, key, now)

    state.last_market_price[symbol] = price


def news_id(title: str, link: str, published: str) -> str:
    raw = f"{title}|{link}|{published}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def has_keyword(text: str, keywords: Tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(k in lowered for k in keywords)


def news_importance_score(title: str, summary: str) -> int:
    text = f"{title}\n{summary}".lower()
    score = 0

    # 10만명 채널 기준: 가격/수급/규제/거시/전쟁/해킹/거래소만 강하게 통과
    tier_1 = (
        "spot bitcoin etf", "bitcoin etf", "btc etf", "etf inflow", "etf outflow",
        "fed", "fomc", "cpi", "inflation", "interest rate", "rate cut",
        "sec", "lawsuit", "approval", "rejection", "regulation",
        "hack", "exploit", "binance", "coinbase", "exchange",
        "trump", "tariff", "war", "ukraine", "iran", "israel", "oil", "dollar",
        "liquidation", "sell-off", "crash", "surge",
        "금리", "연준", "유가", "달러", "규제", "해킹", "거래소", "전쟁", "승인", "거절",
    )
    tier_2 = (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "price", "rally", "drop", "market", "volume", "whale",
    )
    low_quality = NEWS_BLOCK_KEYWORDS + (
        "op-ed", "opinion", "analysis: ", "guide", "explainer",
        "what happened in crypto today", "roundup", "daily recap", "prediction", "rumor",
    )

    score += sum(3 for k in tier_1 if k in text)
    score += sum(1 for k in tier_2 if k in text)
    score -= sum(4 for k in low_quality if k in text)
    return score


def normalized_news_score(title: str, summary: str) -> int:
    if is_forced_breaking_news(title, summary):
        return 10
    return max(0, min(10, news_importance_score(title, summary)))


def is_forced_breaking_news(title: str, summary: str) -> bool:
    """시장 영향이 큰 지정학/군사 속보는 일반 뉴스 점수를 우회해 통과."""
    text = f"{title}\n{summary}".lower()
    has_breaking = any(k in text for k in BREAKING_FORCE_TERMS)
    # 군사/전쟁 단어가 있으면 시장 키워드가 없어도 속보로 인정. 단, 너무 잡다한 글은 제외.
    has_market = any(k in text for k in BREAKING_MARKET_CONTEXT_TERMS)
    return has_breaking and (has_market or any(k in text for k in ("iran", "hormuz", "israel", "이란", "호르무즈", "이스라엘")))


def is_high_quality_news(title: str, summary: str) -> bool:
    text = f"{title}\n{summary}".lower()
    # 속보 예외: 군함/미사일/호르무즈/이란 등은 일반 점수보다 우선
    if is_forced_breaking_news(title, summary):
        return True
    if any(k in text for k in NEWS_BLOCK_KEYWORDS):
        return False
    if not any(k in text for k in NEWS_KEYWORDS):
        return False
    return news_importance_score(title, summary) >= NEWS_NORMAL_SCORE


def is_urgent_news(title: str, summary: str) -> bool:
    if is_forced_breaking_news(title, summary):
        return True
    text = f"{title}\n{summary}".lower()
    hard_urgent = (
        "breaking", "urgent", "emergency", "sec approves", "sec rejects",
        "hacked", "exploit", "seized", "freeze", "frozen",
        "war", "missile", "attack", "tariff", "cpi", "fomc",
        "속보", "긴급", "해킹", "승인", "거절", "압수", "전쟁", "공격",
    )
    return any(k in text for k in hard_urgent) and news_importance_score(title, summary) >= NEWS_URGENT_SCORE


def news_importance_line(title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()
    if any(k in text for k in ("etf", "sec", "regulation", "lawsuit", "approval", "rejection", "규제", "승인", "거절")):
        return "시장 해석: ETF·규제 이슈라 비트코인 수급에 직접 영향 줄 수 있음."
    if any(k in text for k in ("fed", "fomc", "cpi", "inflation", "interest rate", "rate cut", "금리", "연준")):
        return "시장 해석: 금리 기대가 흔들리면 코인·주식이 같이 움직일 수 있음."
    if any(k in text for k in ("hack", "exploit", "hacked", "해킹")):
        return "시장 해석: 해킹 이슈는 단기 투자심리를 바로 식힐 수 있음."
    if any(k in text for k in ("exchange", "binance", "coinbase", "거래소")):
        return "시장 해석: 거래소 이슈는 수급과 신뢰도에 바로 연결됨."
    if any(k in text for k in ("trump", "tariff", "dollar", "oil", "war", "ukraine", "iran", "israel", "유가", "달러", "전쟁")):
        return "시장 해석: 거시 이슈라 유가·달러·위험자산 분위기를 같이 흔들 수 있음."
    if any(k in text for k in ("liquidation", "sell-off", "whale", "volume")):
        return "시장 해석: 청산·거래량 이슈라 단기 변동성이 커질 수 있음."
    return "시장 해석: 방향보다 시장 반응까지 같이 확인해야 하는 뉴스."

def market_one_liner(btc_24h_pct: float) -> str:
    if btc_24h_pct >= 2.0:
        return "BTC 저항선 테스트 중. 돌파 여부 주목."
    if btc_24h_pct <= -2.0:
        return "변동성 확대 구간. 지지선 반응 확인 필요."
    return "방향성 탐색 구간. 거래량 동반 여부가 핵심."


def fear_greed_label(value: int) -> str:
    if value <= 24:
        return "극도 공포"
    if value <= 49:
        return "공포"
    if value <= 74:
        return "탐욕"
    return "극도 탐욕"


def fear_greed_zone(value: int) -> str:
    if value <= 25:
        return "extreme_fear"
    if value >= 75:
        return "extreme_greed"
    return "normal"


async def send_message(bot: Bot, text: str, disable_preview: bool = False) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=disable_preview)


async def safe_send(bot: Bot, text: str, disable_preview: bool = False) -> None:
    try:
        await send_message(bot, text, disable_preview=disable_preview)
    except Exception:
        logging.exception("Telegram 전송 실패 (chat_id=%s)", CHANNEL_ID)


REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None):
    try:
        async with session.get(url, params=params, timeout=20, headers=REQUEST_HEADERS) as response:
            if response.status != 200:
                logging.warning("fetch_json 실패 status=%s url=%s", response.status, url)
                return None
            return await response.json()
    except Exception:
        logging.exception("fetch_json 예외 url=%s", url)
        return None


async def fetch_text(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=25, headers=REQUEST_HEADERS) as response:
            if response.status != 200:
                logging.warning("fetch_text 실패 status=%s url=%s", response.status, url)
                return None
            return await response.text()
    except Exception:
        logging.exception("fetch_text 예외 url=%s", url)
        return None


def clean_text(value: str, limit: int = 180) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) > limit:
        return value[:limit].rstrip() + "..."
    return value


async def translate_to_korean(session: aiohttp.ClientSession, text: str) -> str:
    text = clean_text(text, limit=450)
    if not text:
        return ""
    # 이미 한글이 섞여 있으면 그대로 사용
    if re.search(r"[가-힣]", text):
        return text
    try:
        data = await fetch_json(
            session,
            "https://translate.googleapis.com/translate_a/single",
            {"client": "gtx", "sl": "en", "tl": "ko", "dt": "t", "q": text},
        )
        if data and isinstance(data, list) and data[0]:
            translated = "".join(part[0] for part in data[0] if part and part[0])
            return clean_text(translated, limit=220) or text
    except Exception:
        logging.exception("뉴스 제목 번역 실패")
    return text


def source_name_from_link(link: str) -> str:
    lowered = (link or "").lower()
    if "coindesk" in lowered:
        return "CoinDesk"
    if "cointelegraph" in lowered:
        return "Cointelegraph"
    if "news.google" in lowered or "google.com" in lowered:
        return "Google News"
    return "RSS"


def build_threads_text(title_ko: str, title: str, summary: str) -> str:
    """스레드용: 출처/링크 없이 3~5문장 느낌으로 짧게 변환."""
    text = f"{title}\n{summary}".lower()

    if is_forced_breaking_news(title, summary):
        return (
            f"속보성 이슈다.\n\n"
            f"{title_ko}\n\n"
            f"중동 리스크가 다시 커지면 유가, 달러, 비트코인이 같이 흔들릴 수 있다.\n\n"
            f"지금은 가격보다 뉴스 반응을 먼저 봐야 하는 구간."
        )[:500]
    if "etf" in text:
        return (
            f"비트코인 ETF 쪽 돈 흐름이 다시 살아나는 중이다.\n\n"
            f"아직 완전 회복은 아니지만, 기관 자금이 천천히 돌아오는 신호다.\n\n"
            f"이 흐름이 계속 쌓이면 가격도 늦게 반응할 수 있다."
        )[:500]
    if any(k in text for k in ("fed", "fomc", "cpi", "inflation", "금리", "연준")):
        return (
            f"지금 시장은 금리 뉴스에 예민하다.\n\n"
            f"금리 기대가 바뀌면 주식이랑 비트코인이 같이 움직인다.\n\n"
            f"그래서 오늘은 차트보다 발표 이후 반응이 더 중요하다."
        )[:500]
    return (
        f"{title_ko}\n\n"
        f"지금은 뉴스 하나에도 시장이 바로 흔들리는 구간이다.\n\n"
        f"가격이 어디서 버티는지 같이 봐야 한다."
    )[:500]


async def publish_to_threads(session: aiohttp.ClientSession, text: str) -> None:
    if not THREADS_AUTO_POST:
        return
    if not THREADS_USER_ID or not THREADS_ACCESS_TOKEN:
        logging.warning("Threads 자동 업로드 설정 없음: THREADS_USER_ID / THREADS_ACCESS_TOKEN 확인")
        return

    try:
        create_url = f"https://graph.threads.net/v1.0/{THREADS_USER_ID}/threads"
        payload = {"access_token": THREADS_ACCESS_TOKEN, "text": text[:500]}
        if THREADS_IMAGE_URL:
            payload["media_type"] = "IMAGE"
            payload["image_url"] = THREADS_IMAGE_URL
        else:
            payload["media_type"] = "TEXT"

        async with session.post(create_url, data=payload, timeout=30) as response:
            created = await response.json()
            if response.status >= 300:
                logging.error("Threads 컨테이너 생성 실패: %s", created)
                return

        creation_id = created.get("id")
        if not creation_id:
            logging.error("Threads creation_id 없음: %s", created)
            return

        publish_url = f"https://graph.threads.net/v1.0/{THREADS_USER_ID}/threads_publish"
        async with session.post(
            publish_url,
            data={"access_token": THREADS_ACCESS_TOKEN, "creation_id": creation_id},
            timeout=30,
        ) as response:
            published = await response.json()
            if response.status >= 300:
                logging.error("Threads 게시 실패: %s", published)
                return
        logging.info("Threads 자동 업로드 완료: %s", published)
    except Exception:
        logging.exception("Threads 자동 업로드 오류")


async def build_korean_news_message(session: aiohttp.ClientSession, title: str, summary: str, link: str) -> Tuple[str, str]:
    title_ko = await translate_to_korean(session, title)
    source = source_name_from_link(link)
    score = normalized_news_score(title, summary)
    line = news_importance_line(title, summary)

    if is_urgent_news(title, summary):
        tag = f"🚨 [속보 · 중요도 {score}/10]"
    elif score >= 8:
        tag = f"🔥 [핵심뉴스 · 중요도 {score}/10]"
    else:
        tag = f"📰 [뉴스 · 중요도 {score}/10]"

    telegram_msg = (
        f"{tag}\n"
        f"{title_ko}\n\n"
        f"시장 해석: {line}\n\n"
        f"출처: {source}\n"
        f"{link}"
    )
    threads_text = build_threads_text(title_ko, title, summary)
    return telegram_msg, threads_text


async def get_binance_ticker_24h(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    return await fetch_json(session, "https://api.binance.com/api/v3/ticker/24hr", {"symbol": symbol})


async def get_binance_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(session, "https://api.binance.com/api/v3/ticker/price", {"symbol": symbol})
    if not data or "price" not in data:
        return None
    return float(data["price"])


async def get_recent_klines(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    return await fetch_json(
        session,
        "https://api.binance.com/api/v3/klines",
        {"symbol": symbol, "interval": "5m", "limit": 3},
    )



async def get_funding_rate(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        {"symbol": symbol},
    )
    if not data or "lastFundingRate" not in data:
        return None
    return float(data["lastFundingRate"]) * 100


async def get_open_interest(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/openInterest",
        {"symbol": symbol},
    )
    if not data or "openInterest" not in data:
        return None
    return float(data["openInterest"])


async def get_orderbook_imbalance(session: aiohttp.ClientSession, symbol: str) -> Optional[Tuple[float, float, float]]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/depth",
        {"symbol": symbol, "limit": "100"},
    )
    if not data:
        return None
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    bid_notional = sum(float(price) * float(qty) for price, qty in bids[:50])
    ask_notional = sum(float(price) * float(qty) for price, qty in asks[:50])
    if ask_notional <= 0:
        return None
    imbalance = bid_notional / ask_notional
    return imbalance, bid_notional, ask_notional


def futures_signal_comment(symbol: str, funding_pct: float, oi_change_pct: float, imbalance: float) -> Optional[str]:
    coin = symbol.replace("USDT", "")
    if funding_pct >= 0.05 and oi_change_pct >= 3:
        return f"{coin} 롱 쏠림 강함. 올라가도 추격보다 눌림 확인이 더 안전한 구간."
    if funding_pct <= -0.05 and oi_change_pct >= 3:
        return f"{coin} 숏 쏠림 강함. 아래로 밀려도 숏 추격은 조심할 자리."
    if oi_change_pct >= 5:
        return f"{coin} 미결제약정이 빠르게 늘어남. 곧 변동성 커질 수 있는 구간."
    if imbalance >= 1.8:
        return f"{coin} 매수 호가가 두꺼움. 단기 지지 시도는 있지만 가짜 지지도 조심."
    if imbalance <= 0.55:
        return f"{coin} 매도 호가가 두꺼움. 위로 갈수록 물량에 막힐 수 있는 자리."
    return None


async def futures_flow_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                for symbol in SYMBOLS:
                    now = utc_now()
                    funding = await get_funding_rate(session, symbol)
                    oi = await get_open_interest(session, symbol)
                    ob = await get_orderbook_imbalance(session, symbol)
                    if funding is None or oi is None or ob is None:
                        continue

                    old_oi = state.futures_oi_cache.get(symbol)
                    state.futures_oi_cache[symbol] = oi
                    if not old_oi or old_oi <= 0:
                        continue

                    oi_change = ((oi - old_oi) / old_oi) * 100
                    imbalance, bid_notional, ask_notional = ob
                    comment = futures_signal_comment(symbol, funding, oi_change, imbalance)
                    if not comment:
                        continue

                    signal_key = f"futures-flow:{symbol}"
                    last_at = state.futures_last_signal.get(signal_key)
                    if last_at and now - last_at < FUTURES_SIGNAL_COOLDOWN:
                        continue

                    strength = "강함" if abs(oi_change) >= 5 or abs(funding) >= 0.08 else "주의"
                    msg = (
                        f"🧭 [선물 수급 감지 · {strength}]\n"
                        f"{symbol.replace('USDT', '')} 펀딩비 {funding:+.3f}%\n"
                        f"미결제약정 변화 {oi_change:+.1f}%\n"
                        f"오더북 비율 {imbalance:.2f}\n\n"
                        f"{comment}"
                    )
                    await safe_send(bot, msg, disable_preview=True)
                    state.futures_last_signal[signal_key] = now
            except Exception:
                logging.exception("선물 수급 감지 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, FUTURES_FLOW_CHECK_SECONDS - int(elapsed)))


async def get_upbit_btc_krw(session: aiohttp.ClientSession) -> Optional[float]:
    data = await fetch_json(session, "https://api.upbit.com/v1/ticker", {"markets": "KRW-BTC"})
    if not data or not isinstance(data, list):
        return None
    return float(data[0]["trade_price"])


async def get_usd_krw(session: aiohttp.ClientSession) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://api.frankfurter.app/latest",
        {"from": "USD", "to": "KRW"},
    )
    if not data:
        return None
    rates = data.get("rates") or {}
    if "KRW" not in rates:
        return None
    return float(rates["KRW"])


async def get_yahoo_snapshot(session: aiohttp.ClientSession, symbol: str) -> Optional[Tuple[float, float]]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    data = await fetch_json(session, url)
    if not data:
        return None
    chart = data.get("chart") or {}
    results = chart.get("result") or []
    if not results:
        return None
    meta = results[0].get("meta") or {}

    price = meta.get("regularMarketPrice")
    prev = meta.get("previousClose")
    if prev is None:
        prev = meta.get("chartPreviousClose")
    if price is None or prev is None:
        return None

    price_f = float(price)
    prev_f = float(prev)
    if prev_f <= 0:
        return None
    pct = ((price_f - prev_f) / prev_f) * 100
    return price_f, pct


async def get_fear_greed(session: aiohttp.ClientSession) -> Optional[Tuple[int, str]]:
    data = await fetch_json(session, "https://api.alternative.me/fng/")
    if not data:
        return None
    rows = data.get("data") or []
    if not rows:
        return None
    value = int(rows[0].get("value", 0))
    return value, fear_greed_label(value)


async def get_kimchi_premium(session: aiohttp.ClientSession) -> Optional[Tuple[float, float, float]]:
    upbit_krw = await get_upbit_btc_krw(session)
    binance_usdt = await get_binance_price(session, "BTCUSDT")
    usd_krw = await get_usd_krw(session)
    if not upbit_krw or not binance_usdt or not usd_krw or (binance_usdt * usd_krw) <= 0:
        return None
    global_krw = binance_usdt * usd_krw
    premium = ((upbit_krw - global_krw) / global_krw) * 100
    return premium, upbit_krw, binance_usdt


def get_price_15m_ago(history: Deque[Tuple[datetime, float]], now: datetime) -> Optional[float]:
    target = now - timedelta(minutes=15)
    while history and history[0][0] < now - timedelta(hours=3):
        history.popleft()
    candidate = None
    for ts, price in history:
        if ts <= target:
            candidate = price
        else:
            break
    return candidate


async def briefing_scheduler(bot: Bot, state: State) -> None:
    slots = {
        "08": (8, "🌅"),
        "12": (12, "☀️"),
        "21": (21, "🌙"),
    }
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()
                for slot_key, (hour, emoji) in slots.items():
                    if now.hour == hour and now.minute == 0:
                        if state.briefing_sent_dates.get(slot_key) == now.date():
                            continue
                        tickers = {}
                        for symbol in SYMBOLS:
                            t = await get_binance_ticker_24h(session, symbol)
                            if not t:
                                continue
                            tickers[symbol] = (float(t["lastPrice"]), float(t["priceChangePercent"]))
                        if len(tickers) != len(SYMBOLS):
                            continue

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)
                        if not fng or not kimchi:
                            continue
                        fng_value, fng_label = fng
                        premium, _, _ = kimchi

                        btc_price, btc_pct = tickers["BTCUSDT"]
                        eth_price, eth_pct = tickers["ETHUSDT"]
                        sol_price, sol_pct = tickers["SOLUSDT"]
                        line = market_one_liner(btc_pct)
                        msg = (
                            f"{emoji} [시장 브리핑]\n"
                            f"BTC {btc_price:,.0f} USDT ({fmt_pct(btc_pct)})\n"
                            f"ETH {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})\n"
                            f"SOL {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})\n\n"
                            f"😶‍🌫️ 공포탐욕지수: {fng_value} ({fng_label})\n"
                            f"🇰🇷 김치프리미엄: {fmt_pct(premium)}\n\n"
                            f"📊 한 줄 시황: {line}"
                        )
                        await safe_send(bot, msg)
                        state.briefing_sent_dates[slot_key] = now.date()
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, BRIEFING_CHECK_SECONDS - int(elapsed)))


async def market_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                for symbol in SYMBOLS:
                    now = utc_now()
                    ticker = await get_binance_ticker_24h(session, symbol)
                    if not ticker:
                        continue
                    price = float(ticker["lastPrice"])
                    prev_market_price = state.last_market_price.get(symbol)
                    await maybe_send_price_milestone_alert(bot, state, symbol, prev_market_price, price, now)

                    history = state.price_history[symbol]
                    history.append((now, price))

                    old_price = get_price_15m_ago(history, now)
                    if old_price and old_price > 0:
                        pct = ((price - old_price) / old_price) * 100
                        if abs(pct) >= PRICE_CHANGE_THRESHOLD:
                            direction = "상승" if pct > 0 else "하락"
                            signal_key = f"price:{symbol}:{direction}"
                            if not state.is_on_cooldown(signal_key, now):
                                line = (
                                    "추격매수 주의. 눌림목 대기"
                                    if pct > 0
                                    else "지지선 확인 필요. 성급한 매수 금지"
                                )
                                icon = "📈" if pct > 0 else "📉"
                                msg = (
                                    f"{icon} [시장 감지]\n"
                                    f"{symbol.replace('USDT', '')} 15분 {direction} {fmt_pct(pct)}\n"
                                    f"현재가 {price:,.0f} USDT\n"
                                    f"⚠️ {line}"
                                )
                                await safe_send(bot, msg)
                                state.touch_cooldown(signal_key, now)

                    klines = await get_recent_klines(session, symbol)
                    if klines and len(klines) >= 2:
                        prev_vol = float(klines[-2][7])
                        latest_vol = float(klines[-1][7])
                        if prev_vol > 0:
                            ratio = latest_vol / prev_vol
                            if ratio >= VOLUME_SURGE_THRESHOLD:
                                signal_key = f"vol:{symbol}"
                                if not state.is_on_cooldown(signal_key, now):
                                    msg = (
                                        "🔥 [거래량 급증]\n"
                                        f"{symbol.replace('USDT', '')} x{ratio:.2f} 급증\n"
                                        f"5분 거래대금 {latest_vol:,.0f} USDT\n"
                                        "⚠️ 변동성 확대 구간. 포지션 축소 고려"
                                    )
                                    await safe_send(bot, msg)
                                    state.touch_cooldown(signal_key, now)
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_CHECK_SECONDS - int(elapsed)))


def parse_polymarket_prob(market: dict) -> Optional[float]:
    if market.get("probability") is not None:
        return float(market["probability"]) * (100.0 if float(market["probability"]) <= 1 else 1.0)

    prices = market.get("outcomePrices")
    if prices is None:
        return None
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except Exception:
            return None
    if not isinstance(prices, list) or not prices:
        return None
    first = float(prices[0])
    return first * (100.0 if first <= 1 else 1.0)


def market_title(market: dict) -> str:
    return (
        market.get("question")
        or market.get("title")
        or market.get("name")
        or "예측시장"
    )


async def polymarket_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                markets = await fetch_json(
                    session,
                    "https://gamma-api.polymarket.com/markets",
                    {"active": "true", "limit": "20"},
                )
                if not markets or not isinstance(markets, list):
                    raise ValueError("invalid polymarket response")

                for m in markets:
                    title = market_title(m)
                    if not has_keyword(title, POLY_KEYWORDS):
                        continue
                    prob = parse_polymarket_prob(m)
                    if prob is None:
                        continue

                    market_id = str(m.get("id") or m.get("slug") or title)
                    old = state.polymarket_prob_cache.get(market_id)
                    state.polymarket_prob_cache[market_id] = prob
                    if old is None:
                        continue

                    diff = prob - old
                    if abs(diff) >= 10:
                        now = utc_now()
                        signal_key = f"poly:{market_id}"
                        if state.is_on_cooldown(signal_key, now):
                            continue
                        arrow = "▲" if diff > 0 else "▼"
                        msg = (
                            f"🎯 [예측시장]\n"
                            f"{title}\n"
                            f"{old:.0f}% → {prob:.0f}% {arrow}{abs(diff):.0f}%"
                        )
                        await safe_send(bot, msg)
                        state.touch_cooldown(signal_key, now)
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, PREDICTION_CHECK_SECONDS - int(elapsed)))


async def fear_greed_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                fg = await get_fear_greed(session)
                if fg:
                    value, label = fg
                    zone = fear_greed_zone(value)
                    if zone != "normal" and state.last_fng_zone != zone:
                        now = utc_now()
                        signal_key = f"fng:{zone}"
                        if not state.is_on_cooldown(signal_key, now):
                            msg = (
                                "🚨 [공포탐욕지수 경보]\n"
                                f"현재 지수: {value} ({label})\n"
                                "과열/침체 구간 진입. 포지션 리스크 점검 필요."
                            )
                            await safe_send(bot, msg)
                            state.touch_cooldown(signal_key, now)
                    state.last_fng_zone = zone
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, FNG_CHECK_SECONDS - int(elapsed)))


async def kimchi_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                data = await get_kimchi_premium(session)
                if data:
                    premium, upbit_krw, binance_usdt = data
                    zone = "high" if premium >= 3 else "low" if premium <= -3 else "normal"
                    if zone != "normal" and state.last_kimchi_zone != zone:
                        now = utc_now()
                        signal_key = f"kimchi:{zone}"
                        if not state.is_on_cooldown(signal_key, now):
                            msg = (
                                "🇰🇷 [김치프리미엄 경보]\n"
                                f"현재 프리미엄: {fmt_pct(premium)}\n"
                                f"업비트 BTC: {upbit_krw:,.0f}원\n"
                                f"바이낸스 BTC: {binance_usdt:,.0f} USDT"
                            )
                            await safe_send(bot, msg)
                            state.touch_cooldown(signal_key, now)
                    state.last_kimchi_zone = zone
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, KIMCHI_CHECK_SECONDS - int(elapsed)))


async def whale_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                trades = await fetch_json(
                    session,
                    "https://api.binance.com/api/v3/aggTrades",
                    {"symbol": "BTCUSDT", "limit": "200"},
                )
                if trades and isinstance(trades, list):
                    for t in trades:
                        trade_id = int(t.get("a", -1))
                        if trade_id < 0 or state.has_whale_trade(trade_id):
                            continue
                        state.mark_whale_trade(trade_id)

                        price = float(t["p"])
                        qty = float(t["q"])
                        notional = price * qty
                        if notional < WHALE_NOTIONAL_THRESHOLD:
                            continue

                        now = utc_now()
                        signal_key = "whale:btc"
                        if state.is_on_cooldown(signal_key, now):
                            continue

                        side = "매도" if bool(t.get("m")) else "매수"
                        msg = (
                            "🐋 [고래 감지]\n"
                            f"BTC 대형 {side} 체결 포착\n"
                            f"거래규모: {notional:,.0f} USDT\n"
                            "단기 변동성 확대 가능성 주의"
                        )
                        await safe_send(bot, msg)
                        state.touch_cooldown(signal_key, now)
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, WHALE_CHECK_SECONDS - int(elapsed)))


async def fetch_feed_entries(session: aiohttp.ClientSession, url: str) -> list:
    text = await fetch_text(session, url)
    if not text:
        return []
    parsed = feedparser.parse(text)
    return parsed.entries or []


async def news_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                kst = now_kst()
                if state.news_daily_date != kst.date():
                    state.news_daily_date = kst.date()
                    state.news_daily_count = 0

                # 미국장 시간대가 한국 새벽이라 뉴스 차단하지 않음

                if state.news_daily_count < NEWS_DAILY_LIMIT:
                    for feed in RSS_FEEDS:
                        if state.news_daily_count >= NEWS_DAILY_LIMIT:
                            break
                        entries = await fetch_feed_entries(session, feed)
                        for e in entries[:20]:
                            if state.news_daily_count >= NEWS_DAILY_LIMIT:
                                break
                            title = (e.get("title") or "").strip()
                            link = (e.get("link") or "").strip()
                            summary = (e.get("summary") or "").strip()
                            published = (e.get("published") or "").strip()

                            if not title or not link:
                                continue
                            score = news_importance_score(title, summary)
                            if not is_high_quality_news(title, summary):
                                logging.info("뉴스 스킵 score=%s title=%s", score, clean_text(title, 80))
                                continue

                            nid = news_id(title, link, published)
                            if state.has_news(nid):
                                continue

                            now = utc_now()
                            urgent = is_urgent_news(title, summary)
                            if (
                                not urgent
                                and state.last_news_sent_at
                                and (now - state.last_news_sent_at) < NEWS_MIN_INTERVAL
                            ):
                                continue

                            msg, threads_text = await build_korean_news_message(session, title, summary, link)
                            # 링크 미리보기 켬: 텔레그램에서 기사 썸네일/사진이 다시 보이게 함
                            await safe_send(bot, msg, disable_preview=False)
                            await publish_to_threads(session, threads_text)
                            logging.info("뉴스 전송 완료 score=%s urgent=%s title=%s", score, urgent, clean_text(title, 80))
                            state.mark_news(nid)
                            state.last_news_sent_at = now
                            state.news_daily_count += 1
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, NEWS_CHECK_SECONDS - int(elapsed)))


async def referral_scheduler(bot: Bot, state: State) -> None:
    """광고성 정기 메시지 비활성화.

    링크트리/거래소 가입 유도 문구는 정보방 신뢰도를 떨어뜨릴 수 있어
    run_forever에서도 실행하지 않는다. 필요하면 나중에 CONTACT_HANDLE 기반으로
    사람이 직접 문의받는 방식만 별도 추가한다.
    """
    while True:
        await asyncio.sleep(3600)


def is_korean_market_weekday(now: datetime) -> bool:
    """한국장: 월~금 KST."""
    return now.weekday() < 5


def is_us_market_premarket_day(now: datetime) -> bool:
    """미국장 시작 전 알림: 월~금 밤 KST 기준."""
    return now.weekday() < 5


def is_us_market_close_day(now: datetime) -> bool:
    """미국장 마감 알림: 화~토 새벽 KST 기준."""
    return 1 <= now.weekday() <= 5


def in_window(now: datetime, hour: int, minute: int, window_minutes: int = 15) -> bool:
    start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    end = start + timedelta(minutes=window_minutes)
    return start <= now < end


def market_direction_label(*pcts: float) -> str:
    valid = [p for p in pcts if p is not None]
    if not valid:
        return "방향성 확인 필요"
    avg = sum(valid) / len(valid)
    if avg >= 0.35:
        return "위험선호 우위"
    if avg <= -0.35:
        return "리스크오프 우위"
    return "혼조"


def kr_open_conclusion(kospi_pct: float, kosdaq_pct: float, sp_pct: float, nq_pct: float, usd_krw: float) -> str:
    us_flow = market_direction_label(sp_pct, nq_pct)
    if us_flow == "위험선호 우위" and usd_krw < 1450:
        return "미국 선물과 환율이 받쳐주면 반도체·성장주 수급 먼저 확인."
    if us_flow == "리스크오프 우위" or usd_krw >= 1450:
        return "환율·미국 선물이 부담이면 장 초반 추격보다 눌림 확인이 우선."
    if kospi_pct >= 0 and kosdaq_pct < 0:
        return "대형주 쪽이 상대적으로 유리한 흐름. 중소형주는 선별 필요."
    return "오늘은 방향보다 수급 확인이 중요. 장 초반 30분은 무리하지 않는 구간."


def kr_close_conclusion(kospi_pct: float, kosdaq_pct: float, usd_krw: float) -> str:
    if kospi_pct >= 0 and kosdaq_pct >= 0:
        return "국내 위험선호가 살아있는 마감. 내일도 외국인 수급 이어지는지 확인."
    if kospi_pct < 0 and kosdaq_pct < 0:
        return "전반 약세 마감. 내일은 환율과 미국장 반응이 더 중요."
    if kospi_pct >= 0 > kosdaq_pct:
        return "대형주 중심 장세. 중소형주는 아직 힘이 약한 흐름."
    return "종목장 성격이 강한 마감. 지수보다 섹터별 수급 확인 필요."


def us_open_conclusion(sp_pct: float, nq_pct: float, dxy_pct: float, tnx_pct: float) -> str:
    if nq_pct >= 0.4 and tnx_pct <= 0:
        return "나스닥과 금리 조합은 코인에 우호적. BTC 1차 반응 확인."
    if dxy_pct > 0.3 or tnx_pct > 1.0:
        return "달러·금리 상승이면 코인·기술주 둘 다 추격 조심."
    if sp_pct < 0 and nq_pct < 0:
        return "미국장 시작 전 리스크오프. 현금 비중과 손절선 먼저 확인."
    return "미국장 초반 변동성 구간. 첫 30분은 방향 확인이 먼저."


def us_close_conclusion(sp_pct: float, nq_pct: float, dji_pct: float) -> str:
    if sp_pct >= 0 and nq_pct >= 0 and dji_pct >= 0:
        return "미국장 전반 강세. 한국장도 위험선호 이어질 가능성 확인."
    if sp_pct < 0 and nq_pct < 0 and dji_pct < 0:
        return "미국장 전반 약세. 한국장은 방어적 출발 가능성."
    if nq_pct > sp_pct and nq_pct > dji_pct:
        return "나스닥 상대강세. 코인·성장주 쪽 반응 체크."
    return "혼조 마감. 한국장은 환율과 선물 흐름이 방향을 정할 가능성."


def append_if_value(lines: list[str], name: str, snap: Optional[Tuple[float, float]], digits: int = 2) -> None:
    if snap:
        price, pct = snap
        lines.append(f"{name}: {price:,.{digits}f} ({fmt_pct(pct)})")


def session_data_note(lines: list[str], required_min: int = 2) -> str:
    # 데이터가 거의 없을 때만 짧게 알림. '확인중' 도배 방지.
    return "" if len(lines) >= required_min else "\n데이터 일부 지연 중. 핵심 가격 반응 우선 확인."


async def market_session_scheduler(bot: Bot, state: State) -> None:
    """
    10만명 정보방 운영용 고정 브리핑.
    - 정해진 시간대 10분 창 안에서 1회 전송
    - '확인중' 도배 금지: 받아온 데이터만 보여주고, 없으면 짧은 데이터 지연 문구만 표시
    - Yahoo 응답 실패 대비: BTC/김프/공포탐욕 등 가능한 데이터로 메시지는 무조건 발송

    발송 시간대(KST):
    - 한국장 1시간 전: 08:00~08:10, 월~금
    - 한국장 마감 정리: 15:30~15:40, 월~금
    - 미국장 1시간 전: 21:30~21:40, 월~금
    - 미국장 마감 정리: 05:00~05:10, 화~토
    """

    def in_send_window(now: datetime, hour: int, minute: int, window_minutes: int = 10) -> bool:
        start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        end = start + timedelta(minutes=window_minutes)
        return start <= now < end

    def pct_or_zero(snap: Optional[Tuple[float, float]]) -> float:
        return float(snap[1]) if snap else 0.0

    async def btc_line(session: aiohttp.ClientSession) -> Tuple[str, float]:
        btc = await get_binance_ticker_24h(session, "BTCUSDT")
        if not btc:
            return "BTC: 데이터 지연", 0.0
        price = float(btc["lastPrice"])
        pct = float(btc["priceChangePercent"])
        return f"BTC: {price:,.0f} USDT ({fmt_pct(pct)})", pct

    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()

                if is_korean_market_weekday(now) and in_send_window(now, 8, 0, 10):
                    key = "kr_pre_0800"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        btc_text, _ = await btc_line(session)

                        lines = ["🇰🇷 [한국장 1시간 전]"]
                        append_if_value(lines, "코스피", kospi)
                        append_if_value(lines, "코스닥", kosdaq)
                        if usd_krw:
                            lines.append(f"달러/원: {usd_krw:,.2f}원")
                        append_if_value(lines, "S&P500 선물", sp_fut)
                        append_if_value(lines, "나스닥 선물", nq_fut)
                        lines.append(btc_text)

                        us_flow = market_direction_label(pct_or_zero(sp_fut), pct_or_zero(nq_fut))
                        conclusion = kr_open_conclusion(
                            pct_or_zero(kospi), pct_or_zero(kosdaq),
                            pct_or_zero(sp_fut), pct_or_zero(nq_fut), usd_krw or 0,
                        )
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n시장 분위기: {us_flow}\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        logging.info("한국장 1시간 전 브리핑 전송 완료")

                if is_korean_market_weekday(now) and in_send_window(now, 15, 30, 10):
                    key = "kr_close_1530"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        btc_text, _ = await btc_line(session)

                        kp = pct_or_zero(kospi)
                        kq = pct_or_zero(kosdaq)
                        if kospi and kosdaq:
                            if kp >= 0 and kq >= 0:
                                feature = "지수 동반 강세"
                            elif kp >= 0 > kq:
                                feature = "대형주 상대강세, 중소형주 약세"
                            elif kp < 0 <= kq:
                                feature = "종목장 성격 강화"
                            else:
                                feature = "지수 전반 약세"
                        else:
                            feature = "데이터 지연. 환율·미국 선물 흐름 확인 필요"

                        lines = ["🔔 [한국장 마감 정리]"]
                        append_if_value(lines, "코스피", kospi)
                        append_if_value(lines, "코스닥", kosdaq)
                        if usd_krw:
                            lines.append(f"달러/원: {usd_krw:,.0f}원")
                        lines.append(btc_text)

                        conclusion = kr_close_conclusion(kp, kq, usd_krw or 0)
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n오늘 특징: {feature}\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        logging.info("한국장 마감 브리핑 전송 완료")

                if is_us_market_premarket_day(now) and in_send_window(now, 21, 30, 10):
                    key = "us_pre_2130"
                    if state.market_session_sent_dates.get(key) != now.date():
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, btc_pct = await btc_line(session)

                        lines = ["🇺🇸 [미국장 1시간 전]"]
                        append_if_value(lines, "S&P500 선물", sp_fut)
                        append_if_value(lines, "나스닥 선물", nq_fut)
                        append_if_value(lines, "달러인덱스", dxy)
                        append_if_value(lines, "10년물 금리", tnx)
                        lines.append(btc_text)

                        conclusion = us_open_conclusion(
                            pct_or_zero(sp_fut), pct_or_zero(nq_fut),
                            pct_or_zero(dxy), pct_or_zero(tnx),
                        )
                        if len(lines) <= 2:
                            conclusion = "미국 지수 데이터 지연. 지금은 BTC 가격 반응과 첫 30분 변동성 확인이 먼저."
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        logging.info("미국장 1시간 전 브리핑 전송 완료")

                if is_us_market_close_day(now) and in_send_window(now, 5, 0, 10):
                    key = "us_close_0500"
                    if state.market_session_sent_dates.get(key) != now.date():
                        spx = await get_yahoo_snapshot(session, "%5EGSPC")
                        ixic = await get_yahoo_snapshot(session, "%5EIXIC")
                        dji = await get_yahoo_snapshot(session, "%5EDJI")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)

                        lines = ["🌙 [미국장 마감 정리]"]
                        append_if_value(lines, "S&P500", spx)
                        append_if_value(lines, "나스닥", ixic)
                        append_if_value(lines, "다우", dji)
                        append_if_value(lines, "달러인덱스", dxy)
                        append_if_value(lines, "10년물 금리", tnx)
                        lines.append(btc_text)

                        conclusion = us_close_conclusion(pct_or_zero(spx), pct_or_zero(ixic), pct_or_zero(dji))
                        if len(lines) <= 2:
                            conclusion = "미국장 데이터 지연. BTC와 한국장 선물 반응을 우선 확인."
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        logging.info("미국장 마감 브리핑 전송 완료")

            except Exception:
                logging.exception("market_session_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_SESSION_CHECK_SECONDS - int(elapsed)))

def resolve_telegram_token() -> Tuple[str, str]:
    """Railway 변수명 실수 완화. 값은 노출하지 않고 요약만 로그한다."""
    keys = ("TELEGRAM_TOKEN", "BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN")
    summary: list[str] = []
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            summary.append(f"{key}=없음")
            continue
        stripped = raw.strip()
        if not stripped:
            summary.append(f"{key}=빈값")
            continue
        if key != "TELEGRAM_TOKEN":
            logging.warning("토큰을 %s 에서 사용 중입니다. 가능하면 Railway 에 TELEGRAM_TOKEN 으로 통일하세요.", key)
        return stripped, key
    logging.error(
        "봇 토큰 미설정. 같은 프로젝트의 **환경(Environment): Production** 과 "
        "**지금 로그 나는 서비스(worker)** 의 Variables 에 TELEGRAM_TOKEN 을 두었는지 확인하세요. "
        "[%s]",
        " | ".join(summary),
    )
    return "", ""


async def railway_port_health_server() -> None:
    """Railway 등에서 PORT 에 바인딩하지 않으면 배포 실패·재시작 되는 설정이 많아 둠."""
    port_s = os.getenv("PORT")
    if not port_s:
        return
    port = int(port_s)

    async def ping(_request: web.Request) -> web.Response:
        return web.Response(text="telegram-bot worker ok")

    app = web.Application()
    app.router.add_get("/", ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info("PORT=%s 에 HTTP 헬스 응답 바인딩 (/)", port_s)
    await asyncio.Future()


async def run_forever() -> None:
    token, _ = resolve_telegram_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN 환경변수가 필요합니다.")

    bot = Bot(token=token)
    state = State()

    tasks = [
        asyncio.create_task(briefing_scheduler(bot, state)),
        asyncio.create_task(market_monitor(bot, state)),
        asyncio.create_task(polymarket_monitor(bot, state)),
        asyncio.create_task(fear_greed_monitor(bot, state)),
        asyncio.create_task(kimchi_monitor(bot, state)),
        asyncio.create_task(whale_monitor(bot, state)),
        asyncio.create_task(news_monitor(bot, state)),
        asyncio.create_task(futures_flow_monitor(bot, state)),
        asyncio.create_task(market_session_scheduler(bot, state)),
    ]
    port = os.getenv("PORT")
    if port:
        tasks.append(asyncio.create_task(railway_port_health_server()))
    logging.info(
        "워커 루프 시작 (브리핑·시장·뉴스 등 비동기 스케줄러 실행). CHANNEL=%s PORT=%s",
        CHANNEL_ID,
        port or "미사용",
    )
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    while True:
        try:
            asyncio.run(run_forever())
        except RuntimeError as e:
            if "TELEGRAM_TOKEN" in str(e):
                logging.error(
                    "토큰 없음: 로그 상단의 '환경 요약' 을 참고하고, Railway 에서 Production 환경·worker 서비스 "
                    "Variables 에 TELEGRAM_TOKEN(또는 BOT_TOKEN 등) 확인 후 재배포하세요."
                )
                time.sleep(60)
                continue
            raise
        except Exception:
            logging.exception("run_forever 재시작 (예외)")
            time.sleep(10)
            continue
