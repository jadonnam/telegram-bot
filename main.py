import asyncio
import hashlib
import html
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Deque, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
import feedparser
from telegram import Bot
from telegram.error import InvalidToken


# ============================================================
# CONFIG
# ============================================================

CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@jadonnam")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
KST = ZoneInfo("Asia/Seoul")

MARKET_CHECK_SECONDS = 5 * 60
NEWS_CHECK_SECONDS = 15 * 60
FUTURES_FLOW_CHECK_SECONDS = 15 * 60
ALPHA_FLOW_CHECK_SECONDS = 60
FNG_CHECK_SECONDS = 5 * 60
KIMCHI_CHECK_SECONDS = 10 * 60
WHALE_CHECK_SECONDS = 60
BRIEFING_CHECK_SECONDS = 30
MARKET_SESSION_CHECK_SECONDS = 30

PRICE_CHANGE_THRESHOLD = 1.5
VOLUME_SURGE_THRESHOLD = 4.0
WHALE_NOTIONAL_THRESHOLD = 3_000_000

SIGNAL_COOLDOWN = timedelta(minutes=45)
FUTURES_SIGNAL_COOLDOWN = timedelta(minutes=90)

BTC_PRICE_MILESTONES = (60000, 70000, 75000, 80000, 85000, 90000, 100000)
PRICE_MILESTONE_COOLDOWN = timedelta(hours=12)
PRICE_MILESTONE_BUFFER_PCT = 0.15

NEWS_DAILY_LIMIT = 3
NEWS_MIN_INTERVAL = timedelta(minutes=60)
NEWS_URGENT_MIN_INTERVAL = timedelta(minutes=30)
NEWS_MAX_PER_SCAN = 1
NEWS_URGENT_SCORE = 9
NEWS_NORMAL_SCORE = 8
NEWS_TITLE_SIMILARITY_BLOCK_HOURS = 24
NEWS_RECENT_TITLE_LIMIT = 200

ALPHA_BIG_TRADE_NOTIONAL = 1_000_000
ALPHA_CVD_NOTIONAL_THRESHOLD = 8_000_000
ALPHA_IMBALANCE_THRESHOLD = 0.68

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://news.google.com/rss/search?q=(Iran%20OR%20Hormuz%20OR%20UAE%20OR%20Israel%20OR%20missile%20OR%20warship%20OR%20tanker)%20(US%20Navy%20OR%20oil%20OR%20attack%20OR%20strike)&hl=ko&gl=KR&ceid=KR:ko",
)

NEWS_KEYWORDS = (
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "etf",
    "fed", "fomc", "cpi", "inflation", "interest rate", "rate cut",
    "sec", "regulation", "tariff", "trump", "dollar", "oil",
    "hack", "exploit", "exchange", "binance", "coinbase", "kraken",
    "war", "missile", "attack", "hormuz", "iran", "israel", "tanker", "navy",
    "금리", "유가", "달러", "연준", "규제", "해킹", "거래소",
    "전쟁", "미사일", "공격", "호르무즈", "이란", "이스라엘", "군함", "유조선",
)

NEWS_BLOCK_KEYWORDS = (
    "airdrop", "fork", "ecash", "developer", "developers", "github",
    "testnet", "protocol upgrade", "whitepaper", "podcast", "interview",
    "opinion", "guide", "how to", "recap", "daily crypto news",
    "market wrap", "price prediction", "sponsored", "press release",
    "newsletter", "magazine",
)

BREAKING_FORCE_TERMS = (
    "missile", "strike", "warship", "tanker", "hormuz", "iran", "israel",
    "oil", "war", "liquidation", "sell-off", "crash", "hacked", "exploit",
    "미사일", "피격", "군함", "유조선", "호르무즈", "이란", "이스라엘",
    "유가", "전쟁", "청산", "급락", "해킹",
)

THREADS_AUTO_POST = os.getenv("THREADS_AUTO_POST", "false").lower() == "true"
THREADS_USER_ID = os.getenv("THREADS_USER_ID")
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN")
THREADS_IMAGE_URL = os.getenv("THREADS_IMAGE_URL")

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

# ============================================================
# HOLIDAY FILTER
# ============================================================

# Nager.Date API로 연도별 공휴일 자동 로드.
# 실패 시 FALLBACK_HOLIDAYS를 사용.
HOLIDAY_CACHE: Dict[str, set[str]] = {
    "KR": set(),
    "US": set(),
}
HOLIDAY_CACHE_YEARS: set[str] = set()

# API 실패 대비 최소 백업값. 필요하면 매년 여기에 추가 가능.
FALLBACK_HOLIDAYS = {
    "KR": {
        "2026-01-01",
        "2026-02-16", "2026-02-17", "2026-02-18",
        "2026-03-01",
        "2026-05-05",
        "2026-06-06",
        "2026-08-15",
        "2026-09-24", "2026-09-25", "2026-09-26",
        "2026-10-03",
        "2026-10-09",
        "2026-12-25",
    },
    "US": {
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
    },
}


async def warm_holiday_cache(session: aiohttp.ClientSession, year: int) -> None:
    key = str(year)
    if key in HOLIDAY_CACHE_YEARS:
        return

    for country in ("KR", "US"):
        loaded = set()
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
        data = await fetch_json(session, url)

        if isinstance(data, list):
            for row in data:
                date_s = row.get("date")
                if date_s:
                    loaded.add(date_s)

        # 미국 주식시장 휴장에 가까운 Good Friday는 공휴일 API에 없을 수 있어 수동 보강
        if country == "US":
            loaded.update(FALLBACK_HOLIDAYS.get("US", set()))

        # API 실패해도 백업 공휴일은 유지
        loaded.update(FALLBACK_HOLIDAYS.get(country, set()))

        HOLIDAY_CACHE[country].update(loaded)

    HOLIDAY_CACHE_YEARS.add(key)


def is_kr_holiday_day(day) -> bool:
    return day.strftime("%Y-%m-%d") in HOLIDAY_CACHE.get("KR", set())


def is_us_holiday_day(day) -> bool:
    return day.strftime("%Y-%m-%d") in HOLIDAY_CACHE.get("US", set())



# ============================================================
# STATE
# ============================================================

class State:
    def __init__(self) -> None:
        self.price_history: Dict[str, Deque[Tuple[datetime, float]]] = defaultdict(deque)
        self.cooldowns: Dict[str, datetime] = {}

        self.news_seen_ids: Deque[str] = deque(maxlen=3000)
        self.news_seen_set = set()
        self.news_recent_titles: Deque[Tuple[datetime, str]] = deque(maxlen=NEWS_RECENT_TITLE_LIMIT)
        self.news_daily_date: Optional[date] = None
        self.news_daily_count = 0
        self.last_news_sent_at: Optional[datetime] = None

        self.briefing_sent_dates: Dict[str, date] = {}
        self.market_session_sent_dates: Dict[str, date] = {}

        self.last_fng_zone = "normal"
        self.last_kimchi_zone = "normal"
        self.futures_oi_cache: Dict[str, float] = {}
        self.futures_last_signal: Dict[str, datetime] = {}

        self.last_market_price: Dict[str, float] = {}
        self.price_milestone_cooldowns: Dict[str, datetime] = {}

        self.whale_seen_ids: Deque[str] = deque(maxlen=8000)
        self.whale_seen_set = set()

        self.alpha_seen_ids: Deque[str] = deque(maxlen=10000)
        self.alpha_seen_set = set()

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

    def has_whale_trade(self, trade_id: str) -> bool:
        return trade_id in self.whale_seen_set

    def mark_whale_trade(self, trade_id: str) -> None:
        if trade_id in self.whale_seen_set:
            return
        if len(self.whale_seen_ids) == self.whale_seen_ids.maxlen:
            old = self.whale_seen_ids.popleft()
            self.whale_seen_set.discard(old)
        self.whale_seen_ids.append(trade_id)
        self.whale_seen_set.add(trade_id)

    def has_alpha_trade(self, trade_id: str) -> bool:
        return trade_id in self.alpha_seen_set

    def mark_alpha_trade(self, trade_id: str) -> None:
        if trade_id in self.alpha_seen_set:
            return
        if len(self.alpha_seen_ids) == self.alpha_seen_ids.maxlen:
            old = self.alpha_seen_ids.popleft()
            self.alpha_seen_set.discard(old)
        self.alpha_seen_ids.append(trade_id)
        self.alpha_seen_set.add(trade_id)


# ============================================================
# UTIL
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_kst() -> datetime:
    return datetime.now(KST)


def fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


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
    return f"━━━━━━━━━━━━━━\n{title}\n━━━━━━━━━━━━━━"


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


def format_price_level(level: float) -> str:
    return f"{level / 1000:.0f}K" if level >= 1000 else f"{level:,.0f}"


def clean_text(value: str, limit: int = 180) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) > limit:
        return value[:limit].rstrip() + "..."
    return value


def news_id(title: str, link: str, published: str) -> str:
    raw = f"{title}|{link}|{published}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_news_url(link: str) -> str:
    link = (link or "").strip()
    link = re.sub(r"[?&](utm_[^=]+|utm_source|utm_medium|utm_campaign|utm_term|utm_content)=[^&]+", "", link)
    link = re.sub(r"[?&]output=amp", "", link)
    link = link.split("#")[0]
    return link.rstrip("/")


def normalize_title_for_dedup(title: str) -> str:
    title = clean_text(title, limit=300).lower()
    title = re.sub(r"[^a-z0-9가-힣\s]", " ", title)
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
    url_hash = hashlib.sha256(normalize_news_url(link).encode("utf-8")).hexdigest()
    if state.has_news(url_hash):
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


def is_telegram_auth_failure(exc: BaseException) -> bool:
    if isinstance(exc, InvalidToken):
        return True
    err = str(exc)
    return "Unauthorized" in err or "InvalidToken" in err


async def send_message(bot: Bot, text: str, disable_preview: bool = False) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=disable_preview)


async def safe_send(bot: Bot, text: str, disable_preview: bool = False) -> None:
    try:
        await send_message(bot, text, disable_preview=disable_preview)
    except Exception as e:
        if is_telegram_auth_failure(e):
            logging.error("Telegram 토큰 인증 실패. BotFather에서 새 토큰 발급 후 TELEGRAM_TOKEN 교체 필요.")
            return
        logging.exception("Telegram 전송 실패 chat_id=%s", CHANNEL_ID)


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


async def fetch_rss(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=25, headers=REQUEST_HEADERS) as response:
            if response.status != 200:
                logging.warning("fetch_rss 실패 status=%s url=%s", response.status, url)
                return feedparser.parse("")
            body = await response.text()
            return feedparser.parse(body)
    except Exception:
        logging.exception("fetch_rss 예외 url=%s", url)
        return feedparser.parse("")


# ============================================================
# MARKET DATA: BYBIT → OKX → BINANCE
# ============================================================

async def get_market_ticker(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    # 가격/24h 변동률 안정화:
    # 1순위 Bybit: lastPrice + price24hPcnt
    # 2순위 OKX: last + open24h로 직접 계산
    # 3순위 Binance: lastPrice + priceChangePercent

    # 1) Bybit
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        price = float(item["lastPrice"])

        pct_raw = item.get("price24hPcnt")
        if pct_raw is not None:
            pct = float(pct_raw) * 100
        else:
            prev_price = float(item.get("prevPrice24h") or 0)
            pct = ((price - prev_price) / prev_price) * 100 if prev_price > 0 else 0.0

        return {
            "lastPrice": price,
            "priceChangePercent": pct,
            "volume24h": float(item.get("turnover24h", 0) or 0),
            "source": "Bybit",
        }
    except Exception:
        pass

    # 2) OKX
    okx_symbol = symbol.replace("USDT", "-USDT")
    data = await fetch_json(
        session,
        "https://www.okx.com/api/v5/market/ticker",
        {"instId": okx_symbol},
    )
    try:
        item = data["data"][0]
        price = float(item["last"])

        open_24h = float(item.get("open24h") or 0)
        if open_24h > 0:
            pct = ((price - open_24h) / open_24h) * 100
        else:
            pct = float(item.get("chg24h") or 0) * 100

        return {
            "lastPrice": price,
            "priceChangePercent": pct,
            "volume24h": float(item.get("volCcy24h", 0) or 0),
            "source": "OKX",
        }
    except Exception:
        pass

    # 3) Binance
    data = await fetch_json(
        session,
        "https://api.binance.com/api/v3/ticker/24hr",
        {"symbol": symbol},
    )
    try:
        return {
            "lastPrice": float(data["lastPrice"]),
            "priceChangePercent": float(data["priceChangePercent"]),
            "volume24h": float(data.get("quoteVolume", 0) or 0),
            "source": "Binance",
        }
    except Exception:
        return None


async def get_binance_ticker_24h(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    return await get_market_ticker(session, symbol)


async def get_binance_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    ticker = await get_market_ticker(session, symbol)
    return float(ticker["lastPrice"]) if ticker else None


async def get_recent_klines(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/kline",
        {"category": "linear", "symbol": symbol, "interval": "5", "limit": 3},
    )
    try:
        rows = list(reversed((data.get("result") or {}).get("list", [])))
        converted = []
        for r in rows:
            ts, o, h, l, c, vol, turnover = r[:7]
            converted.append([ts, o, h, l, c, vol, ts, turnover])
        if converted:
            return converted
    except Exception:
        pass

    return await fetch_json(
        session,
        "https://api.binance.com/api/v3/klines",
        {"symbol": symbol, "interval": "5m", "limit": 3},
    )


async def get_funding_rate(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return float(item.get("fundingRate", 0)) * 100
    except Exception:
        pass

    data = await fetch_json(session, "https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
    try:
        return float(data["lastFundingRate"]) * 100
    except Exception:
        return None


async def get_open_interest(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return float(item.get("openInterest", 0))
    except Exception:
        pass

    data = await fetch_json(session, "https://fapi.binance.com/fapi/v1/openInterest", {"symbol": symbol})
    try:
        return float(data["openInterest"])
    except Exception:
        return None


async def get_orderbook_imbalance(session: aiohttp.ClientSession, symbol: str) -> Optional[Tuple[float, float, float]]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/orderbook",
        {"category": "linear", "symbol": symbol, "limit": "50"},
    )

    bids = asks = None
    try:
        result = data.get("result") or {}
        bids = result.get("b") or []
        asks = result.get("a") or []
    except Exception:
        pass

    if not bids or not asks:
        data = await fetch_json(
            session,
            "https://fapi.binance.com/fapi/v1/depth",
            {"symbol": symbol, "limit": "100"},
        )
        if data:
            bids = data.get("bids") or []
            asks = data.get("asks") or []

    if not bids or not asks:
        return None

    bid_notional = sum(float(price) * float(qty) for price, qty in bids[:50])
    ask_notional = sum(float(price) * float(qty) for price, qty in asks[:50])
    if ask_notional <= 0:
        return None
    return bid_notional / ask_notional, bid_notional, ask_notional


async def get_bybit_recent_trades(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/recent-trade",
        {"category": "linear", "symbol": symbol, "limit": "100"},
    )
    try:
        return (data.get("result") or {}).get("list", []) or []
    except Exception:
        return None


# ============================================================
# EXTERNAL DATA
# ============================================================

async def get_upbit_btc_krw(session: aiohttp.ClientSession) -> Optional[float]:
    data = await fetch_json(session, "https://api.upbit.com/v1/ticker", {"markets": "KRW-BTC"})
    try:
        return float(data[0]["trade_price"])
    except Exception:
        return None


async def get_usd_krw(session: aiohttp.ClientSession) -> Optional[float]:
    data = await fetch_json(session, "https://api.frankfurter.app/latest", {"from": "USD", "to": "KRW"})
    try:
        return float(data["rates"]["KRW"])
    except Exception:
        return None


async def get_yahoo_snapshot(session: aiohttp.ClientSession, symbol: str) -> Optional[Tuple[float, float]]:
    data = await fetch_json(session, f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}")
    try:
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta.get("regularMarketPrice"))
        prev = float(meta.get("previousClose") or meta.get("chartPreviousClose"))
        if prev <= 0:
            return None
        return price, ((price - prev) / prev) * 100
    except Exception:
        return None


async def get_fear_greed(session: aiohttp.ClientSession) -> Optional[Tuple[int, str]]:
    data = await fetch_json(session, "https://api.alternative.me/fng/")
    try:
        value = int(data["data"][0]["value"])
    except Exception:
        return None
    if value <= 24:
        label = "극도 공포"
    elif value <= 49:
        label = "공포"
    elif value <= 74:
        label = "탐욕"
    else:
        label = "극도 탐욕"
    return value, label


async def get_kimchi_premium(session: aiohttp.ClientSession) -> Optional[Tuple[float, float, float]]:
    upbit_krw = await get_upbit_btc_krw(session)
    global_usdt = await get_binance_price(session, "BTCUSDT")
    usd_krw = await get_usd_krw(session)
    if not upbit_krw or not global_usdt or not usd_krw:
        return None
    global_krw = global_usdt * usd_krw
    if global_krw <= 0:
        return None
    premium = ((upbit_krw - global_krw) / global_krw) * 100
    return premium, upbit_krw, global_usdt


# ============================================================
# NEWS
# ============================================================

async def translate_to_korean(session: aiohttp.ClientSession, text: str) -> str:
    text = clean_text(text, limit=450)
    if not text or re.search(r"[가-힣]", text):
        return text
    data = await fetch_json(
        session,
        "https://translate.googleapis.com/translate_a/single",
        {"client": "gtx", "sl": "en", "tl": "ko", "dt": "t", "q": text},
    )
    try:
        translated = "".join(part[0] for part in data[0] if part and part[0])
        return clean_text(translated, limit=220) or text
    except Exception:
        return text



# ============================================================
# MARKET DATA FALLBACKS - OKX / CoinGecko
# ============================================================

COINGECKO_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
}

OKX_SYMBOLS = {
    "BTCUSDT": "BTC-USDT",
    "ETHUSDT": "ETH-USDT",
    "SOLUSDT": "SOL-USDT",
}


async def get_okx_market_ticker(session: aiohttp.ClientSession, symbol: str):
    inst_id = OKX_SYMBOLS.get(symbol)
    if not inst_id:
        return None

    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
    data = await fetch_json(session, url)
    try:
        item = (data.get("data") or [])[0]
        last = float(item.get("last"))
        open_24h = float(item.get("open24h") or last)
        pct = ((last - open_24h) / open_24h * 100.0) if open_24h else 0.0
        return {
            "symbol": symbol,
            "lastPrice": str(last),
            "priceChangePercent": str(pct),
            "source": "OKX",
        }
    except Exception:
        return None


async def get_coingecko_market_ticker(session: aiohttp.ClientSession, symbol: str):
    coin_id = COINGECKO_IDS.get(symbol)
    if not coin_id:
        return None

    url = (
        "https://api.coingecko.com/api/v3/simple/price"
        f"?ids={coin_id}&vs_currencies=usd&include_24hr_change=true"
    )
    data = await fetch_json(session, url)
    try:
        item = data.get(coin_id) or {}
        last = float(item.get("usd"))
        pct = float(item.get("usd_24h_change") or 0.0)
        return {
            "symbol": symbol,
            "lastPrice": str(last),
            "priceChangePercent": str(pct),
            "source": "CoinGecko",
        }
    except Exception:
        return None


_BASE_GET_MARKET_TICKER = get_market_ticker


async def get_market_ticker(session: aiohttp.ClientSession, symbol: str):
    try:
        primary = await _BASE_GET_MARKET_TICKER(session, symbol)
        if primary:
            return primary
    except Exception:
        logging.warning("primary get_market_ticker 실패 symbol=%s", symbol)

    try:
        okx = await get_okx_market_ticker(session, symbol)
        if okx:
            return okx
    except Exception:
        logging.warning("OKX fallback 실패 symbol=%s", symbol)

    try:
        cg = await get_coingecko_market_ticker(session, symbol)
        if cg:
            return cg
    except Exception:
        logging.warning("CoinGecko fallback 실패 symbol=%s", symbol)

    return None


async def get_okx_btc_candles(session: aiohttp.ClientSession, bar: str = "5m", limit: int = 40):
    url = f"https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar={bar}&limit={limit}"
    data = await fetch_json(session, url)
    candles = []
    try:
        for row in data.get("data", []):
            ts = int(row[0])
            close = float(row[4])
            candles.append((ts, close))
        candles.sort(key=lambda x: x[0])
    except Exception:
        return []
    return candles


def source_name_from_link(link: str) -> str:
    lowered = (link or "").lower()
    if "coindesk" in lowered:
        return "CoinDesk"
    if "cointelegraph" in lowered:
        return "Cointelegraph"
    if "news.google" in lowered or "google.com" in lowered:
        return "Google News"
    return "RSS"


def news_importance_score(title: str, summary: str) -> int:
    text = f"{title}\n{summary}".lower()
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


def is_forced_breaking_news(title: str, summary: str) -> bool:
    text = f"{title}\n{summary}".lower()
    geo_terms = (
        "iran", "israel", "hormuz", "warship", "navy", "tanker", "missile", "strike", "oil", "war",
        "이란", "이스라엘", "호르무즈", "군함", "해군", "유조선", "미사일", "피격", "전쟁", "유가",
    )
    market_terms = (
        "bitcoin", "btc", "crypto", "market", "oil", "dollar", "stock", "risk",
        "비트코인", "코인", "시장", "유가", "달러", "주식", "위험자산",
    )
    return any(k in text for k in geo_terms) and any(k in text for k in market_terms)


def normalized_news_score(title: str, summary: str) -> int:
    if is_forced_breaking_news(title, summary):
        return 10
    return max(0, min(10, news_importance_score(title, summary)))


def is_high_quality_news(title: str, summary: str) -> bool:
    text = f"{title}\n{summary}".lower()
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


def is_urgent_news(title: str, summary: str) -> bool:
    if is_forced_breaking_news(title, summary):
        return True
    text = f"{title}\n{summary}".lower()
    true_urgent = (
        "sec approves", "sec rejects", "fomc", "cpi",
        "hacked", "exploit", "freeze", "frozen", "seized",
        "liquidation", "sell-off", "crash",
        "missile", "strike", "warship", "hormuz", "tanker", "iran", "israel",
        "승인", "거절", "해킹", "압수", "동결", "청산", "급락",
        "미사일", "피격", "군함", "호르무즈", "이란", "이스라엘", "유조선",
    )
    return any(k in text for k in true_urgent) and news_importance_score(title, summary) >= NEWS_URGENT_SCORE


def news_importance_line(title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()
    if any(k in text for k in ("etf", "sec", "regulation", "approval", "rejection", "규제", "승인", "거절")):
        return "ETF·규제 이슈라 비트코인 수급에 직접 영향 줄 수 있음."
    if any(k in text for k in ("fed", "fomc", "cpi", "inflation", "interest rate", "rate cut", "금리", "연준")):
        return "금리 기대가 흔들리면 코인·주식이 같이 움직일 수 있음."
    if any(k in text for k in ("hack", "exploit", "hacked", "해킹")):
        return "해킹 이슈는 단기 투자심리를 바로 식힐 수 있음."
    if any(k in text for k in ("exchange", "binance", "coinbase", "kraken", "거래소")):
        return "거래소 이슈는 수급과 신뢰도에 바로 연결됨."
    if any(k in text for k in ("trump", "tariff", "dollar", "oil", "war", "iran", "israel", "유가", "달러", "전쟁")):
        return "거시·지정학 이슈라 유가·달러·위험자산 분위기를 같이 흔들 수 있음."
    if any(k in text for k in ("liquidation", "sell-off", "whale", "volume", "청산")):
        return "청산·거래량 이슈라 단기 변동성이 커질 수 있음."
    return "방향보다 시장 반응까지 같이 확인해야 하는 뉴스."


def build_threads_text(title_ko: str, title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()
    if is_forced_breaking_news(title, summary):
        return (
            f"속보성 이슈다.\n\n"
            f"{title_ko}\n\n"
            f"유가, 달러, 비트코인이 같이 흔들릴 수 있는 구간이다.\n\n"
            f"지금은 가격보다 뉴스 이후 시장 반응을 먼저 봐야 한다."
        )[:500]
    if "etf" in text:
        return (
            "비트코인 ETF 쪽 돈 흐름은 계속 봐야 한다.\n\n"
            "단기 가격보다 중요한 건 큰돈이 빠지는지, 다시 들어오는지다.\n\n"
            "ETF 유입이 유지되면 가격은 늦게 반응할 수 있다."
        )[:500]
    return (
        f"{title_ko}\n\n"
        "지금은 뉴스 하나에도 시장이 바로 흔들리는 구간이다.\n\n"
        "가격이 어디서 버티는지 같이 봐야 한다."
    )[:500]


async def publish_to_threads(session: aiohttp.ClientSession, text: str) -> None:
    if not THREADS_AUTO_POST:
        return
    if not THREADS_USER_ID or not THREADS_ACCESS_TOKEN:
        logging.warning("Threads 자동 업로드 설정 없음")
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
    except Exception:
        logging.exception("Threads 자동 업로드 오류")


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
            btc_line = f"\n\n📊 현재 BTC: {btc_price:,.0f} USDT ({fmt_pct(btc_pct)}, {flow})"
        except Exception:
            btc_line = ""

    if is_urgent_news(title, summary):
        tag = f"🚨 [속보 · 중요도 {score}/10]"
    elif score >= 8:
        tag = f"🔥 [핵심뉴스 · 중요도 {score}/10]"
    else:
        tag = f"📰 [뉴스 · 중요도 {score}/10]"

    telegram_msg = (
        f"{tag}\n"
        f"{title_ko}\n\n"
        f"관찰: {line}\n"
        f"리스크: 뉴스 직후 과한 추격은 변동성에 휘말릴 수 있음.\n"
        f"대응: BTC 가격 반응과 거래량 동반 여부 확인."
        f"{btc_line}\n\n"
        f" {source}\n"
        f"{link}"
    )
    return telegram_msg, build_threads_text(title_ko, title, summary)


async def fetch_feed_entries(session: aiohttp.ClientSession, url: str) -> list:
    text = await fetch_text(session, url)
    if not text:
        return []
    parsed = feedparser.parse(text)
    return parsed.entries or []


# ============================================================
# MONITORS
# ============================================================

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
                f"🚨 [BTC 핵심 구간 돌파]\n"
                f"기준가: {level_text} 달러\n"
                f"현재가: {price:,.0f} USDT\n\n"
                f"관찰: 심리 저항선을 위로 넘긴 구간.\n"
                f"리스크: 돌파 직후 위꼬리/휩쏘 가능성.\n"
                f"대응: {level_text} 위에서 15~30분 버티면 추세 유지로 판단."
            )
        else:
            msg = (
                f"⚠️ [BTC 핵심 구간 이탈]\n"
                f"기준가: {level_text} 달러\n"
                f"현재가: {price:,.0f} USDT\n\n"
                f"관찰: 심리 지지선이 깨진 구간.\n"
                f"리스크: 단기 청산 물량과 변동성 확대.\n"
                f"대응: {level_text} 빠른 회복 실패 시 추가 눌림 주의."
            )

        await safe_send(bot, msg)
        state.price_milestone_cooldowns[key] = now + PRICE_MILESTONE_COOLDOWN

    state.last_market_price[symbol] = price


async def market_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                for symbol in SYMBOLS:
                    now = utc_now()
                    ticker = await get_market_ticker(session, symbol)
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
                                icon = "📈" if pct > 0 else "📉"
                                line = "추격보다 눌림 확인" if pct > 0 else "지지선 반응 확인"
                                msg = (
                                    f"{icon} [시장 감지]\n"
                                    f"{symbol.replace('USDT', '')} 15분 {direction} {fmt_pct(pct)}\n"
                                    f"현재가 {price:,.0f} USDT\n\n"
                                    f"관찰: 단기 변동성 확대.\n"
                                    f"대응: {line}."
                                )
                                await safe_send(bot, msg, disable_preview=True)
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
                                        f"{symbol.replace('USDT', '')} 거래대금 x{ratio:.2f}\n"
                                        f"최근 5분 거래대금 {latest_vol:,.0f} USDT\n\n"
                                        "대응: 변동성 확대 구간. 무리한 추격보다 확인 우선."
                                    )
                                    await safe_send(bot, msg, disable_preview=True)
                                    state.touch_cooldown(signal_key, now)
            except Exception:
                logging.exception("market_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_CHECK_SECONDS - int(elapsed)))


def futures_signal_comment(symbol: str, funding_pct: float, oi_change_pct: float, imbalance: float) -> Optional[str]:
    coin = symbol.replace("USDT", "")
    if funding_pct >= 0.05 and oi_change_pct >= 3:
        return f"{coin} 롱 쏠림 강함. 추격보다 눌림 확인이 안전."
    if funding_pct <= -0.05 and oi_change_pct >= 3:
        return f"{coin} 숏 쏠림 강함. 숏 스퀴즈 가능성 주의."
    if oi_change_pct >= 5:
        return f"{coin} 미결제약정 빠르게 증가. 변동성 확대 가능."
    if imbalance >= 1.8:
        return f"{coin} 매수 호가 우위. 단기 지지 시도 가능."
    if imbalance <= 0.55:
        return f"{coin} 매도 호가 우위. 위로 갈수록 저항 가능."
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
                    imbalance, _, _ = ob
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
                        f"대응: {comment}"
                    )
                    await safe_send(bot, msg, disable_preview=True)
                    state.futures_last_signal[signal_key] = now
            except Exception:
                logging.exception("futures_flow_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, FUTURES_FLOW_CHECK_SECONDS - int(elapsed)))


async def alpha_flow_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                symbol = "BTCUSDT"
                now = utc_now()
                trades = await get_bybit_recent_trades(session, symbol)
                if not trades:
                    elapsed = (utc_now() - started).total_seconds()
                    await asyncio.sleep(max(5, ALPHA_FLOW_CHECK_SECONDS - int(elapsed)))
                    continue

                buy_notional = sell_notional = big_buy = big_sell = 0.0
                fresh_count = 0

                for t in trades:
                    trade_id = str(t.get("execId") or t.get("i") or t.get("T") or t)
                    if state.has_alpha_trade(trade_id):
                        continue
                    state.mark_alpha_trade(trade_id)
                    fresh_count += 1

                    try:
                        price = float(t.get("price") or t.get("p"))
                        size = float(t.get("size") or t.get("v"))
                        side = str(t.get("side") or t.get("S") or "").lower()
                    except Exception:
                        continue

                    notional = price * size
                    if side == "buy":
                        buy_notional += notional
                        if notional >= ALPHA_BIG_TRADE_NOTIONAL:
                            big_buy += notional
                    elif side == "sell":
                        sell_notional += notional
                        if notional >= ALPHA_BIG_TRADE_NOTIONAL:
                            big_sell += notional

                total = buy_notional + sell_notional
                if fresh_count == 0 or total <= 0:
                    elapsed = (utc_now() - started).total_seconds()
                    await asyncio.sleep(max(5, ALPHA_FLOW_CHECK_SECONDS - int(elapsed)))
                    continue

                buy_ratio = buy_notional / total
                sell_ratio = sell_notional / total
                cvd = buy_notional - sell_notional

                if big_buy >= ALPHA_BIG_TRADE_NOTIONAL or big_sell >= ALPHA_BIG_TRADE_NOTIONAL:
                    signal_key = "alpha:bigtrade:btc"
                    if not state.is_on_cooldown(signal_key, now):
                        if big_buy > big_sell:
                            msg = (
                                "🐋 [알파 수급 감지]\n"
                                "BTC 대형 매수 체결 우세\n\n"
                                f"대형 매수: {big_buy:,.0f} USDT\n"
                                f"대형 매도: {big_sell:,.0f} USDT\n\n"
                                "대응: 단기 지지 또는 돌파 시도 가능성 확인."
                            )
                        else:
                            msg = (
                                "🐋 [알파 수급 감지]\n"
                                "BTC 대형 매도 체결 우세\n\n"
                                f"대형 매수: {big_buy:,.0f} USDT\n"
                                f"대형 매도: {big_sell:,.0f} USDT\n\n"
                                "대응: 단기 저항 또는 눌림 가능성 확인."
                            )
                        await safe_send(bot, msg, disable_preview=True)
                        state.touch_cooldown(signal_key, now)

                if abs(cvd) >= ALPHA_CVD_NOTIONAL_THRESHOLD:
                    side_label = "매수" if cvd > 0 else "매도"
                    ratio = buy_ratio if cvd > 0 else sell_ratio
                    if ratio >= ALPHA_IMBALANCE_THRESHOLD:
                        signal_key = f"alpha:cvd:{side_label}"
                        if not state.is_on_cooldown(signal_key, now):
                            msg = (
                                "📊 [체결강도 감지]\n"
                                f"BTC {side_label} 체결 강도 우세\n\n"
                                f"매수 체결: {buy_notional:,.0f} USDT\n"
                                f"매도 체결: {sell_notional:,.0f} USDT\n"
                                f"CVD Proxy: {cvd:+,.0f} USDT\n"
                                f"{side_label} 비중: {ratio * 100:.1f}%\n\n"
                                "대응: 한쪽 체결이 과하게 쏠리는 구간."
                            )
                            await safe_send(bot, msg, disable_preview=True)
                            state.touch_cooldown(signal_key, now)
            except Exception:
                logging.exception("alpha_flow_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, ALPHA_FLOW_CHECK_SECONDS - int(elapsed)))


async def whale_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                trades = await get_bybit_recent_trades(session, "BTCUSDT")
                if trades:
                    for t in trades:
                        trade_id = str(t.get("execId") or t.get("i") or t.get("T") or t)
                        if state.has_whale_trade(trade_id):
                            continue
                        state.mark_whale_trade(trade_id)

                        try:
                            price = float(t.get("price") or t.get("p"))
                            qty = float(t.get("size") or t.get("v"))
                            side_raw = str(t.get("side") or t.get("S") or "").lower()
                        except Exception:
                            continue

                        notional = price * qty
                        if notional < WHALE_NOTIONAL_THRESHOLD:
                            continue

                        now = utc_now()
                        signal_key = "whale:btc"
                        if state.is_on_cooldown(signal_key, now):
                            continue

                        side = "매수" if side_raw == "buy" else "매도"
                        msg = (
                            "🐋 [고래 감지]\n"
                            f"BTC 대형 {side} 체결 포착\n"
                            f"거래규모: {notional:,.0f} USDT\n\n"
                            "대응: 단기 변동성 확대 가능성 주의."
                        )
                        await safe_send(bot, msg, disable_preview=True)
                        state.touch_cooldown(signal_key, now)
            except Exception:
                logging.exception("whale_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, WHALE_CHECK_SECONDS - int(elapsed)))


LEGACY_NEWS_TASK_DISABLED = True

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


# ============================================================
# BRIEFINGS
# ============================================================

def fear_greed_zone(value: int) -> str:
    if value <= 25:
        return "extreme_fear"
    if value >= 75:
        return "extreme_greed"
    return "normal"


def market_one_liner(btc_24h_pct: float) -> str:
    if btc_24h_pct >= 2.0:
        return "매수세가 다시 붙는 구간. 돌파 후 거래량 유지가 핵심."
    if btc_24h_pct <= -2.0:
        return "변동성 커진 구간. 지금은 반등보다 지지선 확인이 먼저."
    if btc_24h_pct >= 0.3:
        return "위험자산 분위기 살아나는 중. 거래량 붙으면 추가 반등 가능."
    if btc_24h_pct <= -0.3:
        return "살짝 눌리는 흐름. 급락보다 지지 확인 구간에 가까움."
    return "큰 방향은 아직 안 나왔고, 수급 붙는 쪽으로 시장이 움직일 가능성 큼."



# ============================================================
# FINAL MESSAGE STYLE HELPERS
# ============================================================

def compact_section(title: str) -> str:
    return "━━━━━━━━━━━━━━\n" + title + "\n━━━━━━━━━━━━━━"

def live_news_header(score: int) -> str:
    if score >= 26:
        return "🚨 중요 실시간 시장 이슈"
    if score >= 18:
        return "⚡ 실시간 시장 이슈"
    return "🟡 체크 실시간 시장 이슈"


def market_one_liner(btc_24h_pct: float) -> str:
    if btc_24h_pct >= 2.0:
        return "매수세가 다시 붙는 구간. 돌파 후 거래량 유지가 핵심."
    if btc_24h_pct <= -2.0:
        return "변동성 커진 구간. 지금은 반등보다 지지선 확인이 먼저."
    if btc_24h_pct >= 0.3:
        return "위험자산 분위기 살아나는 중. 거래량 붙으면 추가 반등 가능."
    if btc_24h_pct <= -0.3:
        return "살짝 눌리는 흐름. 급락보다 지지 확인 구간에 가까움."
    return "큰 방향은 아직 안 나왔고, 수급 붙는 쪽으로 시장이 움직일 가능성 큼."


def kr_open_focus(nq_pct: float, sox_pct: float, usd_krw: float) -> str:
    if nq_pct >= 0.4 or sox_pct >= 0.8:
        return "미국 AI·반도체 강세 영향 이어지는 중.\n오늘 한국장은 외국인 수급 + 반도체 흐름 먼저 체크해야함."
    if nq_pct <= -0.4:
        return "나스닥 쪽 부담이 남아있음.\n장 초반 추격보다 반도체 방어력 확인이 먼저."
    if usd_krw >= 1450:
        return "환율이 높은 구간이라 외국인 수급이 오늘 방향을 정할 가능성 큼."
    return "큰 방향보다 수급 확인이 먼저.\n장 초반 30분은 무리하지 않는 구간."


def kr_close_focus(kospi_pct: float, kosdaq_pct: float, usd_krw: float) -> str:
    if kospi_pct >= 0.5 and kosdaq_pct >= 0.3:
        return "지수 동반 강세. 내일도 외국인 수급과 반도체 흐름이 핵심."
    if kospi_pct >= 0.5 and kosdaq_pct < 0:
        return "대형주 쪽으로 돈이 몰린 장. 삼성전자·하이닉스 수급 확인 필요."
    if kospi_pct < 0 and kosdaq_pct < 0:
        return "전반적으로 힘이 빠진 장. 환율과 미국 선물 반응이 더 중요해짐."
    if usd_krw >= 1450:
        return "환율 부담이 남아있어서 종목보다 수급 방향 확인이 우선."
    return "지수보다 종목장 성격이 강한 하루. 강한 섹터만 살아남는 흐름."


def us_close_focus(nq_pct: float, sox_pct: float, nvda_pct: float) -> str:
    if sox_pct >= 1.0 or nvda_pct >= 1.5:
        return "AI·반도체 쪽으로 다시 수급 몰리는 분위기.\n특히 HBM·데이터센터 관련주 강세 계속 이어지는 중."
    if nq_pct >= 0.5:
        return "나스닥 상대강세.\nAI·빅테크 수급이 한국장까지 이어질지 체크."
    if nq_pct <= -0.5:
        return "기술주 쪽 부담이 남아있음.\n한국장은 반도체 방어력부터 확인 필요."
    return "지수별 온도차가 있는 장.\n한국장은 환율과 반도체 수급이 방향을 정할 가능성."


def us_pre_focus(nq_pct: float, dxy_pct: float, tnx_pct: float) -> str:
    if nq_pct >= 0.4:
        return "나스닥 선물이 강함.\n오늘도 AI·빅테크 쪽으로 돈이 붙는지 봐야함."
    if nq_pct <= -0.4:
        return "나스닥 선물이 약함.\n장 초반 기술주 매도 압력부터 확인 필요."
    if dxy_pct > 0.25 or tnx_pct > 0.25:
        return "달러·금리 부담이 있어서 초반 변동성 커질 수 있음."
    return "큰 방향은 아직 안 나왔지만,\n첫 30분 수급 붙는 쪽이 오늘 흐름을 만들 가능성 큼."


def impact_one_liner(category: str, title: str, summary: str) -> str:
    impact = build_market_impact_line(category, title, summary)
    impact = impact.replace("시장 영향은 가격 반응 확인하면서 봐야함.", "가격 반응과 거래량 붙는지 확인 필요.")
    return impact








# ============================================================
# POST ERROR QUALITY HELPERS
# ============================================================

def compact_section(title: str) -> str:
    return "━━━━━━━━━━━━━━\n" + title + "\n━━━━━━━━━━━━━━"


def snap_pct(snap) -> float:
    try:
        return float(snap[1]) if snap else 0.0
    except Exception:
        return 0.0


def snapshot_line(name: str, snap, digits: int = 2) -> str:
    if not snap:
        return ""
    price, pct = snap
    return f"{move_icon(float(pct))} {name}: {float(price):,.{digits}f} ({fmt_pct(float(pct))})"


def btc_brief_line(price: float, pct: float) -> str:
    return f"{move_icon(pct)} BTC: {price:,.0f} USDT ({fmt_pct(pct)})"


def market_mood_label(*pcts: float) -> str:
    vals = []
    for x in pcts:
        try:
            vals.append(float(x))
        except Exception:
            pass
    if not vals:
        return "🟡 눈치보기 장세"
    avg = sum(vals) / len(vals)
    if avg >= 0.55:
        return "🟢 위험선호"
    if avg <= -0.55:
        return "🔴 리스크오프"
    if max(vals) - min(vals) >= 1.0:
        return "🟡 종목장"
    return "🟡 눈치보기 장세"


def clean_news_body_for_message(title: str, summary: str, source: str = "") -> str:
    title_clean = html_clean(strip_news_source_tail(title or ""), 220).strip()
    body = html_clean(summary or "", 280).strip()
    body = re.sub(r"https?://\S+", "", body)
    body = body.replace(" ", " ").replace("…", "...").strip()

    for s in (source, "Google News", "조선일보", "한국경제", "경향신문", "blog.google", "Korea IT Times", "프라임경제"):
        if s:
            body = body.replace(str(s), "").strip()

    if title_clean and body:
        body_no_space = re.sub(r"\s+", "", body)
        title_no_space = re.sub(r"\s+", "", title_clean)
        if body_no_space == title_no_space or title_no_space in body_no_space[: len(title_no_space) + 30]:
            return ""

    if len(body) < 18:
        return ""
    return body.strip()


def live_news_header(score: int) -> str:
    if score >= 28:
        return "🚨 [시장 속보]"
    if score >= 20:
        return "⚡ [실시간 시장 이슈]"
    return "🟡 [체크할 시장 이슈]"


def impact_one_liner(category: str, title: str, summary: str) -> str:
    impact = build_market_impact_line(category, title, summary)
    impact = impact.replace("시장 영향은 가격 반응 확인하면서 봐야함.", "가격 반응과 거래량 붙는지 확인 필요.")
    impact = impact.replace("빅테크 움직임이라 나스닥 분위기 같이 봐야함.", "AI·빅테크 수급 이어지는지 같이 체크해야함.")
    impact = impact.replace("AI·반도체 쪽 돈 흐름 계속 체크할 자리.", "반도체·AI 관련주 변동성 확대 가능성 체크.")
    return impact


def market_one_liner(btc_24h_pct: float) -> str:
    if btc_24h_pct >= 2.0:
        return "매수세가 다시 붙는 구간. 돌파 후 거래량 유지가 핵심."
    if btc_24h_pct <= -2.0:
        return "변동성 커진 구간. 지금은 반등보다 지지선 확인이 먼저."
    if btc_24h_pct >= 0.3:
        return "위험자산 분위기 살아나는 중. 거래량 붙으면 추가 반등 가능."
    if btc_24h_pct <= -0.3:
        return "살짝 눌리는 흐름. 급락보다 지지 확인 구간에 가까움."
    return "큰 방향은 아직 안 나왔고, 수급 붙는 쪽으로 시장이 움직일 가능성 큼."


async def briefing_scheduler(bot: Bot, state: State) -> None:
    slots = {
        "12": (12, "☀️ 점심 시장 체크"),
        "21": (21, "🌙 밤 시장 체크"),
    }
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()
                for slot_key, (hour, title) in slots.items():
                    if now.hour == hour and now.minute < 5:
                        if state.briefing_sent_dates.get(slot_key) == now.date():
                            continue

                        tickers = {}
                        for symbol in SYMBOLS:
                            t = await get_market_ticker(session, symbol)
                            if t:
                                tickers[symbol] = (float(t["lastPrice"]), float(t["priceChangePercent"]))
                        if "BTCUSDT" not in tickers:
                            continue

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")

                        btc_price, btc_pct = tickers["BTCUSDT"]
                        eth_price, eth_pct = tickers.get("ETHUSDT", (0, 0))
                        sol_price, sol_pct = tickers.get("SOLUSDT", (0, 0))

                        msg = compact_section(title)
                        msg += f"\n{btc_brief_line(btc_price, btc_pct)}"
                        msg += f"\n{move_icon(eth_pct)} ETH: {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})"
                        msg += f"\n{move_icon(sol_pct)} SOL: {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})"

                        if nq_fut:
                            msg += f"\n\n{snapshot_line('나스닥 선물', nq_fut)}"
                        if wti:
                            msg += f"\n{snapshot_line('WTI 유가', wti)}"
                        if sox:
                            msg += f"\n{snapshot_line('필라델피아 반도체지수', sox)}"

                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n\n😶‍🌫️ 공포탐욕지수: {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n🇰🇷 김치프리미엄: {fmt_pct(premium)}"

                        msg += f"\n\n📌 지금 핵심:\n{market_one_liner(btc_pct)}"
                        msg += "\n\n체크할 것:"
                        msg += "\n- BTC 1차 지지선 유지 여부"
                        msg += "\n- ETH/SOL로 알트 수급 번지는지"
                        msg += "\n- 미국 선물·환율·반도체 흐름"

                        await safe_send(bot, msg, disable_preview=True)
                        state.briefing_sent_dates[slot_key] = now.date()
            except Exception:
                logging.exception("briefing_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, BRIEFING_CHECK_SECONDS - int(elapsed)))

def is_korean_market_weekday(now: datetime) -> bool:
    return now.weekday() < 5 and not is_kr_holiday_day(now)


def is_us_market_premarket_day(now: datetime) -> bool:
    return now.weekday() < 5 and not is_us_holiday_day(now)


def is_us_market_close_day(now: datetime) -> bool:
    # KST 새벽 05:00 미국장 마감 브리핑은 전날 미국 거래일 기준
    us_session_day = now - timedelta(days=1)
    return us_session_day.weekday() < 5 and not is_us_holiday_day(us_session_day)


def market_direction_label(*pcts: float) -> str:
    vals = [float(x) for x in pcts if x is not None]
    if not vals:
        return "⚪ 데이터 확인 중"
    avg = sum(vals) / len(vals)
    if avg >= 0.45:
        return "🟢 위험선호"
    if avg <= -0.45:
        return "🔴 리스크오프"
    if max(vals) - min(vals) >= 0.9:
        return "🟡 종목장"
    return "🟡 눈치보기"


def market_one_liner(btc_24h_pct: float) -> str:
    if btc_24h_pct >= 2.0:
        return "매수세가 다시 붙는 구간. 돌파 후 거래량 유지가 핵심."
    if btc_24h_pct <= -2.0:
        return "변동성 커진 구간. 지지선 이탈보다 반등 거래량 먼저 확인."
    if btc_24h_pct >= 0.3:
        return "위험자산 분위기 살아나는 중. 거래량 붙으면 추가 반등 가능."
    if btc_24h_pct <= -0.3:
        return "살짝 눌리는 흐름. 급락보다 지지 확인 구간에 가까움."
    return "큰 방향은 아직 안 나왔고, 수급 붙는 쪽으로 시장이 움직일 가능성 큼."


def kr_open_conclusion(kospi_pct: float, kosdaq_pct: float, sp_pct: float, nq_pct: float, usd_krw: float) -> str:
    if nq_pct >= 0.4:
        return "미국 AI·반도체 강세 영향이 이어지는 중. 오늘은 외국인 반도체 수급 먼저 봐야함."
    if nq_pct <= -0.4:
        return "나스닥 쪽 부담이 남아있음. 장 초반 추격보다 반도체 방어력 확인이 먼저."
    if usd_krw >= 1450:
        return "환율이 높은 구간이라 외국인 수급이 오늘 방향을 정할 가능성 큼."
    if kospi_pct >= 0 and kosdaq_pct < 0:
        return "대형주 쪽으로 돈이 몰릴 수 있음. 중소형주는 선별 필요."
    return "큰 방향보다 수급 확인이 먼저. 장 초반 30분은 무리하지 않는 구간."


def kr_close_conclusion(kospi_pct: float, kosdaq_pct: float, usd_krw: float) -> str:
    if kospi_pct >= 0.5 and kosdaq_pct >= 0.3:
        return "지수 동반 강세. 내일도 외국인 수급과 반도체 흐름이 핵심."
    if kospi_pct >= 0.5 and kosdaq_pct < 0:
        return "대형주 쪽으로 돈이 몰린 장. 삼성전자·하이닉스 수급 확인 필요."
    if kospi_pct < 0 and kosdaq_pct < 0:
        return "전반적으로 힘이 빠진 장. 환율과 미국 선물 반응이 더 중요해짐."
    if usd_krw >= 1450:
        return "환율 부담이 남아있어서 종목보다 수급 방향 확인이 우선."
    return "지수보다 종목장 성격이 강한 하루. 강한 섹터만 살아남는 흐름."


def us_open_conclusion(sp_pct: float, nq_pct: float, dxy_pct: float, tnx_pct: float) -> str:
    if nq_pct >= 0.4:
        return "나스닥 선물이 강함. 오늘도 AI·빅테크 쪽으로 돈이 붙는지 봐야함."
    if nq_pct <= -0.4:
        return "나스닥 선물이 약함. 장 초반 기술주 매도 압력부터 확인 필요."
    if dxy_pct > 0.25 or tnx_pct > 0.25:
        return "달러·금리 부담이 있어서 초반 변동성 커질 수 있음."
    return "큰 방향은 아직 안 나왔지만, 첫 30분 수급 붙는 쪽이 오늘 흐름을 만들 가능성 큼."


def us_close_conclusion(sp_pct: float, nq_pct: float, dji_pct: float) -> str:
    if nq_pct >= sp_pct and nq_pct >= dji_pct and nq_pct > 0.4:
        return "나스닥 상대강세. AI·반도체 수급이 한국장까지 이어질지 체크."
    if sp_pct > 0 and nq_pct > 0 and dji_pct > 0:
        return "미국장 전반 강세. 한국장도 위험선호 이어질 가능성 있음."
    if sp_pct < 0 and nq_pct < 0 and dji_pct < 0:
        return "미국장 전반 약세. 한국장은 방어적으로 출발할 가능성 큼."
    return "지수별 온도차가 있는 장. 한국장은 환율과 반도체 수급이 방향을 정할 가능성."


def append_if_value(lines: list[str], name: str, snap: Optional[Tuple[float, float]], digits: int = 2) -> None:
    if snap:
        line = fmt_market_value(name, snap, digits)
        if line:
            lines.append(line)


def session_data_note(lines: list[str], required_min: int = 2) -> str:
    # 사용자에게 '데이터 지연' 문구를 노출하지 않는다.
    return ""








# ============================================================
# FINAL SESSION BRIEFING HELPERS
# ============================================================

def session_section(title: str) -> str:
    return "━━━━━━━━━━━━━━\n" + title + "\n━━━━━━━━━━━━━━"


def safe_pct_from_snapshot(snap) -> float:
    try:
        return float(snap[1]) if snap else 0.0
    except Exception:
        return 0.0


def session_snapshot_line(name: str, snap, digits: int = 2) -> str:
    if not snap:
        return ""
    price, pct = snap
    return f"{move_icon(float(pct))} {name}: {float(price):,.{digits}f} ({fmt_pct(float(pct))})"


def session_btc_line(price: float, pct: float) -> str:
    return f"{move_icon(pct)} BTC: {price:,.0f} USDT ({fmt_pct(pct)})"


def session_mood(*pcts: float) -> str:
    vals = []
    for v in pcts:
        try:
            vals.append(float(v))
        except Exception:
            pass
    if not vals:
        return "🟡 눈치보기 장세"
    avg = sum(vals) / len(vals)
    if avg >= 0.55:
        return "🟢 위험선호"
    if avg <= -0.55:
        return "🔴 리스크오프"
    if max(vals) - min(vals) >= 1.2:
        return "🟡 지수별 온도차"
    return "🟡 눈치보기 장세"


def final_kr_open_focus(nq_pct: float, sox_pct: float, usd_krw: float, wti_pct: float) -> str:
    if sox_pct >= 0.8 or nq_pct >= 0.4:
        return "미국 기술주·반도체 수급이 살아있는 구간.\n오늘 한국장은 외국인 수급과 반도체 대형주 흐름이 핵심."
    if nq_pct <= -0.4 or sox_pct <= -0.8:
        return "미국 기술주 쪽 부담이 남아있음.\n장 초반 추격보다 반도체 방어력 확인이 먼저."
    if usd_krw >= 1450:
        return "환율이 높은 구간이라 외국인 수급이 오늘 방향을 정할 가능성 큼."
    if wti_pct >= 1.0:
        return "유가가 다시 오르는 구간.\n정유·해운보다 물가·환율 부담을 같이 봐야함."
    return "큰 방향보다 수급 확인이 먼저.\n장 초반 30분은 무리하지 않는 구간."


def final_us_pre_focus(sp_pct: float, nq_pct: float, dxy_pct: float, tnx_pct: float, wti_pct: float) -> str:
    if nq_pct >= 0.4:
        return "나스닥 선물이 강함.\n오늘은 기술주 수급이 지수 전체를 끌고 갈 수 있는지 확인."
    if nq_pct <= -0.4:
        return "나스닥 선물이 약함.\n장 초반 기술주 매도 압력부터 확인 필요."
    if dxy_pct > 0.25 or tnx_pct > 0.25:
        return "달러·금리 부담이 있어서 초반 변동성 커질 수 있음."
    if wti_pct >= 1.0:
        return "유가가 강하게 움직이는 구간.\n인플레·금리 기대가 같이 흔들릴 수 있음."
    return "큰 방향은 아직 안 나왔지만,\n첫 30분 수급 붙는 쪽이 오늘 흐름을 만들 가능성 큼."


def final_us_close_focus(sp_pct: float, nq_pct: float, dji_pct: float, sox_pct: float, dxy_pct: float, tnx_pct: float, wti_pct: float) -> str:
    if sox_pct >= 1.0 and nq_pct >= 0.4:
        return "반도체와 나스닥이 같이 강했던 장.\n한국장은 반도체·AI 인프라 수급이 이어지는지 먼저 봐야함."
    if nq_pct >= 0.6:
        return "나스닥 중심 위험선호가 살아난 장.\n성장주 수급이 한국장까지 이어질지 체크."
    if nq_pct <= -0.6 or sox_pct <= -1.0:
        return "기술주·반도체 부담이 남은 장.\n한국장은 장 초반 방어력 확인이 우선."
    if dxy_pct >= 0.3 or tnx_pct >= 0.3:
        return "달러·금리 압력이 남아있음.\n위험자산 반등보다 수급 확인이 먼저."
    if wti_pct >= 1.0:
        return "유가 상승이 다시 부담으로 들어온 장.\n인플레·금리·환율 반응을 같이 봐야함."
    return "지수는 혼조지만 방향을 정할 재료는 남아있음.\n한국장은 환율·반도체·외국인 수급이 핵심."


def final_market_recap_focus(btc_pct: float, eth_pct: float, sol_pct: float, nq_pct: float, sox_pct: float, wti_pct: float, dxy_pct: float) -> str:
    if btc_pct <= -2.0:
        return "BTC가 크게 눌린 구간. 1차 지지선 회복 여부와 알트 수급이 핵심."
    if btc_pct >= 2.0:
        return "BTC에 매수세가 붙은 구간. 거래량 유지되면 위험자산 분위기도 살아날 수 있음."
    if nq_pct >= 0.5 or sox_pct >= 1.0:
        return "미국 기술주·반도체 쪽 수급이 살아있는 흐름. 한국장은 반도체 반응 체크."
    if wti_pct >= 1.0:
        return "유가가 움직인 구간. 물가·달러·위험자산 반응을 같이 봐야함."
    if dxy_pct >= 0.3:
        return "달러가 강한 구간. 위험자산은 추격보다 수급 확인이 먼저."
    return "큰 방향보다 수급 확인 구간. 지수보다 강한 섹터만 살아남는 흐름."


async def market_session_scheduler(bot: Bot, state: State) -> None:
    def is_exact_time(now: datetime, hour: int, minute: int) -> bool:
        return now.hour == hour and now.minute == minute

    async def btc_line(session: aiohttp.ClientSession) -> Tuple[str, float]:
        btc = await get_market_ticker(session, "BTCUSDT")
        if not btc:
            return "⚪ BTC: 가격 확인 중", 0.0
        price = float(btc["lastPrice"])
        pct = float(btc["priceChangePercent"])
        return session_btc_line(price, pct), pct

    async def coin_snapshot(session: aiohttp.ClientSession, symbol: str):
        t = await get_market_ticker(session, symbol)
        if not t:
            return None
        return float(t["lastPrice"]), float(t["priceChangePercent"])

    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()
                await warm_holiday_cache(session, now.year)
                await warm_holiday_cache(session, (now - timedelta(days=1)).year)

                if is_korean_market_weekday(now) and is_exact_time(now, 8, 0):
                    key = "kr_pre_0800"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)
                        eth = await coin_snapshot(session, "ETHUSDT")
                        sol = await coin_snapshot(session, "SOLUSDT")

                        msg = session_section("🌅 한국장 오픈 브리핑")
                        if usd_krw:
                            msg += f"\n💵 달러/원: {usd_krw:,.2f}원"
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"

                        msg += "\n"
                        if sp_fut:
                            msg += f"\n{session_snapshot_line('S&P500 선물', sp_fut)}"
                        if nq_fut:
                            msg += f"\n{session_snapshot_line('나스닥 선물', nq_fut)}"
                        if sox:
                            msg += f"\n{session_snapshot_line('필라델피아 반도체지수', sox)}"
                        if wti:
                            msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                        if dxy:
                            msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"
                        if tnx:
                            msg += f"\n{session_snapshot_line('미10년물 금리', tnx)}"

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n\n😶‍🌫️ 공포탐욕지수: {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n🇰🇷 김치프리미엄: {fmt_pct(premium)}"

                        msg += f"\n\n📌 오늘 핵심:\n{final_kr_open_focus(safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), usd_krw or 0, safe_pct_from_snapshot(wti))}"
                        msg += f"\n\n🧭 시장 분위기: {session_mood(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += "\n⚠️ 장 초반 추격매수보다 거래량 확인이 우선."
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        state.briefing_sent_dates["08"] = now.date()

                if is_us_market_premarket_day(now) and is_exact_time(now, 21, 30):
                    key = "us_pre_2130"
                    if state.market_session_sent_dates.get(key) != now.date():
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        btc_text, _ = await btc_line(session)
                        eth = await coin_snapshot(session, "ETHUSDT")
                        sol = await coin_snapshot(session, "SOLUSDT")

                        msg = session_section("🌆 미국장 프리뷰")
                        if sp_fut:
                            msg += f"\n{session_snapshot_line('S&P500 선물', sp_fut)}"
                        if nq_fut:
                            msg += f"\n{session_snapshot_line('나스닥 선물', nq_fut)}"
                        if sox:
                            msg += f"\n{session_snapshot_line('필라델피아 반도체지수', sox)}"
                        if wti:
                            msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                        if dxy:
                            msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"
                        if tnx:
                            msg += f"\n{session_snapshot_line('미10년물 금리', tnx)}"
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"

                        msg += f"\n\n📌 오늘 핵심:\n{final_us_pre_focus(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(dxy), safe_pct_from_snapshot(tnx), safe_pct_from_snapshot(wti))}"
                        msg += f"\n\n🧭 미국장 분위기: {session_mood(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += "\n\n체크할 것:\n- S&P500·나스닥 초반 방향\n- 반도체지수 수급\n- 달러·금리 동반 상승 여부\n- BTC 80K/79K 반응"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

                if is_us_market_close_day(now) and is_exact_time(now, 5, 0):
                    key = "us_close_0500"
                    if state.market_session_sent_dates.get(key) != now.date():
                        spx = await get_yahoo_snapshot(session, "%5EGSPC")
                        ixic = await get_yahoo_snapshot(session, "%5EIXIC")
                        dji = await get_yahoo_snapshot(session, "%5EDJI")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)
                        eth = await coin_snapshot(session, "ETHUSDT")
                        sol = await coin_snapshot(session, "SOLUSDT")

                        msg = session_section("🌙 미국장 마감 정리")
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"

                        msg += "\n"
                        if spx:
                            msg += f"\n{session_snapshot_line('S&P500', spx)}"
                        if ixic:
                            msg += f"\n{session_snapshot_line('나스닥', ixic)}"
                        if dji:
                            msg += f"\n{session_snapshot_line('다우', dji)}"
                        if sox:
                            msg += f"\n{session_snapshot_line('필라델피아 반도체', sox)}"
                        if wti:
                            msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                        if dxy:
                            msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"
                        if tnx:
                            msg += f"\n{session_snapshot_line('미10년물 금리', tnx)}"

                        msg += f"\n\n📌 오늘 핵심:\n{final_us_close_focus(safe_pct_from_snapshot(spx), safe_pct_from_snapshot(ixic), safe_pct_from_snapshot(dji), safe_pct_from_snapshot(sox), safe_pct_from_snapshot(dxy), safe_pct_from_snapshot(tnx), safe_pct_from_snapshot(wti))}"
                        msg += f"\n\n🧭 미국장 분위기: {session_mood(safe_pct_from_snapshot(spx), safe_pct_from_snapshot(ixic), safe_pct_from_snapshot(dji), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += "\n\n체크할 것:\n- 한국장 반도체 대형주 수급\n- 환율과 외국인 매매\n- BTC 80K 재안착 여부"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

            except Exception:
                logging.exception("market_session_scheduler 오류")
            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_SESSION_CHECK_SECONDS - int(elapsed)))

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
                            await safe_send(bot, msg, disable_preview=True)
                            state.touch_cooldown(signal_key, now)
                    state.last_fng_zone = zone
            except Exception:
                logging.exception("fear_greed_monitor 오류")

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
                                f"글로벌 BTC: {binance_usdt:,.0f} USDT"
                            )
                            await safe_send(bot, msg, disable_preview=True)
                            state.touch_cooldown(signal_key, now)
                    state.last_kimchi_zone = zone
            except Exception:
                logging.exception("kimchi_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, KIMCHI_CHECK_SECONDS - int(elapsed)))
# ============================================================
# LIVE MARKET ROOM QUALITY + MEDIA
# ============================================================

LIVE_NEWS_DAILY_LIMIT = 48
LIVE_NEWS_MAX_PER_SCAN = 1
LIVE_NEWS_MIN_INTERVAL = timedelta(minutes=25)
LIVE_NIGHT_NEWS_MIN_INTERVAL = timedelta(minutes=35)
LIVE_TITLE_SIMILARITY_BLOCK_HOURS = 18
LIVE_TITLE_SIMILARITY_THRESHOLD = 0.52
LIVE_RECAP_HOURS = (9, 13, 18, 23)
BTC_LEVEL_ALERT_COOLDOWN_SEC = 45 * 60

MARKET_IMPACT_TERMS = (
    "nasdaq", "s&p", "dow", "stock", "shares", "pre-market", "after hours", "earnings", "guidance",
    "nvidia", "tesla", "apple", "microsoft", "meta", "amazon", "google", "alphabet", "broadcom", "micron",
    "fed", "cpi", "ppi", "interest rate", "rate cut", "treasury", "yield",
    "oil", "wti", "brent", "dollar", "dxy", "gold", "copper",
    "iran", "israel", "hormuz", "missile", "strike", "ceasefire", "sanction", "nuclear",
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "etf", "liquidation", "crypto",
    "semiconductor", "ai", "data center", "cloud", "chip",
    "나스닥", "다우", "주가", "증시", "선물", "프리장", "시간외", "실적", "가이던스",
    "엔비디아", "테슬라", "애플", "마이크로소프트", "메타", "아마존", "구글", "알파벳", "브로드컴", "마이크론",
    "연준", "금리", "물가", "국채", "수익률",
    "유가", "원유", "달러", "금", "구리",
    "이란", "이스라엘", "호르무즈", "미사일", "공습", "휴전", "제재", "핵",
    "비트코인", "이더리움", "솔라나", "ETF", "청산", "코인",
    "반도체", "AI", "데이터센터", "클라우드", "칩",
    "삼성전자", "하이닉스", "SK하이닉스", "코스피", "환율", "외국인",

    "ionq", "rigetti", "qbts", "quantum", "quantum computing", "양자", "양자컴퓨터", "아이온큐", "리게티",
    "palantir", "pltr", "팔란티어",
    "amd", "arm", "tsmc", "asml", "oracle", "orcl", "coreweave", "dell", "supermicro", "smci",
    "vertiv", "vst", "ge vernova", "nuclear", "uranium", "power grid", "electricity", "energy demand",
    "원전", "우라늄", "전력", "전력망", "전기", "에너지 수요", "버티브",
    "lockheed", "boeing", "defense", "drone", "ship", "shipping", "tariff", "rare earth", "battery",
    "국방", "방산", "드론", "해운", "관세", "희토류", "배터리", "2차전지",
    "현대차", "기아", "LG에너지솔루션", "두산에너빌리티", "한화에어로스페이스",
)

LIVE_HARD_BLOCK_TERMS = (
    "coinmarketcap",
    "price chart", "market cap", "가격, 차트", "시가총액",
    "swift student challenge", "google for korea", "구글 포 코리아",
    "google news", "구글 뉴스", "via google news",
    "맛집", "학생", "challenge", "행사", "성과급 논란",
    "webinar", "summit", "booth", "fan meeting", "박람회", "컨퍼런스", "세미나", "축제", "초청행사", "티켓 오픈",
    "migrant worker", "migrant workers", "shelter", "laboring", "human rights", "refugee",
    "celebrity", "sports", "movie", "music", "crime", "accident", "weather",
    "이주 노동자", "노동자", "쉼터", "인권", "난민", "연예", "스포츠", "범죄", "사고", "날씨",
    "사설", "칼럼", "인터뷰", "기고",
)

LIVE_CATEGORY_FEEDS = (
    ("🇺🇸", "미국", "https://news.google.com/rss/search?q=(Nvidia%20OR%20Tesla%20OR%20Apple%20OR%20Meta%20OR%20Microsoft%20OR%20Amazon%20OR%20Google%20OR%20Nasdaq%20OR%20Fed%20OR%20CPI%20OR%20earnings%20OR%20guidance%20OR%20semiconductor%20OR%20AI%20OR%20IonQ%20OR%20Rigetti%20OR%20Palantir%20OR%20AMD%20OR%20Oracle%20OR%20CoreWeave%20OR%20Vertiv%20OR%20nuclear%20OR%20defense)&hl=ko&gl=KR&ceid=KR:ko"),
    ("🌍", "세계", "https://news.google.com/rss/search?q=(oil%20OR%20WTI%20OR%20dollar%20OR%20Iran%20OR%20Israel%20OR%20Hormuz%20OR%20missile%20OR%20ceasefire%20OR%20sanction%20OR%20China%20OR%20supply%20chain%20OR%20tariff%20OR%20rare%20earth%20OR%20shipping%20OR%20uranium%20OR%20power%20grid)&hl=ko&gl=KR&ceid=KR:ko"),
    ("🇰🇷", "한국", "https://news.google.com/rss/search?q=(%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90%20OR%20SK%ED%95%98%EC%9D%B4%EB%8B%89%EC%8A%A4%20OR%20%EC%BD%94%EC%8A%A4%ED%94%BC%20OR%20%ED%99%98%EC%9C%A8%20OR%20%EC%99%B8%EA%B5%AD%EC%9D%B8%20OR%20%EB%B0%98%EB%8F%84%EC%B2%B4%20OR%20%ED%95%9C%ED%99%94%EC%97%90%EC%96%B4%EB%A1%9C%EC%8A%A4%ED%8E%98%EC%9D%B4%EC%8A%A4%20OR%20%EB%91%90%EC%82%B0%EC%97%90%EB%84%88%EB%B9%8C%EB%A6%AC%ED%8B%B0%20OR%20LG%EC%97%90%EB%84%88%EC%A7%80%EC%86%94%EB%A3%A8%EC%85%98%20OR%20%ED%98%84%EB%8C%80%EC%B0%A8%20OR%20%EA%B8%B0%EC%95%84)&hl=ko&gl=KR&ceid=KR:ko"),
    ("🟠", "코인", "https://news.google.com/rss/search?q=(Bitcoin%20OR%20BTC%20OR%20Ethereum%20OR%20ETF%20OR%20crypto%20liquidation%20OR%20Solana%20OR%20stablecoin%20OR%20tokenization)&hl=ko&gl=KR&ceid=KR:ko"),
)


def html_clean(value: str, limit: int = 500) -> str:
    value = value or ""
    value = html.unescape(value)
    value = re.sub(r"&#x[0-9a-fA-F]{1,8};", " ", value)
    value = re.sub(r"&#\d{1,8};", " ", value)
    value = value.replace("\xa0", " ").replace("&nbsp;", " ")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"https?://\S+", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit].strip()


def has_market_impact(title: str, summary: str) -> bool:
    text_value = f"{title} {summary}".lower()
    return any(k.lower() in text_value for k in MARKET_IMPACT_TERMS)


def is_hard_blocked_live_news(title: str, summary: str, link: str = "") -> bool:
    text_value = f"{title} {summary}".lower()
    if any(k.lower() in text_value for k in LIVE_HARD_BLOCK_TERMS):
        return True
    try:
        host = urlparse(link or "").netloc.lower()
    except Exception:
        host = ""
    if "coinmarketcap.com" in host or "coinpaprika.com" in host or "coingecko.com" in host:
        return True
    return False


def strip_news_source_tail(title: str) -> str:
    title = html_clean(title, 180)
    if " - " in title:
        title = title.rsplit(" - ", 1)[0].strip()
    return title


def live_news_dedup_hashes(title: str, link: str) -> Tuple[str, str]:
    nu = normalize_news_url(link)
    url_hash = hashlib.sha256(nu.encode("utf-8")).hexdigest()
    tn = normalize_title_for_dedup(html_clean(title or "", 400))
    combo_hash = hashlib.sha256(f"{tn}|{nu}".encode("utf-8")).hexdigest()
    return url_hash, combo_hash


def is_duplicate_live_news(state: State, title: str, link: str, now: datetime) -> bool:
    url_hash, combo_hash = live_news_dedup_hashes(title, link)
    if url_hash in state.live_news_seen_set or combo_hash in state.live_news_seen_set:
        return True
    if not getattr(state, "live_recent_titles", None):
        return False
    cutoff = now - timedelta(hours=LIVE_TITLE_SIMILARITY_BLOCK_HOURS)
    while state.live_recent_titles and state.live_recent_titles[0][0] < cutoff:
        state.live_recent_titles.popleft()
    cand = strip_news_source_tail(title or "")
    for _, old_title in state.live_recent_titles:
        if title_similarity(cand, old_title) >= LIVE_TITLE_SIMILARITY_THRESHOLD:
            return True
    return False


def remember_live_news_hashes(state: State, title: str, link: str) -> None:
    url_hash, combo_hash = live_news_dedup_hashes(title, link)
    for nid in (url_hash, combo_hash):
        if len(state.live_news_seen_ids) == state.live_news_seen_ids.maxlen:
            old = state.live_news_seen_ids.popleft()
            state.live_news_seen_set.discard(old)
        state.live_news_seen_ids.append(nid)
        state.live_news_seen_set.add(nid)


def mostly_english(text_value: str) -> bool:
    if not text_value:
        return False
    letters = re.findall(r"[A-Za-z]", text_value)
    korean = re.findall(r"[가-힣]", text_value)
    return len(letters) > max(20, len(korean) * 2)


async def ensure_korean_text(session: aiohttp.ClientSession, value: str) -> str:
    value = html_clean(value, 220)
    if not value:
        return ""
    if mostly_english(value):
        value = await translate_to_korean(session, value)
    return html_clean(value, 220)


def live_news_score(title: str, summary: str, category: str, link: str = "") -> int:
    text_low = f"{title} {summary}".lower()
    if is_hard_blocked_live_news(title, summary, link):
        return -100
    if not has_market_impact(title, summary):
        return -30
    score = sum(2 for k in MARKET_IMPACT_TERMS if k.lower() in text_low)
    if category == "미국" and any(k in text_low for k in ("nvidia", "tesla", "earnings", "guidance", "nasdaq", "fed", "cpi", "엔비디아", "테슬라", "실적", "가이던스", "나스닥", "연준", "금리")):
        score += 10
    if category == "세계" and any(k in text_low for k in ("oil", "dollar", "iran", "israel", "hormuz", "missile", "strike", "sanction", "nuclear", "ceasefire", "유가", "달러", "이란", "이스라엘", "호르무즈", "미사일", "공습", "제재", "핵", "휴전")):
        score += 10
    if category == "한국" and any(k in text_low for k in ("삼성전자", "하이닉스", "sk하이닉스", "코스피", "환율", "외국인", "반도체")):
        score += 10
    if category == "코인" and any(k in text_low for k in ("bitcoin", "btc", "ethereum", "eth", "etf", "liquidation", "비트코인", "이더리움", "청산")):
        score += 10
    return score


def is_night_kst(now: datetime) -> bool:
    return 1 <= now.astimezone(KST).hour < 7


def is_live_news_allowed(title: str, summary: str, category: str, now: datetime, link: str = "") -> bool:
    score = live_news_score(title, summary, category, link)
    if score < 8:
        return False
    if is_night_kst(now):
        text_low = f"{title} {summary}".lower()
        night_terms = ("missile", "strike", "war", "hormuz", "oil", "fed", "cpi", "crash", "surge", "liquidation", "미사일", "공습", "전쟁", "호르무즈", "유가", "연준", "금리", "급락", "급등", "청산")
        return score >= 18 and any(k in text_low for k in night_terms)
    return True



SOURCE_MAP = {
    "v.daum.net": "다음뉴스",
    "news.daum.net": "다음뉴스",
    "n.news.naver.com": "네이버뉴스",
    "news.naver.com": "네이버뉴스",
    "yna.co.kr": "연합뉴스",
    "yonhapnewstv.co.kr": "연합뉴스TV",
    "chosun.com": "조선일보",
    "biz.chosun.com": "조선비즈",
    "joongang.co.kr": "중앙일보",
    "donga.com": "동아일보",
    "hankyung.com": "한국경제",
    "mk.co.kr": "매일경제",
    "sedaily.com": "서울경제",
    "edaily.co.kr": "이데일리",
    "khan.co.kr": "경향신문",
    "coindesk.com": "CoinDesk",
    "cointelegraph.com": "Cointelegraph",
    "reuters.com": "Reuters",
    "bloomberg.com": "Bloomberg",
    "cnbc.com": "CNBC",
    "seekingalpha.com": "Seeking Alpha",
    "koreaittimes.com": "Korea IT Times",
}

HIGH_IMPORTANCE_TERMS = (
    "전쟁", "공습", "미사일", "드론", "호르무즈", "봉쇄", "휴전", "제재", "핵",
    "war", "strike", "missile", "hormuz", "ceasefire", "sanction", "nuclear",
    "FOMC", "CPI", "PPI", "금리", "연준", "파월", "ETF 승인", "청산", "급락", "급등",
)
MID_IMPORTANCE_TERMS = (
    "엔비디아", "삼성전자", "SK하이닉스", "하이닉스", "AI", "반도체", "HBM",
    "아이온큐", "양자", "팔란티어", "테슬라", "메타", "구글", "오라클",
    "nvidia", "samsung", "hynix", "semiconductor", "ionq", "quantum", "palantir", "tesla",
)


def compact_source_name(value: str) -> str:
    value = html_clean(value or "", 60)
    if not value:
        return "뉴스"
    low = value.lower()
    if low.startswith("www."):
        low = low[4:]
    if low in SOURCE_MAP:
        return SOURCE_MAP[low]
    for domain, name in SOURCE_MAP.items():
        if domain in low:
            return name
    return value.replace(" - Google News", "").strip()[:40] or "뉴스"


def source_name_from_entry(entry, fallback: str = "Google News") -> str:
    # 1) RSS source title 우선
    source = getattr(entry, "source", None)
    try:
        title = source.get("title")
        if title:
            return compact_source_name(title)
    except Exception:
        pass

    # 2) 링크 도메인 기반 정리
    link = getattr(entry, "link", "") or ""
    try:
        host = urlparse(link).netloc.replace("www.", "")
        if host:
            return compact_source_name(host)
    except Exception:
        pass

    # 3) 제목 뒤의 "- 매체명" 사용
    title = html_clean(getattr(entry, "title", ""), 200)
    if " - " in title:
        return compact_source_name(title.rsplit(" - ", 1)[-1])
    return compact_source_name(fallback)


def extract_entry_image_url(entry) -> Optional[str]:
    # 1) media_content / media_thumbnail
    for attr in ("media_content", "media_thumbnail"):
        rows = getattr(entry, attr, None)
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    url = row.get("url")
                    if url and url.startswith("http"):
                        return url

    # 2) entry.links enclosure / image
    links = getattr(entry, "links", None)
    if isinstance(links, list):
        for row in links:
            if not isinstance(row, dict):
                continue
            href = row.get("href")
            typ = str(row.get("type", "")).lower()
            rel = str(row.get("rel", "")).lower()
            if href and href.startswith("http") and ("image" in typ or rel in ("enclosure", "thumbnail")):
                return href

    # 3) summary 안 img 태그
    summary = getattr(entry, "summary", "") or ""
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', summary)
    if m and m.group(1).startswith("http"):
        return m.group(1)

    return None










async def resolve_entry_image_url(session: aiohttp.ClientSession, entry) -> Optional[str]:
    try:
        image_url = extract_entry_image_url(entry)
        if image_url:
            return image_url
    except Exception:
        pass

    try:
        summary = getattr(entry, "summary", "") or ""
        for marker, quote in (('src="', '"'), ("src='", "'")):
            idx = summary.find(marker)
            if idx >= 0:
                start = idx + len(marker)
                end = summary.find(quote, start)
                if end > start:
                    url = summary[start:end]
                    if url.startswith("http"):
                        return url
    except Exception:
        pass

    return None

def news_importance_label(title: str, summary: str) -> str:
    t = f"{title} {summary}".lower()
    if any(k.lower() in t for k in HIGH_IMPORTANCE_TERMS):
        return "🚨 중요"
    if any(k.lower() in t for k in MID_IMPORTANCE_TERMS):
        return "🟡 체크"
    return "⚪ 참고"


def build_market_impact_line(category: str, title: str, summary: str) -> str:
    t = f"{title} {summary}".lower()

    if any(k in t for k in ("hormuz", "호르무즈", "oil", "wti", "brent", "유가", "원유", "opec", "해협", "선박", "해운")):
        return "유가·해운·인플레 압력으로 번질 수 있어서 위험자산 반응 체크."
    if any(k in t for k in ("iran", "israel", "missile", "strike", "ceasefire", "sanction", "nuclear", "이란", "이스라엘", "미사일", "공습", "휴전", "제재", "핵", "전쟁", "드론")):
        return "지정학 리스크 이슈. 유가·달러·코인 변동성 커질 수 있음."
    if any(k in t for k in ("fed", "fomc", "cpi", "ppi", "interest rate", "rate cut", "powell", "연준", "금리", "물가", "파월", "국채", "수익률", "달러")):
        return "금리·달러 민감 이슈. 나스닥·코인 방향에 바로 영향 줄 수 있음."
    if any(k in t for k in ("ionq", "아이온큐", "rigetti", "리게티", "quantum", "양자")):
        return "양자컴퓨터 테마 수급 체크. 기대감은 크지만 변동성도 큰 구간."
    if any(k in t for k in ("nvidia", "엔비디아", "semiconductor", "반도체", "hbm", "micron", "마이크론", "broadcom", "브로드컴", "amd", "tsmc", "asml", "chip", "칩")):
        return "AI·반도체 수급 이슈. 삼성전자·하이닉스·나스닥까지 같이 봐야함."
    if any(k in t for k in ("oracle", "오라클", "coreweave", "dell", "supermicro", "smci", "cloud", "data center", "데이터센터", "클라우드", "ai infrastructure", "ai 인프라")):
        return "AI 인프라·데이터센터 투자 흐름. 전력·광통신·반도체까지 연결됨."
    if any(k in t for k in ("vertiv", "vst", "ge vernova", "nuclear", "uranium", "power", "electricity", "전력", "원전", "우라늄", "에너지", "전력망")):
        return "AI 전력 수요 테마. 전력·원전·인프라 관련주 반응 체크."
    if any(k in t for k in ("defense", "drone", "lockheed", "boeing", "국방", "방산", "드론", "한화에어로스페이스")):
        return "방산·지정학 수급 이슈. 방산주와 원자재 변동성 같이 봐야함."
    if any(k in t for k in ("shipping", "tariff", "rare earth", "supply chain", "해운", "관세", "희토류", "공급망")):
        return "공급망·관세 이슈. 물가와 기업 마진에 영향 줄 수 있음."
    if any(k in t for k in ("tesla", "테슬라", "apple", "애플", "meta", "메타", "google", "구글", "amazon", "아마존", "microsoft", "마이크로소프트", "palantir", "팔란티어")):
        return "빅테크·성장주 수급 이슈. 나스닥 분위기 같이 확인 필요."
    if any(k in t for k in ("earnings", "guidance", "실적", "가이던스", "eps", "매출")):
        return "실적 이슈. 숫자보다 가이던스와 장 후반 수급이 더 중요."
    if any(k in t for k in ("bitcoin", "btc", "비트코인", "ethereum", "eth", "etf", "청산", "liquidation", "tokenization", "stablecoin", "토큰화", "스테이블코인", "거래소")):
        return "코인 수급 이슈. BTC 가격 반응과 거래량 동반 여부 체크."
    if any(k in t for k in ("samsung", "삼성전자", "hynix", "하이닉스", "kospi", "코스피", "환율", "외국인", "현대차", "기아", "lg에너지솔루션", "두산에너빌리티")):
        return "한국장 수급 이슈. 외국인·환율·반도체 흐름 같이 봐야함."
    return "시장 영향은 가격 반응과 거래량 붙는지 확인 필요."


def build_news_body_line(category: str, title: str, summary: str, impact: str) -> str:
    raw = html_clean(summary, 180)
    if raw and not mostly_english(raw) and raw not in title:
        return raw
    return impact



def clean_news_body_for_message(title: str, summary: str, source: str = "") -> str:
    title_clean = html_clean(strip_news_source_tail(title or ""), 220).strip()
    body = html_clean(summary or "", 260).strip()

    body = re.sub(r"https?://\S+", "", body)
    body = body.replace(" ", " ").replace("... -", "").strip()

    for s in (source, "Google News", "조선일보", "한국경제", "경향신문", "blog.google", "Korea IT Times"):
        if s:
            body = body.replace(str(s), "").strip()

    if title_clean and body:
        body_no_space = re.sub(r"\s+", "", body)
        title_no_space = re.sub(r"\s+", "", title_clean)
        if body_no_space == title_no_space or title_no_space in body_no_space[: len(title_no_space) + 20]:
            body = ""

    if len(body) < 18:
        body = ""

    return body.strip()


def live_news_header(score: int) -> str:
    if score >= 26:
        return "🚨 중요 실시간 시장 이슈"
    if score >= 18:
        return "⚡ 실시간 시장 이슈"
    return "🟡 체크 실시간 시장 이슈"












# ============================================================
# FINAL STABLE ROOM HELPERS
# ============================================================

def compact_section(title: str) -> str:
    return "━━━━━━━━━━━━━━\n" + title + "\n━━━━━━━━━━━━━━"


def snap_pct(snap) -> float:
    try:
        return float(snap[1]) if snap else 0.0
    except Exception:
        return 0.0


def snapshot_line(name: str, snap, digits: int = 2) -> str:
    if not snap:
        return ""
    price, pct = snap
    return f"{move_icon(float(pct))} {name}: {float(price):,.{digits}f} ({fmt_pct(float(pct))})"


def btc_brief_line(price: float, pct: float) -> str:
    return f"{move_icon(pct)} BTC: {price:,.0f} USDT ({fmt_pct(pct)})"


def market_mood_label(*pcts: float) -> str:
    vals = []
    for x in pcts:
        try:
            vals.append(float(x))
        except Exception:
            pass
    if not vals:
        return "🟡 눈치보기 장세"
    avg = sum(vals) / len(vals)
    if avg >= 0.55:
        return "🟢 위험선호"
    if avg <= -0.55:
        return "🔴 리스크오프"
    if max(vals) - min(vals) >= 1.0:
        return "🟡 종목장"
    return "🟡 눈치보기 장세"


def polish_korean_news_text(text_value: str) -> str:
    s = text_value or ""
    fixes = {
        "호르무즈 해협 선박 지원": "호르무즈 해협 선박 지원",
        "유가 압력": "유가 압력",
        "공급망 지연은 여전히 남아 있음": "공급망 지연 리스크는 남아있음",
        "공급망 지연은 여전히 ​​남아 있음": "공급망 지연 리스크는 남아있음",
        "위험자산 반응 체크": "위험자산 반응 확인 필요",
        "빅테크 움직임이라 나스닥 분위기 같이 봐야함.": "AI·빅테크 수급이 나스닥 흐름을 좌우할 수 있음.",
        "시장 영향은 가격 반응 확인하면서 봐야함.": "가격 반응과 거래량 동반 여부 확인 필요.",
    }
    for old, new in fixes.items():
        s = s.replace(old, new)
    s = s.replace("\\n", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def compact_message(msg: str, limit: int = 1500) -> str:
    msg = polish_korean_news_text(msg)
    if len(msg) <= limit:
        return msg
    return msg[:limit - 40].rstrip() + "\n\n…내용 압축됨"


def normalize_news_importance(score: int) -> int:
    try:
        score = int(score)
    except Exception:
        score = 10
    return max(1, min(10, round(score / 3)))


def live_news_header(score: int) -> str:
    if score >= 27:
        return "🚨 시장 속보"
    if score >= 20:
        return "⚡ 실시간 시장 이슈"
    return "🟡 체크할 시장 이슈"


def newsroom_keyword_score(title: str, summary: str, category: str = "") -> int:
    txt = f"{title} {summary} {category}".lower()
    score = 10
    critical = ("fed","fomc","powell","파월","연준","금리","cpi","pce","실업수당","고용","전쟁","공습","미사일","호르무즈","봉쇄","etf","sec","엔비디아","nvidia","hbm","삼성전자","sk하이닉스","btc","비트코인","청산")
    high = ("ai","반도체","데이터센터","cloud","클라우드","oil","wti","brent","유가","원유","달러","dxy","10년물","테슬라","tesla","아이온큐","ionq","양자")
    low = ("맛집","행사","기념","인터뷰","칼럼","opinion","blog","생활","문화")
    score += sum(3 for k in critical if k in txt)
    score += sum(2 for k in high if k in txt)
    score -= sum(2 for k in low if k in txt)
    if any(k in txt for k in ("호르무즈","전쟁","공습","미사일","봉쇄")) and any(k in txt for k in ("유가","원유","wti","brent","석유")):
        score += 5
    if any(k in txt for k in ("엔비디아","nvidia","hbm","데이터센터")):
        score += 4
    if any(k in txt for k in ("cpi","pce","fomc","fed","파월","연준","금리")):
        score += 4
    return max(1, min(35, score))


def clean_news_body_for_message(title: str, summary: str, source: str = "") -> str:
    title_clean = html_clean(strip_news_source_tail(title or ""), 220).strip()
    body = html_clean(summary or "", 300).strip()
    body = re.sub(r"https?://\S+", "", body)
    body = body.replace(" ", " ").replace("…", "...").strip()

    for s in (source, "Google News", "조선일보", "한국경제", "경향신문", "blog.google", "Korea IT Times", "프라임경제", "Reuters", "로이터"):
        if s:
            body = body.replace(str(s), "").strip()

    body = polish_korean_news_text(body)

    if title_clean and body:
        body_no_space = re.sub(r"\s+", "", body)
        title_no_space = re.sub(r"\s+", "", title_clean)
        if body_no_space == title_no_space or title_no_space in body_no_space[: len(title_no_space) + 30]:
            return ""

    if len(body) < 18:
        return ""
    return body.strip()


def related_assets_for_news(title: str, summary: str = "") -> str:
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
    if any(k in txt for k in ("tsmc","台積電","반도체 장비","asml","실리콘 포토닉스")):
        found.append("TSMC · 장비 · 반도체")
    if any(k in txt for k in ("rare earth","희토류","tariff","관세","supply chain")):
        found.append("공급망 · 희토류 · 무역")
    if any(k in txt for k in ("uranium","우라늄","nuclear","원전","pwr")):
        found.append("우라늄 · 원전 · 에너지")

    if found:
        return " / ".join(dict.fromkeys(found[:3]))

    if any(k in txt for k in ("호르무즈","이란","중동","원유","유가","wti","brent","석유","해운","선박","공급망")):
        found = ["WTI · 에너지 · 해운"]
        if any(k in txt for k in ("달러","금리","인플레","물가")):
            found.append("달러 · 금리 · 인플레")
        if any(k in txt for k in ("방산","미사일","군함","드론","전쟁")):
            found.append("방산 · 지정학")
        return " / ".join(found)

    pairs = [
        (("nvidia","엔비디아","gpu","ai칩","hbm"), "NVDA · 반도체 · AI"),
        (("samsung","삼성전자","삼전","sk하이닉스","하이닉스"), "삼성전자 · SK하이닉스 · HBM"),
        (("amd",), "AMD · 반도체"),
        (("google","alphabet","구글","알파벳"), "GOOGL · 클라우드 · AI"),
        (("amazon","아마존","aws"), "AMZN · 클라우드 · 소비"),
        (("microsoft","마이크로소프트","openai"), "MSFT · AI · 클라우드"),
        (("tesla","테슬라","로보택시"), "TSLA · 전기차 · AI"),
        (("ionq","아이온큐","quantum","양자"), "IONQ · 양자컴퓨터"),
        (("gold","금값","금","dxy"), "금 · 달러 · 금리"),
        (("bitcoin","btc","비트코인","crypto","코인"), "BTC · ETH · SOL"),
        (("kospi","코스피","외국인","환율"), "KOSPI · 환율 · 외국인 수급"),
    ]

    found = []
    for keys, label in pairs:
        if any(k in txt for k in keys):
            found.append(label)
    return " / ".join(found[:3]) if found else "시장 전체 · 관련 섹터 확인"


def impact_one_liner(category: str, title: str, summary: str) -> str:
    txt = f"{title} {summary} {category}".lower()

    if any(k in txt for k in ("호르무즈","유가","원유","wti","brent","석유","해운")):
        return "유가·해운·인플레 압력으로 번질 수 있어 위험자산 반응 확인 필요."
    if any(k in txt for k in ("엔비디아","nvidia","hbm","반도체","데이터센터")):
        return "AI·반도체 수급에 직접 연결될 수 있어 삼성전자·SK하이닉스 흐름 확인 필요."
    if any(k in txt for k in ("fed","fomc","파월","연준","금리","cpi","pce","실업수당","고용")):
        return "금리 기대와 달러 방향을 바꿀 수 있어 나스닥·BTC 동반 반응 확인 필요."
    if any(k in txt for k in ("bitcoin","btc","비트코인","etf","sec")):
        return "비트코인 수급에 직접 영향 줄 수 있어 현물 ETF·거래량 반응 확인 필요."

    try:
        return polish_korean_news_text(build_market_impact_line(category, title, summary))
    except Exception:
        return "가격 반응과 거래량 동반 여부 확인 필요."


def kr_open_focus(nq_pct: float, sox_pct: float, usd_krw: float) -> str:
    if nq_pct >= 0.4 or sox_pct >= 0.8:
        return "미국 AI·반도체 강세 영향 이어지는 중.\n오늘 한국장은 외국인 수급 + 반도체 흐름 먼저 체크해야함."
    if nq_pct <= -0.4:
        return "나스닥 쪽 부담이 남아있음.\n장 초반 추격보다 반도체 방어력 확인이 먼저."
    if usd_krw >= 1450:
        return "환율이 높은 구간이라 외국인 수급이 오늘 방향을 정할 가능성 큼."
    return "큰 방향보다 수급 확인이 먼저.\n장 초반 30분은 무리하지 않는 구간."


def us_pre_focus(nq_pct: float, dxy_pct: float, tnx_pct: float) -> str:
    if nq_pct >= 0.4:
        return "나스닥 선물이 강함.\n오늘도 AI·빅테크 쪽으로 돈이 붙는지 봐야함."
    if nq_pct <= -0.4:
        return "나스닥 선물이 약함.\n장 초반 기술주 매도 압력부터 확인 필요."
    if dxy_pct > 0.25 or tnx_pct > 0.25:
        return "달러·금리 부담이 있어서 초반 변동성 커질 수 있음."
    return "큰 방향은 아직 안 나왔지만,\n첫 30분 수급 붙는 쪽이 오늘 흐름을 만들 가능성 큼."


def us_close_focus(nq_pct: float, sox_pct: float, nvda_pct: float) -> str:
    if sox_pct >= 1.0 or nvda_pct >= 1.5:
        return "AI·반도체 쪽으로 다시 수급 몰리는 분위기.\n특히 HBM·데이터센터 관련주 강세 계속 이어지는 중."
    if nq_pct >= 0.5:
        return "나스닥 상대강세.\nAI·빅테크 수급이 한국장까지 이어질지 체크."
    if nq_pct <= -0.5:
        return "기술주 쪽 부담이 남아있음.\n한국장은 반도체 방어력부터 확인 필요."
    return "지수별 온도차가 있는 장.\n한국장은 환율과 반도체 수급이 방향을 정할 가능성."


def kr_close_focus(kospi_pct: float, kosdaq_pct: float, usd_krw: float) -> str:
    if kospi_pct >= 0.5 and kosdaq_pct >= 0.3:
        return "지수 동반 강세. 내일도 외국인 수급과 반도체 흐름이 핵심."
    if kospi_pct >= 0.5 and kosdaq_pct < 0:
        return "대형주 쪽으로 돈이 몰린 장. 삼성전자·하이닉스 수급 확인 필요."
    if kospi_pct < 0 and kosdaq_pct < 0:
        return "전반적으로 힘이 빠진 장. 환율과 미국 선물 반응이 더 중요해짐."
    if usd_krw >= 1450:
        return "환율 부담이 남아있어서 종목보다 수급 방향 확인이 우선."
    return "지수보다 종목장 성격이 강한 하루. 강한 섹터만 살아남는 흐름."


async def build_live_news_message(
    session: aiohttp.ClientSession,
    category_emoji: str,
    category: str,
    title: str,
    summary: str,
    source: str,
    link: str = "",
) -> str:
    title_clean = strip_news_source_tail(title or "")
    title_ko = await ensure_korean_text(session, polish_korean_news_text(title_clean))

    try:
        body = clean_news_body_for_message(title_clean, summary, source)
    except Exception:
        body = ""

    impact = impact_one_liner(category, title_clean, summary)

    body_ko = ""
    if body and not mostly_english(body):
        try:
            body_ko = await ensure_korean_text(session, body)
            body_ko = polish_korean_news_text(body_ko)
        except Exception:
            body_ko = ""

    body_norm = re.sub(r"\s+", "", body_ko or "")
    title_norm = re.sub(r"\s+", "", title_ko or "")
    impact_norm = re.sub(r"\s+", "", impact or "")

    show_body = bool(body_ko)
    if body_norm == title_norm or body_norm == impact_norm:
        show_body = False
    if impact_norm and body_norm and impact_norm in body_norm:
        show_body = False

    try:
        raw_score = live_news_score(title_clean, summary, category, link)
    except Exception:
        raw_score = 18

    raw_score = max(raw_score, newsroom_keyword_score(title_clean, summary, category))
    importance = normalize_news_importance(raw_score)
    header = live_news_header(raw_score)
    related = related_assets_for_news(title_clean, summary)

    btc_line_text = ""
    try:
        btc = await get_market_ticker(session, "BTCUSDT")
        if btc:
            btc_price = float(btc["lastPrice"])
            btc_pct = float(btc["priceChangePercent"])
            btc_line_text = (
                "\n\n📊 현재 BTC:\n"
                f"{btc_price:,.0f} USDT ({fmt_pct(btc_pct)})"
            )
    except Exception:
        btc_line_text = ""

    msg = compact_section(header)
    msg += f"\n{category_emoji} {title_ko}\n"

    if show_body:
        msg += f"\n{body_ko}\n"

    msg += f"\n🔥 중요도: {importance}/10"
    msg += f"\n🏷 관련: {related}"
    msg += f"\n\n📌 시장 영향:\n{impact}"
    msg += btc_line_text

    return compact_message(msg, limit=1500)

async def send_news_card(bot: Bot, text: str, image_url: Optional[str] = None) -> None:
    if image_url:
        try:
            await bot.send_photo(chat_id=CHANNEL_ID, photo=image_url, caption=text[:1024], parse_mode=None)
            return
        except Exception as e:
            if is_telegram_auth_failure(e):
                logging.error("Telegram 토큰 인증 실패. BotFather에서 새 토큰 발급 후 TELEGRAM_TOKEN 교체 필요.")
                return
            logging.warning("이미지 전송 실패. 텍스트로 대체 image=%s", image_url)
    await safe_send(bot, text, disable_preview=True)



async def btc_key_level_monitor(bot: Bot, state: State) -> None:
    if not hasattr(state, "btc_key_level_last"):
        state.btc_key_level_last = {}

    levels = [80000, 79000, 78000, 75000]

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ticker = await get_market_ticker(session, "BTCUSDT")
                if ticker:
                    price = float(ticker["lastPrice"])
                    pct = float(ticker["priceChangePercent"])
                    now = utc_now()

                    for level in levels:
                        key = f"btc_level_{level}"
                        last = state.btc_key_level_last.get(key, {})
                        prev_side = last.get("side")
                        sent_at = last.get("sent_at")
                        buffer = max(80, level * 0.001)
                        if price >= level + buffer:
                            side = "above"
                        elif price <= level - buffer:
                            side = "below"
                        else:
                            continue

                        cooldown_ok = True
                        if sent_at:
                            try:
                                cooldown_ok = (now - sent_at).total_seconds() >= BTC_LEVEL_ALERT_COOLDOWN_SEC
                            except Exception:
                                cooldown_ok = True

                        if prev_side and prev_side != side and cooldown_ok:
                            direction = "회복" if side == "above" else "이탈"
                            icon = "🟢" if side == "above" else "🔴"
                            msg = (
                                compact_section("⚠️ BTC 핵심 레벨 감지")
                                + f"\n{icon} BTC {level:,.0f} USDT {direction}"
                                + f"\n현재가: {price:,.0f} USDT ({fmt_pct(pct)})"
                                + "\n\n📌 시장 영향:"
                                + "\n단기 변동성 확대 구간. 추격보다 거래량 동반 여부 확인 필요."
                            )
                            await safe_send(bot, msg, disable_preview=True)
                            state.btc_key_level_last[key] = {"side": side, "sent_at": now}
                        elif not prev_side:
                            state.btc_key_level_last[key] = {"side": side, "sent_at": None}

            except Exception:
                logging.exception("btc_key_level_monitor 오류")

            await asyncio.sleep(60)

async def btc_move_chart_monitor(bot: Bot, state: State) -> None:
    if not hasattr(state, "btc_move_chart_last_sent"):
        state.btc_move_chart_last_sent = None
        state.btc_move_chart_last_side = None

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                candles = await get_okx_btc_candles(session, bar="5m", limit=36)
                if len(candles) >= 8:
                    start_price = candles[-7][1]
                    last_price = candles[-1][1]
                    move_pct = ((last_price - start_price) / start_price * 100.0) if start_price else 0.0

                    side = None
                    if move_pct <= -1.2:
                        side = "drop"
                    elif move_pct >= 1.2:
                        side = "pump"

                    now = utc_now()
                    cooldown_ok = True
                    if state.btc_move_chart_last_sent:
                        cooldown_ok = (now - state.btc_move_chart_last_sent).total_seconds() >= 60 * 30

                    if side and cooldown_ok:
                        labels = [str(i + 1) for i in range(len(candles[-18:]))]
                        values = [round(x[1], 2) for x in candles[-18:]]

                        chart = {
                            "type": "line",
                            "data": {
                                "labels": labels,
                                "datasets": [{
                                    "label": "BTC 5m",
                                    "data": values,
                                    "fill": False,
                                    "tension": 0.25
                                }]
                            },
                            "options": {
                                "plugins": {
                                    "legend": {"display": False},
                                    "title": {"display": True, "text": "BTC 5m chart"}
                                }
                            }
                        }

                        import json
                        from urllib.parse import quote
                        chart_url = "https://quickchart.io/chart?width=900&height=500&c=" + quote(json.dumps(chart, ensure_ascii=False))

                        icon = "🔴" if side == "drop" else "🟢"
                        word = "급락" if side == "drop" else "급등"
                        msg = (
                            compact_section(f"{icon} BTC {word} 감지")
                            + f"\\n최근 30분 변동률: {fmt_pct(move_pct)}"
                            + f"\\n현재가: {last_price:,.0f} USDT"
                            + "\\n\\n📌 체크:"
                            + "\\n거래량 동반 여부와 80K/79K 레벨 반응 확인."
                        )

                        skip_cooldown_bump = False
                        try:
                            await bot.send_photo(chat_id=CHANNEL_ID, photo=chart_url, caption=msg[:1024], parse_mode=None)
                        except Exception as e:
                            if is_telegram_auth_failure(e):
                                logging.error("Telegram 토큰 인증 실패. BotFather에서 새 토큰 발급 후 TELEGRAM_TOKEN 교체 필요.")
                                skip_cooldown_bump = True
                            else:
                                await safe_send(bot, msg, disable_preview=True)

                        if not skip_cooldown_bump:
                            state.btc_move_chart_last_sent = now
                            state.btc_move_chart_last_side = side

            except Exception:
                logging.exception("btc_move_chart_monitor 오류")

            await asyncio.sleep(120)


async def live_news_monitor(bot: Bot, state: State) -> None:
    if not hasattr(state, "live_news_seen_set"):
        state.live_news_seen_ids = deque(maxlen=5000)
        state.live_news_seen_set = set()
        state.live_last_sent_at = None
        state.live_news_daily_date = None
        state.live_news_daily_count = 0
        state.live_recent_items = deque(maxlen=30)
        state.live_recent_titles = deque(maxlen=160)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now = utc_now()
                kst_today = now_kst().date()
                if state.live_news_daily_date != kst_today:
                    state.live_news_daily_date = kst_today
                    state.live_news_daily_count = 0
                sent_this_scan = 0
                min_interval = LIVE_NIGHT_NEWS_MIN_INTERVAL if is_night_kst(now) else LIVE_NEWS_MIN_INTERVAL
                if state.live_last_sent_at and now - state.live_last_sent_at < min_interval:
                    await asyncio.sleep(NEWS_CHECK_SECONDS)
                    continue
                for category_emoji, category, feed_url in LIVE_CATEGORY_FEEDS:
                    if state.live_news_daily_count >= LIVE_NEWS_DAILY_LIMIT or sent_this_scan >= LIVE_NEWS_MAX_PER_SCAN:
                        break
                    feed = await fetch_rss(session, feed_url)
                    entries = list(getattr(feed, "entries", []) or [])[:8] if feed else []
                    candidates = []
                    for entry in entries:
                        raw_title = getattr(entry, "title", "") or ""
                        raw_summary = getattr(entry, "summary", "") or ""
                        raw_link = getattr(entry, "link", "") or raw_title
                        if is_duplicate_live_news(state, raw_title, raw_link, now):
                            continue
                        if not is_live_news_allowed(raw_title, raw_summary, category, now, raw_link):
                            continue
                        score = live_news_score(raw_title, raw_summary, category, raw_link)
                        candidates.append((score, entry, raw_title, raw_summary, raw_link))
                    if not candidates:
                        continue
                    candidates.sort(key=lambda x: x[0], reverse=True)
                    score, entry, raw_title, raw_summary, raw_link = candidates[0]
                    source = source_name_from_entry(entry)
                    image_url = await resolve_entry_image_url(session, entry)
                    msg = await build_live_news_message(
                        session, category_emoji, category, raw_title, raw_summary, source, raw_link
                    )
                    await send_news_card(bot, msg, image_url=image_url)
                    remember_live_news_hashes(state, raw_title, raw_link)
                    state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                    state.live_recent_items.append((now, category_emoji, strip_news_source_tail(raw_title), source, score))
                    state.live_last_sent_at = now
                    state.live_news_daily_count += 1
                    sent_this_scan += 1
                await asyncio.sleep(NEWS_CHECK_SECONDS)
            except Exception:
                logging.exception("live_news_monitor 오류")
                await asyncio.sleep(NEWS_CHECK_SECONDS)


async def daily_digest_scheduler(bot: Bot, state: State) -> None:
    if not hasattr(state, "digest_sent_dates"):
        state.digest_sent_dates = {}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now = now_kst()
                key_morning = f"night_digest:{now.date()}"
                key_evening = f"day_digest:{now.date()}"
                if now.hour == 7 and now.minute < 5 and state.digest_sent_dates.get(key_morning) != now.date():
                    btc = await get_market_ticker(session, "BTCUSDT")
                    eth = await get_market_ticker(session, "ETHUSDT")
                    sol = await get_market_ticker(session, "SOLUSDT")
                    lines = [f"{now.month}/{now.day} 밤 사이 있었던 일"]
                    idx = 1
                    for name, snap in (("BTC", btc), ("ETH", eth), ("SOL", sol)):
                        if snap:
                            lines.append(f"{idx}. {name} {float(snap['lastPrice']):,.0f} USDT ({fmt_pct(float(snap['priceChangePercent']))})")
                            idx += 1
                    lines.append(f"{idx}. 미국 선물·유가·달러 흐름 체크")
                    idx += 1
                    lines.append(f"{idx}. 한국장은 반도체·환율 영향 먼저 봐야함")
                    await safe_send(bot, "\n\n".join(lines), disable_preview=True)
                    state.digest_sent_dates[key_morning] = now.date()
                if now.hour == 18 and now.minute < 5 and state.digest_sent_dates.get(key_evening) != now.date():
                    msg = f"{now.month}/{now.day} 오늘 있었던 일\n\n1. 한국장은 반도체·환율 흐름이 핵심\n\n2. 미국장 전에는 나스닥 선물 먼저 확인\n\n3. 코인은 BTC 지지선 유지 여부가 중요\n\n4. 유가·달러 움직이면 위험자산 같이 흔들릴 수 있음"
                    await safe_send(bot, msg, disable_preview=True)
                    state.digest_sent_dates[key_evening] = now.date()
                await asyncio.sleep(60)
            except Exception:
                logging.exception("daily_digest_scheduler 오류")
                await asyncio.sleep(60)


async def macro_pulse_monitor(bot: Bot, state: State) -> None:
    while True:
        await asyncio.sleep(30 * 60)


async def live_recap_scheduler(bot: Bot, state: State) -> None:
    if not hasattr(state, "recap_sent_keys"):
        state.recap_sent_keys = set()
    while True:
        try:
            now = now_kst()
            if now.hour in LIVE_RECAP_HOURS and now.minute < 5:
                key = f"recap:{now.date()}:{now.hour}"
                if key not in state.recap_sent_keys and hasattr(state, "live_recent_items"):
                    items = list(state.live_recent_items)[-8:]
                    if items:
                        top = sorted(items, key=lambda x: x[-1], reverse=True)[:3]
                        lines = ["👀 지금 시장에서 많이 보는 것"]
                        for i, (_ts, emoji, title, source, _score) in enumerate(top, 1):
                            lines.append(f"{i}. {emoji} {strip_news_source_tail(title)}\n {source}")
                        await safe_send(bot, "\n\n".join(lines), disable_preview=True)
                        state.recap_sent_keys.add(key)
            await asyncio.sleep(60)
        except Exception:
            logging.exception("live_recap_scheduler 오류")
            await asyncio.sleep(60)

# ============================================================
# RUNTIME
# ============================================================

def resolve_telegram_token() -> Tuple[str, str]:
    keys = ("TELEGRAM_TOKEN", "BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN")
    summary = []
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
            logging.warning("토큰을 %s 에서 사용 중입니다. 가능하면 TELEGRAM_TOKEN 으로 통일하세요.", key)
        return stripped, key

    logging.error("봇 토큰 미설정 [%s]", " | ".join(summary))
    return "", ""


async def railway_port_health_server() -> None:
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
    logging.info("PORT=%s HTTP 헬스 응답 시작", port_s)
    await asyncio.Future()


async def liquidation_monitor(bot: Bot, state: State) -> None:
    cooldown_key_long = "liquidation:long"
    cooldown_key_short = "liquidation:short"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = "https://fapi.binance.com/fapi/v1/allForceOrders"
                async with session.get(
                    url,
                    params={"symbol": "BTCUSDT", "limit": 50},
                    timeout=12,
                    headers=REQUEST_HEADERS,
                ) as response:
                    if response.status != 200:
                        await asyncio.sleep(60)
                        continue
                    data = await response.json()

                if not isinstance(data, list):
                    await asyncio.sleep(60)
                    continue

                total_long = 0.0
                total_short = 0.0

                for row in data:
                    price = float(row.get("price") or 0)
                    qty = float(row.get("origQty") or row.get("executedQty") or 0)
                    notional = price * qty
                    side = row.get("side")

                    if side == "SELL":
                        total_long += notional
                    elif side == "BUY":
                        total_short += notional

                now = utc_now()

                if total_long >= 5_000_000 and not state.is_on_cooldown(cooldown_key_long, now):
                    await safe_send(
                        bot,
                        (
                            "💥 [롱 청산 감지]\n"
                            f"규모: {total_long:,.0f} USDT\n\n"
                            "관찰: 하방 압력이 한 번 크게 나온 구간.\n"
                            "리스크: 반등이 약하면 추가 하락 가능.\n"
                            "대응: 바로 추격보다 반등 강도 확인."
                        ),
                        disable_preview=True,
                    )
                    state.touch_cooldown(cooldown_key_long, now)

                if total_short >= 5_000_000 and not state.is_on_cooldown(cooldown_key_short, now):
                    await safe_send(
                        bot,
                        (
                            "🔥 [숏 청산 감지]\n"
                            f"규모: {total_short:,.0f} USDT\n\n"
                            "관찰: 위로 강제 매수 물량이 나온 구간.\n"
                            "리스크: 급등 직후 위꼬리 가능.\n"
                            "대응: 돌파 유지 여부 먼저 확인."
                        ),
                        disable_preview=True,
                    )
                    state.touch_cooldown(cooldown_key_short, now)

            except Exception:
                logging.exception("liquidation_monitor 오류")

            await asyncio.sleep(60)


def market_direction(funding: float, oi_change: float, imbalance: float) -> str:
    if funding > 0.05 and oi_change > 3:
        return "롱 과열 → 숏 우위"
    if funding < -0.05 and oi_change > 3:
        return "숏 과열 → 롱 우위"
    if imbalance > 1.8:
        return "매수 우위"
    if imbalance < 0.6:
        return "매도 우위"
    return "중립"


def build_position_message(direction: str) -> str:
    return (
        "🎯 [포지션 판단]\n\n"
        f"{direction}\n\n"
        "추격 ❌\n"
        "눌림/돌파 유지 확인 ⭕"
    )



async def ops_health_monitor(bot: Bot, state: State) -> None:
    if not hasattr(state, "ops_health_last_sent"):
        state.ops_health_last_sent = None

    while True:
        try:
            now = utc_now()
            if state.ops_health_last_sent is None or (now - state.ops_health_last_sent).total_seconds() >= 60 * 60 * 6:
                logging.info("ops_health_monitor 정상 작동: live/news/briefing/level monitors running")
                state.ops_health_last_sent = now
        except Exception:
            logging.exception("ops_health_monitor 오류")

        await asyncio.sleep(600)


async def overnight_recap_scheduler(bot: Bot, state: State) -> None:
    if not hasattr(state, "overnight_recap_sent_dates"):
        state.overnight_recap_sent_dates = {}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now = now_kst()
                if now.hour == 7 and now.minute == 10:
                    key = "overnight_0710"
                    if state.overnight_recap_sent_dates.get(key) != now.date():
                        btc = await get_market_ticker(session, "BTCUSDT")
                        eth = await get_market_ticker(session, "ETHUSDT")
                        sol = await get_market_ticker(session, "SOLUSDT")
                        nq = await get_yahoo_snapshot(session, "NQ%3DF")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        btc_price = float(btc["lastPrice"]) if btc else 0.0
                        btc_pct = float(btc["priceChangePercent"]) if btc else 0.0
                        eth_price = float(eth["lastPrice"]) if eth else 0.0
                        eth_pct = float(eth["priceChangePercent"]) if eth else 0.0
                        sol_price = float(sol["lastPrice"]) if sol else 0.0
                        sol_pct = float(sol["priceChangePercent"]) if sol else 0.0
                        msg = session_section(f"{now.month}/{now.day} 밤 사이 있었던 일")
                        if btc:
                            msg += f"\n1. BTC {btc_price:,.0f} USDT ({fmt_pct(btc_pct)})"
                        if eth:
                            msg += f"\n2. ETH {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})"
                        if sol:
                            msg += f"\n3. SOL {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})"
                        idx = 4
                        if nq:
                            msg += f"\n{idx}. 나스닥 선물 {fmt_pct(safe_pct_from_snapshot(nq))}"
                            idx += 1
                        if sox:
                            msg += f"\n{idx}. 반도체지수 {fmt_pct(safe_pct_from_snapshot(sox))}"
                            idx += 1
                        if wti:
                            msg += f"\n{idx}. WTI 유가 {fmt_pct(safe_pct_from_snapshot(wti))}"
                            idx += 1
                        if dxy:
                            msg += f"\n{idx}. 달러인덱스 {fmt_pct(safe_pct_from_snapshot(dxy))}"
                        msg += f"\n\n📌 한 줄 정리:\n{final_market_recap_focus(btc_pct, eth_pct, sol_pct, safe_pct_from_snapshot(nq), safe_pct_from_snapshot(sox), safe_pct_from_snapshot(wti), safe_pct_from_snapshot(dxy))}"
                        msg += "\n\n오늘 체크:\n- 한국장 외국인 수급\n- 반도체 대형주 방향\n- BTC 80K/79K 반응"
                        await safe_send(bot, msg, disable_preview=True)
                        state.overnight_recap_sent_dates[key] = now.date()
            except Exception:
                logging.exception("overnight_recap_scheduler 오류")
            await asyncio.sleep(20)


async def run_forever() -> None:
    token, _ = resolve_telegram_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN 환경변수가 필요합니다.")

    bot = Bot(token=token)
    state = State()

    tasks = [
        asyncio.create_task(market_monitor(bot, state)),
        asyncio.create_task(btc_key_level_monitor(bot, state)),
        asyncio.create_task(fear_greed_monitor(bot, state)),
        asyncio.create_task(kimchi_monitor(bot, state)),
        asyncio.create_task(whale_monitor(bot, state)),
        asyncio.create_task(live_news_monitor(bot, state)),
        asyncio.create_task(ops_health_monitor(bot, state)),
        asyncio.create_task(btc_move_chart_monitor(bot, state)),
        asyncio.create_task(macro_pulse_monitor(bot, state)),
        asyncio.create_task(live_recap_scheduler(bot, state)),
        asyncio.create_task(futures_flow_monitor(bot, state)),
        asyncio.create_task(alpha_flow_monitor(bot, state)),
        asyncio.create_task(liquidation_monitor(bot, state)),
        asyncio.create_task(market_session_scheduler(bot, state)),
        asyncio.create_task(briefing_scheduler(bot, state)),
        asyncio.create_task(overnight_recap_scheduler(bot, state)),
    ]

    if os.getenv("PORT"):
        tasks.append(asyncio.create_task(railway_port_health_server()))

    logging.info("워커 루프 시작 CHANNEL=%s", CHANNEL_ID)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    while True:
        try:
            asyncio.run(run_forever())
        except RuntimeError as e:
            if "TELEGRAM_TOKEN" in str(e):
                logging.error("토큰 없음. Railway Variables 에 TELEGRAM_TOKEN 확인.")
                time.sleep(60)
                continue
            raise
        except Exception:
            logging.exception("run_forever 재시작")
            time.sleep(10)
            continue


