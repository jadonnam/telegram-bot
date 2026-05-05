import asyncio
import hashlib
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


async def send_message(bot: Bot, text: str, disable_preview: bool = False) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=disable_preview)


async def safe_send(bot: Bot, text: str, disable_preview: bool = False) -> None:
    try:
        await send_message(bot, text, disable_preview=disable_preview)
    except Exception:
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
        f"출처: {source}\n"
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
        return "BTC 저항선 테스트 중. 돌파 유지 여부가 핵심."
    if btc_24h_pct <= -2.0:
        return "변동성 확대 구간. 지지선 반응 확인 필요."
    return "방향성 탐색 구간. 거래량 동반 여부가 핵심."


async def briefing_scheduler(bot: Bot, state: State) -> None:
    slots = {"08": (8, "🌅"), "12": (12, "☀️"), "21": (21, "🌙")}
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
                            t = await get_market_ticker(session, symbol)
                            if t:
                                tickers[symbol] = (float(t["lastPrice"]), float(t["priceChangePercent"]))
                        if "BTCUSDT" not in tickers:
                            continue

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)

                        btc_price, btc_pct = tickers["BTCUSDT"]
                        eth_price, eth_pct = tickers.get("ETHUSDT", (0, 0))
                        sol_price, sol_pct = tickers.get("SOLUSDT", (0, 0))
                        line = market_one_liner(btc_pct)

                        msg = (
                            f"{emoji} [시장 브리핑]\n"
                            f"{fmt_btc_line(btc_price, btc_pct)}\n"
                            f"{move_icon(eth_pct)} ETH: {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})\n"
                            f"{move_icon(sol_pct)} SOL: {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})\n"
                        )
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n😶‍🌫️ 공포탐욕지수: {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n🇰🇷 김치프리미엄: {fmt_pct(premium)}"
                        msg += f"\n\n📊 한 줄 시황: {line}"

                        await safe_send(bot, msg, disable_preview=True)
                        state.briefing_sent_dates[slot_key] = now.date()
            except Exception:
                logging.exception("briefing_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, BRIEFING_CHECK_SECONDS - int(elapsed)))


def is_korean_market_weekday(now: datetime) -> bool:
    return now.weekday() < 5


def is_us_market_premarket_day(now: datetime) -> bool:
    return now.weekday() < 5


def is_us_market_close_day(now: datetime) -> bool:
    return 1 <= now.weekday() <= 5


def market_direction_label(*pcts: float) -> str:
    return sentiment_label(*pcts)


def kr_open_conclusion(kospi_pct: float, kosdaq_pct: float, sp_pct: float, nq_pct: float, usd_krw: float) -> str:
    us_flow = market_direction_label(sp_pct, nq_pct)
    if us_flow.startswith("🟢") and usd_krw < 1450:
        return "미국 선물과 환율이 받쳐주면 반도체·성장주 수급 먼저 확인."
    if us_flow.startswith("🔴") or usd_krw >= 1450:
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
        line = fmt_market_value(name, snap, digits)
        if line:
            lines.append(line)


def session_data_note(lines: list[str], required_min: int = 2) -> str:
    return "" if len(lines) >= required_min else "\n데이터 일부 지연 중. 핵심 가격 반응 우선 확인."


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
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n🧭 시장 분위기: {mood}\n📌 오늘 결론: {conclusion}"
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
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n📍 오늘 특징: {feature}\n📌 오늘 결론: {conclusion}"
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
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n🧭 시장 분위기: {mood}\n📌 오늘 결론: {conclusion}"
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
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n🧭 마감 분위기: {mood}\n📌 오늘 결론: {conclusion}"
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


async def run_forever() -> None:
    token, _ = resolve_telegram_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN 환경변수가 필요합니다.")

    bot = Bot(token=token)
    state = State()

    tasks = [
        asyncio.create_task(briefing_scheduler(bot, state)),
        asyncio.create_task(market_monitor(bot, state)),
        asyncio.create_task(fear_greed_monitor(bot, state)),
        asyncio.create_task(kimchi_monitor(bot, state)),
        asyncio.create_task(whale_monitor(bot, state)),
        asyncio.create_task(news_monitor(bot, state)),
        asyncio.create_task(futures_flow_monitor(bot, state)),
        asyncio.create_task(alpha_flow_monitor(bot, state)),
        asyncio.create_task(market_session_scheduler(bot, state)),
    ]

    if os.getenv("PORT"):
        tasks.append(asyncio.create_task(railway_port_health_server()))

    logging.info("워커 루프 시작 CHANNEL=%s", CHANNEL_ID)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    while True:
        try:
            asyncio.run(run_foreverasyncio.create_task(liquidation_monitor(bot, state)),())
        except RuntimeError as e:
            if "TELEGRAM_TOKEN" in str(e):
                logging.error("토큰 없음. Railway Variables 에 TELEGRAM_TOKEN 확인.")
                time.sleep(60)
                continue
            raise
        except Exception:
            logging.exception("run_forever 재시작")
            time.sleep(10)
async def liquidation_monitor(bot, state):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data = await fetch_json(
                    session,
                    "https://fapi.binance.com/fapi/v1/allForceOrders",
                    {"symbol": "BTCUSDT", "limit": 50},
                )

                if not data:
                    await asyncio.sleep(10)
                    continue

                total_long = 0
                total_short = 0

                for d in data:
                    price = float(d["price"])
                    qty = float(d["origQty"])
                    notional = price * qty

                    if d["side"] == "SELL":
                        total_long += notional
                    else:
                        total_short += notional

                if total_long > 5_000_000:
                    await safe_send(bot, f"💥 [롱 청산]\n{total_long:,.0f} USDT")

                if total_short > 5_000_000:
                    await safe_send(bot, f"🔥 [숏 청산]\n{total_short:,.0f} USDT")

            except:
                pass

            await asyncio.sleep(20)


def market_direction(funding, oi_change, imbalance):
    if funding > 0.05 and oi_change > 3:
        return "롱 과열 → 숏 우위"
    if funding < -0.05 and oi_change > 3:
        return "숏 과열 → 롱 우위"
    if imbalance > 1.8:
        return "매수 우위"
    if imbalance < 0.6:
        return "매도 우위"
    return "중립"


def build_position_message(direction):
    return (
        "🎯 [포지션 판단]\n\n"
        f"{direction}\n\n"
        "추격 ❌ / 눌림 ⭕"
    )

