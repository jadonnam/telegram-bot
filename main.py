import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Deque, Dict, Optional, Tuple, Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
import feedparser
from telegram import Bot


# =========================
# BASIC CONFIG
# =========================

CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@jadonnam")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
KST = ZoneInfo("Asia/Seoul")

MARKET_CHECK_SECONDS = 5 * 60
NEWS_CHECK_SECONDS = 15 * 60
FUTURES_FLOW_CHECK_SECONDS = 15 * 60
FNG_CHECK_SECONDS = 5 * 60
KIMCHI_CHECK_SECONDS = 10 * 60
WHALE_CHECK_SECONDS = 60
BRIEFING_CHECK_SECONDS = 30
MARKET_SESSION_CHECK_SECONDS = 30

PRICE_CHANGE_THRESHOLD = 1.5
VOLUME_SURGE_THRESHOLD = 4.0
LIQUIDATION_VOLUME_THRESHOLD = 2.5
WHALE_NOTIONAL_THRESHOLD = 3_000_000

SIGNAL_COOLDOWN = timedelta(minutes=45)
FUTURES_SIGNAL_COOLDOWN = timedelta(minutes=90)
PRICE_MILESTONE_COOLDOWN = timedelta(hours=18)

BTC_PRICE_MILESTONES = (60000, 70000, 75000, 80000, 85000, 90000, 100000)

NEWS_DAILY_LIMIT = 5
NEWS_MIN_INTERVAL = timedelta(minutes=60)
NEWS_URGENT_SCORE = 8
NEWS_NORMAL_SCORE = 6

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://news.google.com/rss/search?q=(Iran%20OR%20Hormuz%20OR%20UAE%20OR%20Israel%20OR%20missile%20OR%20warship%20OR%20tanker)%20(US%20Navy%20OR%20oil%20OR%20attack%20OR%20strike)&hl=ko&gl=KR&ceid=KR:ko",
)

NEWS_KEYWORDS = (
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "etf",
    "fed", "fomc", "cpi", "inflation", "interest rate", "rate cut",
    "sec", "regulation", "lawsuit", "tariff", "trump", "dollar", "oil",
    "hack", "exploit", "exchange", "binance", "coinbase",
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
    "breaking", "urgent", "emergency",
    "missile", "attack", "strike", "explosion", "warship", "navy", "tanker",
    "iran", "israel", "hormuz", "uae", "oil", "war",
    "속보", "긴급", "미사일", "피격", "공격", "폭발", "군함", "해군", "유조선",
    "이란", "이스라엘", "호르무즈", "공역", "봉쇄", "전쟁", "유가",
)

BREAKING_MARKET_CONTEXT_TERMS = (
    "bitcoin", "btc", "crypto", "market", "oil", "dollar", "stock", "risk",
    "비트코인", "코인", "시장", "유가", "달러", "주식", "위험자산",
)

THREADS_AUTO_POST = os.getenv("THREADS_AUTO_POST", "false").lower() == "true"
THREADS_USER_ID = os.getenv("THREADS_USER_ID")
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN")
THREADS_IMAGE_URL = os.getenv("THREADS_IMAGE_URL")

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}


# =========================
# STATE
# =========================

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
        self.market_session_sent_dates: Dict[str, date] = {}

        self.last_fng_zone = "normal"
        self.last_kimchi_zone = "normal"
        self.futures_oi_cache: Dict[str, float] = {}
        self.futures_last_signal: Dict[str, datetime] = {}

        self.last_market_price: Dict[str, float] = {}
        self.price_milestone_cooldowns: Dict[str, datetime] = {}

        self.whale_seen_ids: Deque[str] = deque(maxlen=8000)
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


# =========================
# UTIL
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_kst() -> datetime:
    return datetime.now(KST)


def fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


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


def is_price_milestone_on_cooldown(state: State, key: str, now: datetime) -> bool:
    expires_at = state.price_milestone_cooldowns.get(key)
    return bool(expires_at and expires_at > now)


def touch_price_milestone(state: State, key: str, now: datetime) -> None:
    state.price_milestone_cooldowns[key] = now + PRICE_MILESTONE_COOLDOWN


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


# =========================
# MARKET DATA: BYBIT → OKX → BINANCE
# =========================

async def get_market_ticker(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    # 1) Bybit
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return {
            "lastPrice": float(item["lastPrice"]),
            "priceChangePercent": float(item.get("price24hPcnt", 0)) * 100,
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
        return {
            "lastPrice": float(item["last"]),
            "priceChangePercent": float(item.get("chg24h", 0)) * 100,
            "volume24h": float(item.get("volCcy24h", 0) or 0),
            "source": "OKX",
        }
    except Exception:
        pass

    # 3) Binance last fallback
    data = await fetch_json(session, "https://api.binance.com/api/v3/ticker/24hr", {"symbol": symbol})
    try:
        return {
            "lastPrice": float(data["lastPrice"]),
            "priceChangePercent": float(data["priceChangePercent"]),
            "volume24h": float(data.get("quoteVolume", 0) or 0),
            "source": "Binance",
        }
    except Exception:
        return None


# 기존 함수명 유지용 aliases
async def get_binance_ticker_24h(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    return await get_market_ticker(session, symbol)


async def get_binance_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    ticker = await get_market_ticker(session, symbol)
    return float(ticker["lastPrice"]) if ticker else None


async def get_recent_klines(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    # Bybit first
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/kline",
        {"category": "linear", "symbol": symbol, "interval": "5", "limit": 3},
    )
    try:
        rows = (data.get("result") or {}).get("list", [])
        rows = list(reversed(rows))
        converted = []
        for r in rows:
            ts, o, h, l, c, vol, turnover = r[:7]
            converted.append([ts, o, h, l, c, vol, ts, turnover])
        if converted:
            return converted
    except Exception:
        pass

    # Binance fallback
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
        price = meta.get("regularMarketPrice")
        prev = meta.get("previousClose") or meta.get("chartPreviousClose")
        if price is None or prev is None or float(prev) <= 0:
            return None
        pct = ((float(price) - float(prev)) / float(prev)) * 100
        return float(price), pct
    except Exception:
        return None


async def get_fear_greed(session: aiohttp.ClientSession) -> Optional[Tuple[int, str]]:
    data = await fetch_json(session, "https://api.alternative.me/fng/")
    try:
        value = int(data["data"][0]["value"])
        return value, fear_greed_label(value)
    except Exception:
        return None


async def get_kimchi_premium(session: aiohttp.ClientSession) -> Optional[Tuple[float, float, float]]:
    upbit_krw = await get_upbit_btc_krw(session)
    global_usdt = await get_binance_price(session, "BTCUSDT")
    usd_krw = await get_usd_krw(session)
    if not upbit_krw or not global_usdt or not usd_krw or global_usdt * usd_krw <= 0:
        return None
    global_krw = global_usdt * usd_krw
    premium = ((upbit_krw - global_krw) / global_krw) * 100
    return premium, upbit_krw, global_usdt


# =========================
# SIGNAL HELPERS
# =========================

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
            if level in (80000, 90000):
                msg = (
                    f"🚨🚨 [메가 돌파]\n"
                    f"BTC {level_text} 달러 돌파\n\n"
                    f"시장 심리가 바뀌는 핵심 구간.\n"
                    f"FOMO 매수세가 붙을 수 있는 자리.\n\n"
                    f"📌 지금 핵심은 {level_text} 위에서 버티는지 확인."
                )
            else:
                msg = (
                    f"🚨 [핵심 돌파]\n"
                    f"BTC {level_text} 달러 돌파\n\n"
                    f"신규 매수세가 붙을 수 있는 구간.\n\n"
                    f"📌 지금 핵심은 {level_text} 위에서 버티는지 확인."
                )
        else:
            if level in (80000, 90000):
                msg = (
                    f"⚠️⚠️ [메가 이탈]\n"
                    f"BTC {level_text} 달러 붕괴\n\n"
                    f"롱 청산이 커질 수 있는 자리.\n"
                    f"반등 실패 시 추가 하락 열림.\n\n"
                    f"📌 지금 핵심은 {level_text} 회복 여부."
                )
            else:
                msg = (
                    f"⚠️ [핵심 이탈]\n"
                    f"BTC {level_text} 달러 이탈\n\n"
                    f"단기 손절/청산 물량이 나올 수 있는 구간.\n\n"
                    f"📌 지금 핵심은 {level_text} 회복 여부."
                )

        await safe_send(bot, msg)
        touch_price_milestone(state, key, now)

    state.last_market_price[symbol] = price


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


# =========================
# NEWS
# =========================

def news_importance_score(title: str, summary: str) -> int:
    text = f"{title}\n{summary}".lower()
    score = 0

    tier_1 = (
        "spot bitcoin etf", "bitcoin etf", "btc etf", "etf inflow", "etf outflow",
        "fed", "fomc", "cpi", "inflation", "interest rate", "rate cut",
        "sec", "lawsuit", "approval", "rejection", "regulation",
        "hack", "exploit", "binance", "coinbase", "exchange",
        "trump", "tariff", "war", "ukraine", "iran", "israel", "oil", "dollar",
        "liquidation", "sell-off", "crash", "surge", "missile", "attack", "hormuz",
        "금리", "연준", "유가", "달러", "규제", "해킹", "거래소", "전쟁", "승인", "거절",
        "미사일", "피격", "호르무즈", "군함", "유조선",
    )
    tier_2 = ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "price", "rally", "drop", "market", "volume", "whale")
    low_quality = NEWS_BLOCK_KEYWORDS + ("op-ed", "opinion", "guide", "explainer", "roundup", "daily recap", "prediction", "rumor")

    score += sum(3 for k in tier_1 if k in text)
    score += sum(1 for k in tier_2 if k in text)
    score -= sum(4 for k in low_quality if k in text)
    return score


def is_forced_breaking_news(title: str, summary: str) -> bool:
    text = f"{title}\n{summary}".lower()
    has_breaking = any(k in text for k in BREAKING_FORCE_TERMS)
    has_market = any(k in text for k in BREAKING_MARKET_CONTEXT_TERMS)
    return has_breaking and (has_market or any(k in text for k in ("iran", "hormuz", "israel", "이란", "호르무즈", "이스라엘")))


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
    return news_importance_score(title, summary) >= NEWS_NORMAL_SCORE


def is_urgent_news(title: str, summary: str) -> bool:
    if is_forced_breaking_news(title, summary):
        return True
    text = f"{title}\n{summary}".lower()
    hard_urgent = (
        "breaking", "urgent", "emergency", "sec approves", "sec rejects",
        "hacked", "exploit", "seized", "freeze", "frozen",
        "war", "missile", "attack", "tariff", "cpi", "fomc",
        "속보", "긴급", "해킹", "승인", "거절", "압수", "전쟁", "공격", "미사일", "피격",
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
    if any(k in text for k in ("trump", "tariff", "dollar", "oil", "war", "ukraine", "iran", "israel", "유가", "달러", "전쟁", "호르무즈", "이란")):
        return "시장 해석: 거시·지정학 이슈라 유가·달러·위험자산 분위기를 같이 흔들 수 있음."
    if any(k in text for k in ("liquidation", "sell-off", "whale", "volume")):
        return "시장 해석: 청산·거래량 이슈라 단기 변동성이 커질 수 있음."
    return "시장 해석: 방향보다 시장 반응까지 같이 확인해야 하는 뉴스."


async def translate_to_korean(session: aiohttp.ClientSession, text: str) -> str:
    text = clean_text(text, limit=450)
    if not text:
        return ""
    if re.search(r"[가-힣]", text):
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


def build_threads_text(title_ko: str, title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()

    if is_forced_breaking_news(title, summary):
        return (
            f"속보성 이슈다.\n\n"
            f"{title_ko}\n\n"
            f"중동 리스크가 커지면 유가, 달러, 비트코인이 같이 흔들릴 수 있다.\n\n"
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

    btc = await get_binance_ticker_24h(session, "BTCUSDT")
    btc_line = ""
    if btc:
        btc_pct = float(btc.get("priceChangePercent", 0))
        flow = "상승 흐름" if btc_pct > 0 else "하락 압력" if btc_pct < 0 else "보합권"
        btc_line = f"\n\n📊 현재 BTC: {fmt_pct(btc_pct)} ({flow})"

    if is_urgent_news(title, summary):
        telegram_msg = (
            f"🚨 [속보 · 중요도 {score}/10]\n"
            f"{title_ko}\n\n"
            f"{line}"
            f"{btc_line}\n\n"
            f"출처: {source}\n"
            f"{link}"
        )
    elif score >= 8:
        telegram_msg = (
            f"🔥 [핵심뉴스 · 중요도 {score}/10]\n"
            f"{title_ko}\n\n"
            f"{line}"
            f"{btc_line}\n\n"
            f"출처: {source}\n"
            f"{link}"
        )
    else:
        telegram_msg = (
            f"📰 [뉴스 · 중요도 {score}/10]\n"
            f"{title_ko}\n\n"
            f"{line}"
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


# =========================
# MONITORS
# =========================

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
                                line = "추격매수 주의. 눌림목 대기" if pct > 0 else "지지선 확인 필요. 성급한 매수 금지"
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

                            if ratio >= LIQUIDATION_VOLUME_THRESHOLD:
                                signal_key = f"liq:{symbol}"
                                if not state.is_on_cooldown(signal_key, now):
                                    side = "롱 청산 가능성" if old_price and price < old_price else "숏 청산 가능성"
                                    msg = (
                                        "💣 [강제 청산 감지]\n"
                                        f"{symbol.replace('USDT', '')} {side}\n"
                                        f"거래량 x{ratio:.2f}\n"
                                        f"5분 거래대금 {latest_vol:,.0f} USDT\n"
                                        "⚠️ 추세 전환 또는 가속 구간"
                                    )
                                    await safe_send(bot, msg)
                                    state.touch_cooldown(signal_key, now)

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
                logging.exception("market_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_CHECK_SECONDS - int(elapsed)))


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
                        f"🧭 [수급 + 포지션 흐름 · {strength}]\n"
                        f"{symbol.replace('USDT', '')}\n\n"
                        f"펀딩비 {funding:+.3f}%\n"
                        f"OI 변화 {oi_change:+.1f}%\n"
                        f"오더북 비율 {imbalance:.2f}\n\n"
                        f"{comment}\n\n"
                        f"📌 지금은 포지션 쏠림 구간"
                    )
                    await safe_send(bot, msg, disable_preview=True)
                    state.futures_last_signal[signal_key] = now
            except Exception:
                logging.exception("선물 수급 감지 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, FUTURES_FLOW_CHECK_SECONDS - int(elapsed)))


async def whale_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                ticker = await get_market_ticker(session, "BTCUSDT")
                price = ticker["lastPrice"] if ticker else None
                ob = await get_orderbook_imbalance(session, "BTCUSDT")
                if price and ob:
                    imbalance, bid_notional, ask_notional = ob
                    large_side = "매수벽" if bid_notional > ask_notional else "매도벽"
                    notional = max(bid_notional, ask_notional)
                    trade_id = f"{large_side}:{int(notional // 1_000_000)}:{datetime.utcnow().strftime('%Y%m%d%H%M')}"
                    if notional >= WHALE_NOTIONAL_THRESHOLD and not state.has_whale_trade(trade_id):
                        state.mark_whale_trade(trade_id)
                        now = utc_now()
                        signal_key = "whale:btc"
                        if not state.is_on_cooldown(signal_key, now):
                            msg = (
                                "🐋 [대형 호가 감지]\n"
                                f"BTC {large_side} 우세\n"
                                f"규모: {notional:,.0f} USDT\n"
                                f"오더북 비율: {imbalance:.2f}\n"
                                "단기 변동성 확대 가능성 주의"
                            )
                            await safe_send(bot, msg)
                            state.touch_cooldown(signal_key, now)
            except Exception:
                logging.exception("whale_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, WHALE_CHECK_SECONDS - int(elapsed)))


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
                    premium, upbit_krw, global_usdt = data
                    zone = "high" if premium >= 3 else "low" if premium <= -3 else "normal"
                    if zone != "normal" and state.last_kimchi_zone != zone:
                        now = utc_now()
                        signal_key = f"kimchi:{zone}"
                        if not state.is_on_cooldown(signal_key, now):
                            msg = (
                                "🇰🇷 [김치프리미엄 경보]\n"
                                f"현재 프리미엄: {fmt_pct(premium)}\n"
                                f"업비트 BTC: {upbit_krw:,.0f}원\n"
                                f"글로벌 BTC: {global_usdt:,.0f} USDT"
                            )
                            await safe_send(bot, msg)
                            state.touch_cooldown(signal_key, now)
                    state.last_kimchi_zone = zone
            except Exception:
                logging.exception("kimchi_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, KIMCHI_CHECK_SECONDS - int(elapsed)))


async def news_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                kst = now_kst()
                if state.news_daily_date != kst.date():
                    state.news_daily_date = kst.date()
                    state.news_daily_count = 0

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
                            if not urgent and state.last_news_sent_at and (now - state.last_news_sent_at) < NEWS_MIN_INTERVAL:
                                continue

                            msg, threads_text = await build_korean_news_message(session, title, summary, link)
                            await safe_send(bot, msg, disable_preview=False)
                            await publish_to_threads(session, threads_text)

                            logging.info("뉴스 전송 완료 score=%s urgent=%s title=%s", score, urgent, clean_text(title, 80))
                            state.mark_news(nid)
                            state.last_news_sent_at = now
                            state.news_daily_count += 1
            except Exception:
                logging.exception("news_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, NEWS_CHECK_SECONDS - int(elapsed)))


# =========================
# BRIEFINGS
# =========================

def is_korean_market_weekday(now: datetime) -> bool:
    return now.weekday() < 5


def is_us_market_premarket_day(now: datetime) -> bool:
    return now.weekday() < 5


def is_us_market_close_day(now: datetime) -> bool:
    return 1 <= now.weekday() <= 5


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


def append_if_value(lines: list[str], name: str, snap: Optional[Tuple[float, float]], digits: int = 2) -> None:
    if snap:
        price, pct = snap
        lines.append(f"{name}: {price:,.{digits}f} ({fmt_pct(pct)})")


def session_data_note(lines: list[str], required_min: int = 2) -> str:
    return "" if len(lines) >= required_min else "\n데이터 일부 지연 중. 핵심 가격 반응 우선 확인."


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


async def market_session_scheduler(bot: Bot, state: State) -> None:
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
                        conclusion = kr_open_conclusion(pct_or_zero(kospi), pct_or_zero(kosdaq), pct_or_zero(sp_fut), pct_or_zero(nq_fut), usd_krw or 0)
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n시장 분위기: {us_flow}\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

                if is_korean_market_weekday(now) and in_send_window(now, 15, 30, 10):
                    key = "kr_close_1530"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        btc_text, _ = await btc_line(session)
                        kp, kq = pct_or_zero(kospi), pct_or_zero(kosdaq)

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

                if is_us_market_premarket_day(now) and in_send_window(now, 21, 30, 10):
                    key = "us_pre_2130"
                    if state.market_session_sent_dates.get(key) != now.date():
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        btc_text, _ = await btc_line(session)

                        lines = ["🇺🇸 [미국장 1시간 전]"]
                        append_if_value(lines, "S&P500 선물", sp_fut)
                        append_if_value(lines, "나스닥 선물", nq_fut)
                        append_if_value(lines, "달러인덱스", dxy)
                        append_if_value(lines, "10년물 금리", tnx)
                        lines.append(btc_text)

                        conclusion = us_open_conclusion(pct_or_zero(sp_fut), pct_or_zero(nq_fut), pct_or_zero(dxy), pct_or_zero(tnx))
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

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
                        msg = "\n".join(lines) + session_data_note(lines) + f"\n\n📌 오늘 결론: {conclusion}"
                        await safe_send(bot, msg, disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()

            except Exception:
                logging.exception("market_session_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, MARKET_SESSION_CHECK_SECONDS - int(elapsed)))


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
                            t = await get_binance_ticker_24h(session, symbol)
                            if t:
                                tickers[symbol] = (float(t["lastPrice"]), float(t["priceChangePercent"]))
                        if len(tickers) < 1:
                            continue

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)

                        btc_price, btc_pct = tickers.get("BTCUSDT", (0, 0))
                        eth_price, eth_pct = tickers.get("ETHUSDT", (0, 0))
                        sol_price, sol_pct = tickers.get("SOLUSDT", (0, 0))
                        line = market_one_liner(btc_pct)

                        msg = (
                            f"{emoji} [시장 브리핑]\n"
                            f"BTC {btc_price:,.0f} USDT ({fmt_pct(btc_pct)})\n"
                            f"ETH {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})\n"
                            f"SOL {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})\n"
                        )
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n😶‍🌫️ 공포탐욕지수: {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n🇰🇷 김치프리미엄: {fmt_pct(premium)}"
                        msg += f"\n\n📊 한 줄 시황: {line}"

                        await safe_send(bot, msg)
                        state.briefing_sent_dates[slot_key] = now.date()
            except Exception:
                logging.exception("briefing_scheduler 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, BRIEFING_CHECK_SECONDS - int(elapsed)))


# =========================
# SERVER / RUNTIME
# =========================

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

    logging.info("워커 루프 시작. CHANNEL=%s PORT=%s", CHANNEL_ID, port or "미사용")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    while True:
        try:
            asyncio.run(run_forever())
        except RuntimeError as e:
            if "TELEGRAM_TOKEN" in str(e):
                logging.error("토큰 없음: Railway Variables 에 TELEGRAM_TOKEN 확인 후 재배포하세요.")
                time.sleep(60)
                continue
            raise
        except Exception:
            logging.exception("run_forever 재시작")
            time.sleep(10)
            continue
