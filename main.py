import asyncio
import hashlib
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Any, Deque, Dict, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo
from urllib.parse import quote, urlparse

import aiohttp
from aiohttp import web
import feedparser
from telegram import Bot
from telegram.error import BadRequest, InvalidToken, NetworkError, RetryAfter, TimedOut


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
VOLUME_SURGE_MIN_NOTIONAL = {
    "BTCUSDT": 5_000_000,
    "ETHUSDT": 3_000_000,
    "SOLUSDT": 1_000_000,
}
VOLUME_SURGE_COOLDOWN_SEC = 90 * 60
VOLUME_SURGE_DAILY_LIMIT = 5
VOLUME_SURGE_REPEAT_COIN_COOLDOWN_SEC = 90 * 60
TOPIC_COOLDOWNS = {
    "sol_etf": timedelta(hours=6),
    "btc_etf": timedelta(hours=2),
    "eth_security": timedelta(hours=3),
    "oil_hormuz": timedelta(hours=2),
    "korea_semiconductor": timedelta(hours=2),
    "korea_flow": timedelta(hours=1),
    "rates_macro": timedelta(hours=2),
    "liquidation": timedelta(hours=1),
}

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

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

FETCH_WARN_COOLDOWN = timedelta(minutes=10)
FETCH_WARN_LAST_AT: Dict[str, datetime] = {}
RUNTIME_ENABLE_VOLUME_ALERT = True
RUNTIME_ENABLE_ALT_VOLUME_ALERT = False
SEND_DEDUP_WINDOW_SECONDS = 120
_LAST_SENT_HASH_AT: Dict[str, datetime] = {}

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
        self.volume_surge_last: Dict[str, datetime] = {}
        self.volume_surge_daily_date: Optional[date] = None
        self.volume_surge_daily_count = 0
        self.volume_surge_last_coins: Dict[str, datetime] = {}

        self.whale_seen_ids: Deque[str] = deque(maxlen=8000)
        self.whale_seen_set = set()

        self.alpha_seen_ids: Deque[str] = deque(maxlen=10000)
        self.alpha_seen_set = set()
        self.recap_used_news_titles: set[str] = set()
        self.recap_used_news_date: Optional[date] = None
        self.recap_used_topics: set[str] = set()
        self.coin_topic_last_sent: Dict[str, datetime] = {}
        self.topic_last_sent: Dict[str, datetime] = {}
        self.coin_live_daily_date: Optional[date] = None
        self.coin_live_daily_count = 0
        self.sol_etf_daily_date: Optional[date] = None
        self.sol_etf_daily_count = 0

        self.digest_sent_dates: Dict[str, date] = {}
        self.overnight_recap_sent_dates: Dict[str, date] = {}
        self.live_news_seen_ids: Deque[str] = deque(maxlen=12000)
        self.live_news_seen_set: set[str] = set()
        self.live_last_sent_at: Optional[datetime] = None
        self.live_news_daily_date: Optional[date] = None
        self.live_news_daily_count = 0
        self.live_recent_items: Deque[Any] = deque(maxlen=80)
        self.live_recent_titles: Deque[Tuple[datetime, str]] = deque(maxlen=600)
        self.recap_sent_keys: set[str] = set()
        self.macro_pulse_last_pcts: Optional[Dict[str, float]] = None
        self.macro_pulse_last_sent: Optional[datetime] = None

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
        return "🔴 관망 우위"
    return "🟡 혼조"


def env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on", "y"):
        return True
    if value in ("0", "false", "no", "off", "n"):
        return False
    return default


def env_int(name: str, default: int, *, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        v = int(str(raw).strip(), 10)
    except ValueError:
        return default
    if min_value is not None:
        v = max(min_value, v)
    if max_value is not None:
        v = min(max_value, v)
    return v


def env_float(name: str, default: float, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        v = float(str(raw).strip())
    except ValueError:
        return default
    if min_value is not None:
        v = max(min_value, v)
    if max_value is not None:
        v = min(max_value, v)
    return v


def _parse_extra_usdt_symbols() -> tuple[str, ...]:
    raw = (os.getenv("EXTRA_USDT_SYMBOLS") or "").strip()
    if not raw:
        return ()
    out: list[str] = []
    for p in raw.split(","):
        s = p.strip().upper()
        if not s:
            continue
        if not s.endswith("USDT"):
            s = f"{s}USDT"
        if s in SYMBOLS or s in out:
            continue
        if not re.fullmatch(r"[A-Z0-9]{5,20}", s):
            continue
        out.append(s)
    return tuple(out[:12])


EXTRA_USDT_SYMBOLS: tuple[str, ...] = _parse_extra_usdt_symbols()
ALT_PULSE_INTERVAL_SEC = env_int("ALT_PULSE_INTERVAL_SEC", 7200, min_value=600, max_value=86400)
ENABLE_ALT_PULSE = env_bool("ENABLE_ALT_PULSE", bool(EXTRA_USDT_SYMBOLS))


COIN_TOPIC_COOLDOWN = timedelta(minutes=env_int("LIVE_COIN_TOPIC_COOLDOWN_MINUTES", 120, min_value=30, max_value=1440))
TOPIC_DEFAULT_COOLDOWN = timedelta(minutes=env_int("LIVE_TOPIC_DEFAULT_COOLDOWN_MINUTES", 28, min_value=5, max_value=240))


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


def _telegram_forum_kwargs() -> dict:
    raw = (os.getenv("TELEGRAM_MESSAGE_THREAD_ID") or "").strip()
    if raw.isdigit():
        return {"message_thread_id": int(raw)}
    return {}


def truncate_telegram_utf16_units(s: str, max_units: int) -> str:
    """텔레그램 길이 제한은 UTF-16 코드 유닛 기준. 초과분은 잘라냄."""
    if max_units <= 0:
        return ""
    out: list[str] = []
    n = 0
    for ch in s or "":
        o = ord(ch)
        units = 2 if o > 0xFFFF or (0xD800 <= o <= 0xDFFF) else 1
        if n + units > max_units:
            break
        out.append(ch)
        n += units
    return "".join(out).rstrip()


TELEGRAM_SEND_MAX_RETRIES = env_int("TELEGRAM_SEND_MAX_RETRIES", 6, min_value=1, max_value=15)
TELEGRAM_MAX_MESSAGE_UNITS = env_int("TELEGRAM_MAX_MESSAGE_UNITS", 4000, min_value=500, max_value=4096)
TELEGRAM_MAX_CAPTION_UNITS = env_int("TELEGRAM_MAX_CAPTION_UNITS", 1000, min_value=200, max_value=1024)
TELEGRAM_MIN_SEND_INTERVAL_SEC = env_float("TELEGRAM_MIN_SEND_INTERVAL_SEC", 0.35, min_value=0.0, max_value=5.0)
_CHANNEL_SEND_LOCK = asyncio.Lock()
_CHANNEL_LAST_SEND_MONO: float = 0.0


async def _throttle_telegram_channel_send() -> None:
    global _CHANNEL_LAST_SEND_MONO
    gap = TELEGRAM_MIN_SEND_INTERVAL_SEC
    if gap <= 0:
        return
    async with _CHANNEL_SEND_LOCK:
        mono = time.monotonic()
        wait = gap - (mono - _CHANNEL_LAST_SEND_MONO)
        if wait > 0:
            await asyncio.sleep(wait)
        _CHANNEL_LAST_SEND_MONO = time.monotonic()


async def _telegram_send_with_retry(coro_factory, *, op: str) -> None:
    await _throttle_telegram_channel_send()
    last_err: Optional[BaseException] = None
    for attempt in range(TELEGRAM_SEND_MAX_RETRIES):
        try:
            await coro_factory()
            return
        except RetryAfter as e:
            last_err = e
            sec = float(getattr(e, "retry_after", 0) or 0) + 0.75 + min(attempt, 10) * 0.2
            logging.warning("Telegram RetryAfter %.1fs op=%s attempt %s/%s", sec, op, attempt + 1, TELEGRAM_SEND_MAX_RETRIES)
            await asyncio.sleep(sec)
        except (TimedOut, NetworkError) as e:
            last_err = e
            await asyncio.sleep(1.5 + attempt * 0.75)
            logging.warning("Telegram timeout/network op=%s attempt %s/%s", op, attempt + 1, TELEGRAM_SEND_MAX_RETRIES)
        except InvalidToken:
            raise
        except BadRequest as e:
            logging.warning("Telegram BadRequest op=%s err=%s", op, e)
            raise
    if last_err:
        raise last_err


async def send_message(bot: Bot, text: str, disable_preview: bool = False) -> None:
    extra = _telegram_forum_kwargs()
    raw = text or ""
    limits = [TELEGRAM_MAX_MESSAGE_UNITS, 3000, 2000, 1200, 600]
    last_bad: Optional[BaseException] = None
    for lim in limits:
        body = truncate_telegram_utf16_units(raw, lim).strip() or "…"

        async def _do(b: str = body) -> None:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=b,
                disable_web_page_preview=disable_preview,
                **extra,
            )

        try:
            await _telegram_send_with_retry(_do, op="send_message")
            return
        except BadRequest as e:
            last_bad = e
            el = str(e).lower()
            if "too long" in el or "message is too long" in el or "message_length" in el:
                logging.warning("send_message 길이 초과, 더 짧게 재시도 lim=%s", lim)
                continue
            raise
    if last_bad:
        raise last_bad


async def safe_send(bot: Bot, text: str, disable_preview: bool = False) -> None:
    now = utc_now()
    text_key = hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()
    last_sent = _LAST_SENT_HASH_AT.get(text_key)
    if last_sent and (now - last_sent).total_seconds() < SEND_DEDUP_WINDOW_SECONDS:
        logging.info("safe_send dedup skipped window=%ss", SEND_DEDUP_WINDOW_SECONDS)
        return
    try:
        await send_message(bot, text, disable_preview=disable_preview)
        _LAST_SENT_HASH_AT[text_key] = now
    except Exception as e:
        if is_telegram_auth_failure(e):
            logging.error("Telegram 토큰 인증 실패. BotFather에서 새 토큰 발급 후 TELEGRAM_TOKEN 교체 필요.")
            return
        logging.exception("Telegram 전송 실패 chat_id=%s", CHANNEL_ID)


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None):
    try:
        async with session.get(url, params=params, timeout=20, headers=REQUEST_HEADERS) as response:
            if response.status != 200:
                key = f"{response.status}:{url}"
                now = utc_now()
                last = FETCH_WARN_LAST_AT.get(key)
                if not last or now - last >= FETCH_WARN_COOLDOWN:
                    FETCH_WARN_LAST_AT[key] = now
                    is_geo_block = (
                        response.status in (403, 451)
                        and ("binance.com" in url or "bybit.com" in url)
                    )
                    is_okx_funding_bad_req = (
                        response.status == 400
                        and "okx.com/api/v5/public/funding-rate" in url
                    )
                    if is_geo_block or is_okx_funding_bad_req:
                        logging.debug("fetch_json 제한 status=%s url=%s", response.status, url)
                    else:
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
            LAST_GOOD_KLINES[symbol] = converted
            return converted
    except Exception:
        pass

    data = await fetch_json(
        session,
        "https://api.binance.com/api/v3/klines",
        {"symbol": symbol, "interval": "5m", "limit": 3},
    )
    if data:
        LAST_GOOD_KLINES[symbol] = data
        return data

    okx_symbol = OKX_SYMBOLS.get(symbol)
    if okx_symbol:
        okx_data = await fetch_json(
            session,
            "https://www.okx.com/api/v5/market/candles",
            {"instId": okx_symbol, "bar": "5m", "limit": "3"},
        )
        try:
            rows = list(reversed(okx_data.get("data", [])))
            converted = []
            for r in rows:
                ts, o, h, l, c, _, vol_ccy, vol = r[:8]
                converted.append([ts, o, h, l, c, vol_ccy, ts, vol])
            if converted:
                LAST_GOOD_KLINES[symbol] = converted
                return converted
        except Exception:
            pass

    return LAST_GOOD_KLINES.get(symbol)


async def get_funding_rate(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        val = float(item.get("fundingRate", 0)) * 100
        LAST_GOOD_FUNDING[symbol] = val
        return val
    except Exception:
        pass

    data = await fetch_json(session, "https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
    try:
        val = float(data["lastFundingRate"]) * 100
        LAST_GOOD_FUNDING[symbol] = val
        return val
    except Exception:
        pass

    okx_symbol = OKX_FUNDING_SYMBOLS.get(symbol) or f"{symbol.replace('USDT', '-USDT')}-SWAP"
    if okx_symbol:
        data = await fetch_json(session, "https://www.okx.com/api/v5/public/funding-rate", {"instId": okx_symbol})
        try:
            row = (data.get("data") or [])[0]
            val = float(row.get("fundingRate", 0)) * 100
            LAST_GOOD_FUNDING[symbol] = val
            return val
        except Exception:
            logging.debug("OKX funding-rate fallback 실패 symbol=%s instId=%s", symbol, okx_symbol)

    return LAST_GOOD_FUNDING.get(symbol)


async def get_open_interest(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        val = float(item.get("openInterest", 0))
        LAST_GOOD_OPEN_INTEREST[symbol] = val
        return val
    except Exception:
        pass

    data = await fetch_json(session, "https://fapi.binance.com/fapi/v1/openInterest", {"symbol": symbol})
    try:
        val = float(data["openInterest"])
        LAST_GOOD_OPEN_INTEREST[symbol] = val
        return val
    except Exception:
        pass

    okx_symbol = OKX_SYMBOLS.get(symbol)
    if okx_symbol:
        data = await fetch_json(session, "https://www.okx.com/api/v5/public/open-interest", {"instId": okx_symbol})
        try:
            row = (data.get("data") or [])[0]
            val = float(row.get("oi", 0))
            LAST_GOOD_OPEN_INTEREST[symbol] = val
            return val
        except Exception:
            pass

    return LAST_GOOD_OPEN_INTEREST.get(symbol)


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

OKX_FUNDING_SYMBOLS = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
}

LAST_GOOD_TICKER: Dict[str, dict] = {}
LAST_GOOD_KLINES: Dict[str, list] = {}
LAST_GOOD_FUNDING: Dict[str, float] = {}
LAST_GOOD_OPEN_INTEREST: Dict[str, float] = {}


async def get_binance_market_ticker(session: aiohttp.ClientSession, symbol: str):
    data = await fetch_json(
        session,
        "https://api.binance.com/api/v3/ticker/24hr",
        {"symbol": symbol},
    )
    try:
        return {
            "symbol": symbol,
            "lastPrice": str(float(data["lastPrice"])),
            "priceChangePercent": str(float(data["priceChangePercent"])),
            "volume24h": float(data.get("quoteVolume", 0) or 0),
            "source": "Binance",
        }
    except Exception:
        return None


async def get_bybit_market_ticker(session: aiohttp.ClientSession, symbol: str):
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        last = float(item["lastPrice"])
        pct_raw = item.get("price24hPcnt")
        if pct_raw is not None:
            pct = float(pct_raw) * 100
        else:
            prev_price = float(item.get("prevPrice24h") or 0)
            pct = ((last - prev_price) / prev_price) * 100 if prev_price > 0 else 0.0
        return {
            "symbol": symbol,
            "lastPrice": str(last),
            "priceChangePercent": str(pct),
            "volume24h": float(item.get("turnover24h", 0) or 0),
            "source": "Bybit",
        }
    except Exception:
        return None


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
        f"?ids={coin_id}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true"
    )
    data = await fetch_json(session, url)
    try:
        item = data.get(coin_id) or {}
        last = float(item.get("usd"))
        pct = float(item.get("usd_24h_change") or 0.0)
        vol = float(item.get("usd_24h_vol") or 0.0)
        return {
            "symbol": symbol,
            "lastPrice": str(last),
            "priceChangePercent": str(pct),
            "volume24h": vol,
            "source": "CoinGecko",
        }
    except Exception:
        return None


async def get_market_ticker(session: aiohttp.ClientSession, symbol: str):
    # Railway 지역 제한 대비 순서:
    # Binance -> Bybit -> OKX -> CoinGecko -> last good cache
    sources = (
        get_binance_market_ticker,
        get_bybit_market_ticker,
        get_okx_market_ticker,
        get_coingecko_market_ticker,
    )
    for fn in sources:
        try:
            row = await fn(session, symbol)
            if row:
                LAST_GOOD_TICKER[symbol] = row
                return row
        except Exception:
            logging.debug("ticker fallback 실패 symbol=%s source=%s", symbol, fn.__name__)

    return LAST_GOOD_TICKER.get(symbol)


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


async def get_okx_ohlc_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    *,
    bar: str = "15m",
    limit: int = 64,
) -> list[tuple[int, float, float, float, float, float]]:
    inst = OKX_SYMBOLS.get(symbol)
    if not inst:
        return []
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst}&bar={bar}&limit={limit}"
    data = await fetch_json(session, url)
    out: list[tuple[int, float, float, float, float, float]] = []
    try:
        for row in data.get("data", []):
            out.append(
                (int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5] or 0))
            )
        out.sort(key=lambda x: x[0])
    except Exception:
        return []
    return out


TRADINGVIEW_OKX_SYMBOLS = {
    "BTCUSDT": "OKX:BTCUSDT",
    "ETHUSDT": "OKX:ETHUSDT",
    "SOLUSDT": "OKX:SOLUSDT",
}

CHART_IMG_API_BASE = "https://api.chart-img.com"


def chart_img_api_key() -> str:
    return (os.getenv("CHART_IMG_API_KEY") or os.getenv("TRADINGVIEW_CHART_API_KEY") or "").strip()


def tradingview_symbol_for_usdt(symbol: str) -> str:
    return TRADINGVIEW_OKX_SYMBOLS.get(symbol, f"OKX:{symbol}")


async def fetch_tradingview_chart_storage_url(
    session: aiohttp.ClientSession,
    tv_symbol: str,
    *,
    interval: str = "15m",
    support: Optional[float] = None,
    resist: Optional[float] = None,
) -> Optional[str]:
    """chart-img.com → TradingView advanced chart 공개 스냅샷 URL."""
    api_key = chart_img_api_key()
    if not api_key:
        return None

    body: dict = {
        "symbol": tv_symbol,
        "interval": interval,
        "width": 920,
        "height": 520,
        "theme": "dark",
        "style": "candle",
        "timezone": "Asia/Seoul",
        "format": "png",
        "range": "1D",
        "studies": [{"name": "Volume", "forceOverlay": True}],
    }
    drawings: list[dict] = []
    if support and support > 0:
        drawings.append(
            {
                "name": "Horizontal Line",
                "input": {"price": round(support, 6), "text": "지지"},
                "override": {"lineWidth": 1, "lineColor": "rgb(67,160,71)"},
            }
        )
    if resist and resist > 0:
        drawings.append(
            {
                "name": "Horizontal Line",
                "input": {"price": round(resist, 6), "text": "저항"},
                "override": {"lineWidth": 1, "lineColor": "rgb(229,57,53)"},
            }
        )
    if drawings:
        body["drawings"] = drawings

    headers = {"x-api-key": api_key, "content-type": "application/json"}
    url = f"{CHART_IMG_API_BASE}/v2/tradingview/advanced-chart/storage"
    try:
        async with session.post(
            url,
            json=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=50),
        ) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, dict) and data.get("url"):
                    return str(data["url"])
            logging.warning(
                "chart-img v2 storage status=%s symbol=%s",
                response.status,
                tv_symbol,
            )
    except Exception:
        logging.exception("chart-img v2 storage failed symbol=%s", tv_symbol)

    params = {
        "symbol": tv_symbol,
        "interval": interval,
        "width": 920,
        "height": 520,
        "theme": "dark",
        "key": api_key,
    }
    data = await fetch_json(session, f"{CHART_IMG_API_BASE}/v1/tradingview/advanced-chart/storage", params)
    if isinstance(data, dict) and data.get("url"):
        return str(data["url"])
    return None


def primary_symbol_for_coin_news(title: str, summary: str, coin_type: str) -> str:
    raw = f"{title} {summary}".lower()
    if coin_type in ("sol_alt_flow",) or any(
        k in raw for k in ("solana", "솔라나", "firedancer", " firedancer", "jump crypto")
    ):
        return "SOLUSDT"
    if coin_type in ("eth_security",) or any(
        k in raw for k in ("ethereum", "이더리움", " ether", " eth ", "eth ")
    ):
        return "ETHUSDT"
    if any(k in raw for k in ("bitcoin", "btc", "비트코인")):
        return "BTCUSDT"
    return "BTCUSDT"


def _sma(vals: list[float], n: int) -> float:
    if not vals:
        return 0.0
    if len(vals) < n:
        return sum(vals) / len(vals)
    return sum(vals[-n:]) / n


def _fmt_trade_price(symbol: str, price: float) -> str:
    if price >= 1000:
        return f"{price:,.0f}"
    if price >= 10:
        return f"{price:,.2f}"
    return f"{price:.4f}"


def coin_article_interpretation(title: str, summary: str, coin_type: str) -> str:
    t = f"{title} {summary}".lower()
    if "firedancer" in t or ("jump" in t and "sol" in t):
        return "Firedancer는 SOL 체인 성능·수수료 이슈라, 헤드라인보다 SOL 거래량·TPS 반응을 먼저 보면 됩니다."
    if any(k in t for k in ("infrastructure", "rollout", "인프라", "출시", "메인넷")):
        return "인프라·출시 뉴스는 가격보다 체인 사용량·수수료·거래량이 먼저 움직이는 경우가 많습니다."
    if any(k in t for k in ("clarity", "클래리티", "congress", "의회", "법안")):
        return "규제·입법은 단기 심리(펀딩)가 먼저, 현물·ETF 수급은 며칠 늦게 따라오는 편입니다."
    if "etf" in t and any(k in t for k in ("inflow", "outflow", "유입", "유출")):
        return "ETF 유입·유출 숫자가 나오면 BTC·ETH가 같은 방향으로 움직이는지가 핵심입니다."
    if coin_type == "volatility":
        return "급등·급락 뉴스는 펀딩·미결제가 한쪽으로 쏠렸는지부터 확인하는 편이 낫습니다."
    return ""


def build_trade_desk_lines(
    symbol: str,
    candles: list[tuple[int, float, float, float, float, float]],
    funding: Optional[float],
) -> list[str]:
    if len(candles) < 12:
        return []
    tag = symbol.replace("USDT", "")
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    last = closes[-1]
    window = min(48, len(candles))
    hi = max(highs[-window:])
    lo = min(lows[-window:])
    mid = (hi + lo) / 2
    e9 = _sma(closes, 9)
    e21 = _sma(closes, 21)
    if last >= e9 >= e21:
        trend = "단기 상승"
    elif last <= e9 <= e21:
        trend = "단기 하락"
    else:
        trend = "횡보"

    pf = _fmt_trade_price(symbol, last)
    plo = _fmt_trade_price(symbol, lo)
    phi = _fmt_trade_price(symbol, hi)
    pmid = _fmt_trade_price(symbol, mid)

    lines = [f"추세(15m·TV {tag}): {trend} · 박스 {plo}~{phi}"]
    span = hi - lo
    pos = (last - lo) / span if span > 0 else 0.5

    if pos >= 0.72:
        lines.append(f"지금 {pf} — 박스 상단. {phi} 돌파 확인 전 추격은 부담")
        lines.append(f"관심: {phi} 위 15m 마감 · 되밀림 {pmid}~{phi} 지지")
    elif pos <= 0.28:
        lines.append(f"지금 {pf} — 박스 하단. {plo} 이탈이 관건")
        lines.append(f"관심: {plo}~{pmid} 반등 · 이탈 시 약세 이어질 수 있음")
    else:
        lines.append(f"지금 {pf} — 박스 중단({pmid} 부근)")
        lines.append(f"관심: 저항 {phi} · 지지 {plo}")

    if funding is not None:
        if funding >= 0.03:
            lines.append(f"펀딩 +{funding:.3f}% — 롱 쏠림, 급락 시 청산 주의")
        elif funding <= -0.01:
            lines.append(f"펀딩 {funding:.3f}% — 숏 우위")

    return lines[:4]


def build_okx_candlestick_chart_url(
    candles: list[tuple[int, float, float, float, float, float]],
    symbol: str,
    *,
    support: float,
    resist: float,
) -> str:
    from urllib.parse import quote

    if len(candles) < 8:
        return ""
    tag = symbol.replace("USDT", "")
    subset = candles[-36:]
    data = []
    for i, row in enumerate(subset):
        _, o, h, l, c, _ = row
        data.append(
            {"t": str(i + 1), "o": round(o, 6), "h": round(h, 6), "l": round(l, 6), "c": round(c, 6)}
        )
    chart: dict = {
        "type": "candlestick",
        "data": {"datasets": [{"label": tag, "data": data}]},
        "options": {
            "plugins": {
                "title": {"display": True, "text": f"{tag} · OKX 15m"},
                "legend": {"display": False},
            },
            "scales": {
                "x": {"display": True, "ticks": {"maxTicksLimit": 6}},
                "y": {"position": "right"},
            },
        },
    }
    if support > 0 and resist > 0:
        chart["options"]["plugins"]["annotation"] = {
            "annotations": {
                "sup": {
                    "type": "line",
                    "yMin": support,
                    "yMax": support,
                    "borderColor": "#43a047",
                    "borderWidth": 1,
                    "borderDash": [5, 4],
                },
                "res": {
                    "type": "line",
                    "yMin": resist,
                    "yMax": resist,
                    "borderColor": "#e53935",
                    "borderWidth": 1,
                    "borderDash": [5, 4],
                },
            }
        }
    encoded = quote(json.dumps(chart, ensure_ascii=False))
    if len(encoded) > 7800:
        chart["options"]["plugins"].pop("annotation", None)
        encoded = quote(json.dumps(chart, ensure_ascii=False))
    return f"https://quickchart.io/chart?width=920&height=520&version=4&c={encoded}"


async def coin_news_trade_desk(
    session: aiohttp.ClientSession,
    title: str,
    summary: str,
    coin_type: str,
) -> Tuple[Optional[str], list[str]]:
    """기사 종목 TradingView 15m 스냅샷 + OKX 데이터 기반 자리·흐름."""
    symbol = primary_symbol_for_coin_news(title, summary, coin_type)
    candles = await get_okx_ohlc_candles(session, symbol, bar="15m", limit=64)
    if len(candles) < 16:
        return None, []
    window = min(48, len(candles))
    lo = min(c[3] for c in candles[-window:])
    hi = max(c[2] for c in candles[-window:])
    funding = await get_funding_rate(session, symbol)
    lines = build_trade_desk_lines(symbol, candles, funding)

    tv_symbol = tradingview_symbol_for_usdt(symbol)
    chart_url = await fetch_tradingview_chart_storage_url(
        session, tv_symbol, interval="15m", support=lo, resist=hi
    )
    if not chart_url:
        chart_url = build_okx_candlestick_chart_url(candles, symbol, support=lo, resist=hi) or None
        if chart_url and not chart_img_api_key():
            logging.info("CHART_IMG_API_KEY 미설정 → QuickChart 대체 차트 사용")
    return chart_url, lines


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
        return "ETF·규제 뉴스는 사람들이 돈 넣고 빼는 속도가 가격보다 빨리 움직여요."
    if any(k in text for k in ("fed", "fomc", "cpi", "inflation", "interest rate", "rate cut", "금리", "연준")):
        return "금리 뉴스는 주식이랑 코인이 같은 줄로 움직이는 날이 많아요."
    if any(k in text for k in ("hack", "exploit", "hacked", "해킹")):
        return "해킹 뉴스는 가격보다 마음(공포)부터 흔들려요."
    if any(k in text for k in ("exchange", "binance", "coinbase", "kraken", "거래소")):
        return "거래소 뉴스는 가격 차이랑 믿을 수 있는지부터 보면 돼요."
    if any(k in text for k in ("trump", "tariff", "dollar", "oil", "war", "iran", "israel", "유가", "달러", "전쟁")):
        return "나라 사이 일이면 유가·달러·코인이 한 덩어리로 같이 움직일 때가 많아요."
    if any(k in text for k in ("liquidation", "sell-off", "whale", "volume", "청산")):
        return "청산이나 거래량 뉴스는 위아래가 크게 출렁일 때가 많아요."
    return "가격보다 거래량이랑 선물 쪽 반응을 먼저 보면 돼요."


async def build_korean_news_message(session: aiohttp.ClientSession, title: str, summary: str, link: str) -> str:
    title_ko = await translate_to_korean(session, title)
    title_ko = html_clean(title_ko, 420).strip()
    source = source_name_from_link(link)
    score = normalized_news_score(title, summary)
    line = news_importance_line(title, summary)
    facts = extract_article_numerical_facts(title, summary, "", max_items=5)

    btc_snap = ""
    btc = await get_market_ticker(session, "BTCUSDT")
    if btc:
        try:
            btc_price = float(btc["lastPrice"])
            btc_pct = float(btc["priceChangePercent"])
            flow = "상승 흐름" if btc_pct > 0.15 else "하락 압력" if btc_pct < -0.15 else "보합권"
            btc_snap = f"BTC {btc_price:,.0f} ({fmt_pct(btc_pct)}, {flow})"
        except Exception:
            btc_snap = ""

    if is_urgent_news(title, summary):
        band = "속보"
    elif score >= 8:
        band = "상단"
    else:
        band = "흐름"

    now = now_kst()
    parts: list[str] = [
        room_line(f"RSS 데스크 · {band} · {score}/10", now),
        "",
        "① 헤드라인",
        title_ko,
        "",
        "② 한 줄 팩트",
        f"· {line}",
    ]
    fact_lines = [line, title_ko]
    extra_facts = [f for f in facts if not _fact_line_redundant(f, fact_lines)]
    if extra_facts:
        parts += ["", "③ 인용 수치"]
        for f in extra_facts:
            parts.append(f"· {f}")
            fact_lines.append(f)
    parts += ["", "④ 소스", f"· 출처: {source}"]
    link_s = (link or "").strip()
    if link_s.startswith("http"):
        parts.append(f"· 🔗 {link_s}")
    if btc_snap:
        parts.append(f"· 지금 {btc_snap}")
    parts += ["", ROOM_DISCLAIMER]
    return compact_message("\n".join(parts), LIVE_MESSAGE_SOFT_LIMIT)


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
        nk = now_kst()
        if direction == "breakout":
            msg = (
                room_line("BTC 핵심 구간 · 돌파", nk)
                + "\n\n① 기준\n"
                f"· {level_text} 달러 위로\n\n② 가격\n"
                f"· 현재 {price:,.0f} USDT\n\n③ 체크\n"
                "· 돌파 직후 위꼬리·휩쏘\n"
                f"· {level_text} 위 15~30분 유지면 추세로만 보면 됨\n\n"
                + ROOM_DISCLAIMER
            )
        else:
            msg = (
                room_line("BTC 핵심 구간 · 이탈", nk)
                + "\n\n① 기준\n"
                f"· {level_text} 달러 아래\n\n② 가격\n"
                f"· 현재 {price:,.0f} USDT\n\n③ 체크\n"
                "· 단기 청산·변동성 확대\n"
                f"· {level_text} 빠른 회복 실패 시 추가 눌림\n\n"
                + ROOM_DISCLAIMER
            )

        await safe_send(bot, msg)
        state.price_milestone_cooldowns[key] = now + PRICE_MILESTONE_COOLDOWN

    state.last_market_price[symbol] = price


async def market_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                kst_today = now_kst().date()
                if state.volume_surge_daily_date != kst_today:
                    state.volume_surge_daily_date = kst_today
                    state.volume_surge_daily_count = 0
                surge_rows = []
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
                        if symbol != "BTCUSDT":
                            continue
                        if abs(pct) >= PRICE_CHANGE_THRESHOLD:
                            direction = "상승" if pct > 0 else "하락"
                            signal_key = f"price:{symbol}:{direction}"
                            if not state.is_on_cooldown(signal_key, now):
                                nk = now_kst()
                                line = "추격보다 눌림 확인" if pct > 0 else "지지선 반응 확인"
                                msg = (
                                    room_line(f"시장 감지 · BTC 15분 {direction}", nk)
                                    + "\n\n① 숫자\n"
                                    f"· {fmt_pct(pct)} · 현재 {price:,.0f} USDT\n\n② 체크\n"
                                    f"· {line}\n\n"
                                    + ROOM_DISCLAIMER
                                )
                                await safe_send(bot, msg, disable_preview=True)
                                state.touch_cooldown(signal_key, now)

                    klines = await get_recent_klines(session, symbol)
                    allow_volume = symbol == "BTCUSDT" or RUNTIME_ENABLE_ALT_VOLUME_ALERT
                    if RUNTIME_ENABLE_VOLUME_ALERT and allow_volume and klines and len(klines) >= 2:
                        prev_vol = float(klines[-2][7])
                        latest_vol = float(klines[-1][7])
                        if prev_vol > 0:
                            ratio = latest_vol / prev_vol
                            min_notional = float(VOLUME_SURGE_MIN_NOTIONAL.get(symbol, 1_000_000))
                            if ratio >= VOLUME_SURGE_THRESHOLD and latest_vol >= min_notional:
                                last_sent = state.volume_surge_last.get(symbol)
                                repeat_last = state.volume_surge_last_coins.get(symbol)
                                if state.volume_surge_daily_count >= VOLUME_SURGE_DAILY_LIMIT:
                                    logging.info("volume_surge skipped reason=daily_limit symbol=%s", symbol)
                                    continue
                                if repeat_last and (now - repeat_last).total_seconds() < VOLUME_SURGE_REPEAT_COIN_COOLDOWN_SEC:
                                    logging.info("volume_surge skipped reason=repeat_coin_cooldown symbol=%s", symbol)
                                    continue
                                if not last_sent or (now - last_sent).total_seconds() >= VOLUME_SURGE_COOLDOWN_SEC:
                                    surge_rows.append((symbol, ratio, latest_vol))
                                    state.volume_surge_last[symbol] = now
                if surge_rows:
                    surge_rows.sort(key=lambda x: x[1], reverse=True)
                    top_symbol = surge_rows[0][0] if surge_rows else ""
                    top_symbols = [x[0] for x in surge_rows[:3]]
                    nk = now_kst()
                    parts = [room_line("거래대금 급증", nk), "", "① 스냅샷"]
                    if {"BTCUSDT", "ETHUSDT", "SOLUSDT"}.issubset(set(top_symbols)):
                        parts.append("· BTC·ETH·SOL 동시에 대금 들어옴")
                        parts.append("· 단기 반등 시도는 나올 수 있음")
                    else:
                        for symbol, ratio, latest_vol in surge_rows[:3]:
                            coin = symbol.replace("USDT", "")
                            parts.append(f"· {coin} x{ratio:.1f} (5m 누적 {latest_vol/1_000_000:.1f}M USDT)")
                    for symbol, _ratio, _latest in surge_rows[:3]:
                        state.volume_surge_last_coins[symbol] = utc_now()
                    state.volume_surge_daily_count += 1
                    logging.info("volume_surge sent count_today=%s symbols=%s", state.volume_surge_daily_count, ",".join(top_symbols))
                    parts += ["", "② 체크"]
                    if top_symbol == "BTCUSDT":
                        parts.append("· 레벨 유지·80K 전후만")
                    else:
                        parts.append("· 알트 수급 이어지는지")
                    parts += ["", ROOM_DISCLAIMER]
                    await safe_send(bot, "\n".join(parts), disable_preview=True)
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
                    nk = now_kst()
                    coin = symbol.replace("USDT", "")
                    msg = (
                        room_line(f"선물 수급 · {coin} · {strength}", nk)
                        + "\n\n① 숫자\n"
                        f"· 펀딩 {funding:+.3f}%\n"
                        f"· 미결제 {oi_change:+.1f}%\n"
                        f"· 호가비 {imbalance:.2f}\n\n② 메모\n"
                        f"· {comment}\n\n"
                        + ROOM_DISCLAIMER
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
                        nk = now_kst()
                        if big_buy > big_sell:
                            msg = (
                                room_line("알파 체결 · BTC 대형 매수 우세", nk)
                                + "\n\n① 규모\n"
                                f"· 매수 {big_buy:,.0f} / 매도 {big_sell:,.0f} USDT\n\n② 체크\n"
                                "· 단기 지지·돌파 시도 가능성만\n\n"
                                + ROOM_DISCLAIMER
                            )
                        else:
                            msg = (
                                room_line("알파 체결 · BTC 대형 매도 우세", nk)
                                + "\n\n① 규모\n"
                                f"· 매수 {big_buy:,.0f} / 매도 {big_sell:,.0f} USDT\n\n② 체크\n"
                                "· 단기 저항·눌림 가능성만\n\n"
                                + ROOM_DISCLAIMER
                            )
                        await safe_send(bot, msg, disable_preview=True)
                        state.touch_cooldown(signal_key, now)

                if abs(cvd) >= ALPHA_CVD_NOTIONAL_THRESHOLD:
                    side_label = "매수" if cvd > 0 else "매도"
                    ratio = buy_ratio if cvd > 0 else sell_ratio
                    if ratio >= ALPHA_IMBALANCE_THRESHOLD:
                        signal_key = f"alpha:cvd:{side_label}"
                        if not state.is_on_cooldown(signal_key, now):
                            nk = now_kst()
                            msg = (
                                room_line(f"체결강도 · BTC {side_label} 우세", nk)
                                + "\n\n① 숫자\n"
                                f"· 매수 {buy_notional:,.0f} / 매도 {sell_notional:,.0f} USDT\n"
                                f"· CVD proxy {cvd:+,.0f} USDT\n"
                                f"· {side_label} 비중 {ratio * 100:.1f}%\n\n② 체크\n"
                                "· 한쪽 체결 과밀 구간\n\n"
                                + ROOM_DISCLAIMER
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
                        nk = now_kst()
                        msg = (
                            room_line(f"고래 체결 · BTC 대형 {side}", nk)
                            + "\n\n① 규모\n"
                            f"· {notional:,.0f} USDT\n\n② 체크\n"
                            "· 단기 변동성만\n\n"
                            + ROOM_DISCLAIMER
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

                            msg = await build_korean_news_message(session, title, summary, link)
                            await safe_send(bot, msg, disable_preview=False)

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


def btc_brief_line(price: float, pct: float) -> str:
    return f"{move_icon(pct)} BTC: {price:,.0f} USDT ({fmt_pct(pct)})"


_WEEKDAY_KO = ("월", "화", "수", "목", "금", "토", "일")

ROOM_BRAND = (os.getenv("ROOM_BRAND") or "자돈남 DESK").strip() or "자돈남 DESK"
ROOM_DISCLAIMER = (
    "※ 본 카드는 시장 정보·데이터 정리용입니다. 매매 권유·투자 자문이 아닙니다.\n"
    "※ 레버리지·청산·슬리피지·거래소·규제 리스크는 항상 전제합니다."
)

# 대형 채널용: 카드마다 동일한 섹션 레이블로 가독성·운영 톤 통일
SEC_DESK_SNAP = "① 스냅 · 가격·환율·거시"
SEC_DESK_RISK = "② 리스크 · F&G·김치·펀딩·OI"
SEC_DESK_OPS = "③ 운영 메모"
SEC_DESK_TONE = "④ 톤 · 메이저 3종"
# 미국 현물 마감 카드는 앞에 ①②가 이미 쓰이므로 리스크 이하만 번호를 한 칸 밀어 중복 ②를 제거
US_CLOSE_DESK_RISK = "③ 리스크 · F&G·김치·펀딩·OI"
US_CLOSE_DESK_OPS = "④ 운영 메모"
US_CLOSE_DESK_TONE = "⑤ 톤 · 메이저 3종"
SEC_NEWS_FACT = "① 헤드"
SEC_NEWS_CONTEXT = "② 해석"
SEC_NEWS_CHECK = "③ 볼 것"


def desk_voice_line(now: datetime, title_seed: str = "") -> str:
    """짧은 데스크 한 줄(로테이션 · 기기마다 문구가 바뀌게)."""
    lines = (
        "· 이슈만 짧게 정리했습니다.",
        "· 출처·링크 기준으로 팩트 위주로만 남겼습니다.",
        "· 숫자·맥락만 올립니다. 매매 권유는 없습니다.",
        "· 데스크에서 흐름만 점검한 카드입니다.",
        "· 낚시성·과장 제목은 걸러서, 확인 가능한 팩트만 남겼습니다.",
    )
    h = hashlib.md5(f"{now.day}:{now.hour}:{title_seed}".encode("utf-8")).hexdigest()
    return lines[int(h, 16) % len(lines)]


def room_host_line(now: datetime, seed: str, slot: str) -> str:
    """방 운영자 톤 한 줄 — 인사·코멘트 로테이션."""
    wd = now.weekday()
    hour = now.hour
    pool: Tuple[str, ...]
    if slot == "macro":
        pool = (
            "· 데스크 켜둔 상태예요. 오늘은 자산끼리 방향이 갈리는지부터 볼게요.",
            "· 맥박만 짧게 찍습니다. 추격보다 범위·체결 쪽이 안전해 보여요.",
            "· BTC랑 유가·달러가 엇갈리면 잠깐 노이즈 구간일 수 있어요.",
        )
    elif slot == "level":
        pool = (
            "· 레벨 터치라서 짧게 알려드려요. 유지되는지만 먼저 보시면 됩니다.",
            "· 숫자 한 줄 찍고 갑니다. 여기서 버티는지가 오늘 톤을 정해요.",
            "· 데스크에서 레벨만 체크 중이에요. 큰 매매보다 확인용으로 보세요.",
        )
    elif slot == "digest":
        if hour < 12:
            pool = (
                "· 좋은 아침이에요. 오늘 손대볼 축만 체크리스트로 묶었습니다.",
                "· 장 열리기 전에 데스크만 켜둔 상태예요. 아래 순서대로만 훑어보시면 됩니다.",
            )
        else:
            pool = (
                "· 저녁 데스크입니다. 내일 장·코인 같이 볼 때 아래만 훑어보세요.",
                "· 오늘도 고생 많으셨어요. 밤에는 추격보다 체크리스트만 드릴게요.",
                "· 주말이면 더 짧게 갑니다. 큰 매매보다 레벨·수급만.",
            )
    else:
        if wd >= 5:
            pool = (
                "· 주말 데스크예요. 헤드라인만 짧게 정리했습니다.",
                "· 방 켜둔 상태입니다. 확인용으로만 보시면 됩니다.",
                "· 토·일은 노이즈가 많아서, 출처 있는 팩트만 올립니다.",
            )
        elif hour < 10:
            pool = (
                "· 아침 데스크입니다. 밤새 나온 이슈만 짧게.",
                "· 좋은 아침이에요. 오늘 첫 카드입니다.",
            )
        elif hour >= 20:
            pool = (
                "· 저녁 이슈라 짧게 올립니다.",
                "· 데스크에서 방송 톤으로 한 장만 정리했어요.",
                "· 밤장·뉴스 나올 때마다 이렇게 찍어드릴게요.",
            )
        else:
            pool = (
                "· 데스크에서 방송 톤으로 한 장만 정리했어요.",
                "· 이슈 나오면 이렇게 짧게 찍어드릴게요.",
                "· 광고·리딩 없이, 손대볼 축만 붙였습니다.",
            )
    h = hashlib.md5(f"{slot}:{now.date()}:{hour}:{seed}".encode("utf-8")).hexdigest()
    return pool[int(h, 16) % len(pool)]


def live_news_opener_line(now: datetime, title_seed: str) -> str:
    """데스크 문구 vs 방장 톤 — 로테이션."""
    if not LIVE_NEWS_DESK_VOICE_LINE and not LIVE_ROOM_HOST_LINE:
        return ""
    if LIVE_ROOM_HOST_LINE and LIVE_NEWS_DESK_VOICE_LINE:
        pick_host = int(hashlib.md5(f"opener:{title_seed}:{now.hour}".encode()).hexdigest(), 16) % 5 != 0
        return room_host_line(now, title_seed, "news") if pick_host else desk_voice_line(now, title_seed)
    if LIVE_ROOM_HOST_LINE:
        return room_host_line(now, title_seed, "news")
    return desk_voice_line(now, title_seed)


def room_line(subtitle: str, now: datetime) -> str:
    wd = _WEEKDAY_KO[now.weekday()]
    clock = now.strftime("%H:%M")
    return f"{ROOM_BRAND} · {subtitle} · {now.month}/{now.day}({wd}) · {clock} KST"


def fmt_oi_compact(tag: str, oi: Optional[float]) -> str:
    if oi is None or oi <= 0:
        return ""
    if oi >= 1e9:
        return f"{tag} {oi / 1e9:.1f}B"
    if oi >= 1e6:
        return f"{tag} {oi / 1e6:.1f}M"
    if oi >= 1e3:
        return f"{tag} {oi / 1e3:.0f}K"
    return f"{tag} {oi:.0f}"


def room_open_interest_line(oi_btc: Optional[float], oi_eth: Optional[float], oi_sol: Optional[float]) -> str:
    bits = [fmt_oi_compact("BTC", oi_btc), fmt_oi_compact("ETH", oi_eth), fmt_oi_compact("SOL", oi_sol)]
    bits = [b for b in bits if b]
    if not bits:
        return ""
    return "선물 미결제(참고): " + " · ".join(bits)


def briefing_hook_line(btc_pct: float, eth_pct: float, sol_pct: float) -> str:
    if btc_pct >= 1.0:
        return "BTC 24h 강세 구간. 추격보다 거래대금·지속성(고점 갱신 빈도) 위주로만 보면 됨."
    if btc_pct <= -1.0:
        return "BTC 24h 약세 구간. 반등은 단기 체결·펀딩이 따라오는지, 구간 이탈인지부터 확인."
    if sol_pct >= 1.2 and btc_pct < 0.6 and eth_pct < 0.8:
        return "SOL 단독 강세 성격. 알트는 유동성·스프레드·선물 펀딩 왜곡부터 점검."
    if btc_pct <= -0.5 and eth_pct <= -0.8 and sol_pct <= -0.8:
        return "메이저 동조 약세. 레버·펀딩·선물 OI 변화가 가격보다 선행하는 경우가 많음."
    return "BTC 방향 미확정. 추격 금지, 구간·거래대금·펀딩만 유지."


def briefing_volatility_nudge(btc_pct: float, eth_pct: float, sol_pct: float) -> str:
    mx = max(abs(btc_pct), abs(eth_pct), abs(sol_pct))
    if mx >= 2.5:
        return "변동성 확대 구간. 청산·거래대금 스파이크 동시 모니터링."
    if mx >= 1.2:
        return "평균 이상 변동. 포지션 사이즈·레버리지 우선 점검."
    return "변동 보통. 뉴스 이벤트 전후 체결·스프레드만 짧게 확인."


def _fmt_funding_pct(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"{v:+.4f}%"


async def briefing_scheduler(bot: Bot, state: State) -> None:
    slots = {
        "12": (12, "☀️ 점심 · 코인·시장 체크"),
        "23": (23, "🌙 밤 · 코인·시장 체크"),
    }
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()
                for slot_key, (hour, _title) in slots.items():
                    if now.hour != hour or now.minute > 1:
                        continue
                    if state.briefing_sent_dates.get(slot_key) == now.date():
                        continue

                    rows = await asyncio.gather(*[get_market_ticker(session, s) for s in SYMBOLS])
                    tickers: dict[str, tuple[float, float, float]] = {}
                    for sym, t in zip(SYMBOLS, rows):
                        if not t:
                            continue
                        try:
                            tickers[sym] = (
                                float(t["lastPrice"]),
                                float(t["priceChangePercent"]),
                                float(t.get("volume24h") or 0),
                            )
                        except Exception:
                            continue
                    if "BTCUSDT" not in tickers:
                        continue

                    f_btc, f_eth, f_sol = await asyncio.gather(
                        get_funding_rate(session, "BTCUSDT"),
                        get_funding_rate(session, "ETHUSDT"),
                        get_funding_rate(session, "SOLUSDT"),
                    )

                    fng = await get_fear_greed(session)
                    kimchi = await get_kimchi_premium(session)
                    weekend = is_weekend_mode(now)
                    if not weekend:
                        kospi, kosdaq, usd_krw, nq_fut, wti, sox, dxy = await asyncio.gather(
                            get_yahoo_snapshot(session, "%5EKS11"),
                            get_yahoo_snapshot(session, "%5EKQ11"),
                            get_usd_krw(session),
                            get_yahoo_snapshot(session, "NQ%3DF"),
                            get_yahoo_snapshot(session, "CL%3DF"),
                            get_yahoo_snapshot(session, "%5ESOX"),
                            get_yahoo_snapshot(session, "DX-Y.NYB"),
                        )
                    else:
                        kospi = kosdaq = nq_fut = sox = None
                        usd_krw, wti, dxy = await asyncio.gather(
                            get_usd_krw(session),
                            get_yahoo_snapshot(session, "CL%3DF"),
                            get_yahoo_snapshot(session, "DX-Y.NYB"),
                        )

                    oi_btc, oi_eth, oi_sol = await asyncio.gather(
                        get_open_interest(session, "BTCUSDT"),
                        get_open_interest(session, "ETHUSDT"),
                        get_open_interest(session, "SOLUSDT"),
                    )

                    btc_price, btc_pct, btc_vol = tickers["BTCUSDT"]
                    eth_price, eth_pct, eth_vol = tickers.get("ETHUSDT", (0.0, 0.0, 0.0))
                    sol_price, sol_pct, sol_vol = tickers.get("SOLUSDT", (0.0, 0.0, 0.0))

                    slot_tag = "☀️ 점심 데스크 스냅" if hour == 12 else "🌙 야간 데스크 스냅"
                    msg = room_line(slot_tag, now)
                    msg += f"\n\n{SEC_DESK_SNAP}"
                    msg += f"\n{btc_brief_line(btc_price, btc_pct)}"
                    msg += f"\n{move_icon(eth_pct)} ETH: {eth_price:,.0f} USDT ({fmt_pct(eth_pct)})"
                    msg += f"\n{move_icon(sol_pct)} SOL: {sol_price:,.0f} USDT ({fmt_pct(sol_pct)})"
                    if usd_krw:
                        msg += f"\n달러/원: {usd_krw:,.2f}원"
                    if not weekend:
                        if kospi:
                            msg += f"\n{session_snapshot_line('코스피 선물', kospi)}"
                        if kosdaq:
                            msg += f"\n{session_snapshot_line('코스닥 선물', kosdaq)}"
                    if nq_fut and not weekend:
                        msg += f"\n{session_snapshot_line('나스닥 선물', nq_fut)}"
                    if wti:
                        msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                    if sox and not weekend:
                        msg += f"\n{session_snapshot_line('필라델피아 반도체', sox)}"
                    if dxy:
                        msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"

                    msg += f"\n\n{SEC_DESK_RISK}"
                    if fng:
                        fng_value, fng_label = fng
                        msg += f"\n· 공포탐욕 {fng_value} ({fng_label})"
                    if kimchi:
                        premium, _, _ = kimchi
                        msg += f"\n· 김치프리미엄 {fmt_pct(premium)}"
                    msg += f"\n· 하루 거래금액 BTC {_fmt_usdt_turnover(btc_vol)} · ETH {_fmt_usdt_turnover(eth_vol)} · SOL {_fmt_usdt_turnover(sol_vol)}"
                    msg += f"\n· 펀딩 BTC {_fmt_funding_pct(f_btc)} · ETH {_fmt_funding_pct(f_eth)} · SOL {_fmt_funding_pct(f_sol)}"
                    oi_ln = room_open_interest_line(oi_btc, oi_eth, oi_sol)
                    if oi_ln:
                        msg += f"\n{oi_ln}"

                    msg += f"\n\n{SEC_DESK_OPS}"
                    msg += f"\n· {briefing_hook_line(btc_pct, eth_pct, sol_pct)}"
                    msg += f"\n· {briefing_volatility_nudge(btc_pct, eth_pct, sol_pct)}"
                    if weekend:
                        msg += "\n· 주말: 유가·달러·김치만 짧게"
                    elif not weekend and not is_korean_market_weekday(now):
                        msg += "\n· 휴장: 한국 주식장 말고 코인·ETF·큰 시장만"
                    else:
                        msg += "\n· 한국장 열리면: 외국인·반도체·환율 → 코인은 BTC가 방향"

                    mood = market_direction_label(btc_pct, eth_pct, sol_pct)
                    msg += f"\n\n{SEC_DESK_TONE}\n{mood}"

                    msg += f"\n\n{ROOM_DISCLAIMER}"

                    try:
                        await send_message(
                            bot, compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True
                        )
                        state.briefing_sent_dates[slot_key] = now.date()
                    except Exception:
                        logging.exception("briefing 전송 실패 slot=%s hour=%s", slot_key, hour)
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


def is_weekend_mode(now: datetime) -> bool:
    return now.weekday() >= 5


def market_direction_label(*pcts: float) -> str:
    vals = [float(x) for x in pcts if x is not None]
    if not vals:
        return "⚪ 데이터 대기"
    avg = sum(vals) / len(vals)
    if avg >= 0.45:
        return "🟢 리스크온 성격(성장·코인 쪽 상대 강세)"
    if avg <= -0.45:
        return "🔴 리스크오프 성격(현금·방어 쪽 선호)"
    if max(vals) - min(vals) >= 0.9:
        return "🟡 메이저 간 편차 큼(섹터·테마 분화)"
    return "🟡 방향 중립 · 박스/횡보 가능"


# ============================================================
# FINAL SESSION BRIEFING HELPERS
# ============================================================

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
        return "🟡 방향 중립"
    avg = sum(vals) / len(vals)
    if avg >= 0.55:
        return "🟢 리스크온"
    if avg <= -0.55:
        return "🔴 리스크오프"
    if max(vals) - min(vals) >= 1.2:
        return "🟡 지수 간 온도차"
    return "🟡 방향 중립"


def final_kr_open_focus(nq_pct: float, sox_pct: float, usd_krw: float, wti_pct: float) -> str:
    if sox_pct >= 0.8 or nq_pct >= 0.4:
        return "미국 테크·반도체 선물 양호. 한국장: 외국인·삼전·하이닉스·환율 우선."
    if nq_pct <= -0.4 or sox_pct <= -0.8:
        return "미국 기술주 선물 약세. 한국장: 반도체 방어·수급 이탈 여부."
    if usd_krw >= 1450:
        return "환율 부담 구간. 외국인 매매 방향이 지수보다 선행하는 경우 많음."
    if wti_pct >= 1.0:
        return "유가 급등. 인플레·금리 민감도 상승 가능."
    return "장 초반 30분: 지수 방향보다 거래대금·섹터 순환만 확인."


def final_us_pre_focus(sp_pct: float, nq_pct: float, dxy_pct: float, tnx_pct: float, wti_pct: float) -> str:
    if nq_pct >= 0.4:
        return "나스닥 선물 양호. 장중: 매출·가이던스·매크로 이벤트 캘린더."
    if nq_pct <= -0.4:
        return "나스닥 선물 약세. 장 시작 변동성·기술주 매도 압력 확인."
    if dxy_pct > 0.25 or tnx_pct > 0.25:
        return "달러·금리 부담. 성장주 밸류에션 압박 가능."
    if wti_pct >= 1.0:
        return "유가 급등. CPI 기대·금리 민감."
    return "장 초반: 지수보다 섹터 자금 이동·체결 강도."


def final_us_close_focus(sp_pct: float, nq_pct: float, dji_pct: float, sox_pct: float, dxy_pct: float, tnx_pct: float, wti_pct: float) -> str:
    if sox_pct >= 1.0 and nq_pct >= 0.4:
        return "반도체·나스닥 동반 강세. 익일 한국: 반도체·AI 수급 연동."
    if nq_pct >= 0.6:
        return "나스닥 강세 마감. 성장주 모멘텀 이어짐 여부."
    if nq_pct <= -0.6 or sox_pct <= -1.0:
        return "기술·반도체 약세 마감. 익일: 반등 실패 시 디펜시브."
    if dxy_pct >= 0.3 or tnx_pct >= 0.3:
        return "달러·금리 상승 마감. 금리 민감 섹터 우선 점검."
    if wti_pct >= 1.0:
        return "유가 상승 마감. 인플레 경로 확인."
    return "혼조 마감. 한국: 환율·반도체·외국인만 압축 체크."


def final_market_recap_focus(btc_pct: float, eth_pct: float, sol_pct: float, nq_pct: float, sox_pct: float, wti_pct: float, dxy_pct: float) -> str:
    if btc_pct <= -2.0:
        return "BTC 급락 구간. 반등보다 청산·펀딩·OI 정리 우선."
    if btc_pct >= 2.0:
        return "BTC 급등 구간. 거래대금·지속성·선물 스큐 확인."
    if nq_pct >= 0.5 or sox_pct >= 1.0:
        return "미국 테크·반도체 강세. 한국 반도체 연동."
    if wti_pct >= 1.0:
        return "유가 급등. 달러·금리와 동시 모니터링."
    if dxy_pct >= 0.3:
        return "달러 강세. 신흥·코인 유동성 압박 가능."
    return "섹터별 자금 이동 위주로 단기 범위만 관리."


async def market_session_scheduler(bot: Bot, state: State) -> None:
    def in_time_window(now: datetime, hour: int, minute: int, width_min: int = 4) -> bool:
        if now.hour != hour:
            return False
        return minute <= now.minute < minute + width_min

    async def btc_line(session: aiohttp.ClientSession) -> Tuple[str, float]:
        btc = await get_market_ticker(session, "BTCUSDT")
        if not btc:
            return "⚪ BTC: 시세 스냅 실패", 0.0
        price = float(btc["lastPrice"])
        pct = float(btc["priceChangePercent"])
        return session_btc_line(price, pct), pct

    async def coin_snapshot(session: aiohttp.ClientSession, symbol: str):
        t = await get_market_ticker(session, symbol)
        if not t:
            return None
        return float(t["lastPrice"]), float(t["priceChangePercent"])

    def log_slot(slot_key: str, now: datetime, already_sent: bool, kr_closed: bool, us_holiday: bool, weekend: bool, reason: str) -> None:
        holiday_mode = kr_closed or us_holiday
        logging.info(
            "slot_check key=%s now=%s already_sent=%s kr_closed=%s us_holiday=%s weekend=%s holiday_mode=%s reason=%s",
            slot_key,
            now.strftime("%Y-%m-%d %H:%M"),
            already_sent,
            kr_closed,
            us_holiday,
            weekend,
            holiday_mode,
            reason,
        )

    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()
                await warm_holiday_cache(session, now.year)
                await warm_holiday_cache(session, (now - timedelta(days=1)).year)

                if in_time_window(now, 8, 0):
                    key = "kr_pre_0800"
                    already_sent = state.market_session_sent_dates.get(key) == now.date()
                    kr_closed = not is_korean_market_weekday(now)
                    us_holiday = not is_us_market_premarket_day(now)
                    weekend = is_weekend_mode(now)
                    if already_sent:
                        log_slot(key, now, True, kr_closed, us_holiday, weekend, "already_sent")
                    else:
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

                        oi_btc, oi_eth, oi_sol = await asyncio.gather(
                            get_open_interest(session, "BTCUSDT"),
                            get_open_interest(session, "ETHUSDT"),
                            get_open_interest(session, "SOLUSDT"),
                        )

                        sub = "주말 오전" if weekend else ("휴장일 오전" if kr_closed else "한국장 직전")
                        msg = room_line(f"🌅 {sub}", now)
                        msg += f"\n\n{SEC_DESK_SNAP}"
                        if usd_krw:
                            msg += f"\n달러/원: {usd_krw:,.2f}원"
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"
                        if not weekend and not kr_closed:
                            if kospi:
                                msg += f"\n{session_snapshot_line('코스피 선물', kospi)}"
                            if kosdaq:
                                msg += f"\n{session_snapshot_line('코스닥 선물', kosdaq)}"
                        if weekend:
                            if wti:
                                msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                            if dxy:
                                msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"
                        elif kr_closed:
                            if wti:
                                msg += f"\n{session_snapshot_line('WTI 유가', wti)}"
                            if dxy:
                                msg += f"\n{session_snapshot_line('달러인덱스', dxy)}"
                        else:
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

                        f_btc, f_eth, f_sol = await asyncio.gather(
                            get_funding_rate(session, "BTCUSDT"),
                            get_funding_rate(session, "ETHUSDT"),
                            get_funding_rate(session, "SOLUSDT"),
                        )

                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)
                        msg += f"\n\n{SEC_DESK_RISK}"
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n· 공포탐욕 {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n· 김치 {fmt_pct(premium)}"
                        msg += f"\n· 펀딩 BTC {_fmt_funding_pct(f_btc)} · ETH {_fmt_funding_pct(f_eth)} · SOL {_fmt_funding_pct(f_sol)}"
                        oi_ln = room_open_interest_line(oi_btc, oi_eth, oi_sol)
                        if oi_ln:
                            msg += f"\n{oi_ln}"

                        msg += f"\n\n{SEC_DESK_OPS}"
                        if weekend:
                            msg += "\n· 주말: 코인·유가·달러·김치 위주"
                        elif kr_closed:
                            msg += "\n· 휴장: 한국 주식 제외, 코인·ETF·글로벌 매크로"
                        else:
                            msg += f"\n· {final_kr_open_focus(safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), usd_krw or 0, safe_pct_from_snapshot(wti))}"
                            msg += "\n· 반도체·외국인·환율 → BTC 구간 연동"

                        msg += f"\n\n{SEC_DESK_TONE}\n{session_mood(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += f"\n\n{ROOM_DISCLAIMER}"
                        await safe_send(bot, compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        state.briefing_sent_dates["08"] = now.date()
                        log_slot(key, now, False, kr_closed, us_holiday, weekend, "sent")

                if in_time_window(now, 21, 0):
                    key = "us_pre_2100"
                    already_sent = state.market_session_sent_dates.get(key) == now.date()
                    kr_closed = not is_korean_market_weekday(now)
                    us_holiday = not is_us_market_premarket_day(now)
                    weekend = is_weekend_mode(now)
                    if weekend:
                        log_slot(key, now, already_sent, kr_closed, us_holiday, weekend, "weekend_skip")
                        continue
                    if already_sent:
                        log_slot(key, now, True, kr_closed, us_holiday, weekend, "already_sent")
                    else:
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        sox = await get_yahoo_snapshot(session, "%5ESOX")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        wti = await get_yahoo_snapshot(session, "CL%3DF")
                        btc_text, _ = await btc_line(session)
                        eth = await coin_snapshot(session, "ETHUSDT")
                        sol = await coin_snapshot(session, "SOLUSDT")

                        oi_btc, oi_eth, oi_sol = await asyncio.gather(
                            get_open_interest(session, "BTCUSDT"),
                            get_open_interest(session, "ETHUSDT"),
                            get_open_interest(session, "SOLUSDT"),
                        )
                        f_btc, f_eth, f_sol = await asyncio.gather(
                            get_funding_rate(session, "BTCUSDT"),
                            get_funding_rate(session, "ETHUSDT"),
                            get_funding_rate(session, "SOLUSDT"),
                        )
                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)

                        msg = room_line("🌆 미국장 직전", now)
                        msg += "\n\n① 스냅 · 미국 선물·매크로"
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
                        msg += "\n\n② 스냅 · BTC·ETH·SOL"
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"

                        msg += f"\n\n{SEC_DESK_RISK}"
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n· 공포탐욕 {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n· 김치 {fmt_pct(premium)}"
                        msg += f"\n· 펀딩 BTC {_fmt_funding_pct(f_btc)} · ETH {_fmt_funding_pct(f_eth)} · SOL {_fmt_funding_pct(f_sol)}"
                        oi_ln = room_open_interest_line(oi_btc, oi_eth, oi_sol)
                        if oi_ln:
                            msg += f"\n{oi_ln}"

                        msg += f"\n\n{SEC_DESK_OPS}"
                        if us_holiday:
                            msg += "\n· 미국 휴장: 코인·달러·유가 위주"
                        else:
                            msg += f"\n· {final_us_pre_focus(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(dxy), safe_pct_from_snapshot(tnx), safe_pct_from_snapshot(wti))}"
                            msg += "\n· 장 개시 30분: 체결·스프레드·지수 동조만"

                        msg += f"\n\n{SEC_DESK_TONE}\n{session_mood(safe_pct_from_snapshot(sp_fut), safe_pct_from_snapshot(nq_fut), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += f"\n\n{ROOM_DISCLAIMER}"
                        await safe_send(bot, compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        log_slot(key, now, False, kr_closed, us_holiday, weekend, "sent")

                if in_time_window(now, 5, 0):
                    key = "us_close_0500"
                    already_sent = state.market_session_sent_dates.get(key) == now.date()
                    kr_closed = not is_korean_market_weekday(now)
                    us_holiday = not is_us_market_close_day(now)
                    weekend = is_weekend_mode(now)
                    if weekend:
                        log_slot(key, now, already_sent, kr_closed, us_holiday, weekend, "weekend_skip")
                        continue
                    if already_sent:
                        log_slot(key, now, True, kr_closed, us_holiday, weekend, "already_sent")
                    else:
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

                        oi_btc, oi_eth, oi_sol = await asyncio.gather(
                            get_open_interest(session, "BTCUSDT"),
                            get_open_interest(session, "ETHUSDT"),
                            get_open_interest(session, "SOLUSDT"),
                        )
                        f_btc, f_eth, f_sol = await asyncio.gather(
                            get_funding_rate(session, "BTCUSDT"),
                            get_funding_rate(session, "ETHUSDT"),
                            get_funding_rate(session, "SOLUSDT"),
                        )
                        fng = await get_fear_greed(session)
                        kimchi = await get_kimchi_premium(session)

                        msg = room_line("🌙 미국장 마감", now)
                        msg += "\n\n① 스냅 · BTC·ETH·SOL"
                        msg += f"\n{btc_text}"
                        if eth:
                            msg += f"\n{move_icon(eth[1])} ETH: {eth[0]:,.0f} USDT ({fmt_pct(eth[1])})"
                        if sol:
                            msg += f"\n{move_icon(sol[1])} SOL: {sol[0]:,.0f} USDT ({fmt_pct(sol[1])})"

                        msg += "\n\n② 스냅 · 미국 현물 마감"
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

                        msg += f"\n\n{US_CLOSE_DESK_RISK}"
                        if fng:
                            fng_value, fng_label = fng
                            msg += f"\n· 공포탐욕 {fng_value} ({fng_label})"
                        if kimchi:
                            premium, _, _ = kimchi
                            msg += f"\n· 김치 {fmt_pct(premium)}"
                        msg += f"\n· 펀딩 BTC {_fmt_funding_pct(f_btc)} · ETH {_fmt_funding_pct(f_eth)} · SOL {_fmt_funding_pct(f_sol)}"
                        oi_ln = room_open_interest_line(oi_btc, oi_eth, oi_sol)
                        if oi_ln:
                            msg += f"\n{oi_ln}"

                        msg += f"\n\n{US_CLOSE_DESK_OPS}"
                        if us_holiday:
                            msg += "\n· 미국 휴장: 코인·달러·유가 위주"
                        else:
                            msg += f"\n· {final_us_close_focus(safe_pct_from_snapshot(spx), safe_pct_from_snapshot(ixic), safe_pct_from_snapshot(dji), safe_pct_from_snapshot(sox), safe_pct_from_snapshot(dxy), safe_pct_from_snapshot(tnx), safe_pct_from_snapshot(wti))}"
                        if weekend or kr_closed:
                            msg += "\n· 연휴·주말: BTC 구간·알트 펀딩·유동성"
                        else:
                            msg += "\n· 익일 한국: 반도체·외국인·환율 ↔ BTC 상관"

                        msg += f"\n\n{US_CLOSE_DESK_TONE}\n{session_mood(safe_pct_from_snapshot(spx), safe_pct_from_snapshot(ixic), safe_pct_from_snapshot(dji), safe_pct_from_snapshot(sox), -safe_pct_from_snapshot(dxy), -safe_pct_from_snapshot(tnx))}"
                        msg += f"\n\n{ROOM_DISCLAIMER}"
                        await safe_send(bot, compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                        state.market_session_sent_dates[key] = now.date()
                        log_slot(key, now, False, kr_closed, us_holiday, weekend, "sent")

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
                            nk = now_kst()
                            msg = (
                                room_line("공포탐욕 · 임계", nk)
                                + "\n\n① 지표\n"
                                f"· {value} ({label})\n\n② 운영 메모\n"
                                "· 극단 구간은 추격 매매보다 포지션·레버 점검 우선.\n\n"
                                + ROOM_DISCLAIMER
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
                            nk = now_kst()
                            msg = (
                                room_line("김치프리미엄 · 임계", nk)
                                + "\n\n① 스냅\n"
                                f"· 프리미엄 {fmt_pct(premium)}\n"
                                f"· 업비트 BTC {upbit_krw:,.0f}원\n"
                                f"· 글로벌 BTC {binance_usdt:,.0f} USDT\n\n"
                                "② 운영 메모\n"
                                "· 국내외 괴리 확대 시 차익·유동성·출금 큐만 점검.\n\n"
                                + ROOM_DISCLAIMER
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

LIVE_NEWS_DAILY_LIMIT = env_int("LIVE_NEWS_DAILY_LIMIT", 56, min_value=12, max_value=500)
LIVE_COIN_DAILY_LIMIT = env_int("LIVE_COIN_DAILY_LIMIT", 40, min_value=3, max_value=200)
LIVE_SOL_ETF_DAILY_LIMIT = env_int("LIVE_SOL_ETF_DAILY_LIMIT", 4, min_value=1, max_value=12)
LIVE_NEWS_MAX_PER_SCAN = env_int("LIVE_NEWS_MAX_PER_SCAN", 1, min_value=1, max_value=30)
LIVE_NEWS_MIN_INTERVAL = timedelta(minutes=env_int("LIVE_NEWS_MIN_INTERVAL_MINUTES", 3, min_value=1, max_value=120))
LIVE_NIGHT_NEWS_MIN_INTERVAL = timedelta(minutes=env_int("LIVE_NEWS_NIGHT_INTERVAL_MINUTES", 18, min_value=5, max_value=90))
LIVE_NEWS_POLL_SECONDS = env_int("LIVE_NEWS_POLL_SECONDS", 120, min_value=30, max_value=900)
LIVE_NEWS_FEED_HEAD = env_int("LIVE_NEWS_FEED_HEAD", 18, min_value=5, max_value=40)
LIVE_NEWS_MIN_IMPORTANCE_SEND = env_int("LIVE_NEWS_MIN_IMPORTANCE_SEND", 7, min_value=5, max_value=10)
LIVE_NEWS_DESK_VOICE_LINE = env_bool("LIVE_NEWS_DESK_VOICE_LINE", False)
LIVE_ROOM_HOST_LINE = env_bool("LIVE_ROOM_HOST_LINE", False)
LIVE_NEWS_BTC_CHART = env_bool("LIVE_NEWS_BTC_CHART", True)  # 코인 뉴스: OKX 15m 캔들 + 자리·흐름
LIVE_NEWS_COMPACT_NUMERIC_BLOCK = env_bool("LIVE_NEWS_COMPACT_NUMERIC_BLOCK", True)
LIVE_NEWS_CARD_DISCLAIMER = "※ 정리용 · 투자 권유 아님 · 레버·청산·슬리피지·거래소·규제 리스크 전제."
LIVE_NEWS_NIGHT_COIN_MIN = env_int("LIVE_NEWS_NIGHT_COIN_MIN", 12, min_value=8, max_value=24)
LIVE_NEWS_NIGHT_OTHER_MIN = env_int("LIVE_NEWS_NIGHT_OTHER_MIN", 15, min_value=8, max_value=28)


def live_news_send_grades() -> frozenset[str]:
    raw = (os.getenv("LIVE_NEWS_SEND_GRADES") or "S,A,B").replace(" ", "").upper()
    parts = [p for p in raw.split(",") if p in ("S", "A", "B", "C")]
    return frozenset(parts) if parts else frozenset({"S", "A", "B"})


LIVE_NEWS_SEND_GRADES_SET = live_news_send_grades()
LIVE_NEWS_TAPE_MODE = env_bool("LIVE_NEWS_TAPE_MODE", False)
LIVE_NEWS_TAPE_SKIP_TRANSLATE = env_bool("LIVE_NEWS_TAPE_SKIP_TRANSLATE", False)
LIVE_NEWS_DEDUP_RETENTION_DAYS = env_int("LIVE_NEWS_DEDUP_RETENTION_DAYS", 14, min_value=1, max_value=90)
LIVE_NEWS_SQLITE_DEDUP = env_bool("LIVE_NEWS_SQLITE_DEDUP", True)
ENABLE_BOT_STATE_PERSIST = env_bool("ENABLE_BOT_STATE_PERSIST", True)
ENABLE_MACRO_PULSE = env_bool("ENABLE_MACRO_PULSE", False)
MACRO_PULSE_POLL_MINUTES = env_int("MACRO_PULSE_POLL_MINUTES", 45, min_value=15, max_value=180)
MACRO_PULSE_MIN_MOVE_PCT = env_float("MACRO_PULSE_MIN_MOVE_PCT", 0.35, min_value=0.05, max_value=3.0)
MACRO_PULSE_COOLDOWN_HOURS = env_int("MACRO_PULSE_COOLDOWN_HOURS", 3, min_value=1, max_value=24)
LIVE_TITLE_SIMILARITY_BLOCK_HOURS = 24
LIVE_TITLE_SIMILARITY_THRESHOLD = 0.58
LIVE_RECAP_HOURS = (18,)
LIVE_BTC_MIN_IMPORTANCE = 8
LIVE_MESSAGE_SOFT_LIMIT = 2000
BTC_LEVEL_ALERT_COOLDOWN_SEC = 3 * 60 * 60

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
    "속보", "긴급", "브리핑", "대통령실", "국회", "국무회의",
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
    # 'google news' 문자열은 구글 RSS 제목에 자주 붙어 과차단되므로 제외(스팸은 low_quality 쪽에서 걸러짐).
    "맛집", "학생", "challenge", "행사", "성과급 논란",
    "webinar", "summit", "booth", "fan meeting", "박람회", "컨퍼런스", "세미나", "축제", "초청행사", "티켓 오픈",
    "migrant worker", "migrant workers", "shelter", "laboring", "human rights", "refugee",
    "celebrity", "sports", "movie", "music", "crime", "accident", "weather",
    "이주 노동자", "노동자", "쉼터", "인권", "난민", "연예", "스포츠", "범죄", "사고", "날씨",
    "사설", "칼럼", "인터뷰", "기고", "op-ed", "oped",
    "입시", "수능", "논술", "내신", "홍보", "프로모션", "promotional", "advertorial",
    "press release", "media kit", "제휴 안내", "보도자료",
    "학회", "첫 공개", "서비스 공개", "병원용", "피부", "모발", "의료 ai", "전시",
    "친구 부럽지만", "기초과학", "과제 맡는다", "홍보성",
    "코리아넷뉴스", "재외동포신문", "한민족센터", "aipick", "ai픽",
    "불법 고용", "외국인 선원", "해경", "어선", "적발", "경찰",
    "가격 예측", "price prediction", "forecast", "전망 기사", "sponsored",
    "ufc", "열병식", "백악관 이벤트", "추천 종목", "왜 오르나", "급등 이유", "클릭",
)

EXTRA_LOW_QUALITY_BLOCK_TERMS = (
    "불법 고용", "외국인 선원", "해경", "어선", "적발", "사고", "범죄", "경찰",
    "학회", "가격 예측", "price prediction", "forecast", "전망 기사", "sponsored", "보도자료",
    "ufc", "열병식", "단순 행사", "셀럽", "스포츠", "백악관 이벤트", "클릭유도", "왜 오르나", "급등 이유", "추천 종목",
)

COIN_REQUIRED_KEYWORDS = (
    "etf", "sec", "승인", "유입", "청산", "스테이블코인",
    "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
    "coinbase", "jpmorgan", "jp morgan", "blackrock",
    "암호화폐", "가상자산", "비트코인", "이더리움", "솔라나",
    "whale", "고래", "온체인", "on-chain", "onchain",
    "xrp", "ripple", "defi", "nft", "binance", "bybit", "okx",
    "staking", "airdrop", "memecoin", "밈코인", "tvl", "liquidation",
    "업비트", "빗썸", "inflow", "outflow",
)


def is_crypto_etf_content(title: str, summary: str) -> bool:
    """True only when ETF/SEC context is clearly crypto (BTC/ETH/SOL etc.), not equity/반도체 ETF."""
    raw = f"{title} {summary}"
    t = raw.lower()
    if any(k in t for k in ("spot bitcoin", "spot btc", "spot ether", "spot ethereum", "비트코인 현물", "이더리움 현물", "솔라나 etf", "sol etf")):
        return True
    if any(k in t for k in ("grayscale", "greyscale", "blackrock", "ishares bitcoin", "bitwise", "ark invest")):
        return True
    if "etf" in t and any(k in t for k in ("비트코인", "이더리움", "솔라나", "bitcoin", "ethereum", "solana", "btc", "eth", "crypto", "암호화폐")):
        return True
    if ("sec" in t or "증권거래위원회" in raw) and any(
        k in t for k in ("bitcoin", "btc", "ethereum", "eth", "solana", "솔라나", "sol etf", "비트코인", "이더리움", "암호화폐")
    ):
        return True
    pairs = (
        ("bitcoin", "etf"),
        ("btc", "etf"),
        ("비트코인", "etf"),
        ("ethereum", "etf"),
        ("eth", "etf"),
        ("이더리움", "etf"),
        ("solana", "etf"),
        ("솔라나", "etf"),
    )
    return any(a in t and "etf" in t for a, _ in pairs)


def _etf_loose_commodity_markers(title: str, summary: str) -> bool:
    """원유·WTI 등만 보조 판별. 'wti'를 'twist' 같은 단어 부분문자열로 잡지 않도록 단어 경계 사용."""
    raw = f"{title} {summary}"
    t = raw.lower()
    if any(k in raw for k in ("원유", "구리", "농산물")):
        return True
    if "금 " in raw or "은 " in raw:
        return True
    if re.search(r"\bwti\b", t):
        return True
    if re.search(r"\bbrent\b", t):
        return True
    return False


def etf_asset_kind(title: str, summary: str) -> str:
    """
    ETF 주제 세분화. 'etf' 단독으로는 crypto_etf로 가지 않음.
    반환: crypto_etf | semiconductor_etf | ai_etf | korea_stock_etf | commodity_etf | bond_etf | unknown_equity_etf | none
    """
    raw = f"{title} {summary}"
    t = raw.lower()
    if "etf" not in t and "상장지수" not in raw:
        return "none"

    if any(k in t for k in ("국채 etf", "채권 etf", "treasury etf", "t-bond", "장기국채", "회사채 etf", "bond etf")):
        return "bond_etf"
    if any(
        k in t
        for k in (
            "원유 etf",
            "oil etf",
            "gold etf",
            "silver etf",
            "금 etf",
            "은 etf",
            "commodity etf",
            "원자재 etf",
        )
    ):
        return "commodity_etf"

    if is_crypto_etf_content(title, summary):
        return "crypto_etf"

    if "etf" in t and _etf_loose_commodity_markers(title, summary):
        return "commodity_etf"

    semi_kw = (
        "반도체",
        "semiconductor",
        "hbm",
        "sox",
        "필라델피아",
        "nvidia",
        "엔비디아",
        "gpu",
        "삼성전자",
        "sk하이닉스",
        "하이닉스",
        "hynix",
        "micron",
        "마이크론",
        "tsmc",
        "chip",
        "칩",
    )
    if any(k in t for k in semi_kw):
        return "semiconductor_etf"

    if any(k in t for k in ("inference", "hyperscaler", "hyperscal", "ai etf")) or (
        "etf" in t and any(k in t for k in ("데이터센터", "ai 서버", "ai 인프라"))
    ):
        return "ai_etf"

    if any(
        k in t
        for k in (
            "kodex",
            "코덱스",
            "tiger",
            "타이거",
            "hanaro",
            "하나로",
            "ace etf",
            "kbstar",
            "kb스타",
            "nh아문디",
            "아문디",
            "삼성자산",
            "미래에셋",
            "한국투자",
            "solidx",
        )
    ):
        return "korea_stock_etf"

    if "etf" in t:
        return "unknown_equity_etf"
    return "none"


def live_news_category_label(category: str, title: str, summary: str) -> str:
    if category == "이슈":
        return "실시간 이슈"
    if etf_asset_kind(title, summary) == "semiconductor_etf" and category in ("한국", "미국", "세계"):
        return f"{category} · 반도체"
    return category


def is_major_domestic_semi_catalyst(title: str, summary: str) -> bool:
    """삼성전자·SK하이닉스 직접 실적·수주·대형 설비/캡스 등일 때만 중요도 상단(8+) 허용."""
    raw = f"{title} {summary}"
    t = raw.lower()
    if not any(k in t for k in ("삼성전자", "sk하이닉스", "하이닉스", "samsung", "hynix", "sk hynix")):
        return False
    keys = (
        "실적",
        "earnings",
        "가이던스",
        "guidance",
        "eps",
        "영업이익",
        "수주",
        "contract",
        "납품",
        "capex",
        "설비",
        "투자",
        "대규모",
        "증설",
        "가동",
    )
    return any(k in t for k in keys)


def _live_news_has_domestic_etf_brand(title_ko: str) -> bool:
    tk = (title_ko or "").lower()
    return any(
        k in tk
        for k in (
            "hanaro",
            "하나로",
            "kodex",
            "코덱스",
            "tiger",
            "타이거",
            "ace ",
            "nh아문디",
            "아문디",
            "solidx",
            "kb스타",
            "미래에셋",
            "한국투자",
        )
    )


def kr_equity_etf_depth_paragraph(ek: str, title_ko: str, event_line: str, fact_lines: list[str]) -> str:
    """국내 상장 반도체·AI·지수형 ETF용 4~7줄 분량 정보방 톤."""
    chunks: list[str] = []
    ev = (event_line or "").strip()
    if ev:
        chunks.append(ev)

    brand = _live_news_has_domestic_etf_brand(title_ko or "")
    if brand:
        chunks.append(
            "국내 거래소에 상장돼 환전 부담 없이 테마를 담을 수 있어,\n개인·기관 체감 수급에 바로 붙는 상품임."
        )

    seen = set()
    for ln in fact_lines:
        s = (ln or "").strip()
        if not s or len(s) < 10 or s in seen:
            continue
        if ev and s in ev:
            continue
        seen.add(s)
        chunks.append(s)
        if len(chunks) >= 5:
            break

    tlk = (title_ko or "").lower()
    memory_chain = any(
        k in tlk for k in ("메모리", "memory", "hbm", "ai메모리", "미국ai", "micron", "마이크론", "broadcom", "브로드컴", "nvidia", "엔비디아")
    )

    if ek == "semiconductor_etf":
        if not any(("hbm" in c.lower() or "메모리" in c or "엔비디아" in c or "마이크론" in c) for c in chunks):
            if memory_chain:
                chunks.append(
                    "엔비디아·마이크론·브로드컴 같은 AI 메모리 밸류체인에 노출되는 상품이면,\nHBM·고대역폭 메모리 수요 기대와 같은 축에서 읽히는 뉴스임."
                )
            else:
                chunks.append(
                    "해외 반도체 대표주에 분산 노출되는 구조라,\nAI 메모리·GPU 쪽 기대와 함께 읽히는 경우가 많음."
                )
        chunks.append(
            "국내에선 삼성전자·SK하이닉스 수급이 먼저이고,\n밤에 SOX·NVDA 프리가 크게 움직일 때 국내 선물·현물이 같은 방향으로 붙는지 보조 지표로 쓰면 됨."
        )
    elif ek == "ai_etf":
        chunks.append(
            "데이터센터·GPU·전력 쪽 기대가 한 덩어리로 움직일 때가 많아,\n국내에선 AI 인프라·반도체 대장주 수급이 같은 축으로 움직이는지 확인하는 게 맞음."
        )
        chunks.append(
            "나스닥 선물·빅테크 프리는 참고용으로 두고,\n환율·외국인 순매수가 같은 방향으로 붙는지 같이 읽는 게 좋음."
        )
    else:  # korea_stock_etf
        chunks.append(
            "국내 상장 ETF는 코스피·코스닥 비중과 외국인·기관 자금 배분이 바로 연결되는 경우가 많음."
        )
        chunks.append(
            "장 초반 체결·환율만 같이 보면 방향 잡기가 수월함."
        )

    if len(chunks) > 7:
        chunks = chunks[:7]
    return "\n\n".join(x.strip() for x in chunks if x.strip()).strip()


LIVE_AI_MARKET_ANCHORS = (
    "엔비디아",
    "nvidia",
    "반도체",
    "semiconductor",
    "데이터센터",
    "클라우드",
    "aws",
    "microsoft",
    "msft",
    "google",
    "alphabet",
    "googl",
    "meta",
    "oracle",
    "capex",
    "gpu",
    "hbm",
    "전력",
    "원전",
    "실적",
    "가이던스",
    "earnings",
    "guidance",
    "투자",
    "인수",
    "공급계약",
)

_LIVE_KR_NEWS_QUERY_INNER = (
    "삼성전자 OR SK하이닉스 OR 코스피 OR 환율 OR 외국인 OR 반도체 OR 한화에어로스페이스 OR "
    "두산에너빌리티 OR LG에너지솔루션 OR 현대차 OR 기아 OR 삼성중공업 OR HD현대 OR "
    "조선 OR 해상풍력 OR 관세 OR 트럼프 OR 양자컴 OR 로봇 OR 휴머노이드 OR 한타 OR "
    "중동 OR 재건 OR 실적 OR 공시 OR 베선트 OR ETF"
)
_LIVE_KR_NEWS_RSS = (
    "https://news.google.com/rss/search?q=" + quote("(" + _LIVE_KR_NEWS_QUERY_INNER + ")") + "&hl=ko&gl=KR&ceid=KR:ko"
)

_LIVE_COIN_GOOGLE_Q = (
    "Bitcoin OR BTC OR Ethereum OR ETH OR Solana OR SOL OR XRP OR Ripple OR crypto OR stablecoin OR "
    "ETF OR SEC OR regulation OR liquidation OR DeFi OR Binance OR Bybit OR OKX OR "
    "BlackRock OR Grayscale OR MicroStrategy OR MSTR OR funding OR whale OR options OR "
    "tokenization OR Coinbase OR on-chain OR inflow OR outflow OR staking OR hack OR exploit OR "
    "Avalanche OR AVAX OR Polygon OR MATIC OR Chainlink OR LINK OR Dogecoin OR DOGE OR "
    "memecoin OR airdrop OR TVL OR Arbitrum OR Optimism OR rollup OR L2 OR "
    "가상자산 OR 업비트 OR 온체인 OR 고래 OR 거래대금 OR 밈코인"
)
_LIVE_COIN_GOOGLE_RSS = (
    "https://news.google.com/rss/search?q=" + quote("(" + _LIVE_COIN_GOOGLE_Q + ")") + "&hl=ko&gl=KR&ceid=KR:ko"
)

_LIVE_US_NEWS_QUERY_INNER = (
    "Nvidia OR Tesla OR Apple OR Meta OR Microsoft OR Amazon OR Google OR Nasdaq OR "
    "Fed OR CPI OR PPI OR earnings OR guidance OR semiconductor OR AI OR "
    "IonQ OR Rigetti OR Palantir OR AMD OR Oracle OR CoreWeave OR Vertiv OR nuclear OR defense OR "
    "Bitcoin OR BTC OR Ethereum OR ETH OR crypto OR stablecoin OR Solana OR SOL OR spot ETF OR BlackRock OR SEC"
)
_LIVE_US_NEWS_RSS = (
    "https://news.google.com/rss/search?q=" + quote("(" + _LIVE_US_NEWS_QUERY_INNER + ")") + "&hl=ko&gl=KR&ceid=KR:ko"
)

_LIVE_GOOGLE_TOP_KR_RSS = "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"

_COINDESK_RSS = "https://www.coindesk.com/arc/outboundfeeds/rss/"
_COINTELEGRAPH_RSS = "https://cointelegraph.com/rss"

# 코인·미국(비트코인·ETF·연준) 우선, 한국 주식 속보는 뒤에서 필터링.
LIVE_CATEGORY_FEEDS = (
    ("🔥", "이슈", _LIVE_GOOGLE_TOP_KR_RSS),
    ("🟠", "코인", _LIVE_COIN_GOOGLE_RSS),
    ("🟠", "코인", _COINDESK_RSS),
    ("🟠", "코인", _COINTELEGRAPH_RSS),
    ("🇺🇸", "미국", _LIVE_US_NEWS_RSS),
    ("🌍", "세계", "https://news.google.com/rss/search?q=(oil%20OR%20WTI%20OR%20dollar%20OR%20Iran%20OR%20Israel%20OR%20Hormuz%20OR%20missile%20OR%20ceasefire%20OR%20sanction%20OR%20China%20OR%20supply%20chain%20OR%20tariff%20OR%20rare%20earth%20OR%20shipping%20OR%20uranium%20OR%20power%20grid)&hl=ko&gl=KR&ceid=KR:ko"),
    ("🇰🇷", "한국", _LIVE_KR_NEWS_RSS),
)


def iter_live_news_feeds() -> Tuple[Tuple[str, str, str], ...]:
    """Railway 등에서 이슈·세계 피드를 끄고 싶을 때 ENABLE_LIVE_ISSUE_FEED / ENABLE_LIVE_WORLD_FEED."""
    out: list[Tuple[str, str, str]] = []
    for row in LIVE_CATEGORY_FEEDS:
        _, cat, _ = row
        if cat == "이슈" and not env_bool("ENABLE_LIVE_ISSUE_FEED", True):
            continue
        if cat == "세계" and not env_bool("ENABLE_LIVE_WORLD_FEED", True):
            continue
        out.append(row)
    return tuple(out)


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


def is_non_market_society_noise(title: str, summary: str) -> bool:
    """증시·코인·매크로와 거리 먼 사회/생활 이슈(대형 채널 노이즈)."""
    blob = f"{title} {summary}"
    if "인천공항" in blob and ("주차" in blob or "주차장" in blob):
        return True
    if ("범여권" in blob or "야권" in blob) and any(
        k in blob for k in ("시장", "단일화", "지선", "선거", "시의원", "도지사", "구청장", "군수")
    ):
        if not any(k in blob for k in ("코스피", "코스닥", "증시", "외국인", "기관", "환율", "ETF", "실적")):
            return True
    return False


def is_hard_blocked_live_news(title: str, summary: str, link: str = "") -> bool:
    text_value = f"{title} {summary}".lower()
    if is_non_market_society_noise(title, summary):
        return True
    if any(k.lower() in text_value for k in LIVE_HARD_BLOCK_TERMS):
        return True
    try:
        host = urlparse(link or "").netloc.lower()
    except Exception:
        host = ""
    if "coinmarketcap.com" in host or "coinpaprika.com" in host or "coingecko.com" in host:
        return True
    return False


def low_quality_block_reason(title: str, summary: str) -> str:
    text_value = f"{title} {summary}".lower()
    for term in EXTRA_LOW_QUALITY_BLOCK_TERMS:
        if term.lower() in text_value:
            return f"low_quality:{term}"
    return ""


def recap_title_hash(title: str) -> str:
    normalized = normalize_title_for_dedup(strip_news_source_tail(title or ""))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def is_stale_spot_etf_rehash(title: str, summary: str) -> bool:
    """현물 ETF 승인 당시 리캡·낚시 제목(이미 알려진 이슈) 필터."""
    t = f"{title} {summary}".lower()
    rehash = (
        "etf 승인",
        "bitcoin etf",
        "비트코인 etf",
        "spot etf",
        "spot bitcoin",
        "환호하는",
        "그 이유",
        "의미는",
        "why it matters",
        "what it means",
        "celebrat",
    )
    if not any(k in t for k in rehash):
        return False
    fresh = (
        "유입",
        "유출",
        "inflow",
        "outflow",
        "billion",
        "million",
        "억",
        "조",
        "record",
        "어제",
        "today",
        "일일",
        "daily",
        "sec filing",
        "공시",
        "순매수",
        "net flow",
    )
    return not any(k in t for k in fresh)


def coin_news_block_reason(title: str, summary: str) -> str:
    text_value = f"{title} {summary}".lower()
    raw_h = f"{title} {summary}"
    if is_stale_spot_etf_rehash(title, summary):
        return "coin_stale_etf_rehash"
    if re.search(r"(그 이유와|의미는\s*\?|why it matters|what it means)", raw_h, re.I):
        return "coin_clickbait_question"
    if "solana나의" in raw_h:
        return "coin_broken_translation"
    if "what happened in crypto today" in text_value or "what-happened-in-crypto" in text_value:
        return "coin_daily_roundup_junk"
    if "오늘" in raw_h and "암호화폐" in raw_h and ("업계" in raw_h or "일어난" in raw_h):
        return "coin_daily_roundup_junk"
    if "가격 예측" in text_value or "price prediction" in text_value or "forecast" in text_value:
        return "coin_price_prediction"
    if any(k in text_value for k in ("전망", "관측", "보는 구간", "분위기")) and "etf 승인" not in text_value and "sec" not in text_value:
        return "coin_generic_outlook"
    if not any(k in text_value for k in COIN_REQUIRED_KEYWORDS):
        return "coin_missing_required_keyword"
    return ""


def has_live_ai_market_anchor(text: str) -> bool:
    tl = (text or "").lower()
    return any(a.lower() in tl for a in LIVE_AI_MARKET_ANCHORS)


def text_suggests_generic_ai_product(title: str, summary: str) -> bool:
    raw = f"{title} {summary}"
    tl = raw.lower()
    if re.search(r"\bai\b", tl):
        return True
    if any(k in raw for k in ("인공지능", "생성형")):
        return True
    compact = tl.replace(" ", "")
    if "의료ai" in compact or "ai서비스" in compact:
        return True
    return False


def is_live_ai_without_market_anchor(title: str, summary: str) -> bool:
    if not text_suggests_generic_ai_product(title, summary):
        return False
    return not has_live_ai_market_anchor(f"{title} {summary}")


def _blob_has_crypto_market_signal(blob: str, blob_l: str) -> bool:
    """코인 시장으로 읽을 근거(솔루션·meth 등 오탐 방지)."""
    if any(
        k in blob_l
        for k in (
            "bitcoin",
            "btc",
            "ethereum",
            "solana",
            "xrp",
            "defi",
            "stablecoin",
            "비트코인",
            "이더리움",
            "암호화폐",
            "솔라나",
            "스테이블",
            "펀딩",
            "레버리지",
            "liquidation",
            "청산",
            "현물 etf",
            "spot etf",
            "coinbase",
            "binance",
            "bybit",
            "okx",
            "whale",
            "staking",
            "airdrop",
            "memecoin",
            "arbitrum",
            "optimism",
            "avalanche",
            "polygon",
            "chainlink",
            "dogecoin",
            "rollup",
        )
    ):
        return True
    if any(k in blob for k in ("가상자산", "업비트", "빗썸", "온체인", "고래", "밈코인", "에어드랍")):
        return True
    if re.search(r"\bcrypto(currency|currencies|assets?|market)\b", blob_l):
        return True
    if re.search(r"\bbtc\b", blob_l) or re.search(r"\beth\b", blob_l) or re.search(r"\bsolana\b", blob_l):
        return True
    if re.search(r"\bsol\b", blob_l) and "솔루션" not in blob and "solution" not in blob_l:
        return True
    if re.search(r"\bonchain\b", blob_l) or "on-chain" in blob_l:
        return True
    if re.search(r"\btvl\b", blob_l):
        return True
    if re.search(r"\bavax\b", blob_l) or re.search(r"\bmatic\b", blob_l) or re.search(r"\blink\b", blob_l):
        return True
    if re.search(r"\b(doge|dot|ada)\b", blob_l):
        return True
    if "layer 2" in blob_l or "layer2" in blob_l.replace(" ", "") or re.search(r"\bl2\b", blob_l):
        return True
    return False


def _blob_is_mideast_military(blob: str, blob_l: str) -> bool:
    keys = (
        "이란",
        "iran",
        "사우디",
        "saudi",
        "uae",
        "아랍에미리트",
        "이라크",
        "iraq",
        "이스라엘",
        "israel",
        "후티",
        "houthi",
        "가자",
        "gaza",
        "호르무즈",
        "hormuz",
        "미사일",
        "missile",
        "공습",
        "중동",
        "middle east",
        "걸프",
        "gulf",
    )
    return sum(1 for k in keys if k in blob or k in blob_l) >= 2


def _blob_is_geopolitics_mideast(blob: str, blob_l: str) -> bool:
    """군사 2키 외에, 협상·종전·긴장 완화 등 지정학 보도도 같은 데스크 맥락으로."""
    if _blob_is_mideast_military(blob, blob_l):
        return True
    if "이란" in blob or re.search(r"\biran\b", blob_l):
        dip = (
            "협상",
            "종전",
            "트럼프",
            "trump",
            "biden",
            "바이든",
            "백악관",
            "핵",
            "nuclear",
            "제재",
            "sanction",
            "미사일",
            "missile",
            "이스라엘",
            "israel",
            "호르무즈",
            "hormuz",
            "사우디",
            "saudi",
            "uae",
            "쿠웨이트",
            "kuwait",
            "긴장",
            "완화",
            "휴전",
            "ceasefire",
        )
        return any(k in blob or k in blob_l for k in dip)
    if "이스라엘" in blob or re.search(r"\bisrael\b", blob_l):
        return any(k in blob or k in blob_l for k in ("가자", "gaza", "이란", "iran", "미사일", "missile", "핵", "nuclear"))
    return False


def _blob_is_china_us_geopolitics(blob: str, blob_l: str) -> bool:
    if _blob_is_geopolitics_mideast(blob, blob_l):
        return False
    china_side = bool(
        any(k in blob for k in ("시진핑", "中", "중국", "대만", "台灣"))
        or re.search(r"\bxi jinping\b", blob_l)
        or re.search(r"\bchina\b", blob_l)
    )
    us_side = bool(
        any(k in blob for k in ("트럼프", "Trump", "미국", "Biden", "바이든", "백악관", "정상", "회동", "관저", "방중", "미중", "美中"))
        or re.search(r"\bu\.s\.\b", blob_l)
    )
    if china_side and us_side:
        return True
    if ("summit" in blob_l or "tariff" in blob_l) and ("china" in blob_l or "beijing" in blob_l) and (
        "trump" in blob_l or "u.s." in blob_l or " us " in blob_l
    ):
        return True
    return False


def _blob_is_kr_semiconductor_risk(blob: str, blob_l: str) -> bool:
    if not any(k in blob for k in ("반도체", "semiconductor", "hbm", "nvidia", "엔비디아", "sox")):
        if not any(k in blob for k in ("삼성전자", "SK하이닉스", "하이닉스", "美 반도체", "미국 반도체")):
            return False
    return any(
        k in blob or k in blob_l
        for k in (
            "급락",
            "급등",
            "crash",
            "plunge",
            "sell-off",
            "selloff",
            "월요일",
            "검은",
            "갭",
            "gap",
            "4%",
            "5%",
        )
    )


def _blob_is_kr_corporate_earnings(blob: str, blob_l: str) -> bool:
    if not any(k in blob for k in ("실적", "매출", "분기", "영업이익", "가이던스", "어닝")):
        return False
    return any(
        k in blob
        for k in (
            "LG에너지",
            "LG에너지솔루션",
            "삼성전자",
            "SK하이닉스",
            "SK이노베이션",
            "현대차",
            "기아",
            "POSCO",
            "포스코",
            "NAVER",
            "네이버",
            "카카오",
            "셀트리온",
            "SK바이오",
        )
    )


def effective_live_news_category(category_emoji: str, category: str, title: str, summary: str, link: str) -> Tuple[str, str]:
    blob = f"{title} {summary}"
    blob_l = blob.lower()
    lk = (link or "").lower()
    if "coindesk.com" in lk or "cointelegraph.com" in lk:
        return "🟠", "코인"

    if _blob_is_geopolitics_mideast(blob, blob_l) and not _blob_has_crypto_market_signal(blob, blob_l):
        return "🌍", "세계"
    if _blob_is_kr_corporate_earnings(blob, blob_l) and not _blob_has_crypto_market_signal(blob, blob_l):
        return "🇰🇷", "한국"
    if ("인천공항" in blob or "김포공항" in blob) and ("주차" in blob or "주차장" in blob):
        return "🇰🇷", "한국"

    coin_tokens = (
        "coindesk",
        "cointelegraph",
        "bitcoin",
        "ethereum",
        "solana",
        "비트코인",
        "이더리움",
        "암호화폐",
        "cryptocurrency",
        "defi",
        "stablecoin",
        "스테이블",
        "whale",
        "가상자산",
        "업비트",
        "arbitrum",
        "optimism",
        "memecoin",
        "airdrop",
        "on-chain",
        "onchain",
        "staking",
    )
    if any(k in blob_l for k in coin_tokens):
        return "🟠", "코인"
    if re.search(r"\bbtc\b", blob_l) or re.search(r"\beth\b", blob_l) or re.search(r"\bsolana\b", blob_l):
        return "🟠", "코인"
    if re.search(r"\bsol\b", blob_l) and "솔루션" not in blob and "solution" not in blob_l:
        return "🟠", "코인"
    if "청산" in blob and (
        "liquidation" in blob_l
        or "암호화폐" in blob
        or "비트코인" in blob
        or "bitcoin" in blob_l
        or re.search(r"\bcrypto(currency|currencies|assets?)\b", blob_l)
    ):
        return "🟠", "코인"
    if "거래소" in blob and any(
        k in blob_l
        for k in ("binance", "coinbase", "bybit", "okx", "암호화폐", "cryptocurrency", "비트코인", "bitcoin")
    ):
        return "🟠", "코인"
    if "etf" in blob_l and is_crypto_etf_content(title, summary):
        return "🟠", "코인"
    ek = etf_asset_kind(title, summary)
    if ek == "semiconductor_etf":
        if any(k in blob for k in ("NH", "nh", "KODEX", "Kodex", "TIGER", "Tiger", "코스피", "삼성전자", "하이닉스", "한국")):
            return "🇰🇷", "한국"
        return "🇺🇸", "미국"
    if ek in ("ai_etf", "korea_stock_etf", "unknown_equity_etf", "commodity_etf", "bond_etf"):
        if any(k in blob for k in ("NH", "nh", "KODEX", "Kodex", "TIGER", "Tiger", "코스피", "코스닥", "한국")):
            return "🇰🇷", "한국"
        if ek == "korea_stock_etf":
            return "🇰🇷", "한국"
        return "🇺🇸", "미국"
    if (
        re.search(r"\bhormuz\b", blob_l)
        or "호르무즈" in blob
        or re.search(r"\biran\b", blob_l)
        or "이란" in blob
        or re.search(r"\bisrael\b", blob_l)
        or "이스라엘" in blob
        or re.search(r"\bwti\b", blob_l)
        or re.search(r"\bbrent\b", blob_l)
        or re.search(r"\boil\b", blob_l)
        or "유가" in blob
        or "원유" in blob
    ):
        return "🌍", "세계"
    kr_hosts = (
        ".kr/",
        ".co.kr",
        "naver.com",
        "daum.net",
        "yna.co.kr",
        "chosun.com",
        "joins.com",
        "mk.co.kr",
        "sedaily.com",
        "hankyung.com",
        "etnews.co.kr",
        "zdnet.co.kr",
        "hani.co.kr",
        "mt.co.kr",
    )
    if any(h in lk for h in kr_hosts):
        return "🇰🇷", "한국"
    hangul = len(re.findall(r"[가-힣]", title or ""))
    latin = len(re.findall(r"[A-Za-z]", title or ""))
    if hangul >= 10 and hangul >= max(8, latin * 0.25):
        return "🇰🇷", "한국"
    if any(k in blob for k in ("삼성전자", "SK하이닉스", "삼성SDI", "현대차", "네이버", "카카오", "서울", "코스피", "코스닥", "환율", "산업부", "대한민국", "한국 기업")):
        return "🇰🇷", "한국"
    return category_emoji, category


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


def live_news_title_fingerprint(title: str) -> str:
    """같은 기사가 URL만 다르게 들어올 때 막기 위한 제목 지문."""
    base = normalize_title_for_dedup(strip_news_source_tail(title or ""))
    if not base.strip():
        base = normalize_title_for_dedup(html_clean(title or "", 400))
    return "fp:" + hashlib.sha256(base.encode("utf-8")).hexdigest()


_LIVE_NEWS_DB_LOCK = threading.Lock()


def _live_news_db_path() -> str:
    base = (os.getenv("BOT_DATA_DIR") or ".").strip() or "."
    try:
        os.makedirs(base, exist_ok=True)
    except OSError:
        pass
    return os.path.join(base, "live_news_dedup.sqlite3")


def _live_news_db_init() -> None:
    path = _live_news_db_path()
    with _LIVE_NEWS_DB_LOCK:
        con = sqlite3.connect(path, timeout=30)
        try:
            con.execute("CREATE TABLE IF NOT EXISTS live_news_hashes (h TEXT PRIMARY KEY, ts REAL NOT NULL)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_live_news_hashes_ts ON live_news_hashes(ts)")
            con.commit()
        finally:
            con.close()


def live_news_db_has_recent(hashes: Tuple[str, ...], now_ts: float) -> bool:
    if not LIVE_NEWS_SQLITE_DEDUP or not hashes:
        return False
    cutoff = now_ts - LIVE_NEWS_DEDUP_RETENTION_DAYS * 86400
    try:
        path = _live_news_db_path()
        with _LIVE_NEWS_DB_LOCK:
            con = sqlite3.connect(path, timeout=30)
            try:
                for h in hashes:
                    cur = con.execute("SELECT 1 FROM live_news_hashes WHERE h = ? AND ts >= ?", (h, cutoff))
                    if cur.fetchone():
                        return True
                return False
            finally:
                con.close()
    except Exception:
        logging.exception("live_news_db_has_recent")
        return False


def live_news_db_store_hashes(hashes: Tuple[str, ...], now_ts: float) -> None:
    if not LIVE_NEWS_SQLITE_DEDUP or not hashes:
        return
    try:
        path = _live_news_db_path()
        cutoff = now_ts - LIVE_NEWS_DEDUP_RETENTION_DAYS * 86400
        with _LIVE_NEWS_DB_LOCK:
            con = sqlite3.connect(path, timeout=30)
            try:
                for h in hashes:
                    con.execute("INSERT OR REPLACE INTO live_news_hashes (h, ts) VALUES (?, ?)", (h, now_ts))
                con.execute("DELETE FROM live_news_hashes WHERE ts < ?", (cutoff,))
                con.commit()
            finally:
                con.close()
    except Exception:
        logging.exception("live_news_db_store_hashes")


_BOT_STATE_DB_LOCK = threading.Lock()
_BOT_STATE_SNAPSHOT_KEY = "snapshot_v1"


def _bot_state_db_path() -> str:
    base = (os.getenv("BOT_DATA_DIR") or ".").strip() or "."
    try:
        os.makedirs(base, exist_ok=True)
    except OSError:
        pass
    return os.path.join(base, "bot_state.sqlite3")


def _bot_state_db_init() -> None:
    path = _bot_state_db_path()
    with _BOT_STATE_DB_LOCK:
        con = sqlite3.connect(path, timeout=30)
        try:
            con.execute("CREATE TABLE IF NOT EXISTS bot_kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
            con.commit()
        finally:
            con.close()


def bot_state_get(key: str) -> Optional[str]:
    try:
        path = _bot_state_db_path()
        with _BOT_STATE_DB_LOCK:
            con = sqlite3.connect(path, timeout=30)
            try:
                cur = con.execute("SELECT v FROM bot_kv WHERE k = ?", (key,))
                row = cur.fetchone()
                return str(row[0]) if row else None
            finally:
                con.close()
    except Exception:
        logging.exception("bot_state_get")
        return None


def bot_state_set(key: str, value: str) -> None:
    try:
        path = _bot_state_db_path()
        with _BOT_STATE_DB_LOCK:
            con = sqlite3.connect(path, timeout=30)
            try:
                con.execute("INSERT OR REPLACE INTO bot_kv (k, v) VALUES (?, ?)", (key, value))
                con.commit()
            finally:
                con.close()
    except Exception:
        logging.exception("bot_state_set")


def _date_dict_to_json(d: Dict[str, date]) -> dict:
    return {str(k): v.isoformat() for k, v in (d or {}).items()}


def _date_dict_from_json(raw: Optional[dict]) -> Dict[str, date]:
    out: Dict[str, date] = {}
    if not raw:
        return out
    for k, v in raw.items():
        if isinstance(v, str):
            try:
                out[str(k)] = date.fromisoformat(v)
            except ValueError:
                continue
    return out


def bot_state_hydrate(state: State) -> None:
    if not ENABLE_BOT_STATE_PERSIST:
        return
    _bot_state_db_init()
    raw = bot_state_get(_BOT_STATE_SNAPSHOT_KEY)
    if not raw:
        return
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.warning("bot_state_hydrate: invalid json")
        return
    today_kst = now_kst().date()

    state.briefing_sent_dates = _date_dict_from_json(data.get("briefing_sent_dates"))
    state.market_session_sent_dates = _date_dict_from_json(data.get("market_session_sent_dates"))
    state.digest_sent_dates = _date_dict_from_json(data.get("digest_sent_dates"))
    state.overnight_recap_sent_dates = _date_dict_from_json(data.get("overnight_recap_sent_dates"))

    nd = data.get("news_daily_date")
    if isinstance(nd, str) and date.fromisoformat(nd) == today_kst:
        state.news_daily_date = today_kst
        state.news_daily_count = int(data.get("news_daily_count") or 0)

    vsd = data.get("volume_surge_daily_date")
    if isinstance(vsd, str) and date.fromisoformat(vsd) == today_kst:
        state.volume_surge_daily_date = today_kst
        state.volume_surge_daily_count = int(data.get("volume_surge_daily_count") or 0)

    cld = data.get("coin_live_daily_date")
    if isinstance(cld, str) and date.fromisoformat(cld) == today_kst:
        state.coin_live_daily_date = today_kst
        state.coin_live_daily_count = int(data.get("coin_live_daily_count") or 0)

    sld = data.get("sol_etf_daily_date")
    if isinstance(sld, str) and date.fromisoformat(sld) == today_kst:
        state.sol_etf_daily_date = today_kst
        state.sol_etf_daily_count = int(data.get("sol_etf_daily_count") or 0)

    lnd = data.get("live_news_daily_date")
    if isinstance(lnd, str) and date.fromisoformat(lnd) == today_kst:
        state.live_news_daily_date = today_kst
        state.live_news_daily_count = int(data.get("live_news_daily_count") or 0)

    lls = data.get("live_last_sent_at")
    if isinstance(lls, str):
        try:
            state.live_last_sent_at = datetime.fromisoformat(lls.replace("Z", "+00:00"))
        except ValueError:
            pass

    lns = data.get("last_news_sent_at")
    if isinstance(lns, str):
        try:
            state.last_news_sent_at = datetime.fromisoformat(lns.replace("Z", "+00:00"))
        except ValueError:
            pass

    rs = data.get("recap_sent_keys")
    if isinstance(rs, list):
        state.recap_sent_keys = {str(x) for x in rs if x}

    rud = data.get("recap_used_news_date")
    if isinstance(rud, str) and date.fromisoformat(rud) == today_kst:
        state.recap_used_news_date = today_kst
        rt = data.get("recap_used_news_titles")
        if isinstance(rt, list):
            state.recap_used_news_titles = {str(x) for x in rt if x}
        rtop = data.get("recap_used_topics")
        if isinstance(rtop, list):
            state.recap_used_topics = {str(x) for x in rtop if x}

    mp = data.get("macro_pulse_last_pcts")
    if isinstance(mp, dict):
        try:
            state.macro_pulse_last_pcts = {str(k): float(v) for k, v in mp.items()}
        except (TypeError, ValueError):
            state.macro_pulse_last_pcts = None
    mps = data.get("macro_pulse_last_sent")
    if isinstance(mps, str):
        try:
            state.macro_pulse_last_sent = datetime.fromisoformat(mps.replace("Z", "+00:00"))
        except ValueError:
            pass


def bot_state_save_snapshot(state: State) -> None:
    if not ENABLE_BOT_STATE_PERSIST:
        return
    _bot_state_db_init()
    today_kst = now_kst().date()
    payload: dict = {
        "briefing_sent_dates": _date_dict_to_json(state.briefing_sent_dates),
        "market_session_sent_dates": _date_dict_to_json(state.market_session_sent_dates),
        "digest_sent_dates": _date_dict_to_json(state.digest_sent_dates),
        "overnight_recap_sent_dates": _date_dict_to_json(state.overnight_recap_sent_dates),
        "news_daily_date": state.news_daily_date.isoformat() if state.news_daily_date else None,
        "news_daily_count": state.news_daily_count,
        "volume_surge_daily_date": state.volume_surge_daily_date.isoformat() if state.volume_surge_daily_date else None,
        "volume_surge_daily_count": state.volume_surge_daily_count,
        "coin_live_daily_date": state.coin_live_daily_date.isoformat() if state.coin_live_daily_date else None,
        "coin_live_daily_count": state.coin_live_daily_count,
        "sol_etf_daily_date": state.sol_etf_daily_date.isoformat() if state.sol_etf_daily_date else None,
        "sol_etf_daily_count": state.sol_etf_daily_count,
        "live_news_daily_date": state.live_news_daily_date.isoformat() if state.live_news_daily_date else None,
        "live_news_daily_count": state.live_news_daily_count,
        "live_last_sent_at": state.live_last_sent_at.isoformat() if state.live_last_sent_at else None,
        "last_news_sent_at": state.last_news_sent_at.isoformat() if state.last_news_sent_at else None,
        "recap_sent_keys": sorted(state.recap_sent_keys),
    }
    if state.recap_used_news_date == today_kst:
        payload["recap_used_news_date"] = today_kst.isoformat()
        payload["recap_used_news_titles"] = sorted(state.recap_used_news_titles)
        payload["recap_used_topics"] = sorted(state.recap_used_topics)
    mp = state.macro_pulse_last_pcts
    if isinstance(mp, dict) and mp:
        payload["macro_pulse_last_pcts"] = mp
    mps = state.macro_pulse_last_sent
    if isinstance(mps, datetime):
        payload["macro_pulse_last_sent"] = mps.isoformat()
    bot_state_set(_BOT_STATE_SNAPSHOT_KEY, json.dumps(payload, ensure_ascii=False))


async def bot_state_persist_loop(state: State) -> None:
    while True:
        await asyncio.sleep(120)
        try:
            bot_state_save_snapshot(state)
        except Exception:
            logging.exception("bot_state_persist_loop")


def is_duplicate_live_news(state: State, title: str, link: str, now: datetime) -> bool:
    url_hash, combo_hash = live_news_dedup_hashes(title, link)
    fp_hash = live_news_title_fingerprint(title)
    if url_hash in state.live_news_seen_set or combo_hash in state.live_news_seen_set or fp_hash in state.live_news_seen_set:
        return True
    if live_news_db_has_recent((url_hash, combo_hash, fp_hash), now.timestamp()):
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
    fp_hash = live_news_title_fingerprint(title)
    now_ts = utc_now().timestamp()
    live_news_db_store_hashes((url_hash, combo_hash, fp_hash), now_ts)
    for nid in (url_hash, combo_hash, fp_hash):
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


def is_recap_title_natural(title: str) -> bool:
    t = html_clean(title or "", 180)
    if not t or len(t) < 12:
        return False
    if "http" in t.lower() or ".com" in t.lower():
        return False
    if t.count("/") >= 2 or t.count("|") >= 2:
        return False
    return len(re.findall(r"[가-힣]", t)) >= 6


def recap_market_keyword(title: str, emoji: str) -> str:
    tl = (title or "").lower()
    if any(k in tl for k in ("etf", "비트코인 etf", "현물 etf", "승인", "상장지수")):
        return "ETF"
    if any(k in tl for k in ("달러", "dxy", "환율", "10년물", "국채", "금리", "연준", "fomc", "cpi", "pce")):
        return "달러"
    if any(k in tl for k in ("호르무즈", "유가", "원유", "해운", "공급망", "hormuz", "oil", "wti", "brent")):
        return "유가"
    if emoji == "🟠" or any(k in tl for k in ("btc", "bitcoin", "비트코인", "이더리움", "etf", "알트", "청산")):
        return "코인"
    if any(k in tl for k in ("코스피", "환율", "외국인", "기관", "연기금", "삼성전자", "하이닉스", "한국")):
        return "한국장"
    if any(k in tl for k in ("반도체", "hbm", "데이터센터", "엔비디아", "nvidia")):
        return "반도체"
    if any(k in tl for k in ("금리", "연준", "달러", "fomc", "cpi", "pce")):
        return "금리"
    return "시장"


def normalize_recap_bucket(keyword: str) -> str:
    if keyword in ("코인", "BTC", "ETH", "SOL", "알트"):
        return "코인"
    return keyword


def recap_weekend_priority(title: str, emoji: str, source: str) -> int:
    tl = (title or "").lower()
    src = (source or "").lower()
    score = 0
    if emoji == "🟠" or any(k in tl for k in ("btc", "bitcoin", "비트코인", "이더리움", "etf", "알트", "청산", "crypto")):
        score += 4
    if any(k in tl for k in ("호르무즈", "유가", "원유", "해운", "공급망", "hormuz", "oil", "wti", "brent", "iran", "israel", "전쟁", "미사일")):
        score += 3
    if "coin" in src or "coindesk" in src or "cointelegraph" in src:
        score += 2
    return score


async def ensure_korean_text(session: aiohttp.ClientSession, value: str) -> str:
    value = html_clean(value, 220)
    if not value:
        return ""
    if mostly_english(value):
        value = await translate_to_korean(session, value)
    return html_clean(value, 220)


def rewrite_coin_news_title(title_ko: str, title: str, summary: str) -> str:
    text = html_clean(f"{title_ko} {title} {summary}", 420)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("Solana나의", "솔라나")
    text = re.sub(r"^(암호화폐\s*상승\s*이유|암호화폐\s*하락\s*이유|why\s+crypto\s+is\s+(up|down))\s*[:\-]\s*", "", text, flags=re.I)
    text = re.sub(r"(가격\s*예측|전망|분석|보고서)\s*[:\-]?\s*", "", text, flags=re.I)
    text = text.replace("공동 창립자는", "공동창업자는")
    text = text.replace("라고 경고했습니다", "라고 봤습니다")
    text = text.replace("가능성 제기", "가능성이 있다는 의견")

    fallback = html_clean(strip_news_source_tail(title_ko or title), 90)
    fallback = re.sub(r"\s+", " ", fallback).strip(" -–—:·.")
    fallback = re.sub(r"(공동\s*창립자는|가격\s*예측|전망|분석|보고서|경고했습니다|가능성\s*제기)", "", fallback).strip()
    return fallback


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
    if category == "코인":
        score += 6
        if any(
            k in text_low
            for k in (
                "bitcoin",
                "btc",
                "ethereum",
                "eth",
                "sol",
                "solana",
                "xrp",
                "etf",
                "liquidation",
                "비트코인",
                "이더리움",
                "솔라나",
                "청산",
                "defi",
                "stablecoin",
                "스테이블",
                "해킹",
                "exploit",
                "inflow",
                "outflow",
                "microstrategy",
                "mstr",
            )
        ):
            score += 10
    if any(k in text_low for k in ("외국인", "기관", "연기금", "국민연금", "투신")):
        score += 8
    if any(k in text_low for k in ("실적", "earnings", "guidance", "가이던스", "eps", "etf")):
        score += 8
    if any(k in text_low for k in ("유가", "wti", "brent", "opec", "원유")):
        score += 6
    if is_live_ai_without_market_anchor(title, summary):
        score -= 30
    if category == "한국" and any(k in text_low for k in ("btc", "bitcoin", "비트코인", "ethereum", "eth", "알트")):
        score -= 10
    if category == "이슈":
        score += 5
    now = now_kst()
    if is_weekend_mode(now) and category == "한국":
        score -= 8
    if is_kr_holiday_day(now) and category == "한국":
        score -= 8
    return score


def is_night_kst(now: datetime) -> bool:
    return 1 <= now.astimezone(KST).hour < 7


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


def _image_dims_from_url(url: str) -> Optional[Tuple[int, int]]:
    if not url:
        return None
    u = url
    m = re.search(r"/(\d{3,5})x(\d{3,5})/", u)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"[?&]w=(\d{3,5})\b", u, re.I)
    if m:
        w = int(m.group(1))
        mh = re.search(r"[?&]h=(\d{3,5})\b", u, re.I)
        if mh:
            return w, int(mh.group(1))
        return w, w
    m = re.search(r"width[=/_-](\d{3,5})", u, re.I)
    if m:
        w = int(m.group(1))
        mh = re.search(r"height[=/_-](\d{3,5})", u, re.I)
        if mh:
            return w, int(mh.group(1))
    return None


def news_image_url_passes_heuristics(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    ul = url.lower()
    deny = (
        "news.google",
        "googleusercontent",
        "gstatic.com",
        "/favicon",
        "favicon.",
        "apple-touch-icon",
        "touch-icon",
        "/logo",
        "logo.svg",
        "logo.png",
        "-logo",
        "_logo",
        "/avatar",
        "avatar.",
        "placeholder",
        "default-image",
        "default_image",
        "no-image",
        "noimage",
        "sprite",
        "/icons/",
        "/icon/",
        "app-icon",
        "og-default",
        "/1x1",
        "blank.gif",
        "pixel.gif",
        "spacer.gif",
    )
    if any(d in ul for d in deny):
        return False
    dims = _image_dims_from_url(url)
    if dims:
        w, h = dims
        if w < 300 or h < 300:
            return False
        ratio = max(w, h) / max(1, min(w, h))
        if ratio > 3.2:
            return False
    else:
        if any(x in ul for x in ("/logo", "favicon", "icon.png", "icon.jpg", "sprite", "avatar")):
            return False
    return True


async def fetch_og_image_url(session: aiohttp.ClientSession, page_url: str) -> Optional[str]:
    if not page_url or not page_url.startswith("http"):
        return None
    try:
        async with session.get(
            page_url,
            timeout=aiohttp.ClientTimeout(total=2.8),
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"},
            allow_redirects=True,
        ) as resp:
            if resp.status >= 400:
                return None
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if ctype and "html" not in ctype and "text" not in ctype:
                return None
            html = await resp.text(errors="ignore")
    except Exception:
        return None
    if not html or len(html) < 200:
        return None
    for pat in (
        r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        r'name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
        r'name=["\']twitter:image:src["\']\s+content=["\']([^"\']+)["\']',
    ):
        m = re.search(pat, html, re.I)
        if m:
            u = (m.group(1) or "").strip()
            if u.startswith("http") and "quickchart.io" not in u.lower() and news_image_url_passes_heuristics(u):
                return u
    return None


async def resolve_article_image_url(
    session: aiohttp.ClientSession, entry, source: str = "", link: str = ""
) -> Optional[str]:
    lk = (link or getattr(entry, "link", "") or "").strip()
    src = (source or "").strip()
    if source_quality_rank(src, lk) not in ("S", "A"):
        return None
    u = await resolve_entry_image_url(session, entry)
    if u and news_image_url_passes_heuristics(u) and "quickchart.io" not in u.lower():
        return u
    og = await fetch_og_image_url(session, lk)
    if og and news_image_url_passes_heuristics(og):
        return og
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
    if category == "한국" and (
        any(k in t for k in ("bitcoin", "btc", "ethereum", "eth", "알트")) or is_crypto_etf_content(title, summary)
    ):
        return "한국장은 환율과 외국인 수급이 먼저. 코인 해석은 분리해서 보는 편이 좋음."

    if is_live_ai_without_market_anchor(title, summary):
        return "거래 관점에선 우선순위 낮음. 서비스·이벤트 소식일 수 있음."

    if any(k in t for k in ("hormuz", "호르무즈", "oil", "wti", "brent", "유가", "원유", "opec", "해협", "선박", "해운")):
        return "호르무즈 쪽 긴장 완화 얘기가 나오면서\n유가 급등세는 잠깐 누그러졌다는 쪽으로 보임.\n\n다만 선박 이동·운임 문제는 아직 남아서\n해운·물류 변수는 계속 챙겨야 하는 상황."
    if any(k in t for k in ("iran", "israel", "missile", "strike", "ceasefire", "sanction", "nuclear", "이란", "이스라엘", "미사일", "공습", "휴전", "제재", "핵", "전쟁", "드론")):
        return "지정학 변수가 커지면 유가·달러·코인까지 변동폭이 한 번에 벌어질 수 있음."
    if any(k in t for k in ("fed", "fomc", "cpi", "ppi", "interest rate", "rate cut", "powell", "연준", "금리", "물가", "파월", "국채", "수익률", "달러")):
        return "금리·달러 쪽 뉴스. 나스닥·코인 방향에 바로 영향 줄 수 있음."
    if any(k in t for k in ("ionq", "아이온큐", "rigetti", "리게티", "quantum", "양자")):
        return "양자컴퓨터 테마 수급 체크. 기대는 크지만 변동폭도 큼."
    if any(k in t for k in ("nvidia", "엔비디아", "semiconductor", "반도체", "hbm", "micron", "마이크론", "broadcom", "브로드컴", "amd", "tsmc", "asml", "chip", "칩")):
        return "반도체 수급 핵심. 국내 반도체·나스닥 동조만 짧게 체크."
    if any(k in t for k in ("oracle", "오라클", "coreweave", "dell", "supermicro", "smci", "cloud", "data center", "데이터센터", "클라우드", "ai infrastructure", "ai 인프라")):
        return "데이터센터·전력·장비 수급이 연달아 움직일 수 있는 자리."
    if any(k in t for k in ("vertiv", "vst", "ge vernova", "nuclear", "uranium", "power", "electricity", "전력", "원전", "우라늄", "에너지", "전력망")):
        return "AI 전력 수요 테마. 전력·원전·인프라 관련주 반응 체크."
    if any(k in t for k in ("defense", "drone", "lockheed", "boeing", "국방", "방산", "드론", "한화에어로스페이스")):
        return "방산·지정학 수급. 방산주랑 원자재 변동폭 같이 보면 됨."
    if any(k in t for k in ("shipping", "tariff", "rare earth", "supply chain", "해운", "관세", "희토류", "공급망")):
        return "물류·관세 쪽 뉴스. 물가랑 기업 마진에 바로 닿을 수 있음."
    if any(k in t for k in ("tesla", "테슬라", "apple", "애플", "meta", "메타", "google", "구글", "amazon", "아마존", "microsoft", "마이크로소프트", "palantir", "팔란티어")):
        return "나스닥 성장주 수급. 지수보다 종목별로 보는 게 낫음."
    if any(k in t for k in ("earnings", "guidance", "실적", "가이던스", "eps", "매출")):
        return "실적 뉴스. 숫자보다 가이던스랑 장 후반 수급이 더 큼."
    if any(k in t for k in ("bitcoin", "btc", "비트코인", "ethereum", "eth", "청산", "liquidation", "tokenization", "stablecoin", "토큰화", "스테이블코인", "거래소")):
        return "코인 수급. BTC 가격이랑 거래량이 같이 붙는지 보면 됨."
    if "etf" in t and is_crypto_etf_content(title, summary):
        return "코인 수급. BTC 가격이랑 거래량이 같이 붙는지 보면 됨."
    if any(k in t for k in ("samsung", "삼성전자", "hynix", "하이닉스", "kospi", "코스피", "환율", "외국인", "현대차", "기아", "lg에너지솔루션", "두산에너빌리티")):
        return "한국장 수급. 외국인·환율·반도체를 같이 보면 됨."
    return "가격이 움직일 때 거래량이 같이 붙는지 보면 됨."


def news_event_type(title: str, summary: str, category: str) -> str:
    t = f"{title} {summary} {category}".lower()
    if any(k in t for k in ("russia", "ukraine", "러시아", "우크라", "크렘린", "nato", "나토")):
        return "geopolitics"
    if any(k in t for k in ("hack", "hacked", "exploit", "breach", "해킹", "보안")):
        return "security"
    if any(k in t for k in ("청산", "liquidation")):
        return "liquidation"
    if any(k in t for k in ("급락", "급등", "volatility")):
        if category in ("한국", "미국") and any(
            k in t for k in ("반도체", "semiconductor", "hbm", "코스피", "kospi", "나스닥", "nasdaq", "주가", "stock")
        ):
            return "semiconductor" if any(k in t for k in ("반도체", "semiconductor", "hbm", "삼성", "하이닉스", "nvidia")) else "general"
        if category == "코인" or any(k in t for k in ("btc", "bitcoin", "비트코인", "crypto", "암호화")):
            return "liquidation"
    if any(k in t for k in ("fomc", "cpi", "pce", "ppi", "fed", "연준", "금리", "파월")):
        return "rates"
    if any(k in t for k in ("hormuz", "iran", "israel", "oil", "wti", "brent", "호르무즈", "이란", "이스라엘", "유가", "원유")):
        return "oil"
    ek = etf_asset_kind(title, summary)
    if ek != "none":
        return ek
    if any(k in t for k in ("환율", "usd/krw", "달러/원", "dxy", "달러인덱스", "외국인")):
        return "fx"
    if any(k in t for k in ("반도체", "hbm", "semiconductor", "엔비디아", "nvidia", "ai server", "데이터센터", "공급망", "supply chain")):
        return "semiconductor"
    if any(k in t for k in ("capex", "capital expenditure", "투자 집행", "설비투자")):
        return "capex"
    if any(k in t for k in ("승인", "거절", "유입", "inflow", "outflow")) and is_crypto_etf_content(title, summary):
        return "crypto_etf"
    return "general"


def event_line_from_news(title_ko: str, title: str, summary: str, category: str) -> str:
    text = f"{title} {summary}".lower()
    if any(k in text for k in ("hormuz", "호르무즈")) and any(
        k in text for k in ("완화", "긴장 완화", "de-escalation", "진정", "긴장")
    ):
        return "호르무즈가 진정됐다는 말이 나와서 유가 급등은 잠깐 쉬는 쪽으로 읽혀요."
    if any(k in text for k in ("nvidia", "엔비디아")) and any(
        k in text for k in ("capex", "capital expenditure", "설비", "투자")
    ):
        return "엔비디아가 공장·장비에 쓸 돈을 더 늘리겠다는 쪽 숫자가 나왔어요."
    if any(k in text for k in ("russia", "ukraine", "러시아", "우크라")) and any(
        k in text for k in ("협상", "대화", "negotiat", "talks", "휴전", "ceasefire")
    ):
        return "러시아·우크라가 다시 대화한다는 말이 나왔어요."
    if any(k in text for k in ("jp morgan", "jpmorgan", "jp모건")) and any(k in text for k in ("sol", "solana", "솔라나", "etf")):
        return "JP모건이 SOL ETF에 돈이 얼마나 들어올지 전망을 냈어요."
    if any(k in text for k in ("sec", "미 증권거래위원회")) and any(k in text for k in ("sol", "solana", "솔라나", "etf")):
        return "미국 증권위가 SOL ETF 심사 일정을 밝혔어요."
    if any(k in text for k in ("aws", "아마존 웹 서비스")) and any(k in text for k in ("stablecoin", "스테이블코인", "결제")):
        return "AWS가 AI 쓸 때 스테이블코인으로 결제하는 시스템을 내놨어요."
    if any(k in text for k in ("iran", "이란")) and any(k in text for k in ("완화", "긴장 완화", "de-escalation", "hormuz", "호르무즈")):
        return "이란이 긴장을 낮추겠다는 말을 했어요."

    base = html_clean(strip_news_source_tail(title_ko or title), 120).strip(" -–—:·.")
    if not base:
        return "제목만 잡힌 이슈입니다."
    if base.endswith(("다", "됨", "함", ".", "음", "요", "임", "음.", "다.", "요.")):
        return base if base.endswith(".") else base + "."
    tails = (" 쪽 보도.", " 기사 기준.", " 이슈 라인.", " 흐름.")
    ix = int(hashlib.md5(base.encode("utf-8")).hexdigest(), 16) % len(tails)
    return base + tails[ix]


LIVE_NEWS_BANNED_PHRASES = (
    "기대감은 살아있지만",
    "기대감이 반영",
    "확인 구간",
    "확인 필요",
    "흐름",
    "보는 중",
    "보는 구간",
    "분위기",
    "실제 돈이 들어오는지",
    "반응 확인",
    "시장 반응 확인",
    "자금 유입 속도가 핵심",
    "먼저 보면 됨",
    "부터 보면 됨",
    "유가 압력",
    "공급망 이슈",
    "같은 방향으로 붙는 느낌",
    "리스크가 존재",
)


def strip_live_news_banned_phrases(text: str) -> str:
    s = text or ""
    for bad in LIVE_NEWS_BANNED_PHRASES:
        s = s.replace(bad, "")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+\.", ".", s).strip()
    return s


def sanitize_news_fact_line(s: str) -> str:
    raw = (s or "").strip()
    raw = re.sub(
        r"Google\s*뉴스.*?(?:더보기|보기)\s*|헤드라인\s*및\s*의견\s*더보기|의견\s*더보기",
        "",
        raw,
        flags=re.I,
    )
    raw = re.sub(r"\s+", " ", raw).strip(" -–—:·.")
    return raw


def why_important_lines(event_type: str, body_ko: str, category: str, title: str, summary: str) -> list[str]:
    lines: list[str] = []
    cleaned_body = strip_live_news_banned_phrases(html_clean(body_ko or "", 420).strip())
    if cleaned_body and not any(bad in cleaned_body for bad in LIVE_NEWS_BANNED_PHRASES):
        lines.append(cleaned_body)

    if event_type in ("etf", "crypto_etf"):
        lines.append("사람들이 사고 판 숫자랑 SEC 말이 먼저 움직이고, 가격은 그다음에 붙는 경우가 많아요.")
    elif event_type == "rates":
        lines.append("달러랑 국채 금리가 먼저 오르면 나스닥 선물이랑 BTC가 같은 방향으로 밀릴 수 있어요.")
    elif event_type == "oil":
        lines.append("유가가 크게 오르면 물가·국채 금리 얘기까지 번져서 나스닥에 부담이 될 수 있어요.")
    elif event_type == "semiconductor":
        lines.append("칩 물량 일정이 바뀌면 삼성전자·SK하이닉스 목표가 말도 같이 움직여요.")
    elif event_type == "fx":
        lines.append("달러/원이 크게 바뀌면 외국인이 주식을 살지 팔지가 엇갈릴 때가 있어요.")
    elif event_type == "liquidation":
        lines.append("큰 청산은 짧게 빚이랑 미결제가 한꺼번에 줄어요.")
    elif event_type == "security":
        lines.append("출금 막히거나 지갑 동결 뜨면 가격 차이가 바로 벌어져요.")
    elif event_type == "geopolitics":
        lines.append("나라 사이 큰 일은 유가·방산·금리 예민한 종목을 한꺼번에 흔들어요.")
    elif event_type == "capex":
        lines.append("빅테크가 데이터센터에 쓸 돈 숫자는 전력·냉각·GPU 얘기로 이어져요.")
    else:
        lines.append("기사 숫자가 예상이랑 다르면 자산들이 잠깐 따로 노는 구간이 나올 수 있어요.")

    return lines[:2]


def is_explanatory_live_news(title: str, summary: str) -> bool:
    t = f"{title} {summary}".lower()
    raw = f"{title} {summary}"
    if etf_asset_kind(title, summary) in (
        "semiconductor_etf",
        "ai_etf",
        "korea_stock_etf",
        "unknown_equity_etf",
        "commodity_etf",
        "bond_etf",
    ):
        return False
    if any(k in t for k in ("russia", "ukraine", "러시아", "우크라")):
        return True
    if any(k in t for k in ("iran", "hormuz", "israel", "이란", "호르무즈", "이스라엘", "미사일", "전쟁", "war")):
        return True
    if ("sec" in t or "증권거래위원회" in raw) and is_crypto_etf_content(title, summary):
        return True
    if "etf" in t and any(k in t for k in ("승인", "거절", "유입", "inflow", "outflow", "listing")):
        return is_crypto_etf_content(title, summary)
    if any(k in t for k in ("fomc", "cpi", "pce", "연준", "fed", "금리")):
        return True
    if any(k in t for k in ("earnings", "guidance", "실적", "가이던스", "eps")):
        return True
    if any(k in t for k in ("semiconductor", "반도체", "hbm", "supply chain", "공급망")):
        return True
    if any(k in t for k in ("capex", "capital expenditure", "설비투자")):
        return True
    if any(k in t for k in ("dxy", "달러인덱스")) and any(k in t for k in ("surge", "plunge", "급등", "급락", "spike")):
        return True
    if any(k in t for k in ("hack", "hacked", "exploit", "해킹")):
        return True
    if "거래소" in t and any(k in t for k in ("해킹", "hack", "exploit", "출금", "동결", "유출")):
        return True
    if any(k in t for k in ("oil", "wti", "brent", "유가", "원유")):
        return True
    return False


def live_news_photo_topic_hit(title: str, summary: str) -> bool:
    t = f"{title} {summary}".lower()
    if any(k in t for k in ("russia", "ukraine", "iran", "israel", "hormuz", "war", "미사일", "전쟁", "러시아", "우크라", "이란", "호르무즈")):
        return True
    if is_crypto_etf_content(title, summary) and any(k in t for k in ("etf", "승인", "거절", "sec")):
        return True
    if etf_asset_kind(title, summary) == "semiconductor_etf":
        return True
    if any(k in t for k in ("oil", "wti", "brent", "유가", "원유")):
        return True
    if any(k in t for k in ("semiconductor", "반도체", "hbm", "nvidia", "엔비디아")):
        return True
    if any(k in t for k in ("earnings", "guidance", "실적", "가이던스")):
        return True
    return False


def live_news_should_send_photo(
    grade: str,
    importance: int,
    title: str,
    summary: str,
    *,
    has_article_image: bool,
) -> bool:
    if not has_article_image:
        return False
    if importance >= 8:
        return True
    return is_explanatory_live_news(title, summary) and importance >= 7


def live_news_severity_label(importance: int, grade: str = "") -> str:
    if grade == "S" or importance >= 10:
        return "상단"
    if importance >= 9:
        return "필독"
    if importance >= 8:
        return "강조"
    if importance >= 7:
        return "체크"
    return "흐름"


def live_news_market_impact_body(event_type: str, category: str, title: str, summary: str) -> str:
    if event_type == "geopolitics":
        s = (
            "휴전·제재 완화 먼저 뜨면 유가·유럽 증시가 제일 큼.\n"
            "미국이랑 유럽 스텝 안 맞을 때도 있음. 지역 나눠서 보면 됨.\n"
            "방산·원자재는 뉴스 하나에도 범위 크게 흔들림."
        )
    elif event_type == "oil":
        s = (
            "호르무즈 한 번 돌면 유가부터. 항공·물류비로 이어짐.\n"
            "선박·운임까지 당일 정상이라고 보긴 이르니 해운은 따로.\n"
            "유가 다시 튀면 나스닥·방산·원자재 한 줄로 감."
        )
    elif event_type in ("rates",):
        s = (
            "금리 기대 바뀌면 달러·국채 먼저. 나스닥 선물이랑 BTC 같은 축으로 밀림.\n"
            "연준 끝나고 숫자가 시장에 얼마나 남는지가 본전."
        )
    elif event_type in ("etf", "crypto_etf"):
        s = (
            "현물 ETF는 일별 유입·보관 수량이 가격 얘기랑 바로 붙음.\n"
            "SEC·공시 문구 나오면 단기론 레버부터 튀는 편."
        )
    elif event_type == "semiconductor_etf":
        s = (
            "국장에선 삼전·하닉이 메모리·HBM 민감도 크니까 테마 자금이 ETF로도 옴.\n"
            "밤에 필반·엔비디아 프리 크게 움직이면 국내 선·현물이 같은 쪽으로 붙는지."
        )
    elif event_type == "semiconductor":
        s = (
            "HBM·GPU 타임라인 바뀌면 국내 반도체 목표가·밸류 같이 감.\n"
            "CAPEX 가이던스랑 전력·냉각이 한 테마로 묶이는지."
        )
    elif event_type == "security":
        s = (
            "출금 지연·동결이면 스프레드랑 온체인 유동성부터.\n"
            "스테이블 페그·거래소 공지 같이."
        )
    elif event_type == "liquidation":
        s = (
            "대형 청산이면 펀딩·미결제 한 번에 줄어듦. 변동성 커짐.\n"
            "BTC·알트 베타 잠깐 깨지는지."
        )
    elif event_type == "ai_etf":
        s = (
            "데이터센터·GPU·전력 기대가 한 덩어리로 움직이는 편.\n"
            "국장에선 AI 인프라·반도체 대장 수급이 같은 축인지."
        )
    elif event_type in ("korea_stock_etf", "unknown_equity_etf"):
        s = (
            "국내 ETF는 코스피·코스닥 비중이 외인·기관 배분이랑 바로 엮임.\n"
            "장 초반 체결이랑 환율 방향만 같이."
        )
    elif event_type == "fx":
        s = (
            "달러/원 급변은 외인 선·현물이 엇갈릴 때 있음.\n"
            "코스피 방어 구간이면 반도체·금융이 먼저."
        )
    elif event_type == "capex":
        s = (
            "빅테크 CAPEX는 GPU·전력·냉각 수급으로 이어짐.\n"
            "나스닥 선물이랑 국내 장비·전력주가 같은 테마인지."
        )
    elif event_type == "commodity_etf":
        s = (
            "원자재 ETF는 인플레 기대랑 달러·금리 축.\n"
            "나스닥 성장주 레버에 간접으로 박히는지."
        )
    elif event_type == "bond_etf":
        s = (
            "채권 ETF 자금은 금리 민감 자산이랑 상관 잠깐 세질 때 있음.\n"
            "달러·나스닥·BTC 한 줄인지."
        )
    elif category == "코인":
        s = (
            "BTC가 방향 잡으면 알트는 베타·유동성 순으로 늦게 붙는 경우 많음.\n"
            "SOL·ETH는 ETF·규제 나올 때 레버부터 튀는 편."
        )
    else:
        s = (
            "숫자나 말 한마디에 시나리오 깨지면 섹터끼리 잠깐 따로 노는 구간 나옴.\n"
            "지수·환율·외인이 한 방향인지만."
        )
    return strip_live_news_banned_phrases(s)


def split_body_fact_lines(body_ko: str, max_lines: int, max_chars: int) -> list[str]:
    raw = strip_live_news_banned_phrases(body_ko or "")
    if not raw:
        return []
    raw = re.sub(r"\s*…+\s*", ". ", raw)
    raw = re.sub(r"\s*\.{3,}\s*", ". ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    parts = re.split(r"(?<=[.!?。])\s+|\n+", raw)
    out: list[str] = []
    for p in parts:
        p = sanitize_news_fact_line(p)
        if not p or any(b in p for b in LIVE_NEWS_BANNED_PHRASES):
            continue
        if len(p) > max_chars * 2:
            start = 0
            while start < len(p) and len(out) < max_lines:
                chunk = p[start : start + max_chars]
                if len(chunk) < max_chars and start + max_chars < len(p):
                    cut = max(chunk.rfind(" "), chunk.rfind(","), chunk.rfind("·"))
                    if cut > max_chars // 2:
                        chunk = p[start : start + cut]
                chunk = chunk.strip()
                if len(chunk) >= 12:
                    out.append(chunk if len(chunk) <= max_chars else chunk[: max_chars - 1].rstrip() + "…")
                start += len(chunk) + 1
                if not chunk:
                    break
            if len(out) >= max_lines:
                break
            continue
        if len(p) > max_chars:
            p = p[: max_chars - 1].rstrip() + "…"
        out.append(p)
        if len(out) >= max_lines:
            break
    return out


def dedupe_fact_lines(lines: list[str], *, sim_threshold: float = 0.82) -> list[str]:
    """RSS에 같은 문장이 두 번 붙거나, 거의 같은 팩트 줄이 연속될 때 한 줄로."""
    out: list[str] = []
    for ln in lines:
        s = (ln or "").strip()
        if len(s) < 12:
            continue
        if any(title_similarity(s, prev) >= sim_threshold for prev in out):
            continue
        out.append(s)
    return out


def impact_channels_block(event_type: str, category: str) -> str:
    if event_type in ("etf", "crypto_etf"):
        lines = ("자금 유입·유출", "승인·규제 서류", "기관 수급", "BTC·ETH 현물")
    elif event_type == "semiconductor_etf":
        lines = ("삼성전자·SK하이닉스", "HBM·AI메모리", "AI 서버·GPU", "SOX·NVDA(참고)")
    elif event_type == "ai_etf":
        lines = ("나스닥 AI", "데이터센터", "GPU", "전력·냉각")
    elif event_type in ("korea_stock_etf", "unknown_equity_etf"):
        lines = ("코스피·코스닥", "외국인·기관", "환율")
    elif event_type == "commodity_etf":
        lines = ("원자재", "인플레", "달러", "나스닥 부담")
    elif event_type == "bond_etf":
        lines = ("달러", "금리", "나스닥", "BTC")
    elif event_type == "rates":
        lines = ("달러", "금리", "나스닥", "BTC")
    elif event_type == "oil":
        lines = ("유가", "인플레", "달러", "나스닥 부담")
    elif event_type == "semiconductor":
        lines = ("HBM", "AI 서버", "삼성전자·SK하이닉스", "나스닥 SOX")
    elif event_type == "fx":
        lines = ("달러/원", "외국인 수급", "코스피")
    elif event_type == "liquidation":
        lines = ("변동성 지수", "BTC·알트", "펀딩")
    elif event_type == "security":
        lines = ("거래소 유동성", "스테이블 페그", "BTC")
    elif event_type == "geopolitics":
        lines = ("유가", "달러", "나스닥", "방산", "BTC")
    elif event_type == "capex":
        lines = ("나스닥 빅테크", "전력·냉각", "반도체 장비")
    else:
        lines = ("유가", "달러", "나스닥", "코스피", "BTC")
    return "\n".join(f"· {x}" for x in lines)


def build_news_body_line(category: str, title: str, summary: str, impact: str) -> str:
    raw = html_clean(summary, 180)
    if raw and not mostly_english(raw) and raw not in title:
        return raw
    return impact














# ============================================================
# FINAL STABLE ROOM HELPERS
# ============================================================

def polish_korean_news_text(text_value: str) -> str:
    s = text_value or ""
    fixes = {
        "호르무즈 해협 선박 지원": "호르무즈 해협 선박 지원",
        "트럼프의 호르무즈 선박 구조": "호르무즈 긴장 완화 기대",
        "석유 압력": "유가 부담",
        "압력 완화": "진정",
        "유가 압력": "유가 부담",
        "공급망 지연은 여전히 남아 있음": "물류·배송 지연은 아직 남아 있음",
        "공급망 지연은 여전히 ​​남아 있음": "물류·배송 지연은 아직 남아 있음",
        "공급망 지연 이슈는 남아있음": "물류·배송 지연은 아직 남아 있음",
        "위험자산 반응 체크": "위험자산은 지수랑 같이 보면 됨",
        "빅테크 움직임이라 나스닥 분위기 같이 봐야함.": "빅테크가 나스닥 방향 끌 가능성 큼.",
        "시장 영향은 가격 반응 확인하면서 봐야함.": "가격 움직일 때 거래량 붙는지 보면 됨.",
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
    return msg[:limit - 40].rstrip() + "\n\n…길어서 잘림"


def normalize_news_importance(score: int) -> int:
    try:
        score = int(score)
    except Exception:
        score = 10
    if score >= 33:
        return 10
    if score >= 28:
        return 9
    if score >= 23:
        return 8
    if score >= 18:
        return 7
    if score >= 14:
        return 6
    if score >= 10:
        return 5
    if score >= 6:
        return 4
    return max(1, min(3, score // 2 + 1))


def source_quality_rank(source: str, link: str) -> str:
    """라이브 뉴스 출처 등급: S > A > B > C (C는 기본 차단, 이미지는 S/A만)."""
    raw = (source or "").strip()
    s = raw.lower()
    lk = (link or "").strip()
    lk_l = lk.lower()
    blob_l = f"{s} {lk_l}"
    host = ""
    try:
        host = urlparse(lk).netloc.replace("www.", "").lower()
    except Exception:
        host = ""

    if any(x in s for x in ("보도자료", "press release", "pr newswire", "prnewswire", "globenewswire", "business wire", "newsfile corp")):
        return "C"
    if "광고" in raw and len(raw) <= 24:
        return "C"
    c_tokens = ("korea it times", "재외동포", "코리아넷", "한민족", "koreaittimes", "koreaitimes")
    if any(x in s for x in c_tokens) or any(x in blob_l for x in ("koreaittimes.com", "koreaitimes.com")):
        return "C"
    if host and any(
        x in host
        for x in (
            "prnewswire.com",
            "globenewswire.com",
            "businesswire.com",
            "newsfilecorp.com",
            "koreaittimes.com",
        )
    ):
        return "C"

    s_hosts = (
        "reuters.com",
        "reuters.net",
        "bloomberg.com",
        "bloomberg.net",
        "ft.com",
        "ftcontent.com",
        "wsj.com",
        "cnbc.com",
        "coindesk.com",
        "theblock.co",
        "theblock.com",
        "yna.co.kr",
        "yonhapnews.co.kr",
        "yonhap.co.kr",
        "infomax.co.kr",
    )
    if host:
        for h in s_hosts:
            if host == h or host.endswith("." + h):
                return "S"
    if any(
        x in host
        for x in (
            "reuters.com",
            "bloomberg.com",
            "ft.com",
            "wsj.com",
            "cnbc.com",
            "coindesk.com",
            "theblock.co",
            "theblock.com",
            "yna.co",
            "yonhap",
            "infomax.",
        )
    ):
        return "S"
    if "ft.com" in lk_l or "financial times" in s:
        return "S"
    if any(m in s for m in ("reuters", "로이터", "bloomberg", "cnbc", "coindesk", "the block", "연합뉴스", "연합인포맥스")) or any(m in raw for m in ("연합뉴스", "연합인포맥스")):
        return "S"
    if any(m in s for m in ("yonhap", "infomax")):
        return "S"

    a_hosts = (
        "hankyung.com",
        "mk.co.kr",
        "biz.chosun.com",
        "sedaily.com",
        "mt.co.kr",
        "edaily.co.kr",
        "investing.com",
        "finance.yahoo.com",
        "news.naver.com",
        "n.news.naver.com",
        "news.daum.net",
        "v.daum.net",
    )
    if host:
        for h in a_hosts:
            if host == h or host.endswith("." + h):
                return "A"
    if "investing.com" in lk_l or "finance.yahoo" in lk_l:
        return "A"
    if any(m in raw for m in ("한국경제", "매일경제", "조선비즈", "서울경제", "머니투데이", "이데일리")):
        return "A"
    if any(m in s for m in ("hankyung", "maeil", "sedaily", "edaily", "moneytoday", "yahoo finance", "야후")):
        return "A"

    return "B"


def live_news_mega_catalyst_bypasses_c_source(title: str, summary: str, link: str) -> bool:
    """C급 출처라도 초대형 이벤트면 라이브 발송 예외 (리캡 후보는 여전히 제외)."""
    t = f"{title} {summary}".lower()
    lk = (link or "").lower()
    raw = f"{title} {summary}"
    if "fomc" in t:
        return True
    if re.search(r"\bcpi\b", t) or re.search(r"\bppi\b", t):
        return True
    if "sec.gov" in lk or "federalreserve.gov" in lk:
        return True
    if ("sec" in t or "증권거래위원회" in raw) and any(k in t for k in ("etf", "승인", "거절", "심사", "서류", "공시")):
        return True
    if any(k in t for k in ("전쟁", "미사일", "공습", "invasion", "nuclear strike", "war ", " war", "gaza", "ceasefire", "휴전")):
        return True
    if any(k in t for k in ("유가 급등", "유가 급락", "oil surges", "oil spikes", "oil plunges", "wti surges", "brent soars", "brent plunges")):
        return True
    return False


def has_clear_source_name(source: str, link: str = "") -> bool:
    src = (source or "").strip().lower()
    lk = (link or "").strip()
    if not src or src in ("rss", "뉴스"):
        return lk.startswith("http")
    if src == "google news":
        return lk.startswith("http")
    return bool(src)


def is_coin_true_critical(title: str, summary: str) -> bool:
    text_low = f"{title} {summary}".lower()
    return any(
        k in text_low
        for k in ("etf 승인", "etf 거절", "sec approves", "sec rejects", "대형 해킹", "hacked", "exploit", "대형 청산")
    )


def newsroom_keyword_score(title: str, summary: str, category: str = "") -> int:
    txt = f"{title} {summary} {category}".lower()
    score = 10
    critical = (
        "fed", "fomc", "powell", "파월", "연준", "금리", "cpi", "pce", "ppi", "실업수당", "고용",
        "전쟁", "공습", "미사일", "호르무즈", "봉쇄", "etf", "sec ", "엔비디아", "nvidia", "hbm",
        "삼성전자", "sk하이닉스", "btc", "비트코인", "청산",
        "실적", "earnings", "guidance", "유가", "wti", "brent", "외국인", "기관", "연기금", "국민연금",
    )
    high = ("ai","반도체","데이터센터","cloud","클라우드","oil","wti","brent","유가","원유","달러","dxy","10년물","테슬라","tesla","아이온큐","ionq","양자")
    low = ("맛집","행사","기념","인터뷰","칼럼","opinion","blog","생활","문화","입시","수능","홍보","프로모션","webinar","summit")
    score += sum(3 for k in critical if k in txt)
    score += sum(2 for k in high if k in txt)
    score -= sum(3 for k in low if k in txt)
    if any(k in txt for k in ("호르무즈","전쟁","공습","미사일","봉쇄")) and any(k in txt for k in ("유가","원유","wti","brent","석유")):
        score += 5
    if any(k in txt for k in ("엔비디아","nvidia","hbm","데이터센터")):
        score += 4
    if any(k in txt for k in ("cpi","pce","fomc","fed","파월","연준","금리")):
        score += 4
    if is_live_ai_without_market_anchor(title, summary):
        score -= 18
    ek = etf_asset_kind(title, summary)
    if ek in ("semiconductor_etf", "ai_etf", "korea_stock_etf", "unknown_equity_etf") and not is_crypto_etf_content(title, summary):
        score -= 8
    return max(1, min(35, score))


def live_news_combined_score(title: str, summary: str, category: str, link: str = "") -> int:
    return max(live_news_score(title, summary, category, link), newsroom_keyword_score(title, summary, category))


def live_news_tier_a_hit(title: str, summary: str) -> bool:
    t = f"{title} {summary}".lower()
    keys = (
        "earnings", "guidance", "실적", "가이던스", "eps", "surprise", "etf",
        "금리", "fed", "fomc", "cpi", "pce", "ppi", "유가", "wti", "brent",
        "반도체", "hbm", "엔비디아", "nvidia", "외국인", "기관", "연기금", "국민연금",
        "청산", "liquidation", "비트코인", "bitcoin",
    )
    return any(k in t for k in keys)


def is_live_news_allowed(title: str, summary: str, category: str, now: datetime, link: str = "") -> bool:
    if low_quality_block_reason(title, summary):
        return False
    if category == "코인" and coin_news_block_reason(title, summary):
        return False
    score = live_news_score(title, summary, category, link)
    min_live_score = 6 if category == "코인" else 7
    if score < min_live_score:
        return False
    combined = live_news_combined_score(title, summary, category, link)
    imp = normalize_news_importance(combined)
    if imp <= 5 and not live_news_tier_a_hit(title, summary):
        return False
    if is_night_kst(now):
        text_low = f"{title} {summary}".lower()
        night_terms = (
            "missile", "strike", "war", "hormuz", "oil", "fed", "cpi", "crash", "surge", "liquidation",
            "미사일", "공습", "전쟁", "호르무즈", "유가", "연준", "금리", "급락", "급등", "청산",
            "bitcoin", "btc", "ethereum", "eth", "sol", "crypto", "etf", "stablecoin", "defi", "hack",
            "비트코인", "이더리움", "솔라나", "스테이블", "해킹",
        )
        night_floor = LIVE_NEWS_NIGHT_COIN_MIN if category == "코인" else LIVE_NEWS_NIGHT_OTHER_MIN
        return combined >= night_floor and any(k in text_low for k in night_terms)
    return True


def clean_news_body_for_message(title: str, summary: str, source: str = "", summary_limit: int = 155) -> str:
    title_clean = html_clean(strip_news_source_tail(title or ""), 220).strip()
    body = html_clean(summary or "", summary_limit).strip()
    body = re.sub(r"https?://\S+", "", body)
    body = body.replace(" ", " ").replace("…", "...").strip()

    for s in (source, "Google News", "조선일보", "한국경제", "경향신문", "blog.google", "Korea IT Times", "프라임경제", "Reuters", "로이터", "v.daum.net", "n.news.naver.com"):
        if s:
            body = body.replace(str(s), "").strip()

    body = polish_korean_news_text(body)
    body = re.sub(
        r"Google\s*뉴스.*?(?:더보기|보기)\s*|헤드라인\s*및\s*의견\s*더보기|의견\s*더보기",
        "",
        body,
        flags=re.I,
    ).strip()

    if title_clean and body:
        body_no_space = re.sub(r"\s+", "", body)
        title_no_space = re.sub(r"\s+", "", title_clean)
        if body_no_space == title_no_space or title_no_space in body_no_space[: len(title_no_space) + 30]:
            return ""
        if title_similarity(title_clean, body) >= 0.65:
            return ""

    if len(body) < 12:
        return ""
    return body.strip()


def related_assets_for_news(title: str, summary: str = "") -> str:
    if is_live_ai_without_market_anchor(title, summary):
        return ""
    blob = f"{title} {summary}"
    txt = blob.lower()
    ek = etf_asset_kind(title, summary)
    if ek == "semiconductor_etf":
        return "HBM · AI메모리 · 삼성전자·SK하이닉스"
    if ek == "crypto_etf":
        return "BTC · ETH · ETF 자금"
    if ek == "ai_etf":
        return "AI메모리 · 데이터센터 · 삼성전자·SK하이닉스"
    if ek in ("korea_stock_etf", "unknown_equity_etf"):
        return "KOSPI · 외국인·기관 · 삼성전자·SK하이닉스"
    if ek == "commodity_etf":
        return "원자재 · 인플레 · 달러"
    if ek == "bond_etf":
        return "금리 · 달러 · 나스닥"
    if _blob_is_geopolitics_mideast(blob, txt):
        return "유가 · 해운 · 공급망"
    if _blob_is_china_us_geopolitics(blob, txt):
        return "관세 · 반도체 밸류체인 · 환율"
    if any(k in txt for k in ("hormuz", "호르무즈", "유가", "원유", "해운", "선박", "supply chain", "공급망")) or re.search(
        r"\b(wti|brent|oil)\b", txt
    ):
        return "유가 · 해운 · 공급망"
    if any(k in txt for k in ("코스피", "kospi", "환율", "외국인", "기관", "연기금", "국민연금", "삼성전자", "sk하이닉스", "삼성sdi", "현대차")):
        return "KOSPI · 환율 · 외국인"
    if any(k in txt for k in ("반도체", "semiconductor", "hbm", "엔비디아", "nvidia", "데이터센터", "ai server", "ai 서버")):
        return "반도체 · HBM · 데이터센터"
    if any(k in txt for k in ("bitcoin", "btc", "ethereum", "eth", "sol", "청산", "liquidation", "알트")) or (
        "etf" in txt and is_crypto_etf_content(title, summary)
    ):
        return "BTC · ETF · 알트"
    if any(k in txt for k in ("fed", "fomc", "cpi", "pce", "ppi", "금리", "연준", "파월", "국채", "달러", "dxy")):
        return "금리 · 달러 · 나스닥"
    return ""


def classify_coin_news_type(title: str, summary: str) -> str:
    txt = f"{title} {summary}".lower()
    ek = etf_asset_kind(title, summary)
    if ek in ("semiconductor_etf", "ai_etf", "korea_stock_etf", "commodity_etf", "bond_etf", "unknown_equity_etf"):
        return "equity_etf_non_crypto"
    if any(k in txt for k in ("sol", "solana", "솔라나")) or "sol etf" in txt or any(k in txt for k in ("jpmorgan", "jp morgan", "jp모건")):
        return "sol_alt_flow"
    if any(k in txt for k in ("eth", "ethereum", "이더리움", "l2", "layer 2", "layer2")):
        if any(k in txt for k in ("보안", "취약", "audit", "hack", "exploit")) or re.search(r"\bsecurity\b", txt):
            return "eth_security"
    if any(k in txt for k in ("청산", "liquidation", "급락", "급등", "volatility", "변동성", "거래량")):
        return "volatility"
    if any(k in txt for k in ("btc", "bitcoin", "비트코인", "현물 etf", "spot etf")):
        return "btc_flow"
    if ek == "crypto_etf" or (any(k in txt for k in ("etf", "sec", "승인", "유입", "inflow")) and is_crypto_etf_content(title, summary)):
        return "etf_flow"
    if any(k in txt for k in ("스테이블코인", "stablecoin", "usdc", "circle", "coinbase", "stripe", "aws")):
        return "stablecoin_payment"
    return "coin_general"


def coin_topic_key(title: str, summary: str) -> str:
    txt = f"{title} {summary}".lower()
    news_type = classify_coin_news_type(title, summary)
    if news_type == "equity_etf_non_crypto":
        return "equity_etf_non_crypto"
    if news_type == "sol_alt_flow" and "etf" in txt:
        return "sol_etf"
    if news_type == "eth_security":
        return "eth_security"
    if news_type == "stablecoin_payment":
        return "stablecoin_payment"
    if news_type == "btc_flow":
        return "btc_flow"
    if news_type == "volatility":
        return "volatility"
    if news_type == "etf_flow":
        return "etf_flow"
    return "coin_general"


def topic_key_for_news(title: str, summary: str, category: str) -> str:
    txt = f"{title} {summary}".lower()
    ek = etf_asset_kind(title, summary)
    if ek == "semiconductor_etf":
        return "semiconductor_etf"
    if ek == "crypto_etf":
        if category == "코인":
            return "crypto_etf:" + coin_topic_key(title, summary)
        return "crypto_etf"
    if category == "코인":
        return coin_topic_key(title, summary)
    if category == "이슈":
        # 제목 지문별로 쿨다운을 쪼개서, 상위 RSS 한 줄이 전체 와이어를 막지 않게 함.
        h = hashlib.sha256(normalize_title_for_dedup(strip_news_source_tail(title or "")).encode("utf-8")).hexdigest()[:12]
        return f"issue:{h}"
    if any(k in txt for k in ("hormuz", "호르무즈", "유가", "wti", "brent", "전쟁", "미사일")):
        return "oil_hormuz"
    if any(k in txt for k in ("fomc", "cpi", "pce", "금리", "연준", "달러", "dxy")):
        return "rates_macro"
    if category == "한국" and any(k in txt for k in ("반도체", "hbm", "하이닉스", "삼성전자")):
        return "korea_semiconductor"
    if category == "한국" and any(k in txt for k in ("외국인", "기관", "수급", "환율", "코스피")):
        return "korea_flow"
    if any(k in txt for k in ("hack", "해킹", "exploit", "거래소")):
        return "exchange_hack"
    if any(k in txt for k in ("청산", "liquidation", "급락", "급등")):
        return "liquidation"
    return "default"


def topic_cooldown_for_key(topic_key: str) -> timedelta:
    return TOPIC_COOLDOWNS.get(topic_key, TOPIC_DEFAULT_COOLDOWN)


def classify_news_grade(title: str, summary: str, category: str) -> str:
    txt = f"{title} {summary}".lower()
    if low_quality_block_reason(title, summary):
        return "C"
    c_terms = (
        "가격 예측", "price prediction", "forecast", "행사", "홍보", "인터뷰", "칼럼",
        "스포츠", "셀럽", "범죄", "사고", "보도자료", "추천 종목",
        "what happened in crypto",
        "오늘 암호화폐 업계",
        "잇슈 머니", "잇슈머니",
    )
    if any(k in txt for k in c_terms):
        return "C"

    s_terms = (
        "etf 승인",
        "etf 거절",
        "fomc",
        "cpi",
        "pce",
        "금리",
        "대형 청산",
        "거래소 해킹",
        "hack",
        "hormuz",
        "호르무즈",
        "유가 급등",
        "btc 급등",
        "btc 급락",
        "sec approves",
        "sec rejects",
    )
    if any(k in txt for k in s_terms):
        return "S"
    if "sec" in txt and is_crypto_etf_content(title, summary):
        return "S"

    a_terms = (
        "반도체", "hbm", "ai 데이터센터", "데이터센터", "기관 수급", "실적", "etf 유입",
        "달러", "금리 흐름", "외국인",
    )
    if any(k in txt for k in a_terms):
        return "A"

    b_terms = ("전망", "코멘트", "해설", "분위기", "관측")
    if any(k in txt for k in b_terms):
        return "B"
    return "B"


def related_assets_for_coin_news(title: str, summary: str) -> str:
    news_type = classify_coin_news_type(title, summary)
    if news_type == "equity_etf_non_crypto":
        return related_assets_for_news(title, summary)
    if news_type == "eth_security":
        return "ETH · 네트워크 · 검증"
    if news_type == "sol_alt_flow":
        txt = f"{title} {summary}".lower()
        if any(k in txt for k in ("firedancer", "infrastructure", "인프라", "rollout")):
            return "SOL · 네트워크 · 인프라"
        if any(k in txt for k in ("jpmorgan", "jp morgan", "jp모건")):
            return "SOL · ETF · 기관수급"
        return "SOL · 알트 · SOL-USDT"
    if news_type == "btc_flow":
        return "BTC · ETF · 기관수급"
    if news_type == "stablecoin_payment":
        return "USDC · Coinbase · 결제"
    if news_type == "volatility":
        return "BTC · 청산 · 변동성"
    if news_type == "etf_flow":
        return "ETF · 유입 · 수급"
    return "BTC · ETH · SOL"


def _coin_brief_redundant(brief: str, ctx: str) -> bool:
    """이미 팩트/헤드에 있는 말이면 코인 한 줄 브리핑은 생략."""
    b = (brief or "").lower()
    c = (ctx or "").lower()
    if ("btc etf" in b or "온체인" in brief) and (
        "etf" in c or "온체인" in c or any(x in c for x in ("비트코인", "btc", "bitcoin", "clarity", "클래리티", "santiment"))
    ):
        return True
    if any(k in b for k in ("입법", "규제", "의회")) and any(k in c for k in ("clarity", "클래리티", "의회", "congress", "법안", "sec")):
        return True
    if "스테이블" in brief and "스테이블" in ctx:
        return True
    if "결제에 스테이블" in brief and ("coinbase" in c or "stripe" in c or "결제" in ctx):
        return True
    return False


def coin_news_brief(news_type: str, title: str, summary: str) -> str:
    txt = f"{title} {summary}".lower()
    if "firedancer" in txt or ("jump" in txt and "sol" in txt):
        return ""
    if any(k in txt for k in ("infrastructure", "rollout", "인프라")) and any(k in txt for k in ("sol", "solana", "솔라나")):
        return ""
    if any(k in txt for k in ("clarity", "클래리티", "congress", "의회", "법안", "legislation")):
        return "규제·입법 보도는 심리(펀딩·SNS)가 먼저, ETF·현물 수급은 며칠 늦게 따라오는 경우가 많아요."
    if any(k in txt for k in ("santiment", "행복", "euphoria", "급증")):
        return "온체인·SNS 심리 지표는 단기 과열 신호로 쓰고, 체결·펀딩으로만 검증하는 편이 안전해요."
    if news_type == "equity_etf_non_crypto":
        return "주식 ETF 뉴스는 반도체 지수랑 삼전·하이닉스가 같은 방향인지만 보면 돼요."
    if news_type == "eth_security":
        return "이더 보안 뉴스는 가격보다 네트워크 믿을 수 있는지 쪽이 먼저 움직여요."
    if news_type == "sol_alt_flow":
        txt = f"{title} {summary}".lower()
        if any(k in txt for k in ("jpmorgan", "jp morgan", "jp모건")):
            return "큰 은행이 SOL ETF 돈 얼마나 들어올지 숫자를 찍으면 잠깐 출렁일 수 있어요."
        return "SOL·알트는 ETF·규제 말 나올 때 빚이 먼저 반응하는 경우가 많아요."
    if news_type == "btc_flow":
        return "BTC ETF랑 온체인 돈 들어오는 얘기는 가격 밑받침 말로 바로 붙어요."
    if news_type == "stablecoin_payment":
        return "결제에 스테이블 쓰겠다는 뉴스는 거래소 가격 차이 구조가 바뀔 수 있어요."
    if news_type == "volatility":
        return "큰 청산이 나오면 펀딩이랑 미결제가 한꺼번에 줄어서 출렁여요."
    if news_type == "etf_flow":
        return "SEC 서류랑 돈 들어온 통계가 나오면 BTC·ETH가 같이 움직이기 쉬워요."
    return "코인은 한 종목보다 BTC랑 규칙·돈 묶임이 같이 움직이는 날이 많아요."


def should_attach_btc_price(news_type: str, title: str, summary: str) -> bool:
    txt = f"{title} {summary}".lower()
    if news_type in ("btc_flow", "volatility"):
        return True
    return any(k in txt for k in ("market-wide", "시장 전반", "risk-on", "risk off"))


def coin_news_flag(news_type: str, importance: int) -> str:
    if importance >= 9:
        return "중대"
    if news_type in ("volatility", "btc_flow") and importance >= 7:
        return "속보"
    return "체크"


def pick_brief_line(seed: str, options: Tuple[str, ...]) -> str:
    if not options:
        return ""
    hv = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    idx = int(hv[:8], 16) % len(options)
    return options[idx]


def styled_market_brief(category: str, title: str, summary: str) -> str:
    t = f"{title} {summary} {category}".lower()
    seed = f"{title}|{summary}|{category}"

    geo_keys = ("hormuz", "호르무즈", "oil", "wti", "brent", "유가", "원유", "해운", "선박", "supply chain", "공급망", "iran", "israel", "missile", "공습")
    semi_keys = ("반도체", "semiconductor", "hbm", "nvidia", "엔비디아", "데이터센터", "cloud", "클라우드", "capex", "gpu", "ai server", "ai 서버", "전력", "원전")
    coin_keys = ("bitcoin", "btc", "ethereum", "eth", "etf", "청산", "liquidation", "alt", "알트", "지지", "저항")
    earn_keys = ("earnings", "guidance", "실적", "가이던스", "eps", "매출", "margin", "마진", "capex", "성장률", "어닝콜")
    kr_keys = ("코스피", "kospi", "코스닥", "환율", "외국인", "기관", "연기금", "국민연금", "삼성전자", "하이닉스")

    if any(k in t for k in geo_keys):
        l1 = pick_brief_line(seed + ":g1", (
            "유가는 잠깐 진정되는 쪽.",
            "유가가 급하게 튀던 구간은 조금 식는 모습.",
            "유가가 위로 쏠리던 힘이 잠깐 쉬는 중.",
        ))
        l2 = pick_brief_line(seed + ":g2", (
            "달러랑 해운쪽은 아직 흔들릴 수 있음.",
            "선박 이슈가 남아있어서 공급망은 계속 봐야함.",
            "물류가 완전히 풀린 건 아니라 해운쪽은 남아있음.",
        ))
        return f"{l1}\n\n{l2}"

    if any(k in t for k in semi_keys):
        l1 = pick_brief_line(seed + ":s1", (
            "반도체쪽으로 다시 돈 들어오는 구간.",
            "AI 서버·칩 쪽으로 수급이 붙는 중.",
            "HBM/서버 투자 라인으로 매수세가 몰리는 편.",
        ))
        l2 = pick_brief_line(seed + ":s2", (
            "외국인 수급이 계속 붙는지가 핵심.",
            "Capex가 꺾이는지만 보면 됨.",
            "전력 라인까지 같이 받쳐주면 더 좋음.",
        ))
        return f"{l1}\n\n{l2}"

    if category == "코인" and any(k in t for k in coin_keys):
        l1 = pick_brief_line(seed + ":c1", (
            "BTC는 핵심 구간 안착 여부가 더 중요.",
            "ETF/청산 이슈라 버티는 힘이 포인트.",
            "위아래 흔들려도 지지선 지키는지가 먼저.",
        ))
        l2 = pick_brief_line(seed + ":c2", (
            "알트까지 수급 번지는지 보는 중.",
            "ETF 쪽 자금이 실제로 들어오는지 봐야함.",
            "거래량이 같이 붙어야 탄력이 이어짐.",
        ))
        return f"{l1}\n\n{l2}"

    if any(k in t for k in earn_keys):
        l1 = pick_brief_line(seed + ":e1", (
            "가이던스가 상향이면 그림은 나쁘지 않음.",
            "실적 숫자보다 콜에서 나온 톤이 더 중요.",
            "매출보다 다음 분기 가이던스에 반응하는 장.",
        ))
        l2 = pick_brief_line(seed + ":e2", (
            "Capex 유지 여부가 핵심.",
            "마진 방어가 확인되면 수급이 붙기 쉬움.",
            "성장률 둔화 신호만 없으면 매수 쪽이 유리.",
        ))
        return f"{l1}\n\n{l2}"

    if category == "한국" or any(k in t for k in kr_keys):
        l1 = pick_brief_line(seed + ":k1", (
            "오늘은 외국인 방향이 중요해보임.",
            "환율이랑 외국인 수급이 장 색깔을 정하는 날.",
            "기관보다 외국인 매매 강도가 먼저 보이는 장.",
        ))
        l2 = pick_brief_line(seed + ":k2", (
            "반도체가 계속 강하면 코스피도 버틸 수 있음.",
            "환율만 과하게 튀지 않으면 지수는 버틸 만함.",
            "삼성전자·하이닉스만 무너지지 않으면 하단은 지켜질 수 있음.",
        ))
        return f"{l1}\n\n{l2}"

    return ""


def trader_brief_from_article(category: str, title: str, summary: str) -> str:
    styled = styled_market_brief(category, title, summary)
    if styled:
        return styled

    title_clean = html_clean(strip_news_source_tail(title or ""), 160).strip()
    snippet = html_clean(summary or "", 280).strip()
    snippet = re.sub(r"https?://\S+", "", snippet)
    snippet = re.sub(r"\s+", " ", snippet).strip()

    if title_clean and snippet:
        tc_ns = re.sub(r"\s+", "", title_clean.lower())
        sn_ns = re.sub(r"\s+", "", snippet.lower())
        prefix_len = min(len(tc_ns), 72)
        if prefix_len > 8 and sn_ns.startswith(tc_ns[:prefix_len]):
            snippet = snippet[len(title_clean) :].strip(" -–—:·.")
        if title_similarity(title_clean, snippet) >= 0.62:
            snippet = ""

    if len(snippet) >= 38:
        return polish_korean_news_text(snippet[:200]).strip()

    fb = build_market_impact_line(category, title, summary)
    return polish_korean_news_text(fb[:130]).strip()


def live_news_hub_bullets(
    event_line: str,
    fact_lines: list[str],
    title_ko: str,
    *,
    prefix: Optional[list[str]] = None,
    max_total: int = 4,
) -> list[str]:
    out: list[str] = []
    seen_norm: list[str] = []

    def add_line(s: str) -> None:
        raw = sanitize_news_fact_line(strip_live_news_banned_phrases((s or "").strip()))
        raw = re.sub(r"\s+\.", ".", raw).strip()
        if len(raw) < 10:
            return
        norm = re.sub(r"\s+", "", raw.lower())
        for prev in seen_norm:
            if norm == prev or (len(norm) >= 28 and norm in prev) or (len(prev) >= 28 and prev in norm):
                return
        for prev_line in out:
            if title_similarity(raw, prev_line) >= 0.82:
                return
        out.append(raw)
        seen_norm.append(norm)

    title_base = html_clean(title_ko or "", 220).strip()
    skip_event = False
    if fact_lines and title_base:
        if max(title_similarity(sanitize_news_fact_line(fl), title_base) for fl in fact_lines) >= 0.52:
            skip_event = True
        elif event_line and title_similarity(event_line, title_base) >= 0.72:
            skip_event = True

    for s in prefix or []:
        if len(out) >= max_total:
            return out[:max_total]
        add_line(s)
    if not skip_event:
        add_line(event_line)
    for fl in fact_lines:
        if len(out) >= max_total:
            break
        fl_clean = sanitize_news_fact_line(fl)
        if event_line and title_similarity(fl_clean, event_line) >= 0.58:
            continue
        if title_base and title_similarity(fl_clean, title_base) >= 0.88:
            continue
        add_line(fl_clean)
    if not out:
        add_line(html_clean(title_ko, 220).strip() or "뉴스 확인")
    return out[:max_total]


def live_news_hub_watch_line(event_type: str, category: str, title: str, summary: str) -> str:
    t = f"{title} {summary}".lower()
    raw = f"{title} {summary}"
    if category == "한국":
        if _blob_is_kr_semiconductor_risk(raw, t):
            return "美 반도체 급락은 월요인 코스피 갭·외국인·삼성·하닉 선물 포지션부터 확인."
        if _blob_is_kr_corporate_earnings(raw, t):
            return "실적 라인: 가이던스·마진·환율이 헤드라인보다 먼저 움직이는 경우가 많음."
        if any(k in raw for k in ("블룸버그", "Bloomberg", "시총", "잇슈 머니", "잇슈머니")):
            return "국내장: 해외언급과 실제 수급(외국인·반도체) 온도차만 한 줄로."
        return "한국장: 코스피·환율·외국인·반도체 대형주를 한 묶음으로."
    if category == "미국":
        if event_type in ("semiconductor", "capex", "semiconductor_etf", "ai_etf"):
            return "미국 반도체·AI칩: 실적·캐파 가이던스가 단기 주가보다 길게 붙는 경우가 많음."
        if event_type in ("rates", "oil", "fx"):
            return "금리·유가·달러 라인. 나스닥·코인은 같은 날 베타만 짧게."
        return "미국장: 나스닥·SOX·대형주 가이던스가 헤드라인보다 길게 가는 날이 많음."
    if category == "세계":
        if _blob_is_geopolitics_mideast(raw, t):
            return "중동 헤드라인은 유가·운임이 지수보다 먼저 움직이는 경우가 많음."
        if _blob_is_china_us_geopolitics(raw, t):
            return "미중 동선: 관세·수출통제·환율이 같은 날 베타를 잡는 경우가 많음."
        return "지정학·에너지: 유가·달러·지수가 같은 날 겹치는지만."
    if category == "코인":
        if any(k in t for k in ("clarity", "클래리티", "congress", "의회", "법안")):
            return "법안·규제 뉴스는 가격보다 펀딩·SNS 반응이 먼저 나오는 경우가 많습니다."
        if event_type in ("etf", "crypto_etf") or "etf" in t:
            return "헤드라인보다 어제 ETF 유입·오늘 펀딩이 맞는지가 먼저입니다."
        if any(k in t for k in ("eth", "ethereum", "이더")):
            return "ETH 이슈도 BTC·펀딩 방향이 같이 가는지 보면 됩니다."
        return "가격·거래대금·펀딩이 같은 쪽인지만 보면 됩니다."
    if "discord" in t or "디스코드" in t:
        return "커뮤니티 규정 이슈는 단기 심리·노이즈 비중이 큼. 체결·펀딩·선물 스큐로만 검증."
    if "blind" in t or "블라인드" in t or "서명" in t or "signature" in t:
        return "지갑·보안 이슈: 거래소 공지·USDT 페그·출금 지연만 압축 확인."
    if "cpi" in t or "물가" in t:
        return "물가 이벤트: 금리 경로·달러·국채 수익률 → BTC 상관 순으로."
    if event_type == "rates":
        return "금리: DXY·10Y 먼저. 코인은 NQ 선물과 단기 베타."
    if event_type == "security":
        return "거래소·지갑: 공지·출금 큐·스테이블 페그 이탈만."
    if event_type == "liquidation":
        return "청산: 레버 감소·OI 변화·알트-BTC 베타 순."
    return "지수·환율·외국인 흐름을 한 줄로 압축."


def live_news_action_bullets(event_type: str, category: str, title: str, summary: str) -> list[str]:
    t = f"{title} {summary}".lower()
    raw = f"{title} {summary}"
    out: list[str] = []
    if category == "코인":
        etf_hit = "etf" in t or "sec" in t or "승인" in t or "거절" in t
        if event_type == "security" or any(k in t for k in ("hack", "exploit", "해킹", "출금", "동결")):
            return ["거래소 공지·출금·USDT 페그만 먼저 확인."]
        if etf_hit:
            return ["전일 ETF 유입 · 오늘 펀딩 · 15~60분 거래대금을 같이 보면 됩니다."]
        return ["15~60분 봉에서 거래량·꼬리(윅)·펀딩 쏠림만 확인."]
    elif category == "세계":
        if _blob_is_geopolitics_mideast(raw, t):
            out.append("지정학: 유가·DXY·10Y·해운·VIX가 같은 날 교차하는지만.")
        else:
            out.append("매크로: 유가·DXY·미 10년물을 한 묶음으로.")
            if _blob_is_china_us_geopolitics(raw, t):
                out.append("미중·대만: 관세·반도체 밸류체인·환율을 같은 날 겹쳐 보는지.")
            else:
                out.append("지수: S&P·나스닥 선물 방향.")
    elif category == "한국":
        if _blob_is_kr_semiconductor_risk(raw, t):
            out.append("한국장: 코스피·환율·외국인 + NQ·SOX·삼성·하닉 갭·선물 동시.")
        elif _blob_is_kr_corporate_earnings(raw, t):
            out.append("증시·실적: 코스피·환율·외국인 + 컨센서스·가이던스·환율 민감도.")
        else:
            out.append("증시: 코스피·환율·외국인·기관·반도체 대형주.")
    elif category == "미국":
        out.append("미국장: 나스닥 선물·SOX·실적 캘린더를 한 줄로.")
        if event_type in ("rates", "oil", "fx"):
            out.append("금리·유가·달러 이슈면 DXY·10Y·원유를 한 묶음으로.")
        elif event_type in ("semiconductor", "capex", "semiconductor_etf", "ai_etf"):
            out.append("반도체·AI칩: 밸류·캐파 가이던스·경쟁사 호가가 같은 날 겹치는지.")
        else:
            out.append("대형주: 서프라이즈보다 가이던스·밸류에이션이 더 길게 감.")
    elif category == "이슈":
        out.append("헤드라인: 숫자·인용·출처만 압축 확인.")
        out.append("큰 그림은 환율·지수 선물 방향과 교차.")
    else:
        out.append("주식·ETF: 외국인·기관·환율.")
        if event_type in ("rates", "oil", "fx"):
            out.append("매크로 이벤트 시 코인은 NQ·DXY와 단기 베타.")
        else:
            out.append("지수 방향과 환율·섹터 로테이션을 같이 보면 됨.")
    return out[:3]


def _numeric_fact_is_btc_price_level_noise(s: str) -> bool:
    """$80,000m 같이 가격대+붙은 m 오탐 제거(다음 단어 might 등과 붙은 경우 포함)."""
    s = (s or "").strip()
    m = re.match(r"^\$\s*([\d,]+(?:\.\d+)?)\s*([kKmMbB])\b", s)
    if not m or not m.group(2):
        return False
    if m.group(2).lower() != "m":
        return False
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        return False
    return 12_000 <= val <= 250_000


def _live_news_numeric_block_is_redundant(category: str, title: str, summary: str, event_line: str, nums: list[str]) -> bool:
    """코인 카드에서 제목에 이미 나온 가격 숫자만 인용 수치로 또 붙이지 않음."""
    if not LIVE_NEWS_COMPACT_NUMERIC_BLOCK:
        return False
    if category != "코인" or len(nums) != 1:
        return False
    n = (nums[0] or "").strip()
    if not n.startswith("$"):
        return False
    digits = re.sub(r"\D", "", n)
    if len(digits) < 4:
        return False
    ctx = re.sub(r"\W", "", f"{title}{summary}{event_line}".lower())
    return digits in ctx


def extract_article_numerical_facts(title: str, summary: str, body: str = "", *, max_items: int = 6) -> list[str]:
    """기사 제목·요약·본문에서 달러·%·억/조·inflow 등 숫자 조각만 짧게 뽑아 데스크 확인용으로 씀."""
    raw = html_clean(f"{title}\n{summary}\n{body}", 4000)
    if not raw.strip():
        return []
    found: list[str] = []
    seen: set[str] = set()

    def add(fragment: str) -> None:
        s = re.sub(r"\s+", " ", fragment).strip()
        s = re.sub(r"[,;:\s·]+$", "", s).strip()
        if len(s) < 3 or len(s) > 92:
            return
        if re.fullmatch(r"\$\s*[\d,]+(?:\.\d+)?", s):
            return
        if re.fullmatch(r"\d+(?:\.\d+)?\s*%", s):
            return
        key = re.sub(r"\s+", "", s).lower()
        for ex in seen:
            if key == ex or (len(key) > 10 and (key in ex or ex in key)):
                return
        seen.add(key)
        found.append(s)

    for m in re.finditer(r"\$\s*[\d,]+(?:\.\d+)?(?:\s*[kKmMbB]\b)?", raw):
        frag = m.group(0).replace(" ", "")
        if _numeric_fact_is_btc_price_level_noise(frag):
            continue
        add(frag)
    for m in re.finditer(r"[\d,.]+\s*(?:million|billion|bn|mn)\b", raw, re.I):
        add(m.group(0))
    for m in re.finditer(r"\b\d+(?:\.\d+)?\s*%", raw):
        frag = m.group(0)
        if re.fullmatch(r"0\.?0*\s*%", frag.strip(), re.I):
            continue
        add(frag)
    for m in re.finditer(r"[\d,]+\.?\d*\s*(?:억|조)(?:\s*원)?", raw):
        add(m.group(0))
    for m in re.finditer(
        r"(?i)(?:net\s*)?(?:inflow|outflow)\s+of\s+[^\n.;]{1,42}|(?:유입|유출)\s*[^\n.;]{0,32}",
        raw,
    ):
        add(m.group(0).strip()[:90])
    return found[:max_items]


def _fact_line_redundant(fact: str, lines: Iterable[str]) -> bool:
    fl = re.sub(r"\s+", "", fact).lower()
    if len(fl) < 4:
        return False
    for h in lines:
        if not h:
            continue
        hl = re.sub(r"\s+", "", str(h)).lower()
        if not hl:
            continue
        if fl in hl or hl in fl:
            return True
    return False


async def build_live_news_message(
    session: aiohttp.ClientSession,
    category_emoji: str,
    category: str,
    title: str,
    summary: str,
    source: str,
    link: str = "",
) -> Tuple[str, Optional[str]]:
    title_clean = strip_news_source_tail(title or "")
    title_ko = await ensure_korean_text(session, polish_korean_news_text(html_clean(title_clean, 200)))

    try:
        raw_score = live_news_score(title_clean, summary, category, link)
    except Exception:
        raw_score = 18
    raw_score = max(raw_score, newsroom_keyword_score(title_clean, summary, category))
    importance = normalize_news_importance(raw_score)
    ek_cap = etf_asset_kind(title_clean, summary)
    if ek_cap in ("semiconductor_etf", "ai_etf", "korea_stock_etf") and not is_crypto_etf_content(title_clean, summary):
        if is_major_domestic_semi_catalyst(title_clean, summary):
            importance = min(importance, 9)
        else:
            importance = min(importance, 7)

    coin_type = ""
    if category == "코인":
        coin_type = classify_coin_news_type(title_clean, summary)

    explanatory = is_explanatory_live_news(title_clean, summary) and importance >= 8
    if explanatory:
        summary_limit = 720
    elif ek_cap in ("semiconductor_etf", "ai_etf", "korea_stock_etf"):
        summary_limit = 400
    else:
        summary_limit = 260
    try:
        body = clean_news_body_for_message(title_clean, summary, source, summary_limit=summary_limit)
    except Exception:
        body = ""

    body_ko = ""
    if body and not mostly_english(body):
        try:
            body_ko = await ensure_korean_text(session, polish_korean_news_text(body))
            bk_lim = 1400 if explanatory else (560 if ek_cap in ("semiconductor_etf", "ai_etf", "korea_stock_etf") else 240)
            body_ko = html_clean(body_ko, bk_lim)
        except Exception:
            body_ko = ""

    title_ns = re.sub(r"\s+", "", title_ko or "")
    body_ns = re.sub(r"\s+", "", body_ko or "")
    show_snippet = bool(body_ko) and body_ns != title_ns and title_ns not in body_ns[: len(title_ns) + 10]
    body_for_facts = body_ko if show_snippet else ""

    event_type = news_event_type(title_clean, summary, category)
    event_line = event_line_from_news(title_ko, title_clean, summary, category)

    rel = related_assets_for_coin_news(title_clean, summary) if category == "코인" else related_assets_for_news(title_clean, summary)
    src_line = f"출처: {source}" if has_clear_source_name(source, link) else ""
    cat_line = live_news_category_label(category, title_clean, summary)

    max_fl = 8 if explanatory else 5
    fc = 280 if explanatory else 210
    fact_lines = dedupe_fact_lines(split_body_fact_lines(body_for_facts, max_fl, fc))
    desk_ctx = ""
    if category == "코인":
        ctx_merge = " ".join(fact_lines + [event_line, title_ko or ""])
        brief = coin_news_brief(coin_type, title_clean, summary)
        if brief and not _coin_brief_redundant(brief, ctx_merge):
            desk_ctx = brief
    fact_lines = dedupe_fact_lines(fact_lines)

    prefix_lines: list[str] = []
    ek_depth = etf_asset_kind(title_clean, summary)
    if ek_depth in ("semiconductor_etf", "ai_etf", "korea_stock_etf") and not is_crypto_etf_content(title_clean, summary):
        etf_facts = split_body_fact_lines(body_for_facts, 6, 240)
        depth_txt = kr_equity_etf_depth_paragraph(ek_depth, title_ko, event_line, etf_facts)
        prefix_lines = [ln.strip() for ln in depth_txt.split("\n") if ln.strip()][:2]

    bullet_cap = 4 if explanatory else 3
    bullets = live_news_hub_bullets(event_line, fact_lines, title_ko, prefix=prefix_lines or None, max_total=bullet_cap)
    article_hook = coin_article_interpretation(title_clean, summary, coin_type) if category == "코인" else ""
    hook = article_hook or live_news_hub_watch_line(event_type, category, title_clean, summary)
    if not article_hook and desk_ctx and category == "코인":
        hook = desk_ctx
    actions = live_news_action_bullets(event_type, category, title_clean, summary)
    trade_lines: list[str] = []

    headline = html_clean(title_ko, 240).strip()
    now = now_kst()
    parts: list[str] = [
        room_line(f"{category_emoji} {cat_line}", now),
        f"〔{importance}/10〕",
        "",
    ]
    parts.append(SEC_NEWS_FACT)
    for b in bullets:
        parts.append(f"· {b}")
    merge_ctx = list(bullets) + list(fact_lines)
    extra_nums = [
        x
        for x in extract_article_numerical_facts(title_clean, summary, body_for_facts, max_items=4)
        if not _fact_line_redundant(x, merge_ctx)
    ]
    if extra_nums and not _live_news_numeric_block_is_redundant(category, title_clean, summary, event_line, extra_nums):
        parts.append(f"· 숫자: {' / '.join(extra_nums[:3])}")
    parts += ["", SEC_NEWS_CONTEXT, f"· {hook}", ""]
    chart_url: Optional[str] = None
    if category == "코인" and importance >= LIVE_BTC_MIN_IMPORTANCE and LIVE_NEWS_BTC_CHART:
        try:
            chart_url, trade_lines = await coin_news_trade_desk(session, title_clean, summary, coin_type)
        except Exception:
            logging.debug("coin_news_trade_desk failed", exc_info=True)
    check_label = "③ 자리 · 흐름" if trade_lines else SEC_NEWS_CHECK
    parts.append(check_label)
    for a in trade_lines if trade_lines else actions[:1]:
        parts.append(f"· {a}")
    link_s = (link or "").strip()
    if link_s.startswith("http"):
        parts += ["", f"🔗 {link_s}"]
    if src_line:
        parts.append(src_line)
    if rel:
        parts.append(f"관련: {rel}")

    parts += ["", LIVE_NEWS_CARD_DISCLAIMER]
    msg = "\n".join(parts)
    return compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), chart_url


async def send_news_card(bot: Bot, text: str, image_url: Optional[str] = None, use_s_grade_photo: bool = False) -> None:
    if use_s_grade_photo and image_url:
        try:
            extra = _telegram_forum_kwargs()
            raw_caption = text or ""
            limits = [TELEGRAM_MAX_CAPTION_UNITS, 800, 500, 300]
            sent = False
            for lim in limits:
                cap_plain = truncate_telegram_utf16_units(raw_caption, lim)
                cap = cap_plain.strip() or "…"

                async def _photo(c: str = cap) -> None:
                    await bot.send_photo(chat_id=CHANNEL_ID, photo=image_url, caption=c, parse_mode=None, **extra)

                try:
                    await _telegram_send_with_retry(_photo, op="send_photo")
                    sent = True
                    rest = raw_caption[len(cap_plain) :]
                    if rest.strip():
                        await safe_send(bot, rest, disable_preview=True)
                    return
                except BadRequest as e:
                    el = str(e).lower()
                    if "caption" in el and ("too long" in el or "too large" in el):
                        logging.warning("send_photo 캡션 길이 초과, 더 짧게 재시도 lim=%s", lim)
                        continue
                    logging.warning("send_photo BadRequest(캡션 외) → 텍스트만 전송 err=%s", e)
                    break
            if not sent:
                logging.warning("send_photo 캡션 단축 후에도 실패 → 텍스트만 image=%s", image_url)
        except Exception as e:
            if is_telegram_auth_failure(e):
                logging.error("Telegram 토큰 인증 실패. BotFather에서 새 토큰 발급 후 TELEGRAM_TOKEN 교체 필요.")
                return
            logging.warning("기사 이미지 전송 실패. 텍스트로 대체 image=%s", image_url)
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
                        if level == 80000:
                            buffer = max(buffer, 250)
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
                            nk = now_kst()
                            if side == "above":
                                check = (
                                    "· 15~60m 종가가 레벨 위에서 마감되는지\n"
                                    "· 펀딩·OI가 한쪽으로 더 쏠리지 않는지"
                                )
                            else:
                                check = (
                                    "· 반등 시 레벨 재진입(회복) 여부\n"
                                    "· 청산·거래대금이 이어지면 범위 하단만"
                                )
                            host = room_host_line(nk, f"btc{level}", "level") if LIVE_ROOM_HOST_LINE else ""
                            msg = (
                                room_line(f"BTC 레벨 · {level:,.0f} · {direction}", nk)
                                + (f"\n\n{host}" if host else "")
                                + f"\n\n① 가격\n"
                                f"· {icon} 현재 {price:,.0f} USDT ({fmt_pct(pct)})\n\n② 체크\n"
                                f"{check}\n\n"
                                + LIVE_NEWS_CARD_DISCLAIMER
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

                        word = "급락" if side == "drop" else "급등"
                        nk = now_kst()
                        msg = (
                            room_line(f"BTC 5m · {word}", nk)
                            + f"\n\n① 숫자\n"
                            f"· 최근 30분 {fmt_pct(move_pct)}\n"
                            f"· 현재 {last_price:,.0f} USDT\n\n② 체크\n"
                            "· 거래량 동반·80K/79K 반응\n\n"
                            + ROOM_DISCLAIMER
                        )
                        cap_txt = truncate_telegram_utf16_units(msg, TELEGRAM_MAX_CAPTION_UNITS)

                        skip_cooldown_bump = False
                        try:
                            extra = _telegram_forum_kwargs()

                            async def _chart() -> None:
                                await bot.send_photo(
                                    chat_id=CHANNEL_ID,
                                    photo=chart_url,
                                    caption=cap_txt,
                                    parse_mode=None,
                                    **extra,
                                )

                            await _telegram_send_with_retry(_chart, op="btc_chart")
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
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if LIVE_NEWS_SQLITE_DEDUP and not getattr(state, "_live_news_sqlite_inited", False):
                    try:
                        _live_news_db_init()
                    except Exception:
                        logging.exception("live_news dedup sqlite init")
                    state._live_news_sqlite_inited = True
                now = utc_now()
                kst_today = now_kst().date()
                if state.live_news_daily_date != kst_today:
                    state.live_news_daily_date = kst_today
                    state.live_news_daily_count = 0
                if state.coin_live_daily_date != kst_today:
                    state.coin_live_daily_date = kst_today
                    state.coin_live_daily_count = 0
                if state.sol_etf_daily_date != kst_today:
                    state.sol_etf_daily_date = kst_today
                    state.sol_etf_daily_count = 0
                sent_this_scan = 0
                min_interval = LIVE_NIGHT_NEWS_MIN_INTERVAL if is_night_kst(now) else LIVE_NEWS_MIN_INTERVAL
                if state.live_last_sent_at and now - state.live_last_sent_at < min_interval:
                    await asyncio.sleep(LIVE_NEWS_POLL_SECONDS)
                    continue
                for category_emoji, category, feed_url in iter_live_news_feeds():
                    if state.live_news_daily_count >= LIVE_NEWS_DAILY_LIMIT or sent_this_scan >= LIVE_NEWS_MAX_PER_SCAN:
                        break
                    feed = await fetch_rss(session, feed_url)
                    entries = list(getattr(feed, "entries", []) or [])[:LIVE_NEWS_FEED_HEAD] if feed else []
                    candidates = []
                    for entry in entries:
                        raw_title = getattr(entry, "title", "") or ""
                        raw_summary = getattr(entry, "summary", "") or ""
                        raw_link = getattr(entry, "link", "") or raw_title
                        cand_emoji, cand_category = effective_live_news_category(
                            category_emoji, category, raw_title, raw_summary, raw_link
                        )
                        grade = classify_news_grade(raw_title, raw_summary, cand_category)
                        topic_key = topic_key_for_news(raw_title, raw_summary, cand_category)
                        topic_cd = topic_cooldown_for_key(topic_key)
                        if is_duplicate_live_news(state, raw_title, raw_link, now):
                            logging.info("live_news blocked reason=duplicate title=%s", clean_text(raw_title, 90))
                            continue
                        if is_hard_blocked_live_news(raw_title, raw_summary, raw_link):
                            logging.info("live_news blocked reason=hard_block title=%s", clean_text(raw_title, 90))
                            continue
                        if grade == "C":
                            logging.info("live_news blocked reason=grade_c title=%s category=%s", clean_text(raw_title, 90), cand_category)
                            continue
                        hard_reason = low_quality_block_reason(raw_title, raw_summary)
                        if hard_reason:
                            logging.info("live_news blocked reason=%s title=%s", hard_reason, clean_text(raw_title, 90))
                            continue
                        if cand_category == "코인":
                            coin_reason = coin_news_block_reason(raw_title, raw_summary)
                            if coin_reason:
                                logging.info("live_news blocked reason=%s title=%s", coin_reason, clean_text(raw_title, 90))
                                continue
                            topic_last = state.coin_topic_last_sent.get(topic_key)
                            if topic_last and (now - topic_last) < COIN_TOPIC_COOLDOWN:
                                logging.info("live_news blocked reason=coin_topic_cooldown topic=%s title=%s", topic_key, clean_text(raw_title, 90))
                                continue
                        topic_last_global = state.topic_last_sent.get(topic_key)
                        topic_on_cooldown = bool(topic_last_global and (now - topic_last_global) < topic_cd)
                        if topic_on_cooldown:
                            logging.info("live_news blocked reason=topic_cooldown topic=%s title=%s", topic_key, clean_text(raw_title, 90))
                        if not is_live_news_allowed(raw_title, raw_summary, cand_category, now, raw_link):
                            logging.info("live_news blocked reason=policy_filter title=%s category=%s", clean_text(raw_title, 90), cand_category)
                            continue
                        score = live_news_score(raw_title, raw_summary, cand_category, raw_link)
                        candidates.append((score, entry, raw_title, raw_summary, raw_link, cand_emoji, cand_category, grade, topic_key, topic_on_cooldown))
                    if not candidates:
                        continue
                    candidates.sort(key=lambda x: x[0], reverse=True)
                    score, entry, raw_title, raw_summary, raw_link, cand_emoji, cand_category, grade, topic_key, topic_on_cooldown = candidates[0]
                    combined_score = max(score, newsroom_keyword_score(raw_title, raw_summary, cand_category))
                    importance = normalize_news_importance(combined_score)
                    ek_live = etf_asset_kind(raw_title, raw_summary)
                    if not is_crypto_etf_content(raw_title, raw_summary):
                        if ek_live in ("semiconductor_etf", "ai_etf", "korea_stock_etf"):
                            if is_major_domestic_semi_catalyst(raw_title, raw_summary):
                                importance = min(importance, 9)
                            else:
                                importance = min(importance, 7)
                        elif ek_live == "unknown_equity_etf":
                            importance = min(importance, 7)
                    if cand_category == "코인" and any(k in f"{raw_title} {raw_summary}".lower() for k in ("jpmorgan", "jp morgan", "jp모건", "sol", "solana", "솔라나", "etf")):
                        importance = min(8, importance)
                    if cand_category == "코인" and importance >= 10 and not is_coin_true_critical(raw_title, raw_summary):
                        importance = 9
                    if importance <= 5:
                        logging.info("live_news blocked reason=score_cutoff importance=%s title=%s", importance, clean_text(raw_title, 90))
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    source = source_name_from_entry(entry)
                    if not raw_title.strip():
                        remember_live_news_hashes(state, raw_title, raw_link)
                        continue
                    if not has_clear_source_name(source, raw_link):
                        logging.info("live_news blocked reason=unclear_source title=%s", clean_text(raw_title, 90))
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    src_rank = source_quality_rank(source, raw_link)
                    title_for_recap = html_clean(strip_news_source_tail(raw_title), 220)
                    tape_fast = bool(LIVE_NEWS_TAPE_MODE and LIVE_NEWS_TAPE_SKIP_TRANSLATE)
                    if tape_fast:
                        title_for_recap = polish_korean_news_text(title_for_recap)
                    else:
                        try:
                            title_for_recap = await ensure_korean_text(session, title_for_recap)
                        except Exception:
                            pass
                    if not tape_fast and mostly_english(title_for_recap):
                        continue
                    if not tape_fast and not is_recap_title_natural(title_for_recap):
                        continue
                    if (grade in ("A", "B") or topic_on_cooldown) and src_rank != "C":
                        recap_grade = grade if grade in ("A", "B") else "A"
                        state.live_recent_items.append((now, cand_emoji, html_clean(title_for_recap, 150), source, combined_score, recap_grade, topic_key))

                    if topic_on_cooldown:
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    if importance < LIVE_NEWS_MIN_IMPORTANCE_SEND or grade not in LIVE_NEWS_SEND_GRADES_SET:
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    if src_rank == "C" and not live_news_mega_catalyst_bypasses_c_source(raw_title, raw_summary, raw_link):
                        logging.info(
                            "live_news blocked reason=source_rank_c title=%s source=%s",
                            clean_text(raw_title, 90),
                            source,
                        )
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    if src_rank == "B" and importance < 8:
                        logging.info(
                            "live_news blocked reason=source_rank_b_importance title=%s importance=%s source=%s",
                            clean_text(raw_title, 90),
                            importance,
                            source,
                        )
                        remember_live_news_hashes(state, raw_title, raw_link)
                        state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                        continue
                    if cand_category == "코인":
                        if state.coin_live_daily_count >= LIVE_COIN_DAILY_LIMIT:
                            logging.info("live_news blocked reason=coin_daily_limit title=%s", clean_text(raw_title, 90))
                            remember_live_news_hashes(state, raw_title, raw_link)
                            state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                            continue
                        if topic_key == "sol_etf" and state.sol_etf_daily_count >= LIVE_SOL_ETF_DAILY_LIMIT:
                            logging.info("live_news blocked reason=sol_etf_daily_limit title=%s", clean_text(raw_title, 90))
                            remember_live_news_hashes(state, raw_title, raw_link)
                            state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                            continue
                    if LIVE_NEWS_TAPE_MODE:
                        tape_title = html_clean(title_for_recap, 300).strip() or html_clean(strip_news_source_tail(raw_title), 300).strip()
                        link_out = (raw_link or "").strip()
                        if not link_out.startswith("http"):
                            remember_live_news_hashes(state, raw_title, raw_link)
                            state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                            continue
                        tape_msg = (
                            room_line("속보 테이프", now_kst())
                            + "\n\n① 헤드\n"
                            + tape_title
                            + "\n\n② 링크\n"
                            + link_out
                            + "\n\n"
                            + ROOM_DISCLAIMER
                        )
                        await safe_send(bot, tape_msg, disable_preview=False)
                    else:
                        image_url = await resolve_article_image_url(session, entry, source, raw_link)
                        msg, btc_chart_url = await build_live_news_message(
                            session, cand_emoji, cand_category, raw_title, raw_summary, source, raw_link
                        )
                        if not msg:
                            logging.info("live_news blocked reason=empty_message title=%s", clean_text(raw_title, 90))
                            remember_live_news_hashes(state, raw_title, raw_link)
                            state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                            continue
                        want_photo = live_news_should_send_photo(
                            grade, importance, raw_title, raw_summary, has_article_image=bool(image_url)
                        )
                        photo_url = btc_chart_url or image_url
                        use_photo = bool(btc_chart_url) or want_photo
                        await send_news_card(bot, msg, image_url=photo_url, use_s_grade_photo=use_photo)
                    if cand_category == "코인":
                        state.coin_topic_last_sent[coin_topic_key(raw_title, raw_summary)] = now
                        state.coin_live_daily_count += 1
                        if topic_key == "sol_etf":
                            state.sol_etf_daily_count += 1
                    state.topic_last_sent[topic_key] = now
                    remember_live_news_hashes(state, raw_title, raw_link)
                    state.live_recent_titles.append((now, strip_news_source_tail(raw_title)))
                    state.live_last_sent_at = now
                    state.live_news_daily_count += 1
                    sent_this_scan += 1
                await asyncio.sleep(LIVE_NEWS_POLL_SECONDS)
            except Exception:
                logging.exception("live_news_monitor 오류")
                await asyncio.sleep(LIVE_NEWS_POLL_SECONDS)


async def build_evening_checklist_message(session: aiohttp.ClientSession, now: datetime) -> str:
    parts: list[str] = [room_line("데스크 체크리스트 · 장전", now)]
    if LIVE_ROOM_HOST_LINE:
        parts += ["", room_host_line(now, "digest", "digest")]
    parts += ["", SEC_DESK_SNAP]
    snap_lines: list[str] = []
    try:
        btc = await get_market_ticker(session, "BTCUSDT")
        if btc:
            p = float(btc["lastPrice"])
            snap_lines.append(f"· BTC {p:,.0f} ({fmt_pct(float(btc['priceChangePercent']))})")
    except Exception:
        pass
    for sym, label in (("ETHUSDT", "ETH"), ("SOLUSDT", "SOL")):
        try:
            t = await get_market_ticker(session, sym)
            if t:
                snap_lines.append(f"· {label} {float(t['lastPrice']):,.0f} ({fmt_pct(float(t['priceChangePercent']))})")
        except Exception:
            pass
    if snap_lines:
        parts.extend(snap_lines)
    else:
        parts.append("· BTC·ETH·SOL: 구간·24h 변동·거래대금")
    parts.append("· 선물: 펀딩·OI·청산 클러스터")
    parts += [
        "",
        "② 이벤트·뉴스",
        "· ETF 순유입·규제 캘린더·온체인 대형 이동",
        "",
        "③ 미국·매크로",
        "· NQ·SOX·금리·DXY·유가",
        "",
        "④ 한국",
        "· 반도체·외국인·원달러 (월요 갭·선물 포지션)",
        "",
        "⑤ 리스크",
        "· 포지션 사이즈·레버·슬리피지·거래소 공지",
        "",
        LIVE_NEWS_CARD_DISCLAIMER,
    ]
    return compact_message("\n".join(parts), LIVE_MESSAGE_SOFT_LIMIT)


async def daily_digest_scheduler(bot: Bot, state: State) -> None:
    while True:
        try:
            now = now_kst()
            key_evening = f"day_digest:{now.date()}"
            # 아침 7시는 overnight_recap_scheduler(7:10)와 겹치므로 제거. 저녁 한 번만.
            if now.hour == 18 and now.minute < 5 and state.digest_sent_dates.get(key_evening) != now.date():
                try:
                    async with aiohttp.ClientSession() as session:
                        msg = await build_evening_checklist_message(session, now)
                    await send_message(bot, msg, disable_preview=True)
                    state.digest_sent_dates[key_evening] = now.date()
                except Exception:
                    logging.exception("daily_digest 전송 실패")
            await asyncio.sleep(60)
        except Exception:
            logging.exception("daily_digest_scheduler 오류")
            await asyncio.sleep(60)


async def macro_pulse_monitor(bot: Bot, state: State) -> None:
    poll_sec = MACRO_PULSE_POLL_MINUTES * 60
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await asyncio.sleep(poll_sec)
                btc = await get_market_ticker(session, "BTCUSDT")
                nq = await get_yahoo_snapshot(session, "NQ%3DF")
                dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                wti = await get_yahoo_snapshot(session, "CL%3DF")
                cur: Dict[str, float] = {}
                if btc:
                    cur["BTC"] = float(btc["priceChangePercent"])
                if nq is not None:
                    cur["NQ"] = safe_pct_from_snapshot(nq)
                if dxy is not None:
                    cur["DXY"] = safe_pct_from_snapshot(dxy)
                if wti is not None:
                    cur["WTI"] = safe_pct_from_snapshot(wti)
                if len(cur) < 2:
                    continue
                baseline = state.macro_pulse_last_pcts
                if baseline is None:
                    state.macro_pulse_last_pcts = dict(cur)
                    continue
                keys = set(cur) & set(baseline)
                if len(keys) < 2:
                    continue
                max_delta = max(abs(cur[k] - baseline[k]) for k in keys)
                now = utc_now()
                last_sent = state.macro_pulse_last_sent
                cooldown_ok = last_sent is None or (now - last_sent).total_seconds() >= MACRO_PULSE_COOLDOWN_HOURS * 3600
                if max_delta < MACRO_PULSE_MIN_MOVE_PCT or not cooldown_ok:
                    continue
                nk = now_kst()
                parts = [room_line("매크로 맥박 · 코인 베타", nk)]
                if LIVE_ROOM_HOST_LINE:
                    parts += ["", room_host_line(nk, "macro", "macro")]
                parts += ["", "① 당일 변동률(스냅)"]
                order = ("BTC", "NQ", "WTI", "DXY")
                for label in order:
                    if label not in cur:
                        continue
                    extra = ""
                    if label in baseline:
                        d = cur[label] - baseline[label]
                        if abs(d) >= 0.08:
                            extra = f" · 직전대비 {'+' if d > 0 else ''}{d:.1f}%p"
                    parts.append(f"· {label} {fmt_pct(cur[label])}{extra}")
                for label in sorted(cur):
                    if label not in order:
                        parts.append(f"· {label} {fmt_pct(cur[label])}")
                btc_v = cur.get("BTC")
                wti_v = cur.get("WTI")
                nq_v = cur.get("NQ")
                if btc_v is not None and wti_v is not None and btc_v * wti_v < 0 and abs(btc_v) >= 0.4 and abs(wti_v) >= 0.4:
                    corr_line = "· BTC·유가가 반대로 움직이면 리스크오프·인플레 우려가 섞인 날일 수 있음."
                elif btc_v is not None and nq_v is not None and btc_v * nq_v > 0 and abs(btc_v) >= 0.5:
                    corr_line = "· BTC·NQ가 같이 가면 베타 확대 구간 — 펀딩·OI 쏠림만 추가 확인."
                else:
                    corr_line = "· BTC·NQ·DXY·유가가 크게 엇갈리면 상관 붕괴·범위 구간일 수 있음."
                parts += [
                    "",
                    "② 체크 · 상관·유동성",
                    corr_line,
                    "",
                    "③ 운영 메모",
                    "· 다자산 동시 출렁임: 추격 금지, 범위·체결만.",
                    "",
                    LIVE_NEWS_CARD_DISCLAIMER,
                ]
                await safe_send(bot, compact_message("\n".join(parts), LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                state.macro_pulse_last_sent = now
                state.macro_pulse_last_pcts = dict(cur)
            except Exception:
                logging.exception("macro_pulse_monitor 오류")


async def live_recap_scheduler(bot: Bot, state: State) -> None:
    while True:
        try:
            if not env_bool("ENABLE_RECAP", True):
                await asyncio.sleep(60)
                continue
            now = now_kst()
            if state.recap_used_news_date != now.date():
                state.recap_used_news_titles = set()
                state.recap_used_news_date = now.date()
                state.recap_used_topics = set()
            if now.hour in LIVE_RECAP_HOURS and now.minute < 5:
                key = f"recap:{now.date()}:{now.hour}"
                if key not in state.recap_sent_keys and hasattr(state, "live_recent_items"):
                    items = list(state.live_recent_items)[-8:]
                    if items:
                        weekend_mode = is_weekend_mode(now)
                        holiday_mode = is_kr_holiday_day(now)
                        logging.info("recap mode weekend=%s holiday_mode=%s key=%s", weekend_mode, holiday_mode, key)
                        if weekend_mode:
                            top = sorted(
                                items,
                                key=lambda x: (recap_weekend_priority(x[2], x[1], x[3]), x[4] if len(x) > 4 else 0),
                                reverse=True,
                            )[:3]
                        else:
                            top = sorted(items, key=lambda x: x[4] if len(x) > 4 else 0, reverse=True)[:3]
                        recap_blocks: list[str] = []
                        idx = 1
                        seen_buckets: set[str] = set()
                        coin_count = 0
                        picked_hashes: list[str] = []
                        picked_topics: list[str] = []
                        for item in top:
                            _ts, emoji, title, source, _score, *extra = item
                            recap_grade = extra[0] if len(extra) >= 1 else "A"
                            recap_topic = extra[1] if len(extra) >= 2 else normalize_recap_bucket(recap_market_keyword(title, emoji))
                            if source_quality_rank(source, "") == "C":
                                logging.info("recap skipped reason=source_rank_c title=%s source=%s", clean_text(title, 90), source)
                                continue
                            if mostly_english(title):
                                logging.info("recap skipped reason=mostly_english title=%s", clean_text(title, 90))
                                continue
                            if not is_recap_title_natural(title):
                                logging.info("recap skipped reason=unnatural_title title=%s", clean_text(title, 90))
                                continue
                            if low_quality_block_reason(title, ""):
                                logging.info("recap skipped reason=low_quality title=%s", clean_text(title, 90))
                                continue
                            if recap_grade not in ("A", "B"):
                                logging.info("recap skipped reason=grade_filter grade=%s title=%s", recap_grade, clean_text(title, 90))
                                continue
                            keyword = recap_market_keyword(title, emoji)
                            bucket = normalize_recap_bucket(keyword)
                            topic_key = recap_topic if recap_topic else bucket
                            title_hash = recap_title_hash(title)
                            if title_hash in state.recap_used_news_titles:
                                logging.info("recap skipped reason=already_used_today title=%s", clean_text(title, 90))
                                continue
                            if topic_key in state.recap_used_topics:
                                logging.info("recap skipped reason=topic_used_today topic=%s title=%s", topic_key, clean_text(title, 90))
                                continue
                            if bucket in seen_buckets:
                                logging.info("recap skipped reason=duplicate_bucket bucket=%s title=%s", bucket, clean_text(title, 90))
                                continue
                            if bucket == "코인" and coin_count >= 2:
                                logging.info("recap skipped reason=coin_limit title=%s", clean_text(title, 90))
                                continue
                            seen_buckets.add(bucket)
                            if bucket == "코인":
                                coin_count += 1
                            picked_hashes.append(title_hash)
                            picked_topics.append(topic_key)
                            title_line = strip_news_source_tail(title)
                            if bucket == "코인":
                                title_line = "핵심 코인 이슈는 기대감 대비 실제 자금 유입이 확인되는지 점검 구간."
                            elif bucket == "유가":
                                title_line = "유가와 해운 변수는 단기 진정과 재확대 가능성을 함께 봐야 하는 흐름."
                            elif bucket in ("금리", "달러"):
                                title_line = "금리·달러 방향이 위험자산 변동성을 키우는지 확인이 필요한 구간."
                            elif bucket in ("ETF",):
                                title_line = "ETF 이슈는 헤드라인보다 실제 수급 반응을 먼저 확인할 구간."
                            recap_blocks.append(f"{idx}. {bucket}\n{title_line}")
                            idx += 1
                            if idx > 3:
                                break
                        if len(recap_blocks) < 3:
                            logging.info("recap skipped reason=not_enough_new_items key=%s count=%s", key, len(recap_blocks))
                            state.recap_sent_keys.add(key)
                            continue
                        recap_msg = (
                            room_line("데스크 리캡 · 장중 이슈", now)
                            + f"\n\n{SEC_NEWS_FACT} · 오늘 축 요약\n\n"
                            + "\n\n".join(recap_blocks)
                            + "\n\n"
                            + ROOM_DISCLAIMER
                        )
                        await safe_send(bot, compact_message(recap_msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                        for h in picked_hashes:
                            state.recap_used_news_titles.add(h)
                        for t in picked_topics:
                            state.recap_used_topics.add(t)
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


async def railway_port_health_server(workers: list[str]) -> None:
    port_s = os.getenv("PORT")
    if not port_s:
        return
    port = int(port_s)

    async def ping(_request: web.Request) -> web.Response:
        return web.Response(text="telegram-bot worker ok")

    async def health(_request: web.Request) -> web.Response:
        commit = (
            os.getenv("RAILWAY_GIT_COMMIT_SHA")
            or os.getenv("SOURCE_COMMIT")
            or os.getenv("HEROKU_SLUG_COMMIT")
            or "unknown"
        )
        payload = {
            "ok": True,
            "service": "telegram-bot",
            "commit": commit,
            "channel": str(CHANNEL_ID),
            "workers": list(workers),
        }
        return web.json_response(payload)

    app = web.Application()
    app.router.add_get("/", ping)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info("PORT=%s HTTP / /health 시작", port_s)
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
                    nk = now_kst()
                    await safe_send(
                        bot,
                        room_line("청산 감지 · 롱", nk)
                        + "\n\n① 규모\n"
                        f"· {total_long:,.0f} USDT\n\n② 체크\n"
                        "· 하방 압력 한 번 크게 나온 구간\n"
                        "· 반등 약하면 추가 하락\n"
                        "· 추격 말고 반등 강도만\n\n"
                        + ROOM_DISCLAIMER,
                        disable_preview=True,
                    )
                    state.touch_cooldown(cooldown_key_long, now)

                if total_short >= 5_000_000 and not state.is_on_cooldown(cooldown_key_short, now):
                    nk = now_kst()
                    await safe_send(
                        bot,
                        room_line("청산 감지 · 숏", nk)
                        + "\n\n① 규모\n"
                        f"· {total_short:,.0f} USDT\n\n② 체크\n"
                        "· 위로 강제 매수 물량\n"
                        "· 급등 직후 위꼬리\n"
                        "· 돌파 유지 여부만\n\n"
                        + ROOM_DISCLAIMER,
                        disable_preview=True,
                    )
                    state.touch_cooldown(cooldown_key_short, now)

            except Exception:
                logging.exception("liquidation_monitor 오류")

            await asyncio.sleep(60)


async def alt_coin_pulse_scheduler(bot: Bot, state: State) -> None:
    """EXTRA_USDT_SYMBOLS(쉼표)에 넣은 USDT 현물 티커를 주기적으로 한 장으로 묶어 보냄."""

    def fmt_px(p: float) -> str:
        if p >= 1000:
            return f"{p:,.2f}"
        if p >= 1:
            return f"{p:,.4f}"
        return f"{p:.6g}"

    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                if not EXTRA_USDT_SYMBOLS:
                    await asyncio.sleep(3600)
                    continue
                rows: list[tuple[str, float, float]] = []
                for sym in EXTRA_USDT_SYMBOLS:
                    t = await get_market_ticker(session, sym)
                    if not t:
                        continue
                    try:
                        price = float(t["lastPrice"])
                        pct = float(t["priceChangePercent"])
                    except Exception:
                        continue
                    rows.append((sym, price, pct))
                if rows:
                    rows.sort(key=lambda x: abs(x[2]), reverse=True)
                    nk = now_kst()
                    parts = [
                        room_line("알트 티커 · 스냅", nk),
                        "",
                        "① 24h 변동률 · 바이낸스 USDT 현물",
                    ]
                    for sym, price, pct in rows[:10]:
                        coin = sym.replace("USDT", "")
                        parts.append(f"· {coin} {fmt_pct(pct)} · {fmt_px(price)} USDT")
                    parts += ["", ROOM_DISCLAIMER]
                    await safe_send(bot, compact_message("\n".join(parts), LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
            except Exception:
                logging.exception("alt_coin_pulse_scheduler 오류")
            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(60.0, float(ALT_PULSE_INTERVAL_SEC) - elapsed))


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
    nk = now_kst()
    return (
        room_line("선물 톤 · 체크", nk)
        + "\n\n① 레짐\n"
        f"· {direction}\n\n② 실행 원칙\n"
        "· 추격 금지\n"
        "· 눌림/돌파 유지·거래대금으로만 확인\n\n"
        + ROOM_DISCLAIMER
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
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now = now_kst()
                if now.hour == 7 and now.minute == 10:
                    key = "overnight_0710"
                    if state.overnight_recap_sent_dates.get(key) != now.date():
                        weekend_mode = is_weekend_mode(now)
                        holiday_mode = not is_korean_market_weekday(now)
                        logging.info("overnight_recap mode weekend=%s holiday_mode=%s", weekend_mode, holiday_mode)
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
                        f_btc, f_eth, f_sol = await asyncio.gather(
                            get_funding_rate(session, "BTCUSDT"),
                            get_funding_rate(session, "ETHUSDT"),
                            get_funding_rate(session, "SOLUSDT"),
                        )
                        msg = room_line("야간 브리핑", now)
                        msg += "\n\n① 스냅 · 코인"
                        if btc:
                            msg += f"\n· BTC {btc_price:,.0f} ({fmt_pct(btc_pct)})"
                        if eth:
                            msg += f"\n· ETH {fmt_pct(eth_pct)}"
                        if sol:
                            msg += f"\n· SOL {fmt_pct(sol_pct)}"
                        msg += f"\n· 펀딩 BTC {_fmt_funding_pct(f_btc)} · ETH {_fmt_funding_pct(f_eth)} · SOL {_fmt_funding_pct(f_sol)}"
                        msg += "\n\n② 스냅 · 거시"
                        if nq:
                            msg += f"\n· 나스닥 선물 {fmt_pct(safe_pct_from_snapshot(nq))}"
                        if sox:
                            msg += f"\n· 반도체 지수 {fmt_pct(safe_pct_from_snapshot(sox))}"
                        if wti:
                            msg += f"\n· 유가 {fmt_pct(safe_pct_from_snapshot(wti))}"
                        if dxy:
                            msg += f"\n· 달러 {fmt_pct(safe_pct_from_snapshot(dxy))}"
                        msg += f"\n\n{SEC_DESK_OPS}\n· {final_market_recap_focus(btc_pct, eth_pct, sol_pct, safe_pct_from_snapshot(nq), safe_pct_from_snapshot(sox), safe_pct_from_snapshot(wti), safe_pct_from_snapshot(dxy))}"
                        if weekend_mode or holiday_mode:
                            msg += "\n· 주말·휴장: 코인·유가·달러·김치"
                        else:
                            msg += "\n· 한국장: 반도체·외국인·환율 ↔ BTC 구간"
                        msg += f"\n\n{ROOM_DISCLAIMER}"
                        try:
                            await send_message(bot, compact_message(msg, LIVE_MESSAGE_SOFT_LIMIT), disable_preview=True)
                            state.overnight_recap_sent_dates[key] = now.date()
                        except Exception:
                            logging.exception("overnight_recap 전송 실패")
            except Exception:
                logging.exception("overnight_recap_scheduler 오류")
            await asyncio.sleep(20)


async def run_forever() -> None:
    # Railway: TELEGRAM_TOKEN·TELEGRAM_CHANNEL_ID. PORT=헬스.
    # 속보 피드: ENABLE_LIVE_ISSUE_FEED=false(이슈 제외), ENABLE_LIVE_WORLD_FEED=false(세계 제외),
    # LIVE_NEWS_COMPACT_NUMERIC_BLOCK=false(가격 인용 수치 블록 항상 표시).
    token, _ = resolve_telegram_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN 환경변수가 필요합니다.")

    bot = Bot(token=token)
    state = State()
    if ENABLE_BOT_STATE_PERSIST:
        try:
            bot_state_hydrate(state)
            logging.info("BOT_DATA_DIR 상태 스냅샷 복원 완료 path=%s", _bot_state_db_path())
        except Exception:
            logging.exception("bot_state_hydrate")
    now = now_kst()

    market_mode_lines = []
    if not is_korean_market_weekday(now):
        market_mode_lines.append("KR market closed")
    us_closed = (not is_us_market_premarket_day(now)) and (not is_us_market_close_day(now))
    if us_closed:
        market_mode_lines.append("US market holiday")
    if is_weekend_mode(now):
        market_mode_lines.append("Weekend crypto mode enabled")
    if market_mode_lines:
        logging.info("Market mode:\n- %s", "\n- ".join(market_mode_lines))

    if chart_img_api_key():
        logging.info("TradingView chart: chart-img API key configured")
    else:
        logging.warning(
            "TradingView chart: CHART_IMG_API_KEY 없음 — 코인 뉴스 차트는 QuickChart 대체 "
            "(https://chart-img.com 무료 키 발급 후 Railway 환경변수 설정)"
        )

    flags = {
        "ENABLE_LIVE_NEWS": env_bool("ENABLE_LIVE_NEWS", True),
        "ENABLE_MARKET_MONITOR": env_bool("ENABLE_MARKET_MONITOR", True),
        "ENABLE_BRIEFING": env_bool("ENABLE_BRIEFING", True),
        "ENABLE_BTC_LEVEL": env_bool("ENABLE_BTC_LEVEL", True),
        "ENABLE_VOLUME_ALERT": env_bool("ENABLE_VOLUME_ALERT", True),
        "ENABLE_RECAP": env_bool("ENABLE_RECAP", True),
        "ENABLE_FUTURES_FLOW": env_bool("ENABLE_FUTURES_FLOW", True),
        "ENABLE_ALPHA_FLOW": env_bool("ENABLE_ALPHA_FLOW", True),
        "ENABLE_WHALE": env_bool("ENABLE_WHALE", True),
        "ENABLE_HEALTH": env_bool("ENABLE_HEALTH", True),
        "ENABLE_ALT_VOLUME_ALERT": env_bool("ENABLE_ALT_VOLUME_ALERT", True),
        "ENABLE_DAILY_DIGEST": env_bool("ENABLE_DAILY_DIGEST", True),
        "ENABLE_MACRO_PULSE": env_bool("ENABLE_MACRO_PULSE", True),
    }

    global RUNTIME_ENABLE_VOLUME_ALERT
    global RUNTIME_ENABLE_ALT_VOLUME_ALERT
    RUNTIME_ENABLE_VOLUME_ALERT = flags["ENABLE_VOLUME_ALERT"]
    RUNTIME_ENABLE_ALT_VOLUME_ALERT = flags["ENABLE_ALT_VOLUME_ALERT"]

    tasks = []
    enabled_workers = []

    if flags["ENABLE_MARKET_MONITOR"]:
        tasks.append(asyncio.create_task(market_monitor(bot, state)))
        enabled_workers.append("market_monitor")
    if flags["ENABLE_BTC_LEVEL"]:
        tasks.append(asyncio.create_task(btc_key_level_monitor(bot, state)))
        tasks.append(asyncio.create_task(btc_move_chart_monitor(bot, state)))
        enabled_workers.extend(["btc_key_level_monitor", "btc_move_chart_monitor"])

    # 유지 모니터는 기본 True (현 운영 동일)
    tasks.append(asyncio.create_task(fear_greed_monitor(bot, state)))
    tasks.append(asyncio.create_task(kimchi_monitor(bot, state)))
    tasks.append(asyncio.create_task(liquidation_monitor(bot, state)))
    enabled_workers.extend(["fear_greed_monitor", "kimchi_monitor", "liquidation_monitor"])
    if ENABLE_ALT_PULSE and EXTRA_USDT_SYMBOLS:
        tasks.append(asyncio.create_task(alt_coin_pulse_scheduler(bot, state)))
        enabled_workers.append("alt_coin_pulse_scheduler")
    if flags["ENABLE_MACRO_PULSE"]:
        tasks.append(asyncio.create_task(macro_pulse_monitor(bot, state)))
        enabled_workers.append("macro_pulse_monitor")

    if flags["ENABLE_WHALE"]:
        tasks.append(asyncio.create_task(whale_monitor(bot, state)))
        enabled_workers.append("whale_monitor")
    if flags["ENABLE_LIVE_NEWS"]:
        tasks.append(asyncio.create_task(live_news_monitor(bot, state)))
        enabled_workers.append("live_news_monitor")
        logging.info(
            "live_news RSS 카테고리: %s",
            " · ".join(cat for _, cat, _ in iter_live_news_feeds()),
        )
    if flags["ENABLE_RECAP"]:
        tasks.append(asyncio.create_task(live_recap_scheduler(bot, state)))
        enabled_workers.append("live_recap_scheduler")
    if flags["ENABLE_FUTURES_FLOW"]:
        tasks.append(asyncio.create_task(futures_flow_monitor(bot, state)))
        enabled_workers.append("futures_flow_monitor")
    if flags["ENABLE_ALPHA_FLOW"]:
        tasks.append(asyncio.create_task(alpha_flow_monitor(bot, state)))
        enabled_workers.append("alpha_flow_monitor")
    if flags["ENABLE_BRIEFING"]:
        tasks.append(asyncio.create_task(market_session_scheduler(bot, state)))
        tasks.append(asyncio.create_task(briefing_scheduler(bot, state)))
        tasks.append(asyncio.create_task(overnight_recap_scheduler(bot, state)))
        enabled_workers.extend(["market_session_scheduler", "briefing_scheduler", "overnight_recap_scheduler"])
    if flags["ENABLE_DAILY_DIGEST"]:
        tasks.append(asyncio.create_task(daily_digest_scheduler(bot, state)))
        enabled_workers.append("daily_digest_scheduler")
    if flags["ENABLE_HEALTH"]:
        tasks.append(asyncio.create_task(ops_health_monitor(bot, state)))
        enabled_workers.append("ops_health_monitor")

    if ENABLE_BOT_STATE_PERSIST:
        tasks.append(asyncio.create_task(bot_state_persist_loop(state)))
        enabled_workers.append("bot_state_persist_loop")

    if os.getenv("PORT"):
        tasks.append(asyncio.create_task(railway_port_health_server(enabled_workers)))
        enabled_workers.append("railway_port_health_server")

    logging.info("워커 루프 시작 CHANNEL=%s", CHANNEL_ID)
    logging.info("활성 워커: %s", ", ".join(enabled_workers))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for _lg in ("httpx", "httpcore"):
        logging.getLogger(_lg).setLevel(logging.WARNING)
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


