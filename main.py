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
PREDICTION_CHECK_SECONDS = 60 * 60
FNG_CHECK_SECONDS = 5 * 60
KIMCHI_CHECK_SECONDS = 10 * 60
WHALE_CHECK_SECONDS = 60
BRIEFING_CHECK_SECONDS = 30
REFERRAL_CHECK_SECONDS = 60
MARKET_SESSION_CHECK_SECONDS = 30

PRICE_CHANGE_THRESHOLD = 1.5
VOLUME_SURGE_THRESHOLD = 3.0
WHALE_NOTIONAL_THRESHOLD = 1_000_000
SIGNAL_COOLDOWN = timedelta(minutes=30)

NEWS_DAILY_LIMIT = 8
NEWS_MIN_INTERVAL = timedelta(minutes=15)
NEWS_BLOCK_HOURS = None  # 미국장 시간대도 뉴스 전송: 새벽 1~7시 차단 해제

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
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


def news_id(title: str, link: str, published: str) -> str:
    raw = f"{title}|{link}|{published}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def has_keyword(text: str, keywords: Tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(k in lowered for k in keywords)


def news_importance_score(title: str, summary: str) -> int:
    text = f"{title}\n{summary}".lower()
    score = 0

    # 시장에 바로 영향 줄 가능성이 큰 이슈
    high_weight = (
        "etf", "sec", "regulation", "lawsuit", "approval", "rejection",
        "fed", "fomc", "cpi", "inflation", "interest rate", "rate cut",
        "trump", "tariff", "dollar", "oil", "hack", "exploit",
        "binance", "coinbase", "exchange", "liquidation", "sell-off",
        "금리", "연준", "유가", "달러", "규제", "해킹", "거래소",
    )
    medium_weight = (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "price", "rally", "drop", "market", "volume", "whale",
    )
    low_quality = (
        "airdrop", "fork", "ecash", "developer", "developers", "github",
        "testnet", "protocol upgrade", "whitepaper", "podcast", "interview",
        "opinion", "guide", "how to", "meme", "nft",
    )

    score += sum(3 for k in high_weight if k in text)
    score += sum(1 for k in medium_weight if k in text)
    score -= sum(3 for k in low_quality if k in text)
    return score


def is_high_quality_news(title: str, summary: str) -> bool:
    text = f"{title}\n{summary}".lower()
    if not any(k in text for k in NEWS_KEYWORDS):
        return False
    return news_importance_score(title, summary) >= 2


def news_importance_line(title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()
    if any(k in text for k in ("etf", "sec", "regulation", "lawsuit", "approval", "rejection", "규제")):
        return "ETF·규제 이슈라 비트코인 수급에 바로 영향 줄 수 있음."
    if any(k in text for k in ("fed", "fomc", "cpi", "inflation", "interest rate", "rate cut", "금리", "연준")):
        return "금리 기대가 흔들리면 코인·주식이 같이 움직일 수 있음."
    if any(k in text for k in ("hack", "exploit", "해킹")):
        return "해킹 이슈는 단기 투자심리를 빠르게 식힐 수 있음."
    if any(k in text for k in ("exchange", "binance", "coinbase", "거래소")):
        return "거래소 이슈는 수급과 신뢰도에 바로 연결됨."
    if any(k in text for k in ("trump", "tariff", "dollar", "oil", "유가", "달러")):
        return "거시 이슈라 위험자산 분위기를 흔들 수 있음."
    if any(k in text for k in ("liquidation", "sell-off", "whale", "volume")):
        return "청산·거래량 이슈라 단기 변동성이 커질 수 있음."
    return "가격 흐름에 영향 줄 수 있는 이슈라 체크할 만함."


def indicator_soft_cta(title: str, summary: str) -> str:
    text = f"{title}\n{summary}".lower()
    if any(k in text for k in ("price", "rally", "drop", "volume", "liquidation", "whale", "btc", "bitcoin")):
        return "이런 뉴스는 가격보다 반응 속도가 중요해서, 나는 직접 만든 보조지표랑 같이 보는 중."
    return "이런 이슈는 시장 반응까지 같이 봐야 해서, 나는 보조지표로 흐름 확인 중."

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


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None):
    async with session.get(url, params=params, timeout=20) as response:
        if response.status != 200:
            return None
        return await response.json()


async def fetch_text(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=25) as response:
        if response.status != 200:
            return None
        return await response.text()


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
    return "RSS"


async def build_korean_news_message(session: aiohttp.ClientSession, title: str, summary: str, link: str) -> str:
    title_ko = await translate_to_korean(session, title)
    summary_ko = await translate_to_korean(session, summary)
    source = source_name_from_link(link)
    why = news_importance_line(title, summary)
    cta = indicator_soft_cta(title, summary)

    return (
        f"📰 [뉴스]\n"
        f"{title_ko}\n\n"
        f"왜 중요함?\n{why}\n\n"
        f"{cta}\n\n"
        f"출처: {source}\n"
        f"{link}"
    )


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
                            if not is_high_quality_news(title, summary):
                                continue

                            nid = news_id(title, link, published)
                            if state.has_news(nid):
                                continue

                            now = utc_now()
                            if (
                                state.last_news_sent_at
                                and (now - state.last_news_sent_at) < NEWS_MIN_INTERVAL
                            ):
                                continue

                            msg = await build_korean_news_message(session, title, summary, link)
                            # 링크 미리보기 켬: 텔레그램에서 기사 썸네일/사진이 다시 보이게 함
                            await safe_send(bot, msg, disable_preview=False)
                            state.mark_news(nid)
                            state.last_news_sent_at = now
                            state.news_daily_count += 1
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, NEWS_CHECK_SECONDS - int(elapsed)))


async def referral_scheduler(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession():
        while True:
            started = utc_now()
            try:
                now = now_kst()
                if now.weekday() in (0, 3) and now.hour == 18 and now.minute == 0:
                    key = f"{now.weekday()}-18"
                    if state.referral_sent_dates.get(key) != now.date():
                        msg = (
                            "💡 이 채널에서 쓰는 거래소\n"
                            "수수료 할인 + 자동매매 봇 세팅 지원\n"
                            "👉 linktr.ee/jadonnam"
                        )
                        await safe_send(bot, msg)
                        state.referral_sent_dates[key] = now.date()
            except Exception:
                pass

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, REFERRAL_CHECK_SECONDS - int(elapsed)))


async def market_session_scheduler(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started = utc_now()
            try:
                now = now_kst()

                if now.hour == 8 and now.minute == 30:
                    key = "kr_open_0830"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        if kospi and kosdaq and usd_krw and sp_fut and nq_fut:
                            us_flow = (
                                "위험선호 우위"
                                if (sp_fut[1] + nq_fut[1]) >= 0
                                else "리스크오프 우위"
                            )
                            issue_line = (
                                "미국 선물 강세 시 외국인 수급 개선 여부 주목."
                                if us_flow == "위험선호 우위"
                                else "달러/원과 반도체 흐름 중심의 보수적 대응 필요."
                            )
                            msg = (
                                "🇰🇷 [한국장 시작 전]\n"
                                "오늘 주목할 이슈 TOP5\n\n"
                                f"1. 코스피 {kospi[0]:,.2f} ({fmt_pct(kospi[1])})\n"
                                f"2. 코스닥 {kosdaq[0]:,.2f} ({fmt_pct(kosdaq[1])})\n"
                                f"3. 달러/원 환율 {usd_krw:,.2f}원\n"
                                f"4. 간밤 미국장 주요 흐름: {us_flow}\n"
                                f"5. 오늘 주목 이슈: {issue_line}\n\n"
                                "⚡ 데이터 출처: Yahoo Finance API"
                            )
                            await safe_send(bot, msg)
                            state.market_session_sent_dates[key] = now.date()

                if now.hour == 15 and now.minute == 30:
                    key = "kr_close_1530"
                    if state.market_session_sent_dates.get(key) != now.date():
                        kospi = await get_yahoo_snapshot(session, "%5EKS11")
                        kosdaq = await get_yahoo_snapshot(session, "%5EKQ11")
                        usd_krw = await get_usd_krw(session)
                        if kospi and kosdaq and usd_krw:
                            if kospi[1] >= 0 and kosdaq[1] >= 0:
                                feature = "지수 동반 강세"
                            elif kospi[1] >= 0 > kosdaq[1]:
                                feature = "대형주 상대강세, 중소형주 약세"
                            elif kospi[1] < 0 <= kosdaq[1]:
                                feature = "종목장 성격 강화"
                            else:
                                feature = "지수 전반 약세"
                            msg = (
                                "🔔 [한국장 마감]\n"
                                f"코스피: {kospi[0]:,.2f} ({fmt_pct(kospi[1])})\n"
                                f"코스닥: {kosdaq[0]:,.2f} ({fmt_pct(kosdaq[1])})\n"
                                f"달러/원: {usd_krw:,.0f}원\n"
                                f"오늘의 특징: {feature}"
                            )
                            await safe_send(bot, msg)
                            state.market_session_sent_dates[key] = now.date()

                if now.hour == 21 and now.minute == 30:
                    key = "us_open_2130"
                    if state.market_session_sent_dates.get(key) != now.date():
                        sp_fut = await get_yahoo_snapshot(session, "ES%3DF")
                        nq_fut = await get_yahoo_snapshot(session, "NQ%3DF")
                        dxy = await get_yahoo_snapshot(session, "DX-Y.NYB")
                        tnx = await get_yahoo_snapshot(session, "%5ETNX")
                        if sp_fut and nq_fut and dxy and tnx:
                            calendar_line = "당일 CPI/FOMC/NFP 등 고변동 지표 일정 확인 필요"
                            msg = (
                                "🇺🇸 [미국장 시작 전]\n"
                                "주목할 이슈 TOP5\n\n"
                                f"1. S&P500 선물 {fmt_pct(sp_fut[1])}\n"
                                f"2. 나스닥 선물 {fmt_pct(nq_fut[1])}\n"
                                f"3. 달러인덱스(DXY) {dxy[0]:.2f} ({fmt_pct(dxy[1])})\n"
                                f"4. 10년물 국채금리 {tnx[0]:.2f} ({fmt_pct(tnx[1])})\n"
                                f"5. 오늘 주요 경제지표: {calendar_line}"
                            )
                            await safe_send(bot, msg)
                            state.market_session_sent_dates[key] = now.date()

                if now.hour == 4 and now.minute == 0:
                    key = "us_close_0400"
                    if state.market_session_sent_dates.get(key) != now.date():
                        spx = await get_yahoo_snapshot(session, "%5EGSPC")
                        ixic = await get_yahoo_snapshot(session, "%5EIXIC")
                        dji = await get_yahoo_snapshot(session, "%5EDJI")
                        if spx and ixic and dji:
                            avg_move = (spx[1] + ixic[1] + dji[1]) / 3
                            core = (
                                "빅테크 주도 위험선호 지속"
                                if avg_move >= 0
                                else "리스크 관리 심리 우세"
                            )
                            msg = (
                                "🌙 [미국장 마감]\n"
                                f"S&P500: {spx[0]:,.2f} ({fmt_pct(spx[1])})\n"
                                f"나스닥: {ixic[0]:,.2f} ({fmt_pct(ixic[1])})\n"
                                f"다우: {dji[0]:,.2f} ({fmt_pct(dji[1])})\n"
                                f"오늘의 핵심: {core}"
                            )
                            await safe_send(bot, msg)
                            state.market_session_sent_dates[key] = now.date()
            except Exception:
                pass

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
        asyncio.create_task(referral_scheduler(bot, state)),
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
