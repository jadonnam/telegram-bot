import asyncio
import hashlib
import json
import os
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Deque, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
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

PRICE_CHANGE_THRESHOLD = 1.5
VOLUME_SURGE_THRESHOLD = 3.0
WHALE_NOTIONAL_THRESHOLD = 1_000_000
SIGNAL_COOLDOWN = timedelta(minutes=30)

NEWS_DAILY_LIMIT = 8
NEWS_MIN_INTERVAL = timedelta(minutes=15)
NEWS_BLOCK_HOURS = (1, 7)  # 01:00 <= now < 07:00 (KST)

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
)
NEWS_KEYWORDS = (
    "btc",
    "eth",
    "sol",
    "etf",
    "cpi",
    "fed",
    "금리",
    "유가",
    "달러",
    "연준",
    "trump",
    "bitcoin",
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


async def send_message(bot: Bot, text: str) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=False)


async def safe_send(bot: Bot, text: str) -> None:
    try:
        await send_message(bot, text)
    except Exception:
        pass


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

                if NEWS_BLOCK_HOURS[0] <= kst.hour < NEWS_BLOCK_HOURS[1]:
                    await asyncio.sleep(max(5, NEWS_CHECK_SECONDS))
                    continue

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
                            if not has_keyword(f"{title}\n{summary}", NEWS_KEYWORDS):
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

                            msg = f"📰 [뉴스]\n{title}\n{link}"
                            await safe_send(bot, msg)
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


async def run_forever() -> None:
    token = os.getenv("TELEGRAM_TOKEN")
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
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(run_forever())
        except Exception:
            continue
