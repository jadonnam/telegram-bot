import asyncio
import hashlib
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
import feedparser
from telegram import Bot


CHANNEL_ID = "@jadonnam"
CHECK_INTERVAL_SECONDS = 5 * 60
NEWS_INTERVAL_SECONDS = 5 * 60
GUIDE_CHECK_INTERVAL_SECONDS = 60
PRICE_CHANGE_THRESHOLD = 1.5
SIGNAL_COOLDOWN = timedelta(minutes=30)
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")

RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
)

KEYWORDS = ("btc", "etf", "cpi", "fed", "금리", "유가")

KST = ZoneInfo("Asia/Seoul")


class State:
    def __init__(self) -> None:
        self.price_history: Dict[str, Deque[Tuple[datetime, float]]] = defaultdict(deque)
        self.cooldowns: Dict[str, datetime] = {}
        self.news_seen_ids: Deque[str] = deque(maxlen=2000)
        self.news_seen_set = set()
        self.last_guide_sent: Dict[str, datetime.date] = {}

    def is_on_cooldown(self, signal_key: str, now: datetime) -> bool:
        expires_at = self.cooldowns.get(signal_key)
        return bool(expires_at and expires_at > now)

    def touch_cooldown(self, signal_key: str, now: datetime) -> None:
        self.cooldowns[signal_key] = now + SIGNAL_COOLDOWN

    def mark_news(self, news_id: str) -> None:
        if news_id in self.news_seen_set:
            return
        if len(self.news_seen_ids) == self.news_seen_ids.maxlen:
            oldest = self.news_seen_ids.popleft()
            self.news_seen_set.discard(oldest)
        self.news_seen_ids.append(news_id)
        self.news_seen_set.add(news_id)

    def has_news(self, news_id: str) -> bool:
        return news_id in self.news_seen_set


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_news_id(title: str, link: str, published: str) -> str:
    raw = f"{title}|{link}|{published}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def has_keyword(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in KEYWORDS)


def format_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


async def send_message(bot: Bot, text: str) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=False)


async def fetch_ticker_24h(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {"symbol": symbol}
    async with session.get(url, params=params, timeout=20) as response:
        if response.status != 200:
            return None
        return await response.json()


async def fetch_recent_klines(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": "5m", "limit": 3}
    async with session.get(url, params=params, timeout=20) as response:
        if response.status != 200:
            return None
        return await response.json()


def get_price_15m_ago(history: Deque[Tuple[datetime, float]], now: datetime) -> Optional[float]:
    target = now - timedelta(minutes=15)
    while history and history[0][0] < now - timedelta(hours=2):
        history.popleft()
    candidate = None
    for ts, price in history:
        if ts <= target:
            candidate = price
        else:
            break
    return candidate


async def market_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started_at = utc_now()
            try:
                for symbol in SYMBOLS:
                    ticker = await fetch_ticker_24h(session, symbol)
                    if not ticker:
                        continue

                    price = float(ticker["lastPrice"])
                    now = utc_now()
                    history = state.price_history[symbol]
                    history.append((now, price))
                    price_15m_ago = get_price_15m_ago(history, now)

                    if price_15m_ago:
                        pct_change = ((price - price_15m_ago) / price_15m_ago) * 100
                        if abs(pct_change) >= PRICE_CHANGE_THRESHOLD:
                            direction = "상승" if pct_change > 0 else "하락"
                            signal_key = f"price:{symbol}:{direction}"
                            if not state.is_on_cooldown(signal_key, now):
                                msg = (
                                    f"[시장 감지]\n"
                                    f"{symbol.replace('USDT', '')} 15분 {direction} {format_pct(pct_change)}\n"
                                    f"현재가 {price:,.2f} USDT"
                                )
                                await send_message(bot, msg)
                                state.touch_cooldown(signal_key, now)

                    klines = await fetch_recent_klines(session, symbol)
                    if klines and len(klines) >= 2:
                        prev_quote_volume = float(klines[-2][7])
                        latest_quote_volume = float(klines[-1][7])
                        if prev_quote_volume > 0:
                            surge_ratio = latest_quote_volume / prev_quote_volume
                            if surge_ratio >= 1.8:
                                signal_key = f"volume:{symbol}"
                                if not state.is_on_cooldown(signal_key, now):
                                    msg = (
                                        f"[시장 감지]\n"
                                        f"{symbol.replace('USDT', '')} 거래량 급증 x{surge_ratio:.2f}\n"
                                        f"최근 5분 거래대금 {latest_quote_volume:,.0f} USDT"
                                    )
                                    await send_message(bot, msg)
                                    state.touch_cooldown(signal_key, now)
            except Exception as exc:
                await send_message(bot, f"[시장 감지]\n모니터링 오류 발생: {type(exc).__name__}")

            elapsed = (utc_now() - started_at).total_seconds()
            await asyncio.sleep(max(5, CHECK_INTERVAL_SECONDS - int(elapsed)))


async def fetch_feed(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=25) as response:
        if response.status != 200:
            return []
        text = await response.text()
    parsed = feedparser.parse(text)
    return parsed.entries or []


async def news_monitor(bot: Bot, state: State) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            started_at = utc_now()
            try:
                for feed_url in RSS_FEEDS:
                    entries = await fetch_feed(session, feed_url)
                    for entry in entries[:15]:
                        title = (entry.get("title") or "").strip()
                        link = (entry.get("link") or "").strip()
                        summary = (entry.get("summary") or "").strip()
                        published = (entry.get("published") or "").strip()
                        text_bundle = f"{title}\n{summary}"

                        if not title or not link:
                            continue
                        if not has_keyword(text_bundle):
                            continue

                        news_id = get_news_id(title, link, published)
                        if state.has_news(news_id):
                            continue

                        msg = f"[뉴스]\n{title}\n{link}"
                        await send_message(bot, msg)
                        state.mark_news(news_id)
            except Exception as exc:
                await send_message(bot, f"[뉴스]\n수집 오류 발생: {type(exc).__name__}")

            elapsed = (utc_now() - started_at).total_seconds()
            await asyncio.sleep(max(5, NEWS_INTERVAL_SECONDS - int(elapsed)))


def should_send_guide(now_kst: datetime, sent_state: Dict[str, datetime.date]) -> Optional[str]:
    slots = {
        "09": 9,
        "21": 21,
    }
    for slot_key, hour in slots.items():
        if now_kst.hour == hour and now_kst.minute == 0:
            if sent_state.get(slot_key) != now_kst.date():
                return slot_key
    return None


async def guide_scheduler(bot: Bot, state: State) -> None:
    while True:
        try:
            now_kst = datetime.now(KST)
            slot = should_send_guide(now_kst, state.last_guide_sent)
            if slot:
                guide_msg = (
                    "[세팅 안내]\n"
                    "자동 정보 알림 채널입니다.\n"
                    "시장 급변/거래량/핵심 뉴스만 전달합니다.\n"
                    "본 채널은 매매 추천을 제공하지 않습니다."
                )
                await send_message(bot, guide_msg)
                state.last_guide_sent[slot] = now_kst.date()
        except Exception:
            # Guide scheduler failures should never stop the process.
            pass

        await asyncio.sleep(GUIDE_CHECK_INTERVAL_SECONDS)


async def main() -> None:
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN 환경변수가 필요합니다.")

    bot = Bot(token=token)
    state = State()

    tasks = [
        asyncio.create_task(market_monitor(bot, state)),
        asyncio.create_task(news_monitor(bot, state)),
        asyncio.create_task(guide_scheduler(bot, state)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception:
            # Keep the bot alive even if unexpected top-level errors occur.
            continue
