# patch_main.py
# 사용법:
# 1) 이 파일을 C:\Users\user\Desktop\telegram-bot 폴더에 넣기
# 2) PowerShell에서: python patch_main.py
# 3) git add . && git commit -m "final high-end patch" && git push

from pathlib import Path
from datetime import datetime

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. telegram-bot 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

start = "# ===== JADONNAM HIGH-END PATCH START ====="
end = "# ===== JADONNAM HIGH-END PATCH END ====="

if start in text and end in text:
    before = text.split(start)[0]
    after = text.split(end)[1]
    text = before + after

patch = r'''
# ===== JADONNAM HIGH-END PATCH START =====
# 기존 긴 main.py를 삭제하지 않고 필요한 함수만 덮어씁니다.
# 적용 기능:
# - Binance 차단 대비: Binance → Bybit → OKX fallback
# - 누락 변수 정의: BREAKING_FORCE_TERMS / THREADS_* 등
# - BTC 80K/90K 메가 돌파·이탈 메시지 강화
# - 거래량 기반 강제 청산 감지
# - 펀딩비 + OI + 오더북 fallback
# - 뉴스 + 현재 BTC 흐름 연결
# - 속보/핵심뉴스/일반뉴스 분리 강화

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

NEWS_NORMAL_SCORE = 6
NEWS_URGENT_SCORE = 8
LIQUIDATION_VOLUME_THRESHOLD = 2.5

async def get_binance_ticker_24h(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    """기존 함수명 유지. Binance 실패 시 Bybit → OKX로 대체."""
    data = await fetch_json(session, "https://api.binance.com/api/v3/ticker/24hr", {"symbol": symbol})
    if data and data.get("lastPrice") is not None:
        return data

    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return {
            "lastPrice": item["lastPrice"],
            "priceChangePercent": float(item.get("price24hPcnt", 0)) * 100,
        }
    except Exception:
        pass

    okx_symbol = symbol.replace("USDT", "-USDT")
    data = await fetch_json(
        session,
        "https://www.okx.com/api/v5/market/ticker",
        {"instId": okx_symbol},
    )
    try:
        item = data["data"][0]
        return {
            "lastPrice": item["last"],
            "priceChangePercent": float(item.get("chg24h", 0)) * 100,
        }
    except Exception:
        return None

async def get_binance_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    ticker = await get_binance_ticker_24h(session, symbol)
    if not ticker or ticker.get("lastPrice") is None:
        return None
    return float(ticker["lastPrice"])

async def get_recent_klines(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    data = await fetch_json(
        session,
        "https://api.binance.com/api/v3/klines",
        {"symbol": symbol, "interval": "5m", "limit": 3},
    )
    if data:
        return data

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
        return converted
    except Exception:
        return None

async def get_funding_rate(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        {"symbol": symbol},
    )
    if data and "lastFundingRate" in data:
        return float(data["lastFundingRate"]) * 100

    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return float(item.get("fundingRate", 0)) * 100
    except Exception:
        return None

async def get_open_interest(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/openInterest",
        {"symbol": symbol},
    )
    if data and "openInterest" in data:
        return float(data["openInterest"])

    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    try:
        item = (data.get("result") or {}).get("list", [])[0]
        return float(item.get("openInterest", 0))
    except Exception:
        return None

async def get_orderbook_imbalance(session: aiohttp.ClientSession, symbol: str) -> Optional[Tuple[float, float, float]]:
    data = await fetch_json(
        session,
        "https://fapi.binance.com/fapi/v1/depth",
        {"symbol": symbol, "limit": "100"},
    )
    bids = asks = None
    if data:
        bids = data.get("bids") or []
        asks = data.get("asks") or []

    if not bids or not asks:
        data = await fetch_json(
            session,
            "https://api.bybit.com/v5/market/orderbook",
            {"category": "linear", "symbol": symbol, "limit": "50"},
        )
        try:
            result = data.get("result") or {}
            bids = result.get("b") or []
            asks = result.get("a") or []
        except Exception:
            return None

    bid_notional = sum(float(price) * float(qty) for price, qty in bids[:50])
    ask_notional = sum(float(price) * float(qty) for price, qty in asks[:50])
    if ask_notional <= 0:
        return None
    imbalance = bid_notional / ask_notional
    return imbalance, bid_notional, ask_notional

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

    threads_text = build_threads_text(title_ko, title, summary)
    return telegram_msg, threads_text

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

# ===== JADONNAM HIGH-END PATCH END =====
'''

marker = 'if __name__ == "__main__":'
if marker not in text:
    raise RuntimeError('main.py에서 if __name__ == "__main__": 위치를 찾지 못했습니다.')

text = text.replace(marker, patch + "\n\n" + marker, 1)
p.write_text(text, encoding="utf-8")

print("패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
