# alpha_patch.py
# main.py에 "고급 수급 알파" 기능만 추가하는 패치
# 추가: Bybit 체결강도, 대형 체결, CVD proxy 감지

from pathlib import Path
from datetime import datetime

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. main.py 있는 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_alpha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

anchor = "WHALE_NOTIONAL_THRESHOLD = 3_000_000"
extra_constants = '''
# 고급 수급 알파 감지
ALPHA_FLOW_CHECK_SECONDS = 60
ALPHA_SIGNAL_COOLDOWN = timedelta(minutes=20)
ALPHA_BIG_TRADE_NOTIONAL = 1_000_000
ALPHA_CVD_NOTIONAL_THRESHOLD = 8_000_000
ALPHA_IMBALANCE_THRESHOLD = 0.68
'''
if "ALPHA_FLOW_CHECK_SECONDS" not in text:
    text = text.replace(anchor, anchor + extra_constants)

state_anchor = "self.whale_seen_set = set()"
state_extra = '''
        self.alpha_seen_ids: Deque[str] = deque(maxlen=10000)
        self.alpha_seen_set = set()
'''
if "self.alpha_seen_ids" not in text:
    text = text.replace(state_anchor, state_anchor + state_extra)

method_anchor = '''    def mark_whale_trade(self, trade_id: str) -> None:
        if trade_id in self.whale_seen_set:
            return
        if len(self.whale_seen_ids) == self.whale_seen_ids.maxlen:
            old = self.whale_seen_ids.popleft()
            self.whale_seen_set.discard(old)
        self.whale_seen_ids.append(trade_id)
        self.whale_seen_set.add(trade_id)
'''
method_extra = method_anchor + '''
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
'''
if "def has_alpha_trade" not in text:
    text = text.replace(method_anchor, method_extra)

alpha_code = r'''
async def get_bybit_recent_trades(session: aiohttp.ClientSession, symbol: str) -> Optional[list]:
    data = await fetch_json(
        session,
        "https://api.bybit.com/v5/market/recent-trade",
        {"category": "linear", "symbol": symbol, "limit": "100"},
    )
    try:
        rows = (data.get("result") or {}).get("list", [])
        return rows or []
    except Exception:
        return None


async def alpha_flow_monitor(bot: Bot, state: State) -> None:
    # 고급 수급 알파 감지.
    # REST 기반이라 WebSocket만큼 정밀하진 않지만, 도배 없이 체결강도/CVD proxy를 감지.
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

                buy_notional = 0.0
                sell_notional = 0.0
                big_buy = 0.0
                big_sell = 0.0
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
                                "📌 단기 지지 또는 돌파 시도 가능성"
                            )
                        else:
                            msg = (
                                "🐋 [알파 수급 감지]\n"
                                "BTC 대형 매도 체결 우세\n\n"
                                f"대형 매수: {big_buy:,.0f} USDT\n"
                                f"대형 매도: {big_sell:,.0f} USDT\n\n"
                                "📌 단기 저항 또는 눌림 가능성"
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
                                f"📊 [체결강도 감지]\n"
                                f"BTC {side_label} 체결 강도 우세\n\n"
                                f"매수 체결: {buy_notional:,.0f} USDT\n"
                                f"매도 체결: {sell_notional:,.0f} USDT\n"
                                f"CVD Proxy: {cvd:+,.0f} USDT\n"
                                f"{side_label} 비중: {ratio * 100:.1f}%\n\n"
                                f"📌 한쪽 체결이 과하게 쏠리는 구간"
                            )
                            await safe_send(bot, msg, disable_preview=True)
                            state.touch_cooldown(signal_key, now)

            except Exception:
                logging.exception("alpha_flow_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, ALPHA_FLOW_CHECK_SECONDS - int(elapsed)))
'''

if "async def alpha_flow_monitor" not in text:
    text = text.replace("# ============================================================\n# BRIEFINGS", alpha_code + "\n\n# ============================================================\n# BRIEFINGS")

task_anchor = "asyncio.create_task(futures_flow_monitor(bot, state)),"
task_extra = task_anchor + "\n        asyncio.create_task(alpha_flow_monitor(bot, state)),"
if "alpha_flow_monitor(bot, state)" not in text:
    text = text.replace(task_anchor, task_extra)

p.write_text(text, encoding="utf-8")
print("알파 패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
