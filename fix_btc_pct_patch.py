# fix_btc_pct_patch.py
# 목적:
# - 뉴스/브리핑에서 BTC가 +0.0%, 보합권으로 잘못 뜨는 문제 수정
# - Bybit price24hPcnt / OKX open24h 기반 24h 변동률 안정 계산
# - 기존 main.py 구조는 유지하고 get_market_ticker 함수만 안전 교체

from pathlib import Path
from datetime import datetime
import re

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. main.py 있는 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_btc_pct_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

new_func = '''async def get_market_ticker(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
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
'''

pattern = r"async def get_market_ticker\(session: aiohttp\.ClientSession, symbol: str\) -> Optional\[dict\]:\n.*?\n\nasync def get_binance_ticker_24h"
text2 = re.sub(pattern, new_func + "\n\nasync def get_binance_ticker_24h", text, flags=re.S)

if text2 == text:
    raise RuntimeError("get_market_ticker 함수 교체 실패. main.py 구조가 예상과 다릅니다.")

p.write_text(text2, encoding="utf-8")

print("BTC 변동률 패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
