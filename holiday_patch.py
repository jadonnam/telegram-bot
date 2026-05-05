# holiday_patch.py
# 목적:
# - 한국 공휴일에는 한국장 브리핑 차단
# - 미국 공휴일에는 미국장 브리핑 차단
# - 공휴일은 Nager.Date API에서 연도별 자동 로드 후 메모리 캐시
# - API 실패 시 최소 수동 백업 공휴일로 안전 처리
#
# 사용:
# cd C:\Users\user\Desktop\telegram-bot
# python holiday_patch.py
# python -m py_compile main.py
# findstr "HOLIDAY_CACHE" main.py
# findstr "warm_holiday_cache" main.py
# git add -A
# git commit -m "add holiday market briefing filter"
# git push

from pathlib import Path
from datetime import datetime
import re

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. main.py 있는 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_holiday_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

holiday_block = '''
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
'''

if "HOLIDAY_CACHE" not in text:
    insert_anchor = 'REQUEST_HEADERS = {\n    "User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36",\n    "Accept": "application/json,text/plain,*/*",\n}\n'
    if insert_anchor not in text:
        raise RuntimeError("REQUEST_HEADERS 위치를 찾지 못했습니다.")
    text = text.replace(insert_anchor, insert_anchor + holiday_block + "\n")

text = re.sub(
    r"def is_korean_market_weekday\(now: datetime\) -> bool:\n\s*return now\.weekday\(\) < 5",
    "def is_korean_market_weekday(now: datetime) -> bool:\n    return now.weekday() < 5 and not is_kr_holiday_day(now)",
    text,
)

text = re.sub(
    r"def is_us_market_premarket_day\(now: datetime\) -> bool:\n\s*return now\.weekday\(\) < 5",
    "def is_us_market_premarket_day(now: datetime) -> bool:\n    return now.weekday() < 5 and not is_us_holiday_day(now)",
    text,
)

text = re.sub(
    r"def is_us_market_close_day\(now: datetime\) -> bool:\n\s*return 1 <= now\.weekday\(\) <= 5",
    (
        "def is_us_market_close_day(now: datetime) -> bool:\n"
        "    # KST 새벽 05:00 미국장 마감 브리핑은 전날 미국 거래일 기준\n"
        "    us_session_day = now - timedelta(days=1)\n"
        "    return us_session_day.weekday() < 5 and not is_us_holiday_day(us_session_day)"
    ),
    text,
)

old = "now = now_kst()\n\n                if is_korean_market_weekday(now)"
new = "now = now_kst()\n                await warm_holiday_cache(session, now.year)\n                await warm_holiday_cache(session, (now - timedelta(days=1)).year)\n\n                if is_korean_market_weekday(now)"
if old in text and "await warm_holiday_cache(session, now.year)" not in text:
    text = text.replace(old, new)

p.write_text(text, encoding="utf-8")

print("공휴일 필터 패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
