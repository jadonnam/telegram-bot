# patch_main_v2.py
# 목적:
# - 뉴스 4~5개 연속 발송 방지
# - 2분마다 속보 도배 방지
# - "attack" 단어 하나 때문에 일반 보안뉴스가 속보 처리되는 문제 수정
# - BTC +0.0% 보합권 문제 완화
#
# 사용:
# 1) 이 파일을 main.py 있는 폴더에 넣기
# 2) python patch_main_v2.py
# 3) git add . && git commit -m "fix news spam and btc flow" && git push

from pathlib import Path
from datetime import datetime
import re

p = Path("main.py")
if not p.exists():
    raise FileNotFoundError("main.py가 없습니다. main.py 있는 폴더에서 실행하세요.")

text = p.read_text(encoding="utf-8")
backup = Path(f"main_backup_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
backup.write_text(text, encoding="utf-8")

text = re.sub(r"NEWS_DAILY_LIMIT\s*=\s*\d+", "NEWS_DAILY_LIMIT = 4", text)
text = re.sub(r"NEWS_MIN_INTERVAL\s*=\s*timedelta\(minutes=\d+\)", "NEWS_MIN_INTERVAL = timedelta(minutes=45)", text)

insert_after = "NEWS_NORMAL_SCORE = 6"
extra = """
# 뉴스 도배 방지: 한 번 RSS 체크마다 최대 1개만 전송
NEWS_MAX_PER_SCAN = 1
# 속보도 최소 간격 적용. 진짜 지정학 속보만 예외로 빠르게 통과.
NEWS_URGENT_MIN_INTERVAL = timedelta(minutes=20)
"""
if "NEWS_MAX_PER_SCAN" not in text:
    text = text.replace(insert_after, insert_after + extra)

new_forced = """def is_forced_breaking_news(title: str, summary: str) -> bool:
    # 지정학/전쟁/유가/군사 충격만 강제 속보 처리.
    # 일반 보안뉴스의 attack 단어 하나로 속보 처리되는 문제 방지.
    text = f"{title}\\n{summary}".lower()

    geo_terms = (
        "iran", "israel", "hormuz", "uae", "warship", "navy", "tanker",
        "missile", "strike", "explosion", "oil", "war",
        "이란", "이스라엘", "호르무즈", "군함", "해군", "유조선",
        "미사일", "피격", "폭발", "전쟁", "유가",
    )
    market_terms = (
        "bitcoin", "btc", "crypto", "market", "oil", "dollar", "stock", "risk",
        "비트코인", "코인", "시장", "유가", "달러", "주식", "위험자산",
    )

    has_geo = any(k in text for k in geo_terms)
    has_market = any(k in text for k in market_terms)
    return has_geo and has_market
"""
text = re.sub(
    r"def is_forced_breaking_news\(title: str, summary: str\) -> bool:\n.*?\n\ndef normalized_news_score",
    new_forced + "\n\ndef normalized_news_score",
    text,
    flags=re.S,
)

new_urgent = """def is_urgent_news(title: str, summary: str) -> bool:
    if is_forced_breaking_news(title, summary):
        return True

    text = f"{title}\\n{summary}".lower()

    # 일반 거래소/소송/보안 기사까지 속보로 보내지 않게 제한
    true_urgent = (
        "sec approves", "sec rejects", "fomc", "cpi",
        "hacked", "exploit", "freeze", "frozen", "seized",
        "liquidation", "sell-off", "crash",
        "승인", "거절", "해킹", "압수", "동결", "청산", "급락",
    )

    if not any(k in text for k in true_urgent):
        return False

    return news_importance_score(title, summary) >= NEWS_URGENT_SCORE
"""
text = re.sub(
    r"def is_urgent_news\(title: str, summary: str\) -> bool:\n.*?\n\ndef news_importance_line",
    new_urgent + "\n\ndef news_importance_line",
    text,
    flags=re.S,
)

old_btc_block = """    btc = await get_binance_ticker_24h(session, "BTCUSDT")
    btc_line = ""
    if btc:
        btc_pct = float(btc.get("priceChangePercent", 0))
        flow = "상승 흐름" if btc_pct > 0 else "하락 압력" if btc_pct < 0 else "보합권"
        btc_line = f"\\n\\n📊 현재 BTC: {fmt_pct(btc_pct)} ({flow})"
"""
new_btc_block = """    btc = await get_binance_ticker_24h(session, "BTCUSDT")
    btc_line = ""
    if btc and btc.get("priceChangePercent") is not None:
        try:
            btc_pct = float(btc.get("priceChangePercent"))
            flow = "상승 흐름" if btc_pct > 0.15 else "하락 압력" if btc_pct < -0.15 else "보합권"
            btc_line = f"\\n\\n📊 현재 BTC: {fmt_pct(btc_pct)} ({flow})"
        except Exception:
            btc_line = ""
"""
if old_btc_block in text:
    text = text.replace(old_btc_block, new_btc_block)

new_news_monitor = """async def news_monitor(bot: Bot, state: State) -> None:
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

                        # 높은 점수 먼저 보내되, 한 번에 1개만
                        candidates = []
                        for e in entries[:20]:
                            title = (e.get("title") or "").strip()
                            link = (e.get("link") or "").strip()
                            summary = (e.get("summary") or "").strip()
                            published = (e.get("published") or "").strip()
                            if not title or not link:
                                continue

                            nid = news_id(title, link, published)
                            if state.has_news(nid):
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
                            state.mark_news(nid)
                            state.last_news_sent_at = now
                            state.news_daily_count += 1
                            sent_this_scan += 1
                            break

            except Exception:
                logging.exception("news_monitor 오류")

            elapsed = (utc_now() - started).total_seconds()
            await asyncio.sleep(max(5, NEWS_CHECK_SECONDS - int(elapsed)))
"""
text = re.sub(
    r"async def news_monitor\(bot: Bot, state: State\) -> None:\n.*?\n\n# =========================\n# BRIEFINGS",
    new_news_monitor + "\n\n# =========================\n# BRIEFINGS",
    text,
    flags=re.S,
)

p.write_text(text, encoding="utf-8")
print("패치 완료")
print(f"수정 파일: {p.resolve()}")
print(f"백업 파일: {backup.resolve()}")
