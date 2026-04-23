#!/usr/bin/env python3
"""
Binance USDT-M Perpetual Futures — Triangle Convergence Scanner (GitHub Actions edition)

- 1회 스캔 후 종료 (스케줄은 Actions cron이 담당)
- Discord 웹훅으로 Embed 알림 전송
- state.json에 마지막 알림 시각을 저장해 중복 알림 방지

환경변수:
    DISCORD_WEBHOOK_URL : Discord 채널 웹훅 URL
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import numpy as np

# ========== 설정 ==========
TIMEFRAMES       = ["15m", "30m"]
KLINE_LIMIT      = 200
TOP_N            = 100
PIVOT_LOOKBACK   = 3
MIN_PIVOTS       = 3
MIN_R2           = 0.85
FLAT_SLOPE_PCT   = 0.0003
APEX_MIN_BARS    = 5
APEX_MAX_BARS    = 50
MIN_COMPRESSION  = 0.30
ALERT_COOLDOWN   = 4 * 3600
CONCURRENCY      = 10
FAPI             = "https://fapi.binance.com"

STATE_PATH = Path(os.getenv("STATE_PATH", "state.json"))
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

TYPE_COLORS = {
    "대칭": 0x3498DB,
    "상승": 0x2ECC71,
    "하강": 0xE74C3C,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tri")


# ========== State ==========
def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
    except Exception as e:
        log.warning(f"state.json 로드 실패 ({e}), 빈 상태로 시작")
        return {}


def save_state(state: dict) -> None:
    cutoff = datetime.now(timezone.utc).timestamp() - ALERT_COOLDOWN * 6
    trimmed = {k: v for k, v in state.items() if v >= cutoff}
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(trimmed, f, indent=2, sort_keys=True)


# ========== Binance API ==========
async def fetch_json(session, url, params=None):
    async with session.get(url, params=params, timeout=15) as r:
        r.raise_for_status()
        return await r.json()


async def get_top_symbols(session, n=TOP_N):
    info = await fetch_json(session, f"{FAPI}/fapi/v1/exchangeInfo")
    perps = {
        s["symbol"] for s in info["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    }
    tickers = await fetch_json(session, f"{FAPI}/fapi/v1/ticker/24hr")
    tickers = [t for t in tickers if t["symbol"] in perps]
    tickers.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)
    return [t["symbol"] for t in tickers[:n]]


async def get_klines(session, symbol, interval, limit):
    data = await fetch_json(
        session,
        f"{FAPI}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    arr = np.array(data, dtype=object)
    highs  = arr[:, 2].astype(float)
    lows   = arr[:, 3].astype(float)
    closes = arr[:, 4].astype(float)
    return highs, lows, closes


# ========== 삼각형 감지 ==========
def find_pivots(series, lookback, kind="high"):
    pivots = []
    for i in range(lookback, len(series) - lookback):
        window = series[i - lookback : i + lookback + 1]
        center = series[i]
        if kind == "high" and center == window.max() and np.argmax(window) == lookback:
            pivots.append(i)
        elif kind == "low" and center == window.min() and np.argmin(window) == lookback:
            pivots.append(i)
    return pivots


def fit_line(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if len(x) < 2:
        return None
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return slope, intercept, r2


def detect_triangle(highs, lows, closes):
    n = len(closes)
    price = closes[-1]

    piv_hi = find_pivots(highs, PIVOT_LOOKBACK, "high")
    piv_lo = find_pivots(lows, PIVOT_LOOKBACK, "low")
    if len(piv_hi) < MIN_PIVOTS or len(piv_lo) < MIN_PIVOTS:
        return None

    piv_hi = piv_hi[-5:]
    piv_lo = piv_lo[-5:]

    hi = fit_line(piv_hi, [highs[i] for i in piv_hi])
    lo = fit_line(piv_lo, [lows[i]  for i in piv_lo])
    if not hi or not lo:
        return None
    hi_slope, hi_b, hi_r2 = hi
    lo_slope, lo_b, lo_r2 = lo
    if hi_r2 < MIN_R2 or lo_r2 < MIN_R2:
        return None

    hi_flat = abs(hi_slope / price) < FLAT_SLOPE_PCT
    lo_flat = abs(lo_slope / price) < FLAT_SLOPE_PCT

    if hi_slope < 0 and lo_slope > 0:
        tri_type = "대칭"
    elif hi_flat and lo_slope > 0:
        tri_type = "상승"
    elif hi_slope < 0 and lo_flat:
        tri_type = "하강"
    else:
        return None

    if hi_slope == lo_slope:
        return None
    apex_x = (lo_b - hi_b) / (hi_slope - lo_slope)
    bars_to_apex = apex_x - (n - 1)
    if not (APEX_MIN_BARS <= bars_to_apex <= APEX_MAX_BARS):
        return None

    hi_now = hi_slope * (n - 1) + hi_b
    lo_now = lo_slope * (n - 1) + lo_b
    if not (lo_now < price < hi_now):
        return None

    start_x = min(piv_hi[0], piv_lo[0])
    start_width = (hi_slope * start_x + hi_b) - (lo_slope * start_x + lo_b)
    now_width = hi_now - lo_now
    if start_width <= 0:
        return None
    compression = 1 - now_width / start_width
    if compression < MIN_COMPRESSION:
        return None

    return {
        "type": tri_type,
        "price": float(price),
        "upper": float(hi_now),
        "lower": float(lo_now),
        "bars_to_apex": int(bars_to_apex),
        "compression": float(compression),
        "hi_r2": float(hi_r2),
        "lo_r2": float(lo_r2),
    }


async def scan_symbol(session, symbol, timeframe):
    try:
        highs, lows, closes = await get_klines(session, symbol, timeframe, KLINE_LIMIT)
        result = detect_triangle(highs, lows, closes)
        if result:
            result["symbol"] = symbol
            result["timeframe"] = timeframe
            return result
    except Exception as e:
        log.warning(f"{symbol} [{timeframe}]: {e}")
    return None


# ========== Discord ==========
def build_embeds(hits, timeframe):
    hits_sorted = sorted(hits, key=lambda x: -x["compression"])
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    embeds = []
    for h in hits_sorted[:10]:  # Discord 한 메시지 최대 10 embed
        url = f"https://www.binance.com/en/futures/{h['symbol']}"
        embeds.append({
            "title": f"{h['symbol']} · {h['type']}",
            "url": url,
            "color": TYPE_COLORS.get(h["type"], 0x95A5A6),
            "fields": [
                {"name": "현재가",  "value": f"{h['price']:.6g}", "inline": True},
                {"name": "범위",    "value": f"{h['lower']:.4g} ~ {h['upper']:.4g}", "inline": True},
                {"name": "Apex",    "value": f"{h['bars_to_apex']}봉 후", "inline": True},
                {"name": "수렴률",  "value": f"{h['compression']*100:.0f}%", "inline": True},
                {"name": "R² (상/하)", "value": f"{h['hi_r2']:.2f} / {h['lo_r2']:.2f}", "inline": True},
                {"name": "TF",      "value": timeframe, "inline": True},
            ],
            "footer": {"text": now},
        })
    return embeds


async def send_discord_webhook(session, hits, timeframe):
    if not hits:
        return
    if not WEBHOOK_URL:
        log.info("DISCORD_WEBHOOK_URL 미설정. 콘솔 출력:")
        for h in sorted(hits, key=lambda x: -x["compression"]):
            log.info(
                f"  {h['symbol']} {h['type']} price={h['price']:.6g} "
                f"apex={h['bars_to_apex']} comp={h['compression']*100:.0f}% "
                f"R2={h['hi_r2']:.2f}/{h['lo_r2']:.2f}"
            )
        return

    content = f"🔺 **Triangle Scan** [{timeframe}] — {len(hits)}건 감지"
    embeds = build_embeds(hits, timeframe)
    try:
        async with session.post(
            WEBHOOK_URL,
            json={"content": content, "embeds": embeds},
            timeout=15,
        ) as r:
            if r.status >= 300:
                text = await r.text()
                log.error(f"Discord 전송 실패 {r.status}: {text}")
            else:
                log.info(f"Discord 알림 전송: {len(embeds)} embed")
    except Exception as e:
        log.error(f"Discord 전송 예외: {e}")


# ========== 메인 ==========
async def run_once():
    seen = load_state()
    async with aiohttp.ClientSession() as session:
        symbols = await get_top_symbols(session, TOP_N)
        sem = asyncio.Semaphore(CONCURRENCY)

        for tf in TIMEFRAMES:
            log.info(f"스캔 시작: {len(symbols)}개 심볼 · {tf}")

            async def bounded(s, tf=tf):
                async with sem:
                    return await scan_symbol(session, s, tf)

            results = await asyncio.gather(*[bounded(s) for s in symbols])
            hits = [r for r in results if r]

            now = datetime.now(timezone.utc).timestamp()
            fresh = []
            for h in hits:
                key = f"{h['symbol']}|{tf}"
                if now - seen.get(key, 0) > ALERT_COOLDOWN:
                    seen[key] = now
                    fresh.append(h)

            log.info(f"[{tf}] 감지 {len(hits)}건 / 신규 알림 {len(fresh)}건")
            await send_discord_webhook(session, fresh, tf)

    save_state(seen)


def main():
    try:
        asyncio.run(run_once())
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
