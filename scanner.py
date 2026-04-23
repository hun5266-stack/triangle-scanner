#!/usr/bin/env python3
"""
Gate.io USDT Perpetual Futures — Triangle Convergence Scanner (GitHub Actions edition)

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
CONCURRENCY      = 5
GATE             = "https://api.gateio.ws"

# Gate.io는 소문자 그대로 사용
INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "4h": "4h", "8h": "8h", "1d": "1d", "7d": "7d",
}

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


# ========== Gate.io API (v4) ==========
async def fetch_json(session, url, params=None, retries=5):
    delay = 0.5
    last_exc = None
    for _ in range(retries):
        try:
            async with session.get(url, params=params, timeout=15) as r:
                if r.status == 429:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 8.0)
                    continue
                r.raise_for_status()
                return await r.json()
        except aiohttp.ClientResponseError as e:
            last_exc = e
            if e.status == 429:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch_json: exhausted retries")


def _display_symbol(contract: str) -> str:
    """BTC_USDT → BTCUSDT (표시용)."""
    return contract.replace("_", "")


async def get_top_symbols(session, n=TOP_N):
    """Gate.io USDT 무기한 선물 중 24시간 quote 거래대금 상위 N개 contract 반환."""
    contracts = await fetch_json(
        session,
        f"{GATE}/api/v4/futures/usdt/contracts",
    )
    active = {
        c["name"] for c in contracts
        if not c.get("in_delisting", False) and c["name"].endswith("_USDT")
    }

    tickers = await fetch_json(
        session,
        f"{GATE}/api/v4/futures/usdt/tickers",
    )
    rows = [t for t in tickers if t["contract"] in active]

    def _vol(t):
        try:
            return float(t.get("volume_24h_quote") or 0)
        except (TypeError, ValueError):
            return 0.0

    rows.sort(key=_vol, reverse=True)
    return [t["contract"] for t in rows[:n]]


async def get_klines(session, symbol, timeframe, limit):
    interval = INTERVAL_MAP.get(timeframe, timeframe)
    data = await fetch_json(
        session,
        f"{GATE}/api/v4/futures/usdt/candlesticks",
        params={"contract": symbol, "interval": interval, "limit": limit},
    )
    if not data:
        raise RuntimeError("empty klines")
    # Gate.io는 시간순(과거→최신)으로 반환, 객체 배열
    opens  = np.array([float(c["o"]) for c in data], dtype=float)
    highs  = np.array([float(c["h"]) for c in data], dtype=float)
    lows   = np.array([float(c["l"]) for c in data], dtype=float)
    closes = np.array([float(c["c"]) for c in data], dtype=float)
    return opens, highs, lows, closes


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
        "hi_slope": float(hi_slope),
        "hi_intercept": float(hi_b),
        "lo_slope": float(lo_slope),
        "lo_intercept": float(lo_b),
        "piv_hi": [int(i) for i in piv_hi],
        "piv_lo": [int(i) for i in piv_lo],
    }


async def scan_symbol(session, symbol, timeframe):
    try:
        opens, highs, lows, closes = await get_klines(session, symbol, timeframe, KLINE_LIMIT)
        result = detect_triangle(highs, lows, closes)
        if result:
            result["symbol"] = symbol
            result["timeframe"] = timeframe
            result["_opens"] = opens
            result["_highs"] = highs
            result["_lows"] = lows
            result["_closes"] = closes
            return result
    except Exception as e:
        log.warning(f"{symbol} [{timeframe}]: {e}")
    return None


# ========== 차트 렌더 ==========
def render_chart(h) -> bytes:
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    opens  = h["_opens"]
    highs  = h["_highs"]
    lows   = h["_lows"]
    closes = h["_closes"]
    n = len(closes)
    ext = int(h["bars_to_apex"])

    # 삼각형 구조가 시작된 지점 기준으로 zoom-in
    first_piv = min(h["piv_hi"][0], h["piv_lo"][0])
    left = max(0, first_piv - 10)
    right = n + ext + 2

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=110)

    # --- 캔들봉 ---
    width = 0.7
    for i in range(n):
        o, c, hi, lo = opens[i], closes[i], highs[i], lows[i]
        up = c >= o
        color = "#26a69a" if up else "#ef5350"
        # 심지 (high-low)
        ax.vlines(i, lo, hi, color=color, linewidth=0.9, zorder=2)
        # 몸통 (open-close)
        body_lo, body_hi = (o, c) if up else (c, o)
        height = body_hi - body_lo
        if height == 0:
            height = (hi - lo) * 0.02 or 1e-9
        ax.add_patch(plt.Rectangle(
            (i - width/2, body_lo), width, height,
            facecolor=color, edgecolor=color, linewidth=0.6, zorder=3,
        ))

    # --- 추세선 (피벗 시작점부터 apex까지만) ---
    line_start = first_piv
    xs = np.arange(line_start, n + ext + 1)
    upper_line = h["hi_slope"] * xs + h["hi_intercept"]
    lower_line = h["lo_slope"] * xs + h["lo_intercept"]
    ax.plot(xs, upper_line, color="#d62728", linewidth=1.5, linestyle="--")
    ax.plot(xs, lower_line, color="#2ca02c", linewidth=1.5, linestyle="--")

    # --- 피벗 점 ---
    ax.scatter(h["piv_hi"], [highs[i] for i in h["piv_hi"]],
               color="#d62728", s=30, zorder=6, edgecolors="white", linewidths=0.8)
    ax.scatter(h["piv_lo"], [lows[i]  for i in h["piv_lo"]],
               color="#2ca02c", s=30, zorder=6, edgecolors="white", linewidths=0.8)

    # --- Apex 지점 표시 ---
    apex_x = n - 1 + ext
    apex_y = (h["hi_slope"] * apex_x + h["hi_intercept"] + h["lo_slope"] * apex_x + h["lo_intercept"]) / 2
    ax.scatter([apex_x], [apex_y], marker="*", s=140, color="#f39c12", zorder=7, edgecolors="black", linewidths=0.5)

    # --- 현재봉 세로선 ---
    ax.axvline(n - 1, color="#3498db", linewidth=0.8, alpha=0.5, linestyle=":")

    type_en = {"대칭": "Symmetrical", "상승": "Ascending", "하강": "Descending"}.get(h["type"], h["type"])
    ax.set_title(
        f"{_display_symbol(h['symbol'])} | {h['timeframe']} | {type_en} "
        f"(comp {h['compression']*100:.0f}%, apex in {ext} bars)"
    )
    ax.grid(True, alpha=0.25)
    ax.set_xlim(left, right)
    visible_slice = slice(left, min(n, right))
    y_min = min(lows[visible_slice].min(), lower_line.min())
    y_max = max(highs[visible_slice].max(), upper_line.max())
    pad = (y_max - y_min) * 0.05
    ax.set_ylim(y_min - pad, y_max + pad)
    ax.set_xlabel("bars")
    ax.set_facecolor("#fafafa")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


# ========== Discord ==========
def build_embeds(hits_sorted, timeframe, now_str):
    embeds = []
    for idx, h in enumerate(hits_sorted):
        contract = h["symbol"]
        url = f"https://www.gate.io/futures/USDT/{contract}"
        embeds.append({
            "title": f"{_display_symbol(contract)} · {h['type']}",
            "url": url,
            "color": TYPE_COLORS.get(h["type"], 0x95A5A6),
            "fields": [
                {"name": "현재가",     "value": f"{h['price']:.6g}", "inline": True},
                {"name": "범위",       "value": f"{h['lower']:.4g} ~ {h['upper']:.4g}", "inline": True},
                {"name": "Apex",       "value": f"{h['bars_to_apex']}봉 후", "inline": True},
                {"name": "수렴률",     "value": f"{h['compression']*100:.0f}%", "inline": True},
                {"name": "R² (상/하)", "value": f"{h['hi_r2']:.2f} / {h['lo_r2']:.2f}", "inline": True},
                {"name": "TF",         "value": timeframe, "inline": True},
            ],
            "image": {"url": f"attachment://chart_{idx}.png"},
            "footer": {"text": now_str},
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

    hits_sorted = sorted(hits, key=lambda x: -x["compression"])[:10]
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    content = f"🔺 **Triangle Scan** [{timeframe}] — {len(hits)}건 감지"
    embeds = build_embeds(hits_sorted, timeframe, now_str)

    form = aiohttp.FormData()
    form.add_field(
        "payload_json",
        json.dumps({"content": content, "embeds": embeds}),
        content_type="application/json",
    )
    for idx, h in enumerate(hits_sorted):
        try:
            png = render_chart(h)
        except Exception as e:
            log.error(f"차트 렌더 실패 {h['symbol']}: {e}")
            continue
        form.add_field(
            f"files[{idx}]",
            png,
            filename=f"chart_{idx}.png",
            content_type="image/png",
        )

    try:
        async with session.post(WEBHOOK_URL, data=form, timeout=30) as r:
            if r.status >= 300:
                text = await r.text()
                log.error(f"Discord 전송 실패 {r.status}: {text}")
            else:
                log.info(f"Discord 알림 전송: {len(embeds)} embed + 차트")
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
