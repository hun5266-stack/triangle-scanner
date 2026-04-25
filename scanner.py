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
TIMEFRAMES       = ["30m"]
KLINE_LIMIT      = 1000         # 30m × 1000 ≈ 20일
TOP_N            = 100
# 멀티 윈도우 스캔: (윈도우 길이, 피벗 lookback) 튜플 리스트
SCAN_WINDOWS     = [(30, 1), (60, 2), (120, 2)]
MIN_PIVOTS       = 2
MIN_R2           = 0.40         # 3피벗 이상일 때만 적용, 2피벗이면 R²=1이라 자동 통과
FLAT_SLOPE_PCT   = 0.0008       # 상단/하단 "평평"의 허용 기울기 (봉당 0.08%)
APEX_MIN_BARS    = 5
APEX_MAX_BARS    = 30
MIN_COMPRESSION  = 0.30
# 장대 필터: 수렴 시작 직전 변곡점에서 수렴 시작점까지의
# (꼬리 포함) 가격 폭이 수렴 박스 시작폭의 IMPULSE_RATIO_MIN배 이상
SWING_LOOKBACK   = 5            # 변곡 정의: 좌우 5봉 둘러봐 최고/최저
IMPULSE_MAX_BACK = 80           # 수렴 시작 직전 최대 80봉까지 변곡 탐색
IMPULSE_RATIO_MIN = 0.9         # 장대폭 ≥ 0.9 × 수렴박스 시작폭
# 품질 체크 임계값 (필터 아님, 알림에 ✓/✗만 표시)
QUALITY_IMPULSE_PCT = 5.0       # 임펄스 % 변동률
QUALITY_VOL_RATIO   = 1.5       # 장대 평균 거래량 / 수렴 평균 거래량
QUALITY_SHORT_WINDOW = 30       # 짧은 패턴 = 30봉 윈도우
ALERT_COOLDOWN   = 4 * 3600
TOP_REFRESH      = 12 * 3600   # top 100 리스트 갱신 주기
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
# state.json 스키마:
#   {
#     "cooldowns": {"BTC_USDT|15m": 1712345678.0, ...},
#     "top": {"ts": 1712345678.0, "symbols": ["BTC_USDT", ...]}
#   }

def load_state() -> dict:
    default = {"cooldowns": {}, "top": {"ts": 0.0, "symbols": []}}
    if not STATE_PATH.exists():
        return default
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default
        cooldowns = {}
        raw_cd = data.get("cooldowns", {})
        if isinstance(raw_cd, dict):
            cooldowns = {k: float(v) for k, v in raw_cd.items() if isinstance(v, (int, float))}
        top_raw = data.get("top") or {}
        top = {
            "ts": float(top_raw.get("ts", 0) or 0),
            "symbols": list(top_raw.get("symbols") or []),
        }
        return {"cooldowns": cooldowns, "top": top}
    except Exception as e:
        log.warning(f"state.json 로드 실패 ({e}), 빈 상태로 시작")
        return default


def save_state(state: dict) -> None:
    cutoff = datetime.now(timezone.utc).timestamp() - ALERT_COOLDOWN * 6
    cooldowns = {k: v for k, v in state.get("cooldowns", {}).items() if v >= cutoff}
    out = {
        "cooldowns": cooldowns,
        "top": state.get("top", {"ts": 0.0, "symbols": []}),
    }
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)


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
    timestamps = np.array([int(c["t"]) for c in data], dtype=np.int64)
    opens   = np.array([float(c["o"]) for c in data], dtype=float)
    highs   = np.array([float(c["h"]) for c in data], dtype=float)
    lows    = np.array([float(c["l"]) for c in data], dtype=float)
    closes  = np.array([float(c["c"]) for c in data], dtype=float)
    volumes = np.array([float(c.get("v", 0)) for c in data], dtype=float)
    return timestamps, opens, highs, lows, closes, volumes


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


def detect_triangle(highs, lows, closes, pivot_lookback=2):
    n = len(closes)
    price = closes[-1]

    piv_hi = find_pivots(highs, pivot_lookback, "high")
    piv_lo = find_pivots(lows, pivot_lookback, "low")
    if len(piv_hi) < MIN_PIVOTS or len(piv_lo) < MIN_PIVOTS:
        return None

    # 최근 피벗만 사용 (추세선은 최근 흐름에 맞춰)
    piv_hi = piv_hi[-3:]
    piv_lo = piv_lo[-3:]

    hi = fit_line(piv_hi, [highs[i] for i in piv_hi])
    lo = fit_line(piv_lo, [lows[i]  for i in piv_lo])
    if not hi or not lo:
        return None
    hi_slope, hi_b, hi_r2 = hi
    lo_slope, lo_b, lo_r2 = lo
    if hi_r2 < MIN_R2 or lo_r2 < MIN_R2:
        return None

    # 정규화 기울기 (봉당 가격변화율)
    hi_norm = hi_slope / price
    lo_norm = lo_slope / price

    T = FLAT_SLOPE_PCT              # 0.03%/bar  — "유의미한 기울기" 기준
    WEDGE_EPS = T * 0.3             # 0.01%/bar  — "같은 방향 미세 기울기" 허용폭

    hi_strong_down = hi_norm < -T
    lo_strong_up   = lo_norm >  T

    # 상승 삼각형: 상단이 평평 (|hi| < T) AND 위로 유의미하게 오르면 안 됨
    #   → hi_norm <= WEDGE_EPS (살짝 하강 쪽은 허용, 상승은 미량만)
    hi_flat_asc = abs(hi_norm) < T and hi_norm <= WEDGE_EPS
    # 하강 삼각형: 하단이 평평 AND 아래로 유의미하게 내려가면 안 됨
    lo_flat_desc = abs(lo_norm) < T and lo_norm >= -WEDGE_EPS

    if hi_strong_down and lo_strong_up:
        tri_type = "대칭"
    elif hi_flat_asc and lo_strong_up:
        tri_type = "상승"
    elif hi_strong_down and lo_flat_desc:
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


def find_swing_before(highs, lows, conv_start_g, lookback=SWING_LOOKBACK, max_back=IMPULSE_MAX_BACK):
    """conv_start_g(글로벌 인덱스) 직전, max_back 봉 안에서 가장 최근 swing pivot 반환.
    좌우 lookback봉 모두보다 높/낮은 봉. (idx, 'high'|'low', price) or None."""
    last = None
    lo_i = max(lookback, conv_start_g - max_back)
    hi_i = conv_start_g - lookback
    for i in range(lo_i, hi_i):
        wh = highs[i - lookback : i + lookback + 1]
        wl = lows[i - lookback : i + lookback + 1]
        if highs[i] == wh.max():
            last = (i, "high", float(highs[i]))
        if lows[i] == wl.min():
            last = (i, "low", float(lows[i]))
    return last


def check_impulse(highs, lows, volumes, end_g, wlen, result):
    """장대 필터. end_g는 윈도우 끝 글로벌 인덱스 (exclusive).
    True면 통과, dict에 impulse 정보 추가."""
    win_start_g = end_g - wlen
    conv_start_local = min(result["piv_hi"][0], result["piv_lo"][0])
    conv_start_g = win_start_g + conv_start_local

    sw = find_swing_before(highs, lows, conv_start_g)
    if not sw:
        return False
    sw_idx, sw_kind, sw_price = sw

    # 장대폭 = 변곡~수렴시작 구간 high-low (꼬리 포함)
    seg_h = float(highs[sw_idx : conv_start_g + 1].max())
    seg_l = float(lows[sw_idx : conv_start_g + 1].min())
    impulse_size = seg_h - seg_l

    # 수렴 박스 시작폭
    upper_at_start = result["hi_slope"] * conv_start_local + result["hi_intercept"]
    lower_at_start = result["lo_slope"] * conv_start_local + result["lo_intercept"]
    sq_start_width = upper_at_start - lower_at_start
    if sq_start_width <= 0:
        return False

    ratio = impulse_size / sq_start_width
    if ratio < IMPULSE_RATIO_MIN:
        return False

    # 품질 지표 (필터 X, 알림에 표시용)
    impulse_pct = (seg_h - seg_l) / seg_l * 100 if seg_l > 0 else 0
    vol_imp = float(volumes[sw_idx : conv_start_g + 1].mean()) if conv_start_g >= sw_idx else 0
    vol_sq  = float(volumes[conv_start_g : end_g].mean()) if end_g > conv_start_g else 0
    vol_ratio = (vol_imp / vol_sq) if vol_sq > 0 else 0.0

    result["impulse_ratio"] = float(ratio)
    result["impulse_dir"]   = "down" if sw_kind == "high" else "up"
    result["impulse_high"]  = seg_h
    result["impulse_low"]   = seg_l
    result["impulse_pct"]   = float(impulse_pct)
    result["vol_ratio"]     = float(vol_ratio)
    result["swing_idx_local"] = int(sw_idx - win_start_g)
    return True


async def scan_symbol(session, symbol, timeframe):
    try:
        timestamps, opens, highs, lows, closes, volumes = await get_klines(session, symbol, timeframe, KLINE_LIMIT)
        n = len(closes)
        # 멀티 윈도우 스캔: 짧은 → 긴 순서. 첫 감지에서 반환 (쿨다운은 symbol|tf 단위).
        for wlen, lookback in SCAN_WINDOWS:
            if n < wlen:
                continue
            w_ts     = timestamps[-wlen:]
            w_opens  = opens[-wlen:]
            w_highs  = highs[-wlen:]
            w_lows   = lows[-wlen:]
            w_closes = closes[-wlen:]
            result = detect_triangle(w_highs, w_lows, w_closes, pivot_lookback=lookback)
            if not result:
                continue
            # 장대 필터
            if not check_impulse(highs, lows, volumes, n, wlen, result):
                continue
            result["symbol"] = symbol
            result["timeframe"] = timeframe
            result["window"] = wlen
            result["_ts"]     = w_ts
            result["_opens"]  = w_opens
            result["_highs"]  = w_highs
            result["_lows"]   = w_lows
            result["_closes"] = w_closes
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
    ax.set_facecolor("#fafafa")

    # --- X축을 시간(KST)으로 ---
    ts = h.get("_ts")
    if ts is not None and len(ts) >= 2:
        from datetime import datetime, timezone, timedelta
        kst = timezone(timedelta(hours=9))
        bar_sec = int(ts[1] - ts[0]) if len(ts) >= 2 else 1800
        def bar_to_dt(idx):
            t0 = int(ts[0]) + int(idx) * bar_sec
            return datetime.fromtimestamp(t0, tz=kst)
        # 적당히 6~8개 tick
        xt_lo = max(left, 0)
        xt_hi = right
        n_ticks = 7
        step = max(1, (xt_hi - xt_lo) // n_ticks)
        ticks = list(range(xt_lo, xt_hi + 1, step))
        labels = []
        for t in ticks:
            d = bar_to_dt(t)
            # 날짜 바뀌면 MM-DD HH:MM, 같으면 HH:MM
            labels.append(d.strftime("%m-%d %H:%M") if d.hour == 0 and d.minute < bar_sec/60 else d.strftime("%H:%M"))
        # 첫 tick은 항상 날짜 포함
        if ticks:
            labels[0] = bar_to_dt(ticks[0]).strftime("%m-%d %H:%M")
        ax.set_xticks(ticks)
        ax.set_xticklabels(labels, rotation=0, fontsize=8)
        ax.set_xlabel("KST")
    else:
        ax.set_xlabel("bars")

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
        arrow = "⬆" if h.get("impulse_dir") == "up" else "⬇"

        # 품질 체크 (필터 아님, 표시용)
        imp_pct = h.get("impulse_pct", 0)
        vol_r   = h.get("vol_ratio", 0)
        wlen    = h.get("window", 0)
        c_imp = "✅" if imp_pct >= QUALITY_IMPULSE_PCT else "❌"
        c_vol = "✅" if vol_r   >= QUALITY_VOL_RATIO   else "❌"
        c_win = "✅" if wlen   == QUALITY_SHORT_WINDOW else "❌"
        score = sum(x == "✅" for x in (c_imp, c_vol, c_win))
        if score == 3: marker = "🔥"
        elif score == 2: marker = "👍"
        elif score == 1: marker = "⚠️"
        else: marker = "🤔"
        quality_value = (
            f"{c_imp} 임펄스 {imp_pct:.1f}% (≥{QUALITY_IMPULSE_PCT:.0f}%)\n"
            f"{c_vol} 거래량비 {vol_r:.2f}× (≥{QUALITY_VOL_RATIO:.1f}×)\n"
            f"{c_win} 윈도우 {wlen}봉 (=30)"
        )
        embeds.append({
            "title": f"{marker} {_display_symbol(contract)} · {h['type']} · {arrow}장대후 수렴",
            "url": url,
            "color": TYPE_COLORS.get(h["type"], 0x95A5A6),
            "fields": [
                {"name": "현재가",     "value": f"{h['price']:.6g}", "inline": True},
                {"name": "범위",       "value": f"{h['lower']:.4g} ~ {h['upper']:.4g}", "inline": True},
                {"name": f"품질 체크 ({score}/3)", "value": quality_value, "inline": False},
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
    state = load_state()
    cooldowns = state["cooldowns"]
    top = state["top"]

    async with aiohttp.ClientSession() as session:
        now_ts = datetime.now(timezone.utc).timestamp()
        age = now_ts - (top.get("ts") or 0)
        cached = top.get("symbols") or []
        if cached and age < TOP_REFRESH:
            symbols = cached
            log.info(f"top100 캐시 사용 (갱신 {age/3600:.1f}h 전, 다음 갱신까지 {(TOP_REFRESH-age)/3600:.1f}h)")
        else:
            symbols = await get_top_symbols(session, TOP_N)
            top = {"ts": now_ts, "symbols": symbols}
            log.info(f"top100 신규 조회: {len(symbols)}개 저장")

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
                if now - cooldowns.get(key, 0) > ALERT_COOLDOWN:
                    cooldowns[key] = now
                    fresh.append(h)

            log.info(f"[{tf}] 감지 {len(hits)}건 / 신규 알림 {len(fresh)}건")
            await send_discord_webhook(session, fresh, tf)

    save_state({"cooldowns": cooldowns, "top": top})


def main():
    try:
        asyncio.run(run_once())
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
