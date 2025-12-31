# -*- coding: utf-8 -*-
"""
فلتر 5 - نسخة Pipeline احترافية (Pydroid + Binance Futures)
- نفس منطق الاكتشاف (run_indicator + filter_fvgs_by_volume_importance + STRICT_FIRST_TOUCH) كما هو.
- تحسين جذري لمنع الحظر: فصل "اكتشاف المناطق" (دوري) عن "اللمس اللحظي" (WebSocket فقط).
- لا يتم جلب بيانات 1m لكل العملات في كل دورة. يتم جلب Base TF "عند الحاجة" فقط (قرب السعر من منطقة).
"""

import math
import time
import json
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re
import datetime

import ssl
import websocket
from threading import Event

# =========================
# BUILD / RUN GUARD
# =========================
BUILD_ID = "PIPELINE-WS-500-v4"
try:
    _running_file = __file__
except Exception:
    _running_file = "<unknown>"
print(f"[BOOT] BUILD={BUILD_ID} | RUNNING FILE={_running_file}", flush=True)

# ============================================================
# LOGGING
# ============================================================
LOG_TO_FILE = True
LOG_FILE = "scanner.log"
PRINT_PROGRESS = True
PROGRESS_EVERY = 25
DIAGNOSTIC_MODE = True

def log_msg(msg: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    out = f"[{ts}] {msg}"
    try:
        print(out, flush=True)
    except Exception:
        pass
    if LOG_TO_FILE:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(out + "\n")
        except Exception:
            pass

# =========================
# SETTINGS
# =========================
EXCHANGE_ID = "binanceusdm"   # ccxt id
TIMEFRAME = "5m"
TOP_N_SYMBOLS = 500

# Pipeline scheduling
DISCOVERY_EVERY_SEC = 180        # افتراضي 3 دقائق
SYMBOLS_REFRESH_EVERY_SEC = 3600 # تحديث ترتيب الرموز كل ساعة
LIVE_TOUCH_LOOP_SEC = 0.25       # حلقة لمس لحظي (WS فقط)

# Data window size (reduces REST load)
BASE_CANDLES_1H = 120  # كانت 300 -> ثقيلة جداً على الموبايل
MIN_BARS_FOR_INDICATOR = 120

# "On demand" base TF (1m/3m) when near a zone
USE_BASE_TF_ON_DEMAND = True
NEAR_ZONE_PCT = 0.35            # قرب السعر من المنطقة (%)
BASE_REFRESH_COOLDOWN_SEC = 300  # لا تحدّث base لنفس الرمز أكثر من كل 5 دقائق

# Touch controls
MIN_DISPLAY_SCORE = 45.0  # إشارات أقل لكنها أقوى
TOUCH_COOLDOWN_SEC = 60

# Alert System (Professional)
ALERT_FLUSH_SEC = 1.2       # كل كم ثانية نطبع تجميعة
ALERT_TOP_N = 8             # أعلى N إشارات في كل تجميعة
ALERT_FILE_JSONL = "alerts.jsonl"  # حفظ التنبيهات
STATE_FILE_JSON = "scanner_state.json"  # حفظ حالة اللمسات/الكولداون
ALERT_INCLUDE_ZONE_BOUNDS = True
STRICT_FIRST_TOUCH_SINCE_CREATION = True
TOUCH_LOOKBACK = 1

# Parallel / networking
PARALLEL_DISCOVERY = True
MAX_WORKERS = 5
MAX_CONCURRENT_REQUESTS = 2
RETRY_MAX = 4
BACKOFF_BASE_SEC = 1.5
BACKOFF_JITTER_SEC = 0.5

# =========================
# REALTIME PRICE FEED (WebSocket) - PYDROID FRIENDLY
# =========================
USE_WS_PRICE_FEED = True
WS_USE_FUTURES_STREAM = True
WS_RECONNECT_DELAY_SEC = 3

# miniTicker works fine; you can switch to bookTicker later if desired.
WS_URL_FUTURES = "wss://fstream.binance.com/ws/!miniTicker@arr"
WS_URL_SPOT    = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

LIVE_PRICE_CACHE: Dict[str, float] = {}
_WS_STOP = Event()
_WS_THREAD: Optional[threading.Thread] = None

def _ws_on_message(ws, message: str):
    try:
        data = json.loads(message)
        if isinstance(data, list):
            for it in data:
                s = it.get("s")
                c = it.get("c")
                if not s or c is None:
                    continue
                try:
                    LIVE_PRICE_CACHE[s] = float(c)
                except Exception:
                    pass
    except Exception:
        pass

def _ws_on_error(ws, error):
    log_msg(f"[WS] error: {error}")

def _ws_on_close(ws, code, msg):
    log_msg(f"[WS] closed: code={code} msg={msg}")

def _ws_price_feed_loop():
    url = WS_URL_FUTURES if WS_USE_FUTURES_STREAM else WS_URL_SPOT
    while not _WS_STOP.is_set():
        try:
            log_msg(f"[WS] connecting: {url}")
            ws = websocket.WebSocketApp(
                url,
                on_message=_ws_on_message,
                on_error=_ws_on_error,
                on_close=_ws_on_close,
            )
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except Exception as e:
            log_msg(f"[WS] exception: {e}")
        for _ in range(int(WS_RECONNECT_DELAY_SEC * 10)):
            if _WS_STOP.is_set():
                break
            time.sleep(0.1)

def start_ws_price_feed():
    global _WS_THREAD
    if not USE_WS_PRICE_FEED:
        return
    if _WS_THREAD and _WS_THREAD.is_alive():
        return
    _WS_STOP.clear()
    _WS_THREAD = threading.Thread(target=_ws_price_feed_loop, daemon=True)
    _WS_THREAD.start()
    log_msg("[WS] price feed started")

def stop_ws_price_feed():
    _WS_STOP.set()

def _sym_to_ws_key(sym: str) -> str:
    return sym.replace("/", "").replace(":USDT", "").replace(":USD", "").upper()

def get_live_price(sym: str, ex=None) -> Optional[float]:
    key = _sym_to_ws_key(sym)
    p = LIVE_PRICE_CACHE.get(key)
    if p is not None:
        return p
    if ex is not None:
        try:
            t = _api_call(lambda: ex.fetch_ticker(sym), what="fetch_ticker")
            return float(t.get("last"))
        except Exception:
            return None
    return None

# =========================
# MEME FILTER
# =========================
EXCLUDE_MEMECOINS = True
MEME_BASE_BLACKLIST = {
    'DOGE','SHIB','PEPE','FLOKI','BONK','WIF','MEME','BABYDOGE','SATS',
    'TURBO','PONKE','BOME','NEIRO','BRETT','MOG','POPCAT','MEW','SLERF',
    '1000SHIB','1000PEPE','1000FLOKI','1000BONK', 'LUNC', 'XEC'
}
MEME_KEYWORDS = ('DOGE','SHIB','PEPE','FLOKI','BONK','MEME','BABYDOGE','WIF','TURBO','PONKE','BOME','NEIRO','BRETT','MOG','POPCAT','MEW','SLERF')

def is_memecoin_symbol(sym: str) -> bool:
    if not EXCLUDE_MEMECOINS:
        return False
    base = sym.split("/")[0].split(":")[0].upper()
    if base in MEME_BASE_BLACKLIST:
        return True
    return any(k in sym.upper() for k in MEME_KEYWORDS)

# ============================================================
# UTILS
# ============================================================
def tf_to_seconds(tf: str) -> int:
    tf = tf.strip()
    if tf.endswith(("S", "s")): return int(tf[:-1])
    if tf.endswith("m"): return int(tf[:-1]) * 60
    if tf.endswith("h"): return int(tf[:-1]) * 3600
    if tf.endswith(("d", "D")): return int(tf[:-1]) * 86400
    if tf.isdigit(): return int(tf) * 60
    raise ValueError(f"Unsupported timeframe: {tf}")

def tv_lower_tf_string_from_chart_tf(chart_tf: str) -> str:
    sec = tf_to_seconds(chart_tf)
    time_min = sec / 60.0
    div = 10.0
    if time_min <= 1.0: return "5S"
    return str(int(math.ceil(time_min / div)))

def choose_base_intrabar_tf(ex, chart_tf: str) -> str:
    try:
        lower_tf_str = tv_lower_tf_string_from_chart_tf(chart_tf)
        if lower_tf_str.endswith("S"):
            return "1m"
        lower_min = int(lower_tf_str)
    except:
        return "1m"
    try:
        tfs = getattr(ex, "timeframes", None)
        supports_3m = (isinstance(tfs, dict) and ("3m" in tfs)) or (tfs is None)
    except Exception:
        supports_3m = True
    if supports_3m and (lower_min % 3 == 0):
        return "3m"
    return "1m"

def format_volume_tv_like(v: float) -> str:
    if pd.isna(v): return "na"
    av = abs(float(v))
    if av >= 1e9: return f"{v/1e9:.3f}B"
    if av >= 1e6: return f"{v/1e6:.3f}M"
    if av >= 1e3: return f"{v/1e3:.3f}K"
    return f"{v:.0f}"

# ============================================================
# INDICATOR LOGIC (unchanged)
# ============================================================
def ta_percentile_nearest_rank(series: np.ndarray, length: int, percentage: float, *, allow_partial_window: bool = True) -> np.ndarray:
    if length <= 0: raise ValueError("length must be > 0")
    s = pd.Series(series, dtype="float64")
    minp = 1 if allow_partial_window else length
    if float(percentage) == 100.0:
        return s.rolling(length, min_periods=minp).max().to_numpy(dtype=float)

    def _nearest_rank(window: np.ndarray) -> float:
        w = window[~np.isnan(window)]
        if w.size == 0: return float("nan")
        w.sort()
        k = int(math.ceil((float(percentage) / 100.0) * w.size))
        k = max(1, min(k, w.size))
        return float(w[k - 1])
    return s.rolling(length, min_periods=minp).apply(_nearest_rank, raw=True).to_numpy(dtype=float)

def run_indicator(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    df = df.copy()
    needed = ["open", "high", "low", "close", "volume", "sumBull", "sumBear", "totalVolume"]
    for c in needed:
        if c not in df.columns:
            return pd.DataFrame(), [], None

    n = len(df)
    o = df["open"].to_numpy(float)
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)

    sumBull = df["sumBull"].to_numpy(float)
    sumBear = df["sumBear"].to_numpy(float)
    totalVol = df["totalVolume"].to_numpy(float)

    percentile_len = int(params.get("percentile_len", 1000))
    filter_threshold = float(params.get("filter_threshold", 10))
    min_bar_index = int(params.get("min_bar_index", 100))
    max_fvgs = int(params.get("max_fvgs", 10))
    mitigationSrc = str(params.get("mitigationSrc", "close")).lower()
    bullGaps = bool(params.get("bullGaps", True))
    bearGaps = bool(params.get("bearGaps", True))
    volumeBars = bool(params.get("volumeBars", True))

    diff = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2: continue
        if pd.isna(c[i-1]) or pd.isna(o[i-1]): continue
        if c[i-1] > o[i-1]:
            if not pd.isna(l[i]) and not pd.isna(h[i-2]):
                diff[i] = (l[i] - h[i-2]) / l[i] * 100.0
        else:
            if not pd.isna(l[i-2]) and not pd.isna(h[i]):
                diff[i] = (l[i-2] - h[i]) / h[i] * 100.0

    p100 = ta_percentile_nearest_rank(diff, percentile_len, 100.0, allow_partial_window=bool(params.get('percentile_allow_partial_window', True)))

    sizeFVG = np.full(n, np.nan, float)
    filterFVG = np.full(n, np.nan, float)
    for i in range(n):
        if pd.isna(diff[i]) or pd.isna(p100[i]): continue
        if p100[i] == 0:
            continue
        sizeFVG[i] = diff[i] / p100[i] * 100.0
        filterFVG[i] = 1.0 if (sizeFVG[i] > filter_threshold) else 0.0

    isBull_gap = np.full(n, np.nan, float)
    isBear_gap = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2 or pd.isna(filterFVG[i]): continue
        f_ok = (filterFVG[i] == 1.0)
        bull_cond = (h[i-2] < l[i]) and (h[i-2] < h[i-1]) and (l[i-2] < l[i]) and f_ok
        bear_cond = (l[i-2] > h[i]) and (l[i-2] > l[i-1]) and (h[i-2] > h[i]) and f_ok
        isBull_gap[i] = 1.0 if bull_cond else 0.0
        isBear_gap[i] = 1.0 if bear_cond else 0.0

    active = []
    history = []
    next_id = 1
    created_id = np.full(n, np.nan, float)
    removed_count = np.zeros(n, float)
    active_count = np.zeros(n, float)

    def pct_int_or_nan(num, den):
        if pd.isna(num) or pd.isna(den) or den == 0: return float("nan")
        return float(int((num / den) * 100.0))

    for i in range(n):
        if not (i > min_bar_index):
            active_count[i] = len(active)
            continue

        prev_total = totalVol[i-1] if i >= 1 else np.nan
        prev_bull = sumBull[i-1] if i >= 1 else np.nan
        prev_bear = sumBear[i-1] if i >= 1 else np.nan
        bullPct = pct_int_or_nan(prev_bull, prev_total)
        bearPct = pct_int_or_nan(prev_bear, prev_total)
        totalV = float(prev_total) if not pd.isna(prev_total) else np.nan

        if isBull_gap[i] == 1.0 and bullGaps:
            body_top = float(l[i])
            body_bot = float(h[i-2])
            mid = (body_top + body_bot) / 2.0
            fvg = {
                "id": next_id, "score": float(sizeFVG[i]), "isBull": True, "bull": bullPct, "bear": bearPct, "totalVol": totalV,
                "created_at_bar": int(i), "removed_at_bar": None, "removed_reason": None,
                "body": {"left": i-1, "right": i+5, "top": body_top, "bottom": body_bot, "text": "", "deleted": False},
                "bullBar": {"left": i-1, "right": i-1, "top": mid, "bottom": body_bot, "text": "", "deleted": False},
                "bearBar": {"left": i-1, "right": i-1, "top": body_top, "bottom": mid, "text": "", "deleted": False},
            }
            active.append(fvg)
            history.append(fvg)
            created_id[i] = next_id
            next_id += 1

        if isBear_gap[i] == 1.0 and bearGaps:
            body_top = float(l[i-2])
            body_bot = float(h[i])
            mid = (body_top + body_bot) / 2.0
            fvg = {
                "id": next_id, "score": float(sizeFVG[i]), "isBull": False, "bull": bullPct, "bear": bearPct, "totalVol": totalV,
                "created_at_bar": int(i), "removed_at_bar": None, "removed_reason": None,
                "body": {"left": i-1, "right": i+5, "top": body_top, "bottom": body_bot, "text": "", "deleted": False},
                "bullBar": {"left": i-1, "right": i-1, "top": mid, "bottom": body_bot, "text": "", "deleted": False},
                "bearBar": {"left": i-1, "right": i-1, "top": body_top, "bottom": mid, "text": "", "deleted": False},
            }
            active.append(fvg)
            history.append(fvg)
            created_id[i] = next_id
            next_id += 1

        idx_loop = 0
        while idx_loop < len(active):
            fvg = active[idx_loop]
            body = fvg["body"]
            src1 = c[i] if mitigationSrc == "close" else l[i]
            src2 = c[i] if mitigationSrc == "close" else h[i]

            left = float(body["left"])
            right_old = float(body["right"])
            top = float(body["top"])
            bot = float(body["bottom"])

            removed = False
            if fvg["isBull"]:
                if (not pd.isna(src1)) and (src1 < bot):
                    removed = True
            else:
                if (not pd.isna(src2)) and (src2 > top):
                    removed = True

            if removed:
                body["deleted"] = True
                fvg["bullBar"]["deleted"] = True
                fvg["bearBar"]["deleted"] = True
                fvg["removed_at_bar"] = int(i)
                fvg["removed_reason"] = "mitigation"
                active.pop(idx_loop)
                removed_count[i] += 1.0
                continue

            body["right"] = int(i + 25)
            body["text"] = format_volume_tv_like(fvg.get("totalVol"))

            if volumeBars:
                mid = (top + bot) / 2.0
                width = int(right_old - left)
                size = width / 200.0
                bull_val = fvg.get("bull")
                bear_val = fvg.get("bear")

                if pd.isna(bull_val):
                    fvg["bullBar"]["right"] = int(left)
                    fvg["bullBar"]["text"] = "na%"
                else:
                    fvg["bullBar"]["right"] = int(left + size * float(bull_val))
                    fvg["bullBar"]["text"] = f"{int(float(bull_val))}%"

                if pd.isna(bear_val):
                    fvg["bearBar"]["right"] = int(left)
                    fvg["bearBar"]["text"] = "na%"
                else:
                    fvg["bearBar"]["right"] = int(left + size * float(bear_val))
                    fvg["bearBar"]["text"] = f"{int(float(bear_val))}%"

                fvg["bearBar"]["top"] = top
                fvg["bearBar"]["bottom"] = mid
                fvg["bullBar"]["top"] = mid
                fvg["bullBar"]["bottom"] = bot
            else:
                fvg["bullBar"]["text"] = ""
                fvg["bearBar"]["text"] = ""
            idx_loop += 1

        i_idx = 0
        while i_idx < len(active):
            fvg = active[i_idx]
            top = float(fvg["body"]["top"])
            bot = float(fvg["body"]["bottom"])
            j_idx = 0
            while j_idx < len(active):
                if i_idx == j_idx:
                    j_idx += 1
                    continue
                other = active[j_idx]
                top1 = float(other["body"]["top"])
                if (top1 < top) and (top1 > bot):
                    fvg["body"]["deleted"] = True
                    fvg["bullBar"]["deleted"] = True
                    fvg["bearBar"]["deleted"] = True
                    fvg["removed_at_bar"] = int(i)
                    fvg["removed_reason"] = "overlap"
                    active.pop(i_idx)
                    removed_count[i] += 1.0
                    break
                j_idx += 1
            i_idx += 1

        while len(active) > max_fvgs:
            old = active.pop(0)
            old["body"]["deleted"] = True
            old["bullBar"]["deleted"] = True
            old["bearBar"]["deleted"] = True
            old["removed_at_bar"] = int(i)
            old["removed_reason"] = "size"
            removed_count[i] += 1.0

        active_count[i] = float(len(active))

    dashboard_last = None
    if n > 0:
        bullCount = sum(1 for f in active if f["isBull"])
        bearCount = len(active) - bullCount
        dashboard_last = {
            "bullCount": bullCount,
            "bearCount": bearCount,
            "bullTotalVolume": sum(float(f["totalVol"]) for f in active if f["isBull"] and not pd.isna(f["totalVol"])),
            "bearTotalVolume": sum(float(f["totalVol"]) for f in active if not f["isBull"] and not pd.isna(f["totalVol"])),
            "active": active,
        }

    out = pd.DataFrame(index=df.index)
    out["created_id"] = created_id
    out["removed_count"] = removed_count
    return out, history, dashboard_last

# ============================================================
# VOLUME IMPORTANCE FILTER (unchanged)
# ============================================================
def filter_fvgs_by_volume_importance(active_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not active_list:
        return []
    valid_vols = [float(f["totalVol"]) for f in active_list if not pd.isna(f["totalVol"]) and f["totalVol"] > 0]
    if not valid_vols:
        return active_list
    median_vol = np.median(valid_vols)
    important = []
    for f in active_list:
        vol = float(f["totalVol"]) if not pd.isna(f["totalVol"]) else 0.0
        b_pct = f["bull"] if not pd.isna(f["bull"]) else 0.0
        s_pct = f["bear"] if not pd.isna(f["bear"]) else 0.0
        is_bull = f["isBull"]
        if is_bull and (b_pct < s_pct):
            continue
        if (not is_bull) and (s_pct < b_pct):
            continue
        if vol < (median_vol * 0.25):
            continue
        important.append(f)
    important.sort(key=lambda x: x.get("score", 0) * (float(x.get("totalVol", 0)) or 1), reverse=True)
    return important

# ============================================================
# NETWORKING / ccxt / anti-ban
# ============================================================
def _make_exchange():
    try:
        import ccxt
    except ImportError:
        log_msg("[CRITICAL] ccxt غير مثبت. ثبّت: pip install ccxt")
        return None
    ex_class = getattr(ccxt, EXCHANGE_ID)
    return ex_class({"enableRateLimit": True, "timeout": 20000, "options": {"defaultType": "future"}})

_THREAD_LOCAL = threading.local()
def _get_thread_exchange():
    ex = getattr(_THREAD_LOCAL, "ex", None)
    if ex is None:
        ex = _make_exchange()
        _THREAD_LOCAL.ex = ex
    return ex

# Cache on disk
CACHE_ENABLED = True
CACHE_COMPRESS = "gzip"
CACHE_DIR = os.path.join(os.path.dirname(__file__) if "__file__" in globals() else ".", "ohlcv_cache")
def _ensure_cache_dir():
    if not CACHE_ENABLED:
        return
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass

def _cache_path(sym, tf):
    _ensure_cache_dir()
    safe = sym.replace("/", "_").replace(":", "_").replace(" ", "_")
    return os.path.join(CACHE_DIR, f"{EXCHANGE_ID}_{safe}_{tf}.pkl.gz")

def _read_cache_df(sym, tf):
    if not CACHE_ENABLED:
        return pd.DataFrame()
    p = _cache_path(sym, tf)
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        return pd.read_pickle(p, compression=CACHE_COMPRESS)
    except Exception:
        try:
            return pd.read_pickle(p)
        except Exception:
            return pd.DataFrame()

def _write_cache_df(sym, tf, df):
    if not CACHE_ENABLED or df.empty:
        return
    try:
        df.to_pickle(_cache_path(sym, tf), compression=CACHE_COMPRESS)
    except Exception:
        pass

# Anti-ban + budget
_API_SEM = threading.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)
_BAN_LOCK = threading.Lock()
_BAN_UNTIL_MS = 0

_BUDGET_LOCK = threading.Lock()
# Smooth pacer: instead of big "sleep 50s", we spread calls evenly.
MAX_CALLS_PER_MIN = 36
_PACER_LAST_CALL = 0.0

def _budget_wait():
    global _PACER_LAST_CALL
    # Keep average rate under MAX_CALLS_PER_MIN by spacing calls.
    min_interval = 60.0 / float(MAX_CALLS_PER_MIN)
    with _BUDGET_LOCK:
        now = time.time()
        if _PACER_LAST_CALL <= 0.0:
            _PACER_LAST_CALL = now
            return
        dt = now - _PACER_LAST_CALL
        if dt < min_interval:
            time.sleep((min_interval - dt) + 0.05)  # small buffer
        _PACER_LAST_CALL = time.time()
def _sleep_until_ms(until_ms: int):
    now_ms = int(time.time() * 1000)
    if until_ms <= now_ms:
        return
    time.sleep((until_ms - now_ms) / 1000.0)

def _api_call(fn, what="api"):
    global _BAN_UNTIL_MS
    for attempt in range(RETRY_MAX):
        now_ms = int(time.time() * 1000)
        with _BAN_LOCK:
            ban_until = _BAN_UNTIL_MS
        if ban_until > now_ms:
            _sleep_until_ms(ban_until)

        _budget_wait()
        with _API_SEM:
            try:
                return fn()
            except Exception as e:
                msg = str(e)
                m = re.search(r"banned\s+until\s+(\d{10,})", msg)
                if m:
                    until = int(m.group(1)) + 2500
                    with _BAN_LOCK:
                        if until > _BAN_UNTIL_MS:
                            _BAN_UNTIL_MS = until
                    log_msg(f"[BAN] IP banned until {_BAN_UNTIL_MS}. Sleeping...")
                    _sleep_until_ms(_BAN_UNTIL_MS)
                    continue
                if attempt < RETRY_MAX - 1:
                    sleep_s = (BACKOFF_BASE_SEC * (2 ** attempt)) + (random.random() * BACKOFF_JITTER_SEC)
                    time.sleep(sleep_s)
                    continue
                raise
    raise RuntimeError(f"API failed: {what}")

def safe_fetch_ohlcv(ex, symbol, timeframe, since=None, limit=None):
    return _api_call(lambda: ex.fetch_ohlcv(symbol, timeframe, since=since, limit=limit), what="fetch_ohlcv")

def fetch_ohlcv_paged(ex, symbol, timeframe, since_ms, limit, max_bars):
    rows = []
    next_since = since_ms
    safety = 0
    while len(rows) < max_bars and safety < 80:
        safety += 1
        batch = safe_fetch_ohlcv(ex, symbol, timeframe, since=next_since, limit=limit)
        if not batch:
            break
        rows.extend(batch)
        next_since = batch[-1][0] + 1
        if len(batch) < limit:
            break
    cols = ["time", "open", "high", "low", "close", "volume"]
    if not rows:
        return pd.DataFrame(columns=cols)
    rows = rows[-max_bars:]
    df = pd.DataFrame(rows, columns=cols)
    df["time"] = pd.to_datetime(df["time"].astype("int64"), unit="ms", utc=True)
    return df

def fetch_ohlcv_range_cached(ex, symbol, timeframe, start_ms, end_ms, max_bars=20000):
    sec = tf_to_seconds(timeframe)
    bar_ms = int(sec * 1000)
    df = _read_cache_df(symbol, timeframe)

    need_full = df.empty
    if not df.empty:
        t0 = int(df["time"].iloc[0].value // 10**6)
        if t0 > start_ms + bar_ms:
            need_full = True

    if need_full:
        df_new = fetch_ohlcv_paged(ex, symbol, timeframe, since_ms=start_ms, limit=1500, max_bars=max_bars)
        if not df_new.empty:
            df = df_new

    if not df.empty:
        t_last = int(df["time"].iloc[-1].value // 10**6)
        if t_last + bar_ms < end_ms:
            df_more = fetch_ohlcv_paged(ex, symbol, timeframe, since_ms=t_last + bar_ms, limit=1500, max_bars=max_bars)
            if not df_more.empty:
                df = pd.concat([df, df_more], ignore_index=True)
                df = df.sort_values("time").drop_duplicates("time", keep="last")

    if df.empty:
        return df

    ms = (df["time"].view("int64") // 10**6).to_numpy()
    mask = (ms >= start_ms) & (ms < end_ms)
    df_slice = df.loc[mask].copy()
    _write_cache_df(symbol, timeframe, df)
    return df_slice

# ============================================================
# Attach volume sums
# ============================================================
def attach_volume_sums_from_high(df_high: pd.DataFrame) -> pd.DataFrame:
    df = df_high.copy()
    o = df["open"].to_numpy(float)
    c = df["close"].to_numpy(float)
    v = df["volume"].to_numpy(float)
    bull = np.where(c > o, v, 0.0)
    bear = np.where(c < o, v, 0.0)
    df["sumBull"] = bull
    df["sumBear"] = bear
    df["totalVolume"] = bull + bear
    return df

def attach_lower_tf_volume_sums_1h(df_high, df_1m, chart_tf):
    if df_1m.empty or df_high.empty:
        return df_high
    df_high = df_high.copy()
    high_sec = tf_to_seconds(chart_tf)
    tf_str = tv_lower_tf_string_from_chart_tf(chart_tf)
    lower_sec = tf_to_seconds(tf_str)
    if lower_sec < 60:
        lower_sec = 60

    base = df_1m.sort_values("time").set_index("time")
    rule = f"{int(lower_sec/60)}T"
    try:
        lowtf = base.resample(rule, label="left", closed="left").agg({
            "open":"first","high":"max","low":"min","close":"last","volume":"sum"
        }).dropna()
    except Exception:
        return attach_volume_sums_from_high(df_high)

    lowtf["bull_v"] = np.where(lowtf["close"] > lowtf["open"], lowtf["volume"], 0.0)
    lowtf["bear_v"] = np.where(lowtf["close"] < lowtf["open"], lowtf["volume"], 0.0)

    high_ns = df_high["time"].view("int64").to_numpy()
    low_ns = lowtf.index.view("int64")
    idx = np.searchsorted(high_ns, low_ns, side="right") - 1
    high_window_ns = np.int64(high_sec) * np.int64(1_000_000_000)

    valid = (idx >= 0) & (idx < len(high_ns))
    valid = valid & (low_ns < (high_ns[idx] + high_window_ns))

    lowtf = lowtf.loc[valid].copy()
    lowtf["bucket"] = idx[valid]
    grp = lowtf.groupby("bucket", sort=False)[["bull_v", "bear_v"]].sum()

    sumBull = np.zeros(len(df_high), float)
    sumBear = np.zeros(len(df_high), float)
    indices = grp.index.to_numpy()
    sumBull[indices] = grp["bull_v"].to_numpy()
    sumBear[indices] = grp["bear_v"].to_numpy()

    df_high["sumBull"] = sumBull
    df_high["sumBear"] = sumBear
    df_high["totalVolume"] = sumBull + sumBear
    return df_high

# ============================================================
# Symbol ranking
# ============================================================
def get_symbols_ranked(ex) -> List[str]:
    vols: Dict[str, float] = {}
    try:
        if hasattr(ex, "fapiPublicGetTicker24hr"):
            data = _api_call(lambda: ex.fapiPublicGetTicker24hr(), what="fapiPublicGetTicker24hr")
            for it in data:
                sid = it.get("symbol")
                if not sid:
                    continue
                try:
                    vols[sid] = float(it.get("quoteVolume", 0) or 0)
                except Exception:
                    pass
    except Exception as e:
        log_msg(f"[WARN] 24h tickers failed: {e}")

    markets = _api_call(lambda: ex.load_markets(), what="load_markets")
    syms: List[Tuple[str, float]] = []
    for s, m in markets.items():
        if m.get("active") and m.get("swap") and m.get("linear") and m.get("quote") == "USDT":
            sid = m.get("id")
            v = vols.get(sid, 0.0)
            if v == 0.0:
                v = vols.get(_sym_to_ws_key(s), 0.0)
            syms.append((s, float(v)))
    syms.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in syms]

# ============================================================
# Strategy params
# ============================================================
PARAMS = {
    "mitigationSrc": "close",
    "bullGaps": True,
    "bearGaps": True,
    "volumeBars": True,
    "percentile_len": 1000,
    "percentile_allow_partial_window": True,
    "filter_threshold": 10,
    "max_fvgs": 10,
    "min_bar_index": 100,
}

# ============================================================
# PIPELINE STATE
# ============================================================
ZONES: Dict[str, List[Dict[str, Any]]] = {}
ZONES_LOCK = threading.Lock()
LAST_TOUCH_TS: Dict[Tuple[str, int], float] = {}
LAST_BASE_REFRESH: Dict[str, float] = {}
TOUCHED_SYMBOLS: set = set()

RANKED_SYMBOLS: List[str] = []
LAST_SYMBOLS_REFRESH_TS = 0.0


# ============================================================
# State Persistence + Alert Aggregation
# ============================================================
def _safe_json_load(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _safe_json_save(path: str, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
    except Exception:
        pass

def _append_jsonl(path: str, obj):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass

def load_state_from_disk():
    global LAST_TOUCH_TS, TOUCHED_SYMBOLS
    st = _safe_json_load(STATE_FILE_JSON, {})
    # LAST_TOUCH_TS stored as list of [sym, zid, ts]
    ltt = st.get("last_touch", [])
    out = {}
    try:
        for it in ltt:
            if not isinstance(it, list) or len(it) != 3:
                continue
            sym, zid, ts = it
            out[(str(sym), int(zid))] = float(ts)
    except Exception:
        out = {}
    LAST_TOUCH_TS = out
    touched = st.get("touched_symbols", [])
    try:
        TOUCHED_SYMBOLS = set(map(str, touched))
    except Exception:
        TOUCHED_SYMBOLS = set()

def save_state_to_disk():
    # Convert tuple keys to list for JSON
    ltt = []
    try:
        for (sym, zid), ts in LAST_TOUCH_TS.items():
            ltt.append([sym, int(zid), float(ts)])
    except Exception:
        ltt = []
    st = {
        "build": BUILD_ID,
        "timeframe": TIMEFRAME,
        "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
        "last_touch": ltt,
        "touched_symbols": sorted(list(TOUCHED_SYMBOLS))[:5000],
    }
    _safe_json_save(STATE_FILE_JSON, st)

class AlertManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._buf = {}   # key=(sym,zid) -> event
        self._last_flush = time.time()

    def push(self, event: Dict[str, Any]):
        # event must include sym,zid,score,price
        key = (event.get("symbol"), int(event.get("zone_id", 0)))
        with self._lock:
            prev = self._buf.get(key)
            if prev is None or float(event.get("score", 0.0)) > float(prev.get("score", 0.0)):
                self._buf[key] = event

    def maybe_flush(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_flush) < ALERT_FLUSH_SEC:
            return
        with self._lock:
            events = list(self._buf.values())
            self._buf.clear()
        self._last_flush = now
        if not events:
            return

        # Sort by score desc
        events.sort(key=lambda e: float(e.get("score", 0.0)), reverse=True)
        top = events[: max(1, int(ALERT_TOP_N))]

        # Print one grouped block
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        log_msg(f"=== ALERTS ({len(top)}/{len(events)}) @ {ts} ===")
        for e in top:
            sym = e.get("symbol")
            price = e.get("price")
            score = e.get("score")
            if ALERT_INCLUDE_ZONE_BOUNDS and ("top" in e) and ("bot" in e):
                log_msg(f"🔥 {sym} | Price={price} | Score={score:.1f} | Zone=[{e['bot']}..{e['top']}]")
            else:
                log_msg(f"🔥 {sym} | Price={price} | Score={score:.1f}")

            # Persist alert
            rec = {
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
                "symbol": sym,
                "price": price,
                "score": float(score),
                "zone_id": int(e.get("zone_id", 0)),
                "zone_top": e.get("top"),
                "zone_bot": e.get("bot"),
                "timeframe": TIMEFRAME,
                "build": BUILD_ID,
            }
            _append_jsonl(ALERT_FILE_JSONL, rec)

        # Save state occasionally
        try:
            save_state_to_disk()
        except Exception:
            pass

ALERTER = AlertManager()
# ============================================================
# Discovery per symbol
# ============================================================
def _compute_zones_for_symbol(sym: str) -> Tuple[str, List[Dict[str, Any]], str]:
    try:
        ex = _get_thread_exchange()
        if ex is None:
            return sym, [], "No exchange"

        high_sec = tf_to_seconds(TIMEFRAME)
        num_candles = max(MIN_BARS_FOR_INDICATOR, int(math.ceil((BASE_CANDLES_1H * 3600) / high_sec)))

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(num_candles * high_sec * 1000)
        end_ms = now_ms + high_sec * 1000

        df_high = fetch_ohlcv_range_cached(ex, sym, TIMEFRAME, start_ms, end_ms, max_bars=num_candles + 50)
        if df_high is None or df_high.empty or len(df_high) < MIN_BARS_FOR_INDICATOR:
            return sym, [], "Not enough high-tf data"

        df_hi2 = attach_volume_sums_from_high(df_high)

        out, hist, dash = run_indicator(df_hi2, PARAMS)
        if not dash or not dash.get("active"):
            return sym, [], "No active FVGs"

        active = filter_fvgs_by_volume_importance(dash["active"])
        zones = []
        for f in active:
            try:
                top_p = float(f["body"]["top"])
                bot_p = float(f["body"]["bottom"])
                sc = float(f.get("score", 0.0))
                if sc < MIN_DISPLAY_SCORE:
                    continue
                zid = int(f.get("id", 0)) or int(f.get("created_at_bar", 0))
                zones.append({
                    "id": zid,
                    "top": max(top_p, bot_p),
                    "bot": min(top_p, bot_p),
                    "score": sc,
                    "isBull": bool(f.get("isBull", True)),
                    "created_at_bar": int(f.get("created_at_bar", 0)),
                })
            except Exception:
                continue

        return sym, zones, f"zones={len(zones)}"
    except Exception as e:
        return sym, [], f"Error: {e}"

def discovery_update(symbols: List[str], batch_name: str):
    t0 = time.time()
    log_msg(f"--- Discovery START ({batch_name}) | symbols={len(symbols)} ---")
    zoned_symbols = 0
    if PARALLEL_DISCOVERY:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futs = {pool.submit(_compute_zones_for_symbol, s): s for s in symbols}
            done = 0
            for fut in as_completed(futs):
                done += 1
                s = futs[fut]
                try:
                    sym, zones, status = fut.result()
                    with ZONES_LOCK:
                        ZONES[sym] = zones
                    if zones:
                        zoned_symbols += 1
                    if DIAGNOSTIC_MODE and done % PROGRESS_EVERY == 0:
                        log_msg(f"[DISCOVERY] {done}/{len(symbols)} done...")
                except Exception as e:
                    if DIAGNOSTIC_MODE:
                        log_msg(f"[DISCOVERY-ERR] {s}: {e}")
    else:
        for i, s in enumerate(symbols, 1):
            sym, zones, status = _compute_zones_for_symbol(s)
            with ZONES_LOCK:
                ZONES[sym] = zones
            if zones:
                zoned_symbols += 1
            if DIAGNOSTIC_MODE and (i % PROGRESS_EVERY == 0):
                log_msg(f"[DISCOVERY] {i}/{len(symbols)} done...")

    log_msg(f"--- Discovery END ({batch_name}) | zones_symbols={zoned_symbols} | took={time.time()-t0:.1f}s ---")

# ============================================================
# Base TF refresh (on demand)
# ============================================================
def maybe_refresh_base_for_symbol(sym: str):
    if not USE_BASE_TF_ON_DEMAND:
        return
    now = time.time()
    last = LAST_BASE_REFRESH.get(sym, 0.0)
    if (now - last) < BASE_REFRESH_COOLDOWN_SEC:
        return

    ex = _make_exchange()
    if ex is None:
        return
    base_tf = choose_base_intrabar_tf(ex, TIMEFRAME)
    high_sec = tf_to_seconds(TIMEFRAME)
    base_sec = tf_to_seconds(base_tf)

    num_candles = max(MIN_BARS_FOR_INDICATOR, int(math.ceil((BASE_CANDLES_1H * 3600) / high_sec)))
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - int(num_candles * high_sec * 1000)
    end_ms = now_ms + high_sec * 1000

    try:
        df_high = fetch_ohlcv_range_cached(ex, sym, TIMEFRAME, start_ms, end_ms, max_bars=num_candles + 50)
        if df_high.empty:
            return
        df_base = fetch_ohlcv_range_cached(ex, sym, base_tf, start_ms, end_ms,
                                          max_bars=int(num_candles * (high_sec/base_sec)) + 400)
        if df_base.empty:
            return

        df_hi2 = attach_lower_tf_volume_sums_1h(df_high, df_base, TIMEFRAME)
        out, hist, dash = run_indicator(df_hi2, PARAMS)
        if not dash or not dash.get("active"):
            with ZONES_LOCK:
                ZONES[sym] = []
            LAST_BASE_REFRESH[sym] = now
            return

        active = filter_fvgs_by_volume_importance(dash["active"])
        zones = []
        for f in active:
            try:
                top_p = float(f["body"]["top"])
                bot_p = float(f["body"]["bottom"])
                sc = float(f.get("score", 0.0))
                if sc < MIN_DISPLAY_SCORE:
                    continue
                zid = int(f.get("id", 0)) or int(f.get("created_at_bar", 0))
                zones.append({
                    "id": zid,
                    "top": max(top_p, bot_p),
                    "bot": min(top_p, bot_p),
                    "score": sc,
                    "isBull": bool(f.get("isBull", True)),
                    "created_at_bar": int(f.get("created_at_bar", 0)),
                })
            except Exception:
                continue

        with ZONES_LOCK:
            ZONES[sym] = zones
        LAST_BASE_REFRESH[sym] = now
        log_msg(f"[BASE-REFRESH] {sym} -> zones={len(zones)} (base={base_tf})")
    except Exception:
        return

# ============================================================
# Live touch loop (WS only)
# ============================================================
def _near_pct(price: float, top: float, bot: float) -> float:
    mid = (top + bot) / 2.0
    if mid == 0:
        return 999.0
    return abs(price - mid) / mid * 100.0

def live_touch_check_once():
    with ZONES_LOCK:
        items = list(ZONES.items())

    if not items:
        return

    now = time.time()
    touched_now = 0
    near_candidates: List[str] = []

    for sym, zones in items:
        if not zones:
            continue
        p = get_live_price(sym, ex=None)
        if p is None:
            continue

        for z in zones:
            top = float(z["top"])
            bot = float(z["bot"])
            zid = int(z["id"])
            if bot <= p <= top:
                key = (sym, zid)
                last = LAST_TOUCH_TS.get(key, 0.0)
                if (now - last) < TOUCH_COOLDOWN_SEC:
                    continue
                LAST_TOUCH_TS[key] = now
                TOUCHED_SYMBOLS.add(sym)
                touched_now += 1
                log_msg(f"🔥 {sym} [LIVE TOUCH | Price={p:.6g} | Score={z.get('score',0):.1f}]")
            else:
                if USE_BASE_TF_ON_DEMAND:
                    if _near_pct(p, top, bot) <= NEAR_ZONE_PCT:
                        near_candidates.append(sym)

    if USE_BASE_TF_ON_DEMAND and near_candidates:
        uniq = []
        seen = set()
        for s in near_candidates:
            if s not in seen:
                seen.add(s)
                uniq.append(s)
        for s in uniq[:3]:
            maybe_refresh_base_for_symbol(s)

# ============================================================
# Main pipeline
# ============================================================
def main():
    log_msg("=== Scanner START (PIPELINE MODE) ===")
    log_msg(f"TIMEFRAME={TIMEFRAME} | TOP_N={TOP_N_SYMBOLS} | WORKERS={MAX_WORKERS} | CONC={MAX_CONCURRENT_REQUESTS}")
    log_msg(f"DISCOVERY_EVERY={DISCOVERY_EVERY_SEC}s | LIVE_LOOP={LIVE_TOUCH_LOOP_SEC}s | BASE_ON_DEMAND={USE_BASE_TF_ON_DEMAND}")

    start_ws_price_feed()

    # Load persisted touch state
    load_state_from_disk()

    global RANKED_SYMBOLS, LAST_SYMBOLS_REFRESH_TS
    ex = _make_exchange()
    if ex is None:
        return

    RANKED_SYMBOLS = [s for s in get_symbols_ranked(ex) if not is_memecoin_symbol(s)]
    RANKED_SYMBOLS = RANKED_SYMBOLS[:TOP_N_SYMBOLS]
    LAST_SYMBOLS_REFRESH_TS = time.time()
    log_msg(f"[SYMBOLS] loaded ranked symbols={len(RANKED_SYMBOLS)} (memes filtered)")

    discovery_update(RANKED_SYMBOLS[:200], batch_name="initial-200")

    rot_idx = 0
    ROTATE_BATCH = 160
    last_discovery = time.time()

    while True:
        live_touch_check_once()
        ALERTER.maybe_flush()
        time.sleep(LIVE_TOUCH_LOOP_SEC)

        now = time.time()

        if (now - LAST_SYMBOLS_REFRESH_TS) >= SYMBOLS_REFRESH_EVERY_SEC:
            try:
                ex2 = _make_exchange()
                if ex2:
                    RANKED_SYMBOLS = [s for s in get_symbols_ranked(ex2) if not is_memecoin_symbol(s)]
                    RANKED_SYMBOLS = RANKED_SYMBOLS[:TOP_N_SYMBOLS]
                    LAST_SYMBOLS_REFRESH_TS = now
                    log_msg(f"[SYMBOLS] refreshed ranked list={len(RANKED_SYMBOLS)}")
            except Exception:
                pass

        if (now - last_discovery) >= DISCOVERY_EVERY_SEC:
            last_discovery = now
            if not RANKED_SYMBOLS:
                continue

            with ZONES_LOCK:
                zoned = [s for s, z in ZONES.items() if z]

            batch = []
            batch.extend(zoned[: max(20, ROTATE_BATCH // 3)])

            if rot_idx >= len(RANKED_SYMBOLS):
                rot_idx = 0
            end = min(len(RANKED_SYMBOLS), rot_idx + ROTATE_BATCH)
            batch.extend(RANKED_SYMBOLS[rot_idx:end])
            rot_idx = end

            uniq = []
            seen = set()
            for s in batch:
                if s not in seen:
                    seen.add(s)
                    uniq.append(s)
            uniq = uniq[:ROTATE_BATCH]

            discovery_update(uniq, batch_name="rotate")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_msg("[STOP] User stopped.")
        try:
            ALERTER.maybe_flush(force=True)
            save_state_to_disk()
        except Exception:
            pass
        stop_ws_price_feed()
