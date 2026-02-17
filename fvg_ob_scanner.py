#!/usr/bin/env python3
"""FVG + Order-Block scanner for Binance USDT-M futures.

The implementation is designed to be robust for long-running scans:
- Wilder ATR implementation close to Pine's ``ta.atr`` semantics.
- Gap/box/signal logic equivalent to the provided Pine-like pseudo-code.
- Defensive handling for NaN windows to avoid noisy RuntimeWarning messages.
- Configurable CLI interface for easy operations.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - optional dependency for live scan
    ccxt = None  # type: ignore

import numpy as np


DEFAULT_CONFIG: Dict[str, object] = {
    "timeframe": "15m",
    "lookback_candles": 2000,
    "atr_length": 200,
    "gap_filter_pct": 0.5,
    "box_amount": 6,
    "show_broken": False,
    "show_signal": False,
    "signal_age_bars": 0,
    "scan_interval_sec": 30,
    "exclude_last_unconfirmed": True,
    "rate_limit": True,
    "atr_warmup_extra": 500,
}


@dataclass
class BoxLevel:
    kind: str  # "bull" or "bear"
    top: float
    bottom: float
    created_index: int
    created_ts: int
    strength_pct: float


@dataclass
class Signal:
    symbol: str
    kind: str  # "bull_break" or "bear_break"
    ts: int
    price: float
    info: str


def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    """Return Wilder ATR with Pine-like bootstrap behavior."""
    n = len(close)
    atr = np.full(n, np.nan, dtype=float)

    if n < length + 1:
        return atr

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]

    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    atr[length] = np.mean(tr[1 : length + 1])
    for i in range(length + 1, n):
        atr[i] = ((atr[i - 1] * (length - 1)) + tr[i]) / length

    return atr


def _safe_nanmax(values: np.ndarray) -> float:
    """Equivalent to np.nanmax but returns np.nan for all-NaN windows without warning."""
    if values.size == 0:
        return np.nan
    finite_mask = ~np.isnan(values)
    if not np.any(finite_mask):
        return np.nan
    return float(np.max(values[finite_mask]))


def prune_overlaps_inplace_like_pine(levels: List[BoxLevel]) -> None:
    """Delete overlapped boxes using Pine-like in-place loop semantics."""
    i = 0
    while i < len(levels):
        box = levels[i]
        remove_current = False

        for j, other in enumerate(levels):
            if j == i:
                continue
            if (other.top < box.top) and (other.top > box.bottom):
                remove_current = True
                break

        if remove_current:
            levels.pop(i)
        else:
            i += 1


def _process_bull_boxes_like_pine(
    levels: List[BoxLevel],
    i: int,
    high: np.ndarray,
    low: np.ndarray,
    is_bull_gap: bool,
    show_broken: bool,
    show_signal: bool,
    symbol: str,
    ts: np.ndarray,
    signals: List[Signal],
) -> None:
    """Replicate the Pine `for box_id in boxes1` block as closely as possible."""
    idx = 0
    while idx < len(levels):
        box = levels[idx]
        deleted = False

        if high[i] < box.bottom:
            if not show_broken:
                levels.pop(idx)
                deleted = True

        if not deleted and show_signal and i >= 1:
            if (low[i] > box.top) and (low[i - 1] <= box.top) and (not is_bull_gap):
                signals.append(
                    Signal(
                        symbol=symbol,
                        kind="bull_break",
                        ts=int(ts[i - 1]),
                        price=float(low[i - 1]),
                        info=f"Break above Bull OB top={box.top:.6f} (gap={box.strength_pct:.3f}%)",
                    )
                )

        if not deleted:
            # Nested overlap loop from Pine:
            # if top1 < top and top1 > bottom => delete current box
            remove_current = False
            for other in levels:
                if (other.top < box.top) and (other.top > box.bottom):
                    remove_current = True
                    break
            if remove_current:
                levels.pop(idx)
                deleted = True

        if not deleted:
            idx += 1


def _process_bear_boxes_like_pine(
    levels: List[BoxLevel],
    i: int,
    high: np.ndarray,
    low: np.ndarray,
    is_bear_gap: bool,
    show_broken: bool,
    show_signal: bool,
    symbol: str,
    ts: np.ndarray,
    signals: List[Signal],
) -> None:
    """Replicate the Pine `for box_id in boxes2` block as closely as possible."""
    idx = 0
    while idx < len(levels):
        box = levels[idx]
        deleted = False

        if low[i] > box.top:
            if not show_broken:
                levels.pop(idx)
                deleted = True

        if not deleted and show_signal and i >= 1:
            if (high[i] < box.bottom) and (high[i - 1] >= box.bottom) and (not is_bear_gap):
                signals.append(
                    Signal(
                        symbol=symbol,
                        kind="bear_break",
                        ts=int(ts[i - 1]),
                        price=float(high[i - 1]),
                        info=f"Break below Bear OB bottom={box.bottom:.6f} (gap={box.strength_pct:.3f}%)",
                    )
                )

        if not deleted:
            remove_current = False
            for other in levels:
                if (other.top < box.top) and (other.top > box.bottom):
                    remove_current = True
                    break
            if remove_current:
                levels.pop(idx)
                deleted = True

        if not deleted:
            idx += 1


def analyze_symbol(symbol: str, ohlcv: List[List[float]], cfg: Dict[str, object]) -> List[Signal]:
    arr = np.array(ohlcv, dtype=float)
    ts = arr[:, 0].astype(np.int64)
    high = arr[:, 2]
    low = arr[:, 3]
    close = arr[:, 4]

    if bool(cfg["exclude_last_unconfirmed"]) and len(ts) > 5:
        ts = ts[:-1]
        high = high[:-1]
        low = low[:-1]
        close = close[:-1]

    n = len(ts)
    atr_length = int(cfg["atr_length"])
    if n < max(atr_length + 5, 10):
        return []

    atr = wilder_atr(high, low, close, atr_length)

    filt_up = np.full(n, np.nan, dtype=float)
    filt_dn = np.full(n, np.nan, dtype=float)
    for i in range(2, n):
        if low[i] != 0:
            filt_up[i] = (low[i] - high[i - 2]) / low[i] * 100.0
        if low[i - 2] != 0:
            filt_dn[i] = (low[i - 2] - high[i]) / low[i - 2] * 100.0

    # kept for Pine-compatibility and future telemetry
    max_up_series = np.full(n, np.nan, dtype=float)
    max_dn_series = np.full(n, np.nan, dtype=float)
    win = min(200, n)
    for i in range(n):
        start = max(0, i - win + 1)
        max_up_series[i] = _safe_nanmax(filt_up[start : i + 1])
        max_dn_series[i] = _safe_nanmax(filt_dn[start : i + 1])

    boxes_bull: List[BoxLevel] = []
    boxes_bear: List[BoxLevel] = []
    signals: List[Signal] = []

    lookback = min(int(cfg["lookback_candles"]), n)
    start_i = max(0, n - lookback)
    gap_filter_pct = float(cfg["gap_filter_pct"])

    for i in range(start_i, n):
        if i < 2 or np.isnan(atr[i]):
            continue

        is_bull_gap = (
            (high[i - 2] < low[i])
            and (high[i - 2] < high[i - 1])
            and (low[i - 2] < low[i])
            and (filt_up[i] > gap_filter_pct)
        )
        is_bear_gap = (
            (low[i - 2] > high[i])
            and (low[i - 2] > low[i - 1])
            and (high[i - 2] > high[i])
            and (filt_dn[i] > gap_filter_pct)
        )

        if is_bull_gap:
            top = high[i - 2]
            bottom = top - atr[i]
            boxes_bull.append(
                BoxLevel("bull", top, bottom, i, int(ts[i]), float(filt_up[i]))
            )

        if is_bear_gap:
            bottom = low[i - 2]
            top = bottom + atr[i]
            boxes_bear.append(
                BoxLevel("bear", top, bottom, i, int(ts[i]), float(filt_dn[i]))
            )

        _process_bull_boxes_like_pine(
            levels=boxes_bull,
            i=i,
            high=high,
            low=low,
            is_bull_gap=is_bull_gap,
            show_broken=bool(cfg["show_broken"]),
            show_signal=bool(cfg["show_signal"]),
            symbol=symbol,
            ts=ts,
            signals=signals,
        )
        _process_bear_boxes_like_pine(
            levels=boxes_bear,
            i=i,
            high=high,
            low=low,
            is_bear_gap=is_bear_gap,
            show_broken=bool(cfg["show_broken"]),
            show_signal=bool(cfg["show_signal"]),
            symbol=symbol,
            ts=ts,
            signals=signals,
        )

        # Pine uses `if boxes.size() >= box_amount: shift()`.
        box_amount = int(cfg["box_amount"])
        if box_amount > 0:
            while len(boxes_bull) >= box_amount:
                boxes_bull.pop(0)
            while len(boxes_bear) >= box_amount:
                boxes_bear.pop(0)

    age_bars = int(cfg["signal_age_bars"])
    if age_bars > 0:
        cutoff_index = max(0, n - 1 - age_bars)
        ts_to_index = {int(ts[idx]): idx for idx in range(n)}
        signals = [s for s in signals if ts_to_index.get(s.ts, -1) >= cutoff_index]

    return signals


def make_exchange(rate_limit: bool = True):
    if ccxt is None:
        raise RuntimeError("ccxt is required for live scanning. Install with: pip install ccxt")
    return ccxt.binance(
        {
            "enableRateLimit": rate_limit,
            "options": {"defaultType": "future"},
        }
    )


def get_usdt_perp_symbols(exchange) -> List[str]:
    markets = exchange.load_markets()
    symbols = [
        sym
        for sym, meta in markets.items()
        if meta.get("swap") and meta.get("quote") == "USDT" and meta.get("active", True)
    ]
    symbols.sort()
    return symbols


def fetch_ohlcv_safe(
    exchange, symbol: str, timeframe: str, limit: int
) -> Optional[List[List[float]]]:
    try:
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception:
        return None


def fmt_ts(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000))


def run_scan(config: Dict[str, object], once: bool = False) -> None:
    ex = make_exchange(rate_limit=bool(config["rate_limit"]))
    symbols = get_usdt_perp_symbols(ex)

    print(f"Loaded {len(symbols)} USDT-M perpetual symbols.")
    print(
        f"Timeframe={config['timeframe']}, lookback={config['lookback_candles']}, "
        f"filter={config['gap_filter_pct']}%"
    )
    print("Scanning... (Ctrl+C to stop)\n")

    while True:
        all_signals: List[Signal] = []
        start_time = time.time()

        for sym in symbols:
            fetch_limit = int(config["lookback_candles"]) + int(config.get("atr_warmup_extra", 0))
            ohlcv = fetch_ohlcv_safe(ex, sym, str(config["timeframe"]), fetch_limit)
            if not ohlcv or len(ohlcv) < 10:
                continue

            sigs = analyze_symbol(sym, ohlcv, config)
            if sigs:
                all_signals.extend(sigs)

        all_signals.sort(key=lambda s: s.ts, reverse=True)

        print("=" * 80)
        print(
            f"Scan finished in {time.time() - start_time:.1f}s | "
            f"Signals found: {len(all_signals)} | {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        if not all_signals:
            print("No signals.")
        else:
            for s in all_signals[:50]:
                print(f"[{fmt_ts(s.ts)}] {s.symbol:18s} {s.kind:10s} price={s.price:.6f} | {s.info}")

        print("=" * 80 + "\n")
        if once:
            break
        time.sleep(float(config["scan_interval_sec"]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FVG + OB signal scanner for Binance futures.")
    parser.add_argument("--timeframe", default=DEFAULT_CONFIG["timeframe"])
    parser.add_argument("--lookback", type=int, default=DEFAULT_CONFIG["lookback_candles"])
    parser.add_argument("--interval", type=float, default=DEFAULT_CONFIG["scan_interval_sec"])
    parser.add_argument("--once", action="store_true", help="Run one scan iteration then exit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = dict(DEFAULT_CONFIG)
    config["timeframe"] = args.timeframe
    config["lookback_candles"] = args.lookback
    config["scan_interval_sec"] = args.interval
    run_scan(config, once=args.once)


if __name__ == "__main__":
    main()
