#!/usr/bin/env python3
"""Executable port of the Smart Money Algo Pro E5 core logic.

This module translates the TradingView Pine Script indicator "Smart Money Algo
Pro E5 - CHADBULL" into Python with 1:1 naming for the packages explicitly
requested by the user: Pullback, Market Structure, Order Block (including Zone
Type and SCOB), Order Flow, Candle, and Structure utilities (PDH/PDL/MID/OTE).

The implementation mirrors the Pine Script execution order so the resulting
state, labels, boxes, and lines match the behaviour of the original indicator
when fed with the same OHLCV series.  Features that belong to other
packages in the Pine file are intentionally omitted, per the latest user
instruction.

The script can be invoked directly or through ``main()`` which handles command
line arguments and optional Binance scanning output to the console. The code is
organised to expose the intermediate state so tests can validate structural
parity against the Pine logic.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import bisect
import dataclasses
import datetime
import json
import math
import os
import re
import sys
import time
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ccxt = None  # type: ignore

try:
    import requests  # type: ignore
    from requests.adapters import HTTPAdapter  # type: ignore
    from urllib3.util.retry import Retry  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    requests = None  # type: ignore
    HTTPAdapter = None  # type: ignore
    Retry = None  # type: ignore


# ----------------------------------------------------------------------------
# Pine compatibility helpers
# ----------------------------------------------------------------------------


NA = float("nan")


def _coerce_float(value: Any, *, default: float = NA) -> float:
    """Safely coerce OHLCV fields to float.

    Some exchanges/symbols may return empty strings or None (e.g., volume="").
    This helper prevents crashes like: ``could not convert string to float: ''``.

    Args:
        value: The value to coerce.
        default: Value returned when coercion fails.

    Returns:
        float: Coerced float or ``default``.
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        if s.lower() in {"nan", "none", "null"}:
            return default
        try:
            return float(s)
        except Exception:
            return default
    try:
        return float(value)
    except Exception:
        return default



# ----------------------------------------------------------------------------
# Persistent Order Block (OB) touch/retest de-duplication (ported from 3d updated)
# ----------------------------------------------------------------------------

_OB_CACHE_DIR = Path(__file__).resolve().parent
_OB_TOUCHED_CACHE_PATH = _OB_CACHE_DIR / "ob_touched_cache.json"
_OB_RETEST_CACHE_PATH = _OB_CACHE_DIR / "ob_retested_cache.json"

def _load_json_set(path: Path) -> set[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {str(x) for x in data}
    except Exception:
        pass
    return set()

def _save_json_set(path: Path, data: set[str]) -> None:
    try:
        path.write_text(json.dumps(sorted(data), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # Never crash due to cache IO
        pass

def _fmt_price_key(v: float) -> str:
    try:
        # Stable-ish representation across runs (avoid float noise)
        return f"{float(v):.10f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)

def _ob_event_key(symbol: str, ob_kind: str, bottom: float, top: float, created_time: int) -> str:
    return f"{symbol}|{ob_kind}|{int(created_time)}|{_fmt_price_key(bottom)}|{_fmt_price_key(top)}"

_OB_TOUCHED_SEEN: set[str] = _load_json_set(_OB_TOUCHED_CACHE_PATH)
_OB_RETEST_SEEN: set[str] = _load_json_set(_OB_RETEST_CACHE_PATH)



ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_DIM = "\033[2m"
ANSI_LABEL = "\033[94m"
ANSI_VALUE_POS = "\033[92m"
ANSI_VALUE_ZERO = "\033[93m"
ANSI_VALUE_NEG = "\033[91m"
ANSI_SYMBOL = ANSI_VALUE_POS
ANSI_ALERT_BULL = ANSI_VALUE_POS
ANSI_ALERT_BEAR = ANSI_VALUE_NEG

ALERT_BULLISH_KEYWORDS = (
    "bull",
    "bullish",
    "long",
    "buy",
    "up",
    "صاعد",
    "صعود",
    "صاعدة",
    "ارتفاع",
    "شراء",
)

ALERT_BEARISH_KEYWORDS = (
    "bear",
    "bearish",
    "short",
    "sell",
    "down",
    "هابط",
    "هابطة",
    "هبوط",
    "انخفاض",
    "بيع",
)
ANSI_HEADER_COLORS = [
    "\033[95m",
    "\033[96m",
    "\033[92m",
    "\033[93m",
    "\033[94m",
]


# -----------------------------------------------------------------------------
# Telegram Settings (عدّل هذه القيم من أعلى الملف)
# -----------------------------------------------------------------------------
# لتفعيل إرسال المخرجات إلى تلجرام:
TELEGRAM_ENABLE = True

# ضع توكن البوت (Bot Token) هنا
TELEGRAM_BOT_TOKEN = "8099231216:AAHxdhqThOi93Q0wesPByfUgIqvyvXMW2b4"  # مثال: "123456789:AA..."

# ضع رقم المحادثة/القناة (Chat ID) هنا
TELEGRAM_CHAT_ID = "119055277"    # مثال: "123456789" أو "-1001234567890"

# إرسال كل مخرجات شاشة المحرر (stdout/stderr) إلى تلجرام بشكل تلقائي
TELEGRAM_MIRROR_STDOUT = True

# إعدادات إضافية
TELEGRAM_PARSE_MODE = ""          # اتركها فارغة للرسائل العادية، أو "HTML" إذا تحتاج
TELEGRAM_DISABLE_WEB_PREVIEW = True
TELEGRAM_MIRROR_FLUSH_SECONDS = 2.0
TELEGRAM_MIRROR_MAX_LINES = 25
TELEGRAM_MAX_MESSAGE_CHARS = 3800  # أقل من 4096 لتجنب رفض تيليجرام

# -----------------------------------------------------------------------------
# Scanner & Event Settings (عدّل هذه القيم من أعلى الملف)
# -----------------------------------------------------------------------------
SCANNER_TIMEFRAME = "15m"
SCANNER_LOOKBACK = 500
SCANNER_CONCURRENCY = 1
SCANNER_FAST_SCAN = True
SCANNER_CONTINUOUS = False
SCANNER_INTERVAL = 2.0
SCANNER_MIN_DAILY_CHANGE = 0.0

# فلتر عمر الأحداث (بعدد الشموع)
EVENT_PRINT_ENABLED = True
EVENT_PRINT_MAX_AGE_BARS = 5

# تفعيل/تعطيل طباعة كل حدث جديد (مفاتيح الطباعة)
# إذا كان المفتاح False فلن يتم طباعته حتى لو كان موجوداً في EVENT_PRINT_KEYS.
EVENT_PRINT_TOGGLES = {
    "BOS": True,
    "BOS_PLUS": True,
    "FUTURE_BOS": True,
    "CHOCH": True,
    "FUTURE_CHOCH": True,
    "MSS": True,
    "MSS_PLUS": True,
    "IDM": True,
    "ORDER_BLOCK": True,
    "BREAKER_BLOCK": True,
    "MITIGATION_BLOCK": True,
    "PROPULSION_BLOCK": True,
    "IDM_OB": True,
    "HIST_IDM_OB": False,
    "EXT_OB": True,
    "HIST_EXT_OB": False,
    "DEMAND_ZONE": False,
    "SUPPLY_ZONE": False,
    "ORDER_FLOW_BREAK_MAJOR": False,
    "ORDER_FLOW_BREAK_MINOR": False,
    "ORDER_FLOW_MAJOR": False,
    "ORDER_FLOW_MINOR": False,
    "SCOB": True,
    "SCOB_BULLISH": True,
    "SCOB_BEARISH": True,
    "INSIDE_BAR": True,
    "INSIDE_BAR_CANDLE": True,
    "FVG": True,
    "LIQUIDITY_LEVELS": True,
    "LIQUIDITY_TOUCH": True,
    "GOLDEN_ZONE": True,
    "GOLDEN_ZONE_TOUCH": True,
    "PDH": True,
    "PDL": True,
    "EQUILIBRIUM": True,
    "SWING_SWEEP": True,
    "X": True,
    "KEY_LEVEL_4H": True,
    "KEY_LEVEL_DAILY": True,
    "KEY_LEVEL_WEEKLY": True,
    "GREEN_CIRCLE": True,
    "RED_CIRCLE": True,
}

# طباعة ملامسة Golden zone لأول مرة فقط
GOLDEN_ZONE_TOUCH_ONCE = True

# -----------------------------------------------------------------------------
# Feature Toggles (تشغيل/إيقاف منطق الكشف)
# -----------------------------------------------------------------------------
# هذه المفاتيح تشغّل منطق الكشف نفسه (وليس فقط الطباعة).
# عدّلها حسب رغبتك. القيم الافتراضية هنا مطابقة لإعداداتك التي ذكرتها.
FEATURE_TOGGLES = {
    # Market Structure
    "BOS_PLUS": False,
    "BOS": False,
    "CHOCH": False,
    "MSS": True,
    "MSS_PLUS": True,

    # FVG / Liquidity
    "FVG": True,
    "LIQUIDITY_LEVEL": True,   # يدعم أيضًا المفتاح القديم "LIQUIDITY_LEVELS"

    # Structure utilities
    "PDH": False,
    "PDL": False,
    "EQUILIBRIUM": False,

    # Sweep / Mark X
    "SWING_SWEEP": False,
    "X": False,

    # Key levels
    "KEY_LEVEL_4H":False,
    "KEY_LEVEL_DAILY": False,
    "KEY_LEVEL_WEEKLY": False,
}

def apply_feature_toggles(inputs: "IndicatorInputs", toggles: Dict[str, bool]) -> None:
    """Map user-friendly feature toggles to internal IndicatorInputs flags.

    Why:
      - Some modules are OFF by default (e.g. ict_structure.showms, liquidity.currentTF, fvg.show_fvg)
      - Users prefer enabling logic using a single switches dict (FEATURE_TOGGLES)
    """

    def _on(key: str) -> bool:
        return bool(toggles.get(key, False))

    # 1) FVG
    if hasattr(inputs, "fvg") and hasattr(inputs.fvg, "show_fvg"):
        inputs.fvg.show_fvg = _on("FVG")

    # 2) Liquidity levels (current timeframe)
    liq_enabled = _on("LIQUIDITY_LEVEL") or _on("LIQUIDITY_LEVELS")
    if hasattr(inputs, "liquidity") and hasattr(inputs.liquidity, "currentTF"):
        inputs.liquidity.currentTF = liq_enabled

    # 3) ICT Market Structure (BOS/MSS/CHOCH)
    ms_enabled = any(_on(k) for k in ("BOS", "BOS_PLUS", "MSS", "MSS_PLUS", "CHOCH"))
    if hasattr(inputs, "ict_structure"):
        if hasattr(inputs.ict_structure, "showms"):
            inputs.ict_structure.showms = ms_enabled
        # Ensure external/internal are available when enabled
        if ms_enabled and hasattr(inputs.ict_structure, "ms_type") and not getattr(inputs.ict_structure, "ms_type", ""):
            inputs.ict_structure.ms_type = "All"

    # 4) Structure utilities (PDH/PDL/Equilibrium/Sweeps/Mark X)
    if hasattr(inputs, "structure_util"):
        su = inputs.structure_util
        if hasattr(su, "showPdh"):
            su.showPdh = _on("PDH")
        if hasattr(su, "showPdl"):
            su.showPdl = _on("PDL")
        if hasattr(su, "showMid"):
            su.showMid = _on("EQUILIBRIUM")
        if hasattr(su, "showSw"):
            su.showSw = _on("SWING_SWEEP")
        if hasattr(su, "markX"):
            su.markX = _on("X")

    # 5) Key Levels
    if hasattr(inputs, "key_levels"):
        kl = inputs.key_levels
        if hasattr(kl, "Show_4H_Levels"):
            kl.Show_4H_Levels = _on("KEY_LEVEL_4H")
        if hasattr(kl, "Show_Daily_Levels"):
            kl.Show_Daily_Levels = _on("KEY_LEVEL_DAILY")
        if hasattr(kl, "Show_Weekly_Levels"):
            kl.Show_Weekly_Levels = _on("KEY_LEVEL_WEEKLY")

EVENT_PRINT_KEYS = {
    "BOS",
    "BOS_PLUS",
    "FUTURE_BOS",
    "CHOCH",
    "FUTURE_CHOCH",
    "MSS",
    "MSS_PLUS",
    "IDM",
    "ORDER_BLOCK",
    "BREAKER_BLOCK",
    "MITIGATION_BLOCK",
    "PROPULSION_BLOCK",
    "IDM_OB",
    "HIST_IDM_OB",
    "EXT_OB",
    "HIST_EXT_OB",
    "DEMAND_ZONE",
    "SUPPLY_ZONE",
    "ORDER_FLOW_BREAK_MAJOR",
    "ORDER_FLOW_BREAK_MINOR",
    "ORDER_FLOW_MAJOR",
    "ORDER_FLOW_MINOR",
    "SCOB",
    "SCOB_BULLISH",
    "SCOB_BEARISH",
    "INSIDE_BAR",
    "INSIDE_BAR_CANDLE",
    "FVG",
    "LIQUIDITY_LEVELS",
    "LIQUIDITY_TOUCH",
    "GOLDEN_ZONE",
    "GOLDEN_ZONE_TOUCH",
    "PDH",
    "PDL",
    "EQUILIBRIUM",
    "SWING_SWEEP",
    "X",
    "KEY_LEVEL_4H",
    "KEY_LEVEL_DAILY",
    "KEY_LEVEL_WEEKLY",
    "GREEN_CIRCLE",
    "RED_CIRCLE",
}

EVENT_PRINT_LABELS = {
    "BOS": "BOS",
    "BOS_PLUS": "BOS+",
    "FUTURE_BOS": "BOS (ملامسة ليبل مستقبلي)",
    "CHOCH": "CHoCH",
    "FUTURE_CHOCH": "CHoCH (ملامسة ليبل مستقبلي)",
    "MSS": "MSS",
    "MSS_PLUS": "MSS+",
    "IDM": "IDM",
    "ORDER_BLOCK": "Order Block",
    "BREAKER_BLOCK": "Breaker Block",
    "MITIGATION_BLOCK": "Mitigation Block",
    "PROPULSION_BLOCK": "Propulsion Block",
    "IDM_OB": "IDM OB",
    "HIST_IDM_OB": "Hist IDM OB",
    "EXT_OB": "EXT OB",
    "HIST_EXT_OB": "Hist EXT OB",
    "DEMAND_ZONE": "Demand Zone",
    "SUPPLY_ZONE": "Supply Zone",
    "ORDER_FLOW_BREAK_MAJOR": "Order Flow Break (Major)",
    "ORDER_FLOW_BREAK_MINOR": "Order Flow Break (Minor)",
    "ORDER_FLOW_MAJOR": "Major OF",
    "ORDER_FLOW_MINOR": "Minor OF",
    "SCOB": "SCOB",
    "SCOB_BULLISH": "Bullish SCOB",
    "SCOB_BEARISH": "Bearish SCOB",
    "INSIDE_BAR": "Inside Bar",
    "INSIDE_BAR_CANDLE": "Inside Bar Candle",
    "FVG": "FVG",
    "LIQUIDITY_LEVELS": "Liquidity Levels",
    "LIQUIDITY_TOUCH": "Liquidity Sweep",
    "GOLDEN_ZONE": "Golden zone",
    "GOLDEN_ZONE_TOUCH": "Golden zone (Touch)",
    "PDH": "PDH",
    "PDL": "PDL",
    "EQUILIBRIUM": "Equilibrium (0.5)",
    "SWING_SWEEP": "Swing Sweep",
    "X": 'Mark "X',
    "KEY_LEVEL_4H": "Key Levels 4H",
    "KEY_LEVEL_DAILY": "Key Levels Daily",
    "KEY_LEVEL_WEEKLY": "Key Levels Weekly",
    "GREEN_CIRCLE": "الدوائر الخضراء",
    "RED_CIRCLE": "الدوائر الحمراء",
}


@dataclass(frozen=True)
class _EditorAutorunDefaults:
    timeframe: str = SCANNER_TIMEFRAME
    candle_limit: int = 500
    max_symbols: int = 600
    recent_bars: int = 2
    concurrency: int = SCANNER_CONCURRENCY
    fast_scan: bool = SCANNER_FAST_SCAN
    continuous_scan: bool = SCANNER_CONTINUOUS
    scan_interval: float = SCANNER_INTERVAL


EDITOR_AUTORUN_DEFAULTS = _EditorAutorunDefaults()

# (اختياري) تصدير الإعدادات كمتغيرات بيئة حتى تعمل دون سطر أوامر
# ملاحظة: لا نطبع التوكن أو الـ chat_id حفاظًا على الخصوصية.
try:
    if TELEGRAM_BOT_TOKEN:
        os.environ.setdefault("8099231216:AAHxdhqThOi93Q0wesPByfUgIqvyvXMW2b4", TELEGRAM_BOT_TOKEN)
    if TELEGRAM_CHAT_ID:
        os.environ.setdefault("119055277", TELEGRAM_CHAT_ID)
except Exception:
    pass



@dataclass(frozen=True)
class BinanceSymbolSelectorConfig:
    """User-facing switches controlling Binance symbol prioritisation."""

    # ``فلتر الارتفاع`` configuration lives here so users can adjust the
    # prioritisation thresholds without hunting through the selector logic.
    # Leave the numeric fields ``None`` to disable any implicit defaults—the
    # user supplies the preferred threshold, timeframe scope, and candle
    # window explicitly when they wish to enable the filter.

    prioritize_top_gainers: bool = True
    top_gainer_metric: str = "percentage"  # {percentage, pricechange, lastprice}
    top_gainer_threshold: Optional[float] = 5
    top_gainer_scope: Optional[str] = "1d"
    top_gainer_candle_window: Optional[int] = 1

DEFAULT_BINANCE_SYMBOL_SELECTOR = BinanceSymbolSelectorConfig(
    prioritize_top_gainers=False
)


def _normalize_direction(value: Any) -> Optional[str]:
    if isinstance(value, str):
        token = value.strip().lower()
        if not token:
            return None
        if token in {
            "bull",
            "bullish",
            "up",
            "long",
            "buy",
            "صاعد",
            "صعود",
            "صاعدة",
            "ارتفاع",
            "شراء",
        }:
            return "bullish"
        if token in {
            "bear",
            "bearish",
            "down",
            "short",
            "sell",
            "هابط",
            "هبوط",
            "هابطة",
            "انخفاض",
            "بيع",
        }:
            return "bearish"
    return None


def _infer_direction_from_text(text: Optional[str]) -> Optional[str]:
    if not isinstance(text, str):
        return None
    lowered = text.lower()
    if any(keyword in lowered for keyword in ALERT_BEARISH_KEYWORDS):
        return "bearish"
    if any(keyword in lowered for keyword in ALERT_BULLISH_KEYWORDS):
        return "bullish"
    return None


def _resolve_direction(*values: Any) -> Optional[str]:
    for value in values:
        direction = _normalize_direction(value)
        if direction:
            return direction
        if isinstance(value, str):
            inferred = _infer_direction_from_text(value)
            if inferred:
                return inferred
    return None


def _color_for_direction(direction: Optional[str], *, fallback: Optional[str] = None) -> Optional[str]:
    norm = _normalize_direction(direction) if direction else None
    if norm == "bullish":
        return ANSI_ALERT_BULL
    if norm == "bearish":
        return ANSI_ALERT_BEAR
    return fallback


def _colorize_directional_text(
    text: Any,
    *,
    direction: Optional[str] = None,
    fallback: Optional[str] = None,
) -> str:
    base = str(text) if text is not None else ""
    resolved = _resolve_direction(direction, base)
    color = _color_for_direction(resolved, fallback=fallback)
    if color:
        return f"{color}{base}{ANSI_RESET}"
    return base


def _format_symbol(symbol: str) -> str:
    return f"{ANSI_SYMBOL}{symbol}{ANSI_RESET}"


@dataclass
class TraceEvent:
    """Single trace entry capturing the runtime decision path."""

    index: int
    timestamp: Optional[int]
    section: str
    message: str
    payload: Dict[str, Any]


@dataclass
class ConditionSpec:
    """Definition of a Pine condition mirrored in Python."""

    name: str
    pine_expression: str


@dataclass
class ConditionEvaluation:
    """Evaluation record for a Pine condition."""

    spec: ConditionSpec
    result: bool
    timestamp: Optional[int]


@dataclass
class TraceComparisonResult:
    """Outcome of comparing runtime trace events with a reference log."""

    matches: bool
    reference_events: int
    current_events: int
    mismatches: List[Dict[str, Any]] = field(default_factory=list)


class ExecutionTracer:
    """Collector that mirrors the Pine execution order for audit purposes."""

    def __init__(self, enabled: bool = False, outfile: Optional[Path] = None) -> None:
        self.enabled = enabled
        self.outfile = outfile
        self._events: List[TraceEvent] = []
        self.comparison: Optional[TraceComparisonResult] = None

    def log(self, section: str, message: str, *, timestamp: Optional[int], **payload: Any) -> None:
        if not self.enabled:
            return
        event = TraceEvent(len(self._events), timestamp, section, message, payload)
        self._events.append(event)

    def emit(self) -> None:
        if not self.enabled or not self.outfile:
            return
        serialised = [
            {
                "index": event.index,
                "timestamp": event.timestamp,
                "section": event.section,
                "message": event.message,
                "payload": _serialize_scalar(event.payload),
            }
            for event in self._events
        ]
        self.outfile.write_text(json.dumps(serialised, indent=2, ensure_ascii=False))

    def clear(self) -> None:
        self._events.clear()

    def compare(self, reference: Path) -> TraceComparisonResult:
        """Compare collected events against a reference JSON trace."""

        if not reference.exists():
            raise FileNotFoundError(f"لم يتم العثور على ملف التتبع المرجعي: {reference}")

        raw_reference = json.loads(reference.read_text())
        ref_events: List[Dict[str, Any]] = []
        for idx, entry in enumerate(raw_reference):
            if not isinstance(entry, dict):
                raise ValueError("صيغة ملف التتبع المرجعي غير صحيحة")
            ref_events.append(self._normalise_reference_event(entry, idx))

        mismatches: List[Dict[str, Any]] = []
        upper = max(len(ref_events), len(self._events))
        for idx in range(upper):
            if idx >= len(ref_events):
                mismatches.append(
                    {
                        "index": idx,
                        "type": "extra_event",
                        "current": self._event_snapshot(self._events[idx]),
                    }
                )
                continue
            if idx >= len(self._events):
                mismatches.append(
                    {
                        "index": idx,
                        "type": "missing_event",
                        "reference": ref_events[idx],
                    }
                )
                continue
            reference_event = ref_events[idx]
            current_event = self._event_snapshot(self._events[idx])
            if reference_event != current_event:
                mismatches.append(
                    {
                        "index": idx,
                        "type": "mismatch",
                        "reference": reference_event,
                        "current": current_event,
                    }
                )

        result = TraceComparisonResult(
            matches=not mismatches and len(ref_events) == len(self._events),
            reference_events=len(ref_events),
            current_events=len(self._events),
            mismatches=mismatches,
        )
        self.comparison = result
        return result

    @staticmethod
    def _normalise_reference_event(entry: Dict[str, Any], index: int) -> Dict[str, Any]:
        payload = entry.get("payload", {})
        if not isinstance(payload, dict):
            payload = {"value": payload}
        return {
            "section": entry.get("section", ""),
            "message": entry.get("message", ""),
            "timestamp": entry.get("timestamp"),
            "payload": _serialize_scalar(payload),
            "index": entry.get("index", index),
        }

    @staticmethod
    def _event_snapshot(event: TraceEvent) -> Dict[str, Any]:
        return {
            "section": event.section,
            "message": event.message,
            "timestamp": event.timestamp,
            "payload": _serialize_scalar(event.payload),
            "index": event.index,
        }


def is_na(value: Any) -> bool:
    """Return True if ``value`` represents Pine ``na``."""

    if isinstance(value, float):
        return math.isnan(value)
    return value is None


def pine_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return value is not None


def _serialize_scalar(value: Any) -> Any:
    """Convert Pine values into JSON-friendly representations."""

    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return round(value, 10)
    if isinstance(value, (list, tuple)):
        return [_serialize_scalar(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize_scalar(v) for k, v in value.items()}
    return value


def _serialize_label(label: "Label") -> Dict[str, Any]:
    return {
        "x": label.x,
        "y": _serialize_scalar(label.y),
        "text": label.text,
        "xloc": label.xloc,
        "yloc": label.yloc,
        "color": label.color,
        "style": label.style,
        "size": label.size,
        "textcolor": label.textcolor,
        "tooltip": label.tooltip,
    }


def _serialize_line(line: "Line") -> Dict[str, Any]:
    return {k: _serialize_scalar(v) for k, v in dataclasses.asdict(line).items()}


def _serialize_box(box: "Box") -> Dict[str, Any]:
    return {k: _serialize_scalar(v) for k, v in dataclasses.asdict(box).items()}


def pine_avg(a: float, b: float) -> float:
    return (a + b) / 2.0


def pine_abs(v: float) -> float:
    return abs(v)


def pine_max(a: float, b: float) -> float:
    return max(a, b)


def pine_min(a: float, b: float) -> float:
    return min(a, b)


def format_price(value: Optional[float]) -> str:
    if value is None:
        return "NaN"
    if isinstance(value, (int, float)):
        value = float(value)
        if math.isnan(value):
            return "NaN"
        return (f"{value:.6f}").rstrip("0").rstrip(".")
    return str(value)


def format_timestamp(value: Optional[Union[int, float]]) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    timestamp = int(value)
    if timestamp <= 0:
        return "—"
    dt = datetime.datetime.fromtimestamp(timestamp / 1000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


class PineArray:
    """List wrapper exposing Pine array helpers."""

    def __init__(self, values: Optional[Iterable[Any]] = None) -> None:
        self.values: List[Any] = list(values) if values is not None else []

    # Pine API --------------------------------------------------------------
    def push(self, value: Any) -> None:
        self.values.append(value)

    def unshift(self, value: Any) -> None:
        self.values.insert(0, value)

    def pop(self) -> Any:
        return self.values.pop()

    def get(self, index: int) -> Any:
        return self.values[index]

    def set(self, index: int, value: Any) -> None:
        self.values[index] = value

    def remove(self, index: int) -> Any:
        return self.values.pop(index)

    def clear(self) -> None:
        self.values.clear()

    def size(self) -> int:
        return len(self.values)

    def indexof(self, value: Any) -> int:
        try:
            return self.values.index(value)
        except ValueError:
            return -1

    # Python conveniences ---------------------------------------------------
    def __len__(self) -> int:  # pragma: no cover - alias
        return len(self.values)

    def __iter__(self):  # pragma: no cover - alias
        return iter(self.values)


@dataclass
class ModuleStateMirror:
    """Container mirroring Pine ``var``/``array`` state for a module."""

    scalars: Dict[str, Any] = field(default_factory=dict)
    arrays: Dict[str, PineArray] = field(default_factory=dict)


@dataclass
class PullbackStateMirror(ModuleStateMirror):
    pass


@dataclass
class MarketStructureStateMirror(ModuleStateMirror):
    pass


@dataclass
class SwingStateMirror(ModuleStateMirror):
    pass


@dataclass
class OrderBlockStateMirror(ModuleStateMirror):
    pass


@dataclass
class SCOBStateMirror(ModuleStateMirror):
    pass


@dataclass
class Label:
    x: int
    y: float
    text: str
    xloc: str
    yloc: str
    color: str
    style: str
    size: str
    textcolor: str
    tooltip: Optional[str] = None

    def set_xy(self, x: int, y: float) -> None:
        self.x = x
        self.y = y

    def set_text(self, value: str) -> None:
        self.text = value

    def set_color(self, value: str) -> None:
        self.color = value

    def set_size(self, value: str) -> None:
        self.size = value

    def set_textcolor(self, value: str) -> None:
        self.textcolor = value

    def set_xloc(self, x: int, xloc: str) -> None:
        self.x = x
        self.xloc = xloc


@dataclass
class Line:
    x1: int
    y1: float
    x2: int
    y2: float
    xloc: str
    color: str
    style: str
    extend: str = "extend.none"
    width: int = 1

    def set_color(self, value: str) -> None:
        self.color = value

    def set_x1(self, value: int) -> None:
        self.x1 = value

    def set_y1(self, value: float) -> None:
        self.y1 = value

    def set_y2(self, value: float) -> None:
        self.y2 = value

    def set_xy1(self, x: int, y: float) -> None:
        self.x1 = x
        self.y1 = y

    def set_xy2(self, x: int, y: float) -> None:
        self.x2 = x
        self.y2 = y

    def set_x2(self, value: int) -> None:
        self.x2 = value

    def set_extend(self, value: str) -> None:
        self.extend = value

    def set_style(self, value: str) -> None:
        self.style = value

    def set_width(self, value: int) -> None:
        self.width = value

    def get_y1(self) -> float:
        return self.y1

    def get_y2(self) -> float:
        return self.y2


@dataclass
class Box:
    left: int
    right: int
    top: float
    bottom: float
    bgcolor: str
    border_color: str
    text: str = ""
    text_color: str = "#000000"
    text_halign: str = "text.align_center"
    text_size: str = "size.auto"
    extend: str = "extend.none"
    border_width: int = 1
    text_valign: str = "text.align_center"
    border_style: str = "line.style_solid"

    def set_right(self, value: int) -> None:
        self.right = value

    def set_bgcolor(self, value: str) -> None:
        self.bgcolor = value

    def set_border_color(self, value: str) -> None:
        self.border_color = value

    def set_text(self, value: str) -> None:
        self.text = value

    def set_text_color(self, value: str) -> None:
        self.text_color = value

    def set_extend(self, value: str) -> None:
        self.extend = value

    def set_lefttop(self, left: int, top: float) -> None:
        self.left = left
        self.top = top

    def set_rightbottom(self, right: int, bottom: float) -> None:
        self.right = right
        self.bottom = bottom

    def set_border_width(self, value: int) -> None:
        self.border_width = value

    def set_border_style(self, value: str) -> None:
        self.border_style = value

    def set_text_halign(self, value: str) -> None:
        self.text_halign = value

    def set_text_valign(self, value: str) -> None:
        self.text_valign = value

    def set_text_size(self, value: str) -> None:
        self.text_size = value

    def set_top(self, value: float) -> None:
        self.top = value

    def set_bottom(self, value: float) -> None:
        self.bottom = value

    def get_top(self) -> float:
        return self.top

    def get_bottom(self) -> float:
        return self.bottom

    def get_left(self) -> int:
        return self.left

    def get_right(self) -> int:
        return self.right


# ----------------------------------------------------------------------------
# Indicator inputs (1:1 with Pine defaults for targeted packages)
# ----------------------------------------------------------------------------


@dataclass
class PullbackInputs:
    showHL: bool = False
    colorHL: str = "#000000"
    showMn: bool = False


@dataclass
class MarketStructureInputs:
    showSMC: bool = True
    lengSMC: int = 40
    colorIDM: str = "color.rgb(0, 0, 0, 20)"
    structure_type: str = "Choch with IDM"
    showCircleHL: bool = True
    bull: str = "color.green"
    bear: str = "color.red"


@dataclass
class OrderBlockInputs:
    extndBox: bool = True
    showExob: bool = True
    showIdmob: bool = True
    showBrkob: bool = True
    txtsiz: str = "size.auto"
    clrtxtextbullbg: str = "color.rgb(76, 175, 79, 86)"
    clrtxtextbearbg: str = "color.rgb(255, 82, 82, 83)"
    clrtxtextbulliembg: str = "color.rgb(76, 175, 79, 86)"
    clrtxtextbeariembg: str = "color.rgb(255, 82, 82, 86)"
    clrtxtextbull: str = "color.green"
    clrtxtextbear: str = "color.red"
    clrtxtextbulliem: str = "color.green"
    clrtxtextbeariem: str = "color.red"
    showPOI: bool = True
    poi_type: str = "Mother Bar"
    colorSupply: str = "#cd5c4800"
    colorDemand: str = "#2f825f00"
    colorMitigated: str = "#c0c0c000"
    showSCOB: bool = True
    scobUp: str = "#0b3ff9"
    scobDn: str = "#da781d"


@dataclass
class DemandSupplyInputs:
    show_order_blocks: bool = False
    ibull_ob_css: str = "#5f6b5d19"
    ibear_ob_css: str = "#ef3a3a19"
    ob_type__: str = "All"
    i_tf_ob: str = ""
    mittigation_filt: str = "wick"
    overlapping_filt: bool = True
    max_obs: int = 8
    length_extend_ob: int = 20
    ob_extend: bool = False
    text_size_ob_: str = "size.normal"
    ob_text_color_1: str = "color.new(#787b86, 0)"
    volume_text: bool = False
    percent_text: bool = False
    show_line_ob_1: bool = False
    line_style_ob_1: str = "line.style_solid"
    show_order_blocks_mtf: bool = False
    ibull_ob_css_2: str = "color.new(#5d606b, 25)"
    ibear_ob_css_2: str = "color.new(#5d606b, 25)"
    ob_type__mtf: str = "All"
    i_tf_ob_mtf: str = "240"
    mittigation_filt_mtf: str = "Wicks"
    overlapping_filt_mtf: bool = True
    max_obs_mtf: int = 4
    length_extend_ob_mtf: int = 20
    ob_extend_mtf: bool = False
    text_size_ob_2: str = "size.small"
    ob_text_color_2: str = "color.new(#787b86, 0)"
    volume_text_2: bool = False
    percent_text_2: bool = False
    show_line_ob_2: bool = False
    line_style_ob_2: str = "line.style_solid"
    v_buy: str = "#00dbff4d"
    v_sell: str = "#e91e634d"
    ob_showlast: int = 5
    iob_showlast: int = 5
    max_width_ob: float = 3.0
    style: str = "Colored"
    v_lookback: int = 10
    ob_loockback: int = 10


@dataclass
class FVGInputs:
    show_fvg: bool = False
    i_tf: str = ""
    i_mtf: str = "HTF"
    i_bullishfvgcolor: str = "color.new(color.green,100)"
    i_bearishfvgcolor: str = "color.new(color.green,90)"
    remove_small: bool = True
    mittigation_filt_fvg: str = "Touch"
    fvg_color_fill: bool = True
    fvg_shade_fill: bool = False
    max_fvg: int = 8
    length_extend: int = 20
    fvg_extend: bool = False
    fvg_extend_B: bool = True
    i_fillByMid: bool = True
    i_deleteonfill: bool = True
    i_textColor: str = "color.white"
    i_tfos: int = 10
    i_mtfos: int = 50
    max_width_fvg: float = 1.5
    i_mtfbearishfvgcolor: str = "color.yellow"
    i_mtfbullishfvgcolor: str = "color.yellow"
    mid_style: str = "Solid"
    i_midPointColor: str = "color.rgb(249, 250, 253, 99)"


@dataclass
class LiquidityInputs:
    currentTF: bool = False
    displayLimit: int = 20
    lowLineColorHTF: str = "#00bbf94d"
    highLineColorHTF: str = "#e91e624d"
    htfTF: str = ""
    _candleType: str = "Close"
    leftBars: int = 20
    mitiOptions: str = "Remove"
    length_extend_liq: int = 20
    extentionMax: bool = False
    _highLineStyleHTF: str = "Solid"
    box_width: float = 2.5
    lineWidthHTF: int = 2
    liquidity_text_color: str = "color.black"
    highBoxBorderColorHTF: str = "color.new(#e91e624d,90)"
    lowBoxBorderColorHTF: str = "color.new(#00bbf94d,90)"
    displayStyle_liq: str = "Boxes"


@dataclass
class OrderFlowInputs:
    showMajoinMiner: bool = False
    showISOB: bool = True
    showMajoinMinerMax: int = 10
    showISOBMax: int = 10
    showTsted: bool = False
    maxTested: int = 20
    ClrMajorOFBull: str = "color.rgb(33, 149, 243, 71)"
    ClrMajorOFBear: str = "color.rgb(33, 149, 243, 72)"
    ClrMinorOFBull: str = "color.rgb(155, 39, 176, 81)"
    ClrMinorOFBear: str = "color.rgb(155, 39, 176, 86)"
    clrObBBTated: str = "color.rgb(136, 142, 252, 86)"


@dataclass
class CandleInputs:
    showISB: bool = False
    colorOSB_up: str = "#0b3ff9"
    showOSB: bool = False
    colorOSB_down: str = "#da781d"
    colorISB: str = "color.rgb(187, 6, 247, 77)"
    label_color_bearish: str = "color.rgb(255, 82, 82, 90)"
    label_color_bullish: str = "color.rgb(33, 149, 243, 90)"
    trendRule: str = "SMA50"


@dataclass
class ConsoleInputs:
    max_age_bars: int = 1


@dataclass
class StructureInputs:
    isOTE: bool = False
    ote1: float = 0.78
    ote2: float = 0.61
    oteclr: str = "#ff95002b"
    sizGd: str = "size.normal"
    showPdh: bool = False
    lengPdh: int = 40
    showPdl: bool = False
    lengPdl: int = 40
    showMid: bool = True
    lengMid: int = 40
    showSw: bool = True
    markX: bool = False
    colorSweep: str = "color.gray"
    showTP: bool = False


@dataclass
class ICTMarketStructureInputs:
    showms: bool = False
    bosColor1: str = "color.green"
    bosColor2: str = "color.red"
    ms_type: str = "All"
    show_equal_highlow: bool = False
    eq_bear_color: str = "#787b86"
    eq_bull_color: str = "#787b86"
    eq_threshold: float = 0.3
    label_sizes_s: str = "Medium"
    swingSize: int = 10
    showSwing: bool = False


@dataclass
class KeyLevelsInputs:
    Show_4H_Levels: bool = False
    Color_4H_Levels: str = "color.orange"
    Style_4H_Levels: str = "Dotted"
    Text_4H_Levels: bool = True
    Show_Daily_Levels: bool = False
    Color_Daily_Levels: str = "#08bcd4"
    Style_Daily_Levels: str = "Dotted"
    Text_Daily_Levels: bool = True
    Show_Monday_Levels: bool = False
    Color_Monday_Levels: str = "color.white"
    Style_Monday_Levels: str = "Dotted"
    Text_Monday_Levels: bool = True
    Show_Weekly_Levels: bool = False
    WeeklyColor: str = "#fffcbc"
    Weekly_style: str = "Dotted"
    WeeklyTextType: bool = True
    Show_Monthly_Levels: bool = False
    MonthlyColor: str = "#098c30"
    Monthly_style: str = "Dotted"
    MonthlyTextType: bool = True
    Show_Quaterly_Levels: bool = False
    quarterlyColor: str = "#bcffd0"
    Quaterly_style: str = "Dotted"
    QuarterlyTextType: bool = True
    Show_Yearly_Levels: bool = False
    YearlyColor: str = "#ffbcdb"
    Yearly_style: str = "Dotted"
    YearlyTextType: bool = True
    labelsize: str = "Small"
    displayStyle: str = "Standard"
    distanceright: int = 25
    radistance: int = 250
    linesize: str = "Small"
    linestyle: str = "Solid"


@dataclass
class SessionInputs:
    is_londonrange_enabled: bool = False
    london_OC: bool = True
    london_HL: bool = True
    is_usrange_enabled: bool = False
    us_OC: bool = True
    us_HL: bool = True
    is_tokyorange_enabled: bool = False
    asia_OC: bool = True
    asia_HL: bool = True
    SessionTextType: bool = False
    Londont: str = "0800-1600"
    USt: str = "1400-2100"
    Asiat: str = "0000-0900"
    LondonColor: str = "color.rgb(15, 13, 13)"
    USColor: str = "color.rgb(190, 8, 236)"
    AsiaColor: str = "color.rgb(33, 5, 241)"
    Short_text_London: bool = True
    Short_text_NY: bool = True
    Short_text_TKY: bool = True


@dataclass
class SwingDetectionInputs:
    cooldownPeriod: int = 10
    showSwing_: bool = True
    swingClr: str = "color.new(color.orange, 0)"
    bullWidth: int = 1
    bullStyle: str = "Dashed"
    bullColor: str = "color.new(color.teal, 0)"
    bearWidth: int = 1
    bearStyle: str = "Dashed"
    bearColor: str = "color.new(color.maroon, 0)"
    display_third: bool = False
    length3: int = 20
    mult: float = 1.0
    atr_Len: int = 500
    upCss: str = "#089981"
    dnCss: str = "#f23645"
    unbrokenCss: str = "#2157f3"


@dataclass
class ZigZagInputs:
    length1: int = 100
    extend: bool = True
    show_ext: bool = True
    show_labels: bool = True
    upcol: str = "#ff1100"
    midcol: str = "#ff5d00"
    dncol: str = "#2157f3"


@dataclass
class SupportResistanceInputs:
    resistanceSupportCount: int = 3
    pivotRange: int = 15
    strength: int = 1
    expandLines: bool = True
    enableZones: bool = False
    zoneWidthType: str = "Dynamic"
    zoneWidth: int = 1
    timeframe1Enabled: bool = True
    timeframe1_: str = ""
    timeframe2Enabled: bool = True
    timeframe2: str = "240"
    timeframe3Enabled: bool = False
    timeframe3: str = "30"
    showBreaks: bool = True
    showRetests: bool = True
    avoidFalseBreaks: bool = True
    falseBreakoutVolumeThresholdOpt: float = 0.3
    inverseBrokenLineColor: bool = True
    lineStyle_: str = "...."
    lineWidth: int = 1
    supportColor: str = "#08998180"
    resistanceColor: str = "#f2364580"
    textColor: str = "#11101051"
    labelsize: str = "Small"
    labelsAlign: str = "Right"
    enableRetestAlerts: bool = True
    enableBreakAlerts: bool = True
    memoryOptimizatonEnabled: bool = True
    debug_labelPivots: str = "None"
    debug_pivotLabelText: bool = False
    debug_showBrokenOnLabel: bool = False
    debug_removeDuplicateRS: bool = True
    debug_lastXResistances: int = 3
    debug_lastXSupports: int = 3
    debug_enabledHistory: bool = True
    debug_maxHistoryRecords: int = 10


@dataclass
class CustomPoint:
    time: int
    price: float
    tr: float


@dataclass
class SupportResistanceLevel:
    rs_type: str
    timeframe: str
    price: float
    points: List[CustomPoint] = field(default_factory=list)
    line: Optional[Line] = None
    box: Optional[Box] = None
    price_label: Optional[Label] = None
    break_label: Optional[Label] = None
    break_line: Optional[Line] = None
    break_box: Optional[Box] = None
    retest_labels: List[Label] = field(default_factory=list)
    is_broken: bool = False
    broken_time: Optional[int] = None
    break_level: Optional[float] = None
    break_tr: float = 0.0
    last_retest_time: Optional[int] = None
    last_retest_bar: Optional[int] = None
    last_break_alert_time: Optional[int] = None
    last_retest_alert_time: Optional[int] = None


@dataclass
class IndicatorInputs:
    pullback: PullbackInputs = field(default_factory=PullbackInputs)
    structure: MarketStructureInputs = field(default_factory=MarketStructureInputs)
    order_block: OrderBlockInputs = field(default_factory=OrderBlockInputs)
    demand_supply: DemandSupplyInputs = field(default_factory=DemandSupplyInputs)
    fvg: FVGInputs = field(default_factory=FVGInputs)
    liquidity: LiquidityInputs = field(default_factory=LiquidityInputs)
    order_flow: OrderFlowInputs = field(default_factory=OrderFlowInputs)
    candle: CandleInputs = field(default_factory=CandleInputs)
    console: ConsoleInputs = field(default_factory=ConsoleInputs)
    structure_util: StructureInputs = field(default_factory=StructureInputs)
    ict_structure: ICTMarketStructureInputs = field(default_factory=ICTMarketStructureInputs)
    key_levels: KeyLevelsInputs = field(default_factory=KeyLevelsInputs)
    sessions: SessionInputs = field(default_factory=SessionInputs)
    swing_detection: SwingDetectionInputs = field(default_factory=SwingDetectionInputs)
    zigzag: ZigZagInputs = field(default_factory=ZigZagInputs)
    support_resistance: SupportResistanceInputs = field(default_factory=SupportResistanceInputs)


# ----------------------------------------------------------------------------
# Utility accessors for historical series
# ----------------------------------------------------------------------------


class SeriesAccessor:
    def __init__(self) -> None:
        self.open: List[float] = []
        self.high: List[float] = []
        self.low: List[float] = []
        self.close: List[float] = []
        self.volume: List[float] = []
        self.time: List[int] = []

    def append(self, candle: Dict[str, float]) -> None:
        self.open.append(candle["open"])
        self.high.append(candle["high"])
        self.low.append(candle["low"])
        self.close.append(candle["close"])
        self.volume.append(candle.get("volume", NA))
        self.time.append(int(candle["time"]))

    def get(self, series: str, offset: int = 0) -> float:
        values = getattr(self, series)
        index = len(values) - 1 - offset
        if index < 0:
            return NA
        return values[index]

    def get_time(self, offset: int = 0) -> int:
        values = self.time
        index = len(values) - 1 - offset
        if index < 0:
            return 0
        return values[index]

    def length(self) -> int:
        return len(self.time)


# ----------------------------------------------------------------------------
# request.security emulation helpers
# ----------------------------------------------------------------------------


def _parse_timeframe_to_seconds(timeframe: str, base_seconds: Optional[int]) -> Optional[int]:
    """Convert timeframe string like "1m", "4H", "D" to seconds.

    Hardened to avoid crashes on malformed values such as "" or "M"/"H" (missing numeric prefix).
    """
    if timeframe in ("", None):
        return base_seconds
    tf = str(timeframe).strip().upper()
    if not tf:
        return base_seconds
    # Handle pure unit strings like "M" or "H" by falling back gracefully
    def _num(part: str) -> Optional[float]:
        part = (part or "").strip()
        if not part:
            return None
        try:
            return float(part)
        except (TypeError, ValueError):
            return None

    if tf.endswith("H"):
        n = _num(tf[:-1])
        return int(n * 3600) if n is not None else base_seconds
    if tf.endswith("D"):
        n = _num(tf[:-1])
        return int(n * 86400) if n is not None else base_seconds
    if tf.endswith("W"):
        n = _num(tf[:-1])
        return int(n * 7 * 86400) if n is not None else base_seconds
    if tf.endswith("M"):
        n = _num(tf[:-1])
        return int(n * 30 * 86400) if n is not None else base_seconds
    if tf.endswith("S"):
        n = _num(tf[:-1])
        return int(n) if n is not None else base_seconds
    if tf.isdigit():
        return int(tf) * 60
    return base_seconds

class SecuritySeries:
    def __init__(self, timeframe_seconds: Optional[int]) -> None:
        self.timeframe_seconds = timeframe_seconds
        self.final_open: List[float] = []
        self.final_high: List[float] = []
        self.final_low: List[float] = []
        self.final_close: List[float] = []
        self.final_volume: List[float] = []
        self.final_time: List[int] = []
        self.pending: Optional[Dict[str, float]] = None
        self.bucket_start: Optional[int] = None

    def _commit_pending(self) -> None:
        if self.pending is None:
            return
        self.final_open.append(self.pending["open"])
        self.final_high.append(self.pending["high"])
        self.final_low.append(self.pending["low"])
        self.final_close.append(self.pending["close"])
        self.final_volume.append(self.pending.get("volume", NA))
        self.final_time.append(int(self.pending["time"]))
        self.pending = None

    def update(self, time_val: Any, open_: Any, high: Any, low: Any, close: Any, volume: Any) -> None:
        # Robust coercion: prevent crashes on empty strings like volume="".
        t = _coerce_float(time_val, default=NA)
        o = _coerce_float(open_, default=NA)
        h = _coerce_float(high, default=NA)
        l = _coerce_float(low, default=NA)
        c = _coerce_float(close, default=NA)
        v = _coerce_float(volume, default=0.0)
        if math.isnan(t) or math.isnan(o) or math.isnan(h) or math.isnan(l) or math.isnan(c):
            return
        if self.timeframe_seconds is None:
            self.final_open.append(o)
            self.final_high.append(h)
            self.final_low.append(l)
            self.final_close.append(c)
            self.final_volume.append(v)
            self.final_time.append(int(t))
            return
        bucket = (int(t) // (self.timeframe_seconds * 1000)) * (self.timeframe_seconds * 1000)
        if self.bucket_start is None or bucket != self.bucket_start:
            if self.pending is not None:
                self._commit_pending()
            self.bucket_start = bucket
            self.pending = {
                "time": float(t),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v),
            }
        else:
            assert self.pending is not None
            self.pending["high"] = max(self.pending["high"], h)
            self.pending["low"] = min(self.pending["low"], l)
            self.pending["close"] = float(c)
            self.pending["time"] = float(t)
            self.pending["volume"] = float(self.pending.get("volume", 0.0) + (v if not math.isnan(v) else 0.0))

    def finalise(self) -> None:
        self._commit_pending()

    def length(self) -> int:
        extra = 1 if self.pending is not None else 0
        return len(self.final_time) + extra

    def _resolve_index(self, offset: int) -> Optional[Tuple[List[float], int]]:
        total = self.length()
        idx = total - 1 - offset
        if idx < 0:
            return None
        if self.pending is not None and idx == len(self.final_time):
            return None
        return ([], idx)

    def _get_from_lists(self, data: List[float], offset: int, pending_key: str) -> float:
        total_final = len(data)
        total = self.length()
        idx = total - 1 - offset
        if idx < 0:
            return NA
        if self.pending is not None and idx == total_final:
            return _coerce_float(self.pending.get(pending_key), default=NA)
        if idx < total_final:
            return data[idx]
        return NA

    def get(self, series: str, offset: int = 0) -> float:
        if series == "open":
            return self._get_from_lists(self.final_open, offset, "open")
        if series == "high":
            return self._get_from_lists(self.final_high, offset, "high")
        if series == "low":
            return self._get_from_lists(self.final_low, offset, "low")
        if series == "close":
            return self._get_from_lists(self.final_close, offset, "close")
        if series == "volume":
            return self._get_from_lists(self.final_volume, offset, "volume")
        raise KeyError(series)

    def get_time(self, offset: int = 0) -> int:
        total_final = len(self.final_time)
        total = self.length()
        idx = total - 1 - offset
        if idx < 0:
            return 0
        if self.pending is not None and idx == total_final:
            return int(self.pending["time"])
        if idx < total_final:
            return self.final_time[idx]
        return 0


# ----------------------------------------------------------------------------
# Runtime state mirroring Pine logic
# ----------------------------------------------------------------------------


class SmartMoneyAlgoProE5:
    """Runtime translation for the requested indicator modules."""

    IDM_TEXT = "I D M"
    CHOCH_TEXT = "CHoCH"
    BOS_TEXT = "B O S"
    PDH_TEXT = "PDH"
    PDL_TEXT = "PDL"
    MID_TEXT = "0.5"

    def __init__(
        self,
        inputs: Optional[IndicatorInputs] = None,
        base_timeframe: Optional[str] = None,
        tracer: Optional[ExecutionTracer] = None,
    ) -> None:
        self.inputs = inputs or IndicatorInputs()
        self.series = SeriesAccessor()
        self.base_tf_seconds: Optional[int] = _parse_timeframe_to_seconds(base_timeframe, None)
        self.base_timeframe = base_timeframe or ""
        self.ticker: str = ""  # set by symbol for persistent OB de-dup
        self.security_series: Dict[str, SecuritySeries] = {}
        self.ob_volume_history: Dict[str, PineArray] = {}
        self.ob_valid_history: Dict[str, bool] = {}
        self.security_bucket_tracker: Dict[str, Optional[int]] = {}
        self.tracer = tracer or ExecutionTracer(False)

        # Labels, boxes, lines ------------------------------------------------
        self.labels: List[Label] = []
        self.lines: List[Line] = []
        self.boxes: List[Box] = []
        self.bar_colors: List[Tuple[int, str]] = []
        self.console_event_log: Dict[str, Dict[str, Any]] = {}
        self.console_printed_events: Dict[str, int] = {}
        self.console_printed_tokens: Dict[str, str] = {}
        self.console_box_status_tally: Dict[str, Counter[str]] = defaultdict(Counter)
        console_inputs = getattr(self.inputs, "console", None)
        if console_inputs is None:
            max_age = 1
        else:
            try:
                max_age = int(getattr(console_inputs, "max_age_bars", 1) or 1)
            except (TypeError, ValueError):
                max_age = 1
        self.console_max_age_bars = max(1, max_age)

        # Mirrors for Pine ``var``/``array`` state ---------------------------
        self.pullback_state = PullbackStateMirror()
        self.market_structure_state = MarketStructureStateMirror()
        self.swing_state = SwingStateMirror()
        self.order_block_state = OrderBlockStateMirror()
        self.scob_state = SCOBStateMirror()

        # Pine condition mirroring -------------------------------------------
        self.condition_specs: Dict[str, ConditionSpec] = {}
        self.condition_trace: List[ConditionEvaluation] = []

        # Persistent state initialisation mirrors Pine ``var`` semantics ------
        self.initialised = False
        self.time_history: List[int] = []
        self.timediff_value: float = 0.0
        self.fvg_gap: int = 0
        self.fvg_removed: int = 0
        self.htfH: float = NA
        self.htfL: float = NA
        self.last_liq_high_time: Optional[int] = None
        self.last_liq_low_time: Optional[int] = None
        self.bullish_OB_Break: bool = False
        self.bearish_OB_Break: bool = False
        self.isb_history: List[bool] = []
        self.order_block_touch_seen: set[str] = set()
        self.golden_zone_touch_seen: set[str] = set()

    # ------------------------------------------------------------------
    # Pine primitive wrappers
    # ------------------------------------------------------------------
    BOX_STATUS_LABELS = {
        "new": "منطقة جديدة",
        "active": "منطقة نشطة",
        "touched": "تمت ملامستها",
        "retest": "إعادة اختبار",
        "breaker": "Breaker",
        "mitigation": "Mitigation",
        "propulsion": "Propulsion",
        "archived": "محفوظة تاريخياً",
    }

    def label_new(
        self,
        x: int,
        y: float,
        text: str,
        xloc: str,
        yloc: str,
        color: str,
        style: str,
        size: str,
        textcolor: str,
        tooltip: Optional[str] = None,
    ) -> Label:
        lbl = Label(x, y, text, xloc, yloc, color, style, size, textcolor, tooltip)
        self.labels.append(lbl)
        self._register_label_event(lbl)
        return lbl

    def line_new(
        self, x1: int, y1: float, x2: int, y2: float, xloc: str, color: str, style: str
    ) -> Line:
        ln = Line(x1, y1, x2, y2, xloc, color, style)
        self.lines.append(ln)
        return ln

    def box_new(
        self,
        left: int,
        right: int,
        top: float,
        bottom: float,
        color: str,
        text: str = "",
        text_color: str = "#000000",
    ) -> Box:
        bx = Box(left, right, top, bottom, color, color, text=text, text_color=text_color)
        self.boxes.append(bx)
        self._register_box_event(bx, status="new")
        self._trace("box.new", "create", timestamp=right, left=left, right=right, top=top, bottom=bottom, color=color, text=text)
        return bx

    def _archive_box(self, box: Optional[Box], hist_text: str, store: PineArray) -> None:
        if not isinstance(box, Box):
            return
        box.set_text(hist_text)
        if box in self.boxes:
            self.boxes.remove(box)
        store.push(box)
        self._register_box_event(box, status="archived")
        self._trace("box.archive", "archive", timestamp=box.right, text=hist_text)

    def _eval_condition(
        self,
        name: str,
        pine_expression: str,
        evaluator: Callable[[], bool],
    ) -> bool:
        """Mirror a Pine ``if`` condition and record its evaluation order."""

        spec = self.condition_specs.get(name)
        if spec is None or spec.pine_expression != pine_expression:
            spec = ConditionSpec(name, pine_expression)
            self.condition_specs[name] = spec
        result = bool(evaluator())
        self.condition_trace.append(
            ConditionEvaluation(spec=spec, result=result, timestamp=self.series.get_time())
        )
        return result

    def _trace(self, section: str, message: str, *, timestamp: Optional[int], **payload: Any) -> None:
        self.tracer.log(section, message, timestamp=timestamp, **payload)

    def _bars_ago_from_time(self, timestamp: Any) -> Optional[int]:
        if self.series.length() == 0:
            return None
        if not isinstance(timestamp, (int, float)):
            return None
        ts = int(timestamp)
        if ts <= 0:
            return None
        idx = bisect.bisect_right(self.series.time, ts) - 1
        if idx < 0:
            return self.series.length()
        return (self.series.length() - 1) - idx

    def _console_event_within_age(self, timestamp: Any) -> bool:
        if self.console_max_age_bars <= 0:
            return True
        bars_ago = self._bars_ago_from_time(timestamp)
        if bars_ago is None:
            return True
        return bars_ago <= self.console_max_age_bars

    def _event_print_within_age(self, timestamp: Any) -> bool:
        if EVENT_PRINT_MAX_AGE_BARS <= 0:
            return True
        bars_ago = self._bars_ago_from_time(timestamp)
        if bars_ago is None:
            return True
        return bars_ago <= EVENT_PRINT_MAX_AGE_BARS

    def _should_print_event(self, key: str, payload: Dict[str, Any]) -> bool:
        if not EVENT_PRINT_ENABLED:
            return False
        if EVENT_PRINT_TOGGLES and not EVENT_PRINT_TOGGLES.get(key, True):
            return False
        if EVENT_PRINT_KEYS and key not in EVENT_PRINT_KEYS:
            return False
        timestamp = payload.get("time") if isinstance(payload, dict) else None
        if not self._event_print_within_age(timestamp):
            return False
        if isinstance(timestamp, (int, float)):
            last_time = self.console_printed_events.get(key)
            if last_time is not None and int(timestamp) == last_time:
                return False
        token = payload.get("fingerprint")
        if token:
            last_token = self.console_printed_tokens.get(key)
            if last_token == token:
                return False
        return True

    def _emit_console_event(self, key: str, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        if not self._should_print_event(key, payload):
            return
        timestamp = payload.get("time")
        if isinstance(timestamp, (int, float)):
            self.console_printed_events[key] = int(timestamp)
        token = payload.get("fingerprint")
        if token:
            self.console_printed_tokens[key] = str(token)
        label = EVENT_PRINT_LABELS.get(key, key)
        display = payload.get("display")
        if display is None:
            price = payload.get("price")
            if isinstance(price, tuple):
                display = " → ".join(format_price(p) for p in price)
            else:
                display = format_price(price if isinstance(price, (int, float)) else None)
        status_display = payload.get("status_display")
        if status_display:
            display = f"{display} [{status_display}]"
        time_display = payload.get("time_display") or format_timestamp(timestamp)
        if time_display and time_display != "—":
            display = f"{display} | {time_display}"
        direction_hint = _resolve_direction(
            payload.get("direction"),
            payload.get("direction_display"),
            payload.get("status"),
            payload.get("text"),
            display,
        )
        colored_display = _colorize_directional_text(
            display,
            direction=direction_hint,
            fallback=ANSI_VALUE_POS,
        )
        print(f"{ANSI_BOLD}{label}{ANSI_RESET}: {colored_display}", flush=True)

    def _record_console_event(self, key: str, payload: Dict[str, Any]) -> None:
        """Store console event (do not print during historical backfills).

        عند تحليل التاريخ الكامل (backfill) فإن طباعة الأحداث أثناء المرور على كل شمعة
        يؤدي إلى سيل كبير من الإشعارات القديمة. لذلك نكتفي بتسجيل الحدث هنا فقط،
        ثم نطبع/نرسل أحدث الأحداث بعد انتهاء المعالجة (انظر scan_binance).
        """
        self.console_event_log[key] = payload

    def _collect_latest_console_events(self) -> Dict[str, Any]:
        """Collect latest console events in a stable order.

        - يفلتر الأحداث القديمة (حسب console_max_age_bars)
        - يرتبها حسب الوقت ثم حسب ترتيب العرض EVENT_DISPLAY_ORDER
        - يمنع التكرار (قد يحدث عند تحديث نفس الحدث بأكثر من صيغة)
        """

        if not isinstance(self.console_event_log, dict) or not self.console_event_log:
            return {}

        order_map: Dict[str, int] = {name: idx for idx, (name, _) in enumerate(EVENT_DISPLAY_ORDER)}
        seen: set[tuple] = set()
        items: list[tuple[int, int, str, Dict[str, Any]]] = []

        def _payload_time(payload: Dict[str, Any]) -> int:
            ts = payload.get("time") or payload.get("ts") or payload.get("timestamp")
            if isinstance(ts, (int, float)):
                return int(ts)
            return 0

        for key, payload in self.console_event_log.items():
            if not isinstance(payload, dict):
                continue
            if not self._console_event_within_age(payload.get("time")):
                continue
            items.append((_payload_time(payload), order_map.get(str(key), 999), str(key), payload))

        items.sort(key=lambda it: (it[0], it[1], it[2]))

        latest: Dict[str, Any] = {}
        for _, _, key, payload in items:
            sig = (
                key,
                payload.get("time"),
                payload.get("price"),
                payload.get("display"),
                payload.get("status"),
            )
            if sig in seen:
                continue
            seen.add(sig)
            latest[key] = payload
        return latest


    def gather_console_metrics(self) -> Dict[str, Any]:
        """Aggregate runtime metrics for console presentation."""

        pullback_arrows = sum(
            1
            for lbl in self.labels
            if lbl.style in ("label.style_arrowdown", "label.style_arrowup")
        )
        choch_labels = sum(1 for lbl in self.labels if "CHoCH" in lbl.text)
        bos_labels = sum(1 for lbl in self.labels if "B O S" in lbl.text or "BOS" in lbl.text)
        idm_labels = sum(1 for lbl in self.labels if "I D M" in lbl.text)
        liquidity_high_lines = getattr(self, "liquidity_high_lines", PineArray())
        liquidity_low_lines = getattr(self, "liquidity_low_lines", PineArray())
        liquidity_high_boxes = getattr(self, "liquidity_high_boxes", PineArray())
        liquidity_low_boxes = getattr(self, "liquidity_low_boxes", PineArray())
        liquidity_objects = (
            liquidity_high_lines.size()
            + liquidity_low_lines.size()
            + liquidity_high_boxes.size()
            + liquidity_low_boxes.size()
        )
        arr_ob_bulls = getattr(self, "arrOBBulls", PineArray())
        arr_ob_bears = getattr(self, "arrOBBears", PineArray())
        arr_ob_bullm = getattr(self, "arrOBBullm", PineArray())
        arr_ob_bearm = getattr(self, "arrOBBearm", PineArray())
        order_flow_boxes = (
            arr_ob_bulls.size()
            + arr_ob_bears.size()
            + arr_ob_bullm.size()
            + arr_ob_bearm.size()
        )
        demand_zone = getattr(self, "demandZone", PineArray())
        supply_zone = getattr(self, "supplyZone", PineArray())
        bullish_gap_holder = getattr(self, "bullish_gap_holder", PineArray())
        bearish_gap_holder = getattr(self, "bearish_gap_holder", PineArray())
        metrics = {
            "labels": len(self.labels),
            "lines": len(self.lines),
            "boxes": len(self.boxes),
            "pullback_arrows": pullback_arrows,
            "choch_labels": choch_labels,
            "bos_labels": bos_labels,
            "idm_labels": idm_labels,
            "demand_zones": demand_zone.size(),
            "supply_zones": supply_zone.size(),
            "bullish_fvg": bullish_gap_holder.size(),
            "bearish_fvg": bearish_gap_holder.size(),
            "liquidity_objects": liquidity_objects,
            "order_flow_boxes": order_flow_boxes,
            "scob_colored_bars": len(self.bar_colors),
        }
        idm_counter = self.console_box_status_tally.get("IDM_OB", Counter())
        ext_counter = self.console_box_status_tally.get("EXT_OB", Counter())

        def _status_total(counter: Dict[str, Any], *keys: str) -> int:
            total = 0
            for key in keys:
                value = counter.get(key, 0) if isinstance(counter, dict) else 0
                if isinstance(value, (int, float)):
                    total += int(value)
            return total

        metrics["idm_ob_new"] = _status_total(idm_counter, "new")
        metrics["idm_ob_touched"] = _status_total(idm_counter, "touched", "retest")
        metrics["ext_ob_new"] = _status_total(ext_counter, "new")
        metrics["ext_ob_touched"] = _status_total(ext_counter, "touched", "retest")
        metrics["current_price"] = self.series.get("close")
        metrics["latest_events"] = self._collect_latest_console_events()
        return metrics

    def _register_label_event(self, label: Label) -> None:
        text = label.text.strip()
        collapsed = text.replace(" ", "")
        key: Optional[str] = None
        if collapsed == "BOS":
            key = "BOS"
        elif collapsed == "BOS+":
            key = "BOS_PLUS"
        elif collapsed == self.CHOCH_TEXT.replace(" ", ""):
            key = "CHOCH"
        elif collapsed == "MSS+":
            key = "MSS_PLUS"
        elif collapsed == "MSS":
            key = "MSS"
        elif collapsed == self.IDM_TEXT.replace(" ", ""):
            key = "IDM"
        elif text.startswith(f"{self.BOS_TEXT} -"):
            key = "FUTURE_BOS"
        elif text.startswith(f"{self.CHOCH_TEXT} -"):
            key = "FUTURE_CHOCH"
        elif text == "X":
            key = "X"
        elif label.style == "label.style_circle":
            if label.color == self.inputs.structure.bear:
                key = "RED_CIRCLE"
            elif label.color == self.inputs.structure.bull:
                key = "GREEN_CIRCLE"
        if key is None:
            if text.startswith(self.PDH_TEXT):
                key = "PDH"
            elif text.startswith(self.PDL_TEXT):
                key = "PDL"
            elif text.startswith(self.MID_TEXT):
                key = "EQUILIBRIUM"
        if key is None:
            normalized = text.lower()
            if "4h" in normalized:
                key = "KEY_LEVEL_4H"
            elif any(token in normalized for token in ("daily", "do", "pdh", "pdl")):
                key = "KEY_LEVEL_DAILY"
            elif any(token in normalized for token in ("weekly", "wo", "pwh", "pwl")):
                key = "KEY_LEVEL_WEEKLY"
        if key:
            if key in ("BOS", "CHOCH"):
                existing = self.console_event_log.get(key)
                if existing and existing.get("source") == "confirmed":
                    return
            payload = {
                "text": label.text,
                "price": label.y,
                "time": label.x,
                "time_display": format_timestamp(label.x),
                "display": f"{label.text} @ {format_price(label.y)}",
                "fingerprint": f"{key}:{_fmt_price_key(label.y)}",
            }
            if key in ("MSS", "MSS_PLUS", "BOS_PLUS"):
                if label.style == "label.style_label_down":
                    payload["direction"] = "bearish"
                    payload["direction_display"] = "هابط"
                elif label.style == "label.style_label_up":
                    payload["direction"] = "bullish"
                    payload["direction_display"] = "صاعد"
            self._record_console_event(key, payload)
            self._trace("label", "register", timestamp=label.x, key=key, text=label.text, price=label.y)

    def _register_structure_break_event(
        self,
        key: str,
        price: float,
        timestamp: int,
        *,
        bullish: bool,
    ) -> None:
        direction_text = "صاعد" if bullish else "هابط"
        display = f"{key} @ {format_price(price)} ({direction_text})"
        payload = {
            "text": key,
            "price": price,
            "time": timestamp,
            "time_display": format_timestamp(timestamp),
            "display": display,
            "direction": "bullish" if bullish else "bearish",
            "direction_display": direction_text,
            "source": "confirmed",
            "fingerprint": f"{key}:{_fmt_price_key(price)}",
        }
        self._record_console_event(key, payload)

    def _register_box_event(self, box: Box, *, status: str = "active", event_time: Optional[int] = None) -> None:
        text = box.text.strip()
        key: Optional[str] = None
        if text == "IDM OB":
            key = "IDM_OB"
        elif text == "EXT OB":
            key = "EXT_OB"
        elif text == "Hist IDM OB":
            key = "HIST_IDM_OB"
        elif text == "Hist EXT OB":
            key = "HIST_EXT_OB"
        elif text == "Golden zone":
            key = "GOLDEN_ZONE"
        if key:
            ts = event_time if isinstance(event_time, int) else box.left
            status_label = self.BOX_STATUS_LABELS.get(status, status)
            status_key = status if isinstance(status, str) and status else "active"
            tally = self.console_box_status_tally[key]
            # De-dup first touch / first retest across runs (runtime is rebuilt per symbol).
            # This mirrors the "touched/retested cache" behaviour in the 3d updated script.
            if key in ("IDM_OB", "EXT_OB") and status_key in ("touched", "retest"):
                sym = getattr(self, "ticker", "") or ""
                if sym:
                    evk = _ob_event_key(sym, key, box.bottom, box.top, box.left)
                    if status_key == "touched":
                        if evk in _OB_TOUCHED_SEEN:
                            return
                        _OB_TOUCHED_SEEN.add(evk)
                        _save_json_set(_OB_TOUCHED_CACHE_PATH, _OB_TOUCHED_SEEN)
                    else:
                        if evk in _OB_RETEST_SEEN:
                            return
                        _OB_RETEST_SEEN.add(evk)
                        _save_json_set(_OB_RETEST_CACHE_PATH, _OB_RETEST_SEEN)

            tally[status_key] += 1
            payload = {
                "text": box.text,
                "price": (box.bottom, box.top),
                "time": ts,
                "time_display": format_timestamp(ts),
                "display": f"{box.text} {format_price(box.bottom)} → {format_price(box.top)}",
                "status": status,
                "status_display": status_label,
                "fingerprint": f"{key}:{_fmt_price_key(box.bottom)}:{_fmt_price_key(box.top)}:{status}",
            }
            self._record_console_event(key, payload)
            self._trace(
                "box",
                "register",
                timestamp=box.right,
                key=key,
                text=box.text,
                top=box.top,
                bottom=box.bottom,
                status=status,
            )
            if key == "GOLDEN_ZONE":
                self._record_golden_zone_touch(box, event_time=ts)

    def _record_range_event(
        self,
        key: str,
        *,
        text: str,
        bottom: float,
        top: float,
        timestamp: int,
        status: Optional[str] = None,
        direction: Optional[str] = None,
    ) -> None:
        status_label = None
        if status:
            status_label = self.BOX_STATUS_LABELS.get(status, status)
        payload = {
            "text": text,
            "price": (bottom, top),
            "time": timestamp,
            "time_display": format_timestamp(timestamp),
            "display": f"{text} {format_price(bottom)} → {format_price(top)}",
            "direction": direction,
            "fingerprint": f"{key}:{_fmt_price_key(bottom)}:{_fmt_price_key(top)}:{status or ''}:{timestamp}",
        }
        if status:
            payload["status"] = status
            payload["status_display"] = status_label
        self._record_console_event(key, payload)

    @staticmethod
    def _direction_from_ob_type(type_val: int) -> str:
        return "bullish" if type_val == -1 else "bearish"

    def _record_golden_zone_touch(self, box: Box, *, event_time: int) -> None:
        if not EVENT_PRINT_ENABLED:
            return
        if EVENT_PRINT_KEYS and "GOLDEN_ZONE_TOUCH" not in EVENT_PRINT_KEYS:
            return
        if EVENT_PRINT_TOGGLES and not EVENT_PRINT_TOGGLES.get("GOLDEN_ZONE_TOUCH", True):
            return
        high = self.series.get("high")
        low = self.series.get("low")
        if math.isnan(high) or math.isnan(low):
            return
        if low > box.top or high < box.bottom:
            return
        token = f"{_fmt_price_key(box.bottom)}:{_fmt_price_key(box.top)}"
        if GOLDEN_ZONE_TOUCH_ONCE and token in self.golden_zone_touch_seen:
            return
        self.golden_zone_touch_seen.add(token)
        touch_label = EVENT_PRINT_LABELS.get("GOLDEN_ZONE_TOUCH", "Golden zone (Touch)")
        self._record_range_event(
            "GOLDEN_ZONE_TOUCH",
            text=touch_label,
            bottom=box.bottom,
            top=box.top,
            timestamp=event_time,
            status="touched",
        )

    def _sync_state_mirrors(self) -> None:
        """Mirror Pine ``var``/``array`` structures into dedicated containers."""

        def _array(name: str) -> PineArray:
            value = getattr(self, name, None)
            return value if isinstance(value, PineArray) else PineArray()

        def _scalar(name: str) -> Any:
            return getattr(self, name, None)

        self.pullback_state.arrays = {
            "arrTopBotBar": _array("arrTopBotBar"),
            "arrTop": _array("arrTop"),
            "arrBot": _array("arrBot"),
            "arrPbHBar": _array("arrPbHBar"),
            "arrPbHigh": _array("arrPbHigh"),
            "arrPbLBar": _array("arrPbLBar"),
            "arrPbLow": _array("arrPbLow"),
            "arrPrevPrsMin": _array("arrPrevPrsMin"),
            "arrPrevIdxMin": _array("arrPrevIdxMin"),
            "arrlstHigh": _array("arrlstHigh"),
            "arrlstLow": _array("arrlstLow"),
        }
        self.pullback_state.scalars = {
            "puHigh": _scalar("puHigh"),
            "puHigh_": _scalar("puHigh_"),
            "puLow": _scalar("puLow"),
            "puLow_": _scalar("puLow_"),
            "puHBar": _scalar("puHBar"),
            "puLBar": _scalar("puLBar"),
        }

        self.market_structure_state.arrays = {
            "arrIdmHigh": _array("arrIdmHigh"),
            "arrIdmLow": _array("arrIdmLow"),
            "arrIdmHBar": _array("arrIdmHBar"),
            "arrIdmLBar": _array("arrIdmLBar"),
            "arrLastH": _array("arrLastH"),
            "arrLastHBar": _array("arrLastHBar"),
            "arrLastL": _array("arrLastL"),
            "arrLastLBar": _array("arrLastLBar"),
            "arrIdmLine": _array("arrIdmLine"),
            "arrIdmLabel": _array("arrIdmLabel"),
            "arrBCLine": _array("arrBCLine"),
            "arrBCLabel": _array("arrBCLabel"),
            "arrHLLabel": _array("arrHLLabel"),
            "arrHLCircle": _array("arrHLCircle"),
        }
        self.market_structure_state.scalars = {
            "mnStrc": _scalar("mnStrc"),
            "prevMnStrc": _scalar("prevMnStrc"),
            "isPrevBos": _scalar("isPrevBos"),
            "findIDM": _scalar("findIDM"),
            "isBosUp": _scalar("isBosUp"),
            "isBosDn": _scalar("isBosDn"),
            "isCocUp": _scalar("isCocUp"),
            "isCocDn": _scalar("isCocDn"),
            "motherHigh": _scalar("motherHigh"),
            "motherLow": _scalar("motherLow"),
            "motherBar": _scalar("motherBar"),
            "H": _scalar("H"),
            "L": _scalar("L"),
            "HBar": _scalar("HBar"),
            "LBar": _scalar("LBar"),
            "lastH": _scalar("lastH"),
            "lastL": _scalar("lastL"),
            "lastHBar": _scalar("lastHBar"),
            "lastLBar": _scalar("lastLBar"),
            "H_lastH": _scalar("H_lastH"),
            "L_lastHH": _scalar("L_lastHH"),
            "H_lastLL": _scalar("H_lastLL"),
            "L_lastL": _scalar("L_lastL"),
            "idmHigh": _scalar("idmHigh"),
            "idmLow": _scalar("idmLow"),
            "idmHBar": _scalar("idmHBar"),
            "idmLBar": _scalar("idmLBar"),
            "lstHlPrs": _scalar("lstHlPrs"),
            "lstHlPrsIdm": _scalar("lstHlPrsIdm"),
            "lstBxIdm": _scalar("lstBxIdm"),
            "lstBx": _scalar("lstBx"),
            "pdh": _scalar("pdh"),
            "pdl": _scalar("pdl"),
            "pdh_line": _scalar("pdh_line"),
            "pdh_label": _scalar("pdh_label"),
            "pdl_line": _scalar("pdl_line"),
            "pdl_label": _scalar("pdl_label"),
            "mid_line": _scalar("mid_line"),
            "mid_label": _scalar("mid_label"),
            "mid_line1": _scalar("mid_line1"),
            "mid_label1": _scalar("mid_label1"),
            "mid_line2": _scalar("mid_line2"),
            "mid_label2": _scalar("mid_label2"),
            "puBar": _scalar("puBar"),
        }

        self.swing_state.arrays = {
            "swingHighArr": _array("swingHighArr"),
            "swingHighTextArr": _array("swingHighTextArr"),
            "swingLowArr": _array("swingLowArr"),
            "swingLowTextArr": _array("swingLowTextArr"),
        }
        self.swing_state.scalars = {
            "swingHighVal": _scalar("swingHighVal"),
            "swingLowVal": _scalar("swingLowVal"),
            "swingHighCounter": _scalar("swingHighCounter"),
            "swingLowCounter": _scalar("swingLowCounter"),
            "isSwingHighCheck": _scalar("isSwingHighCheck"),
            "isSwingLowCheck": _scalar("isSwingLowCheck"),
            "stopPrintingHigh": _scalar("stopPrintingHigh"),
            "stopPrintingLow": _scalar("stopPrintingLow"),
        }

        self.order_block_state.arrays = {
            "demandZone": _array("demandZone"),
            "supplyZone": _array("supplyZone"),
            "demandZoneIsMit": _array("demandZoneIsMit"),
            "supplyZoneIsMit": _array("supplyZoneIsMit"),
            "hist_idm_boxes": _array("hist_idm_boxes"),
            "hist_ext_boxes": _array("hist_ext_boxes"),
            "arrOBTstdo": _array("arrOBTstdo"),
            "arrOBTstd": _array("arrOBTstd"),
            "arrOBTstdTy": _array("arrOBTstdTy"),
            "arrOBBullm": _array("arrOBBullm"),
            "arrOBBearm": _array("arrOBBearm"),
            "arrOBBullisVm": _array("arrOBBullisVm"),
            "arrOBBearisVm": _array("arrOBBearisVm"),
            "arrOBBulls": _array("arrOBBulls"),
            "arrOBBears": _array("arrOBBears"),
            "arrOBBullisVs": _array("arrOBBullisVs"),
            "arrOBBearisVs": _array("arrOBBearisVs"),
            "arrmitOBBull": _array("arrmitOBBull"),
            "arrmitOBBulla": _array("arrmitOBBulla"),
            "arrmitOBBear": _array("arrmitOBBear"),
            "arrmitOBBeara": _array("arrmitOBBeara"),
        }
        self.order_block_state.scalars = {
            "isSweepOBS": _scalar("isSweepOBS"),
            "current_OBS": _scalar("current_OBS"),
            "high_MOBS": _scalar("high_MOBS"),
            "low_MOBS": _scalar("low_MOBS"),
            "isSweepOBD": _scalar("isSweepOBD"),
            "current_OBD": _scalar("current_OBD"),
            "high_MOBD": _scalar("high_MOBD"),
            "low_MOBD": _scalar("low_MOBD"),
        }

        self.scob_state.arrays = {
            "demandZone": _array("demandZone"),
            "supplyZone": _array("supplyZone"),
            "demandZoneIsMit": _array("demandZoneIsMit"),
            "supplyZoneIsMit": _array("supplyZoneIsMit"),
        }
        self.scob_state.scalars = {
            "bar_colors": list(self.bar_colors),
        }

    def snapshot_state(self) -> Dict[str, List[Any]]:
        """Serialize the runtime state for parity comparisons."""

        def serialize_array(array: PineArray) -> List[Any]:
            serialized: List[Any] = []
            for item in array:
                if isinstance(item, Label):
                    serialized.append(_serialize_label(item))
                elif isinstance(item, Line):
                    serialized.append(_serialize_line(item))
                elif isinstance(item, Box):
                    serialized.append(_serialize_box(item))
                else:
                    serialized.append(_serialize_scalar(item))
            return serialized

        if not getattr(self, "initialised", False):
            return {
                "labels": [],
                "lines": [],
                "boxes": [],
                "bar_colors": [],
            }

        self._sync_state_mirrors()

        def serialize_state(mirror: ModuleStateMirror) -> Dict[str, Any]:
            return {
                "scalars": {name: _serialize_scalar(value) for name, value in mirror.scalars.items()},
                "arrays": {name: serialize_array(array) for name, array in mirror.arrays.items()},
            }

        snapshot: Dict[str, List[Any]] = {
            "labels": [_serialize_label(lbl) for lbl in self.labels],
            "lines": [_serialize_line(ln) for ln in self.lines],
            "boxes": [_serialize_box(bx) for bx in self.boxes],
            "bar_colors": [
                {"time": time_val, "color": color}
                for time_val, color in self.bar_colors
            ],
            "condition_trace": [
                {
                    "name": evaluation.spec.name,
                    "pine_expression": evaluation.spec.pine_expression,
                    "result": evaluation.result,
                    "time": evaluation.timestamp,
                }
                for evaluation in self.condition_trace
            ],
            "pullback_labels": serialize_array(getattr(self, "arrHLLabel", PineArray())),
            "pullback_circles": serialize_array(getattr(self, "arrHLCircle", PineArray())),
            "structure_idm_labels": serialize_array(getattr(self, "arrIdmLabel", PineArray())),
            "structure_idm_lines": serialize_array(getattr(self, "arrIdmLine", PineArray())),
            "structure_break_labels": serialize_array(getattr(self, "arrBCLabel", PineArray())),
            "structure_break_lines": serialize_array(getattr(self, "arrBCLine", PineArray())),
            "demand_zones": serialize_array(getattr(self, "demandZone", PineArray())),
            "supply_zones": serialize_array(getattr(self, "supplyZone", PineArray())),
            "order_flow_major_bull": serialize_array(getattr(self, "arrOBBullm", PineArray())),
            "order_flow_major_bear": serialize_array(getattr(self, "arrOBBearm", PineArray())),
            "order_flow_minor_bull": serialize_array(getattr(self, "arrOBBulls", PineArray())),
            "order_flow_minor_bear": serialize_array(getattr(self, "arrOBBears", PineArray())),
            "fvg_bullish_boxes": serialize_array(getattr(self, "bullish_gap_holder", PineArray())),
            "fvg_bullish_fill": serialize_array(getattr(self, "bullish_gap_fill_holder", PineArray())),
            "fvg_bullish_mid_lines": serialize_array(getattr(self, "bullish_mid_holder", PineArray())),
            "fvg_bullish_high_lines": serialize_array(getattr(self, "bullish_high_holder", PineArray())),
            "fvg_bullish_low_lines": serialize_array(getattr(self, "bullish_low_holder", PineArray())),
            "fvg_bullish_labels": serialize_array(getattr(self, "bullish_label_holder", PineArray())),
            "fvg_bearish_boxes": serialize_array(getattr(self, "bearish_gap_holder", PineArray())),
            "fvg_bearish_fill": serialize_array(getattr(self, "bearish_gap_fill_holder", PineArray())),
            "fvg_bearish_mid_lines": serialize_array(getattr(self, "bearish_mid_holder", PineArray())),
            "fvg_bearish_high_lines": serialize_array(getattr(self, "bearish_high_holder", PineArray())),
            "fvg_bearish_low_lines": serialize_array(getattr(self, "bearish_low_holder", PineArray())),
            "fvg_bearish_labels": serialize_array(getattr(self, "bearish_label_holder", PineArray())),
            "liquidity_high_lines": serialize_array(getattr(self, "liquidity_high_lines", PineArray())),
            "liquidity_low_lines": serialize_array(getattr(self, "liquidity_low_lines", PineArray())),
            "liquidity_high_boxes": serialize_array(getattr(self, "liquidity_high_boxes", PineArray())),
            "liquidity_low_boxes": serialize_array(getattr(self, "liquidity_low_boxes", PineArray())),
            "scob_bar_colors": [
                {"time": time_val, "color": color}
                for time_val, color in self.bar_colors
            ],
        }

        snapshot["pullback_state"] = serialize_state(self.pullback_state)
        snapshot["market_structure_state"] = serialize_state(self.market_structure_state)
        snapshot["swing_state"] = serialize_state(self.swing_state)
        snapshot["order_block_state"] = serialize_state(self.order_block_state)
        snapshot["scob_state"] = serialize_state(self.scob_state)
        snapshot["structure_state_flags"] = {
            "lstHlPrsIdm": _serialize_scalar(getattr(self, "lstHlPrsIdm", NA)),
            "lstHlPrs": _serialize_scalar(getattr(self, "lstHlPrs", NA)),
            "bxf_direction": getattr(self, "bxty", 0),
        }

        return snapshot

    # ------------------------------------------------------------------
    # Indicator execution
    # ------------------------------------------------------------------
    def process(self, candles: Sequence[Dict[str, float]]) -> None:
        self.condition_trace.clear()
        for candle in candles:
            self.series.append(candle)
            self._trace(
                "process",
                "append_candle",
                timestamp=self.series.get_time(0),
                open=candle["open"],
                high=candle["high"],
                low=candle["low"],
                close=candle["close"],
                volume=candle.get("volume"),
            )
            if not self.initialised:
                self._initialise_state()
            self._update_bar()

    # ------------------------------------------------------------------
    # State initialisation mirroring Pine ``var`` assignments
    # ------------------------------------------------------------------
    def _initialise_state(self) -> None:
        # Basic cached references to series values
        high = self.series.get("high")
        low = self.series.get("low")
        close = self.series.get("close")
        open_ = self.series.get("open")
        time_val = self.series.get_time()
        prev_time = self.series.get_time(1)

        # High/low storage ----------------------------------------------------
        self.puHigh = high
        self.puLow = low
        self.puHigh_ = high
        self.puLow_ = low
        self.L = low
        self.H = high
        self.idmLow = low
        self.idmHigh = high
        self.lastH = high
        self.lastL = low
        self.H_lastH = high
        self.L_lastHH = low
        self.H_lastLL = high
        self.L_lastL = low
        self.motherHigh = self.series.get("high", 1)
        self.motherLow = self.series.get("low", 1)
        self._trace("initialise", "high_low", timestamp=time_val, high=high, low=low)

        # Bar indices ---------------------------------------------------------
        self.motherBar = prev_time
        self.puBar = None
        self.puHBar = time_val
        self.puLBar = time_val
        self.idmLBar = time_val
        self.idmHBar = time_val
        self.HBar = time_val
        self.LBar = time_val
        self.lastHBar = time_val
        self.lastLBar = time_val
        self._trace("initialise", "bars", timestamp=time_val, motherBar=self.motherBar, HBar=self.HBar, LBar=self.LBar)

        # Swing detection and sweep structures --------------------------------
        self.bullSignalIndex = 0
        self.bearSignalIndex = 0
        self.bullLine: Optional[Line] = None
        self.bearLine: Optional[Line] = None
        self.highLine: Optional[Line] = None
        self.lowLine: Optional[Line] = None
        self.swingHighLbl: Optional[Label] = None
        self.swingLowLbl: Optional[Label] = None
        self.swingHighLblTxt: Optional[Label] = None
        self.swingLowLblTxt: Optional[Label] = None
        self.swingLowVal: float = NA
        self.swingHighVal: float = NA
        self.swingLowCounter = 0
        self.swingHighCounter = 0
        self.isSwingLowCheck = False
        self.isSwingHighCheck = False
        self.stopPrintingLow = False
        self.stopPrintingHigh = False
        self.swingHighArr = PineArray()
        self.swingHighTextArr = PineArray()
        self.swingLowArr = PineArray()
        self.swingLowTextArr = PineArray()
        self.bullishSFP_history: List[bool] = [False, False, False, False]
        self.bearishSFP_history: List[bool] = [False, False, False, False]
        self.pLowVal_history: List[float] = [NA]
        self._trace("initialise", "swing", timestamp=time_val)
        self.pHighVal_history: List[float] = [NA]

        # ZigZag channels ------------------------------------------------------
        self.zigzag_valtop: float = NA
        self.zigzag_valbtm: float = NA
        self.zigzag_os = 0
        self.zigzag_last_top_time: Optional[int] = None
        self.zigzag_last_btm_time: Optional[int] = None
        self._trace("initialise", "zigzag", timestamp=time_val)

        # Support & resistance -------------------------------------------------
        self.sr_levels: List[SupportResistanceLevel] = []
        self.sr_history: List[SupportResistanceLevel] = []
        self.sr_last_cleanup = 0
        self.sr_touch_atr_ratio = 1.0 / 30.0
        self.sr_retest_atr_ratio = 1.0 / 30.0
        self.sr_label_offset_x = 30
        self.sr_retest_spacing = 3
        self.sr_max_traverse = 250
        self.sr_max_retest_labels = 100
        sr_inputs = self.inputs.support_resistance
        self.sr_max_pivots_allowed = 7 if sr_inputs.memoryOptimizatonEnabled else 15
        self._trace("initialise", "support_resistance", timestamp=time_val, max_levels=self.sr_max_pivots_allowed)
        self.sr_timeframes = [
            (1, sr_inputs.timeframe1_, sr_inputs.timeframe1Enabled),
            (2, sr_inputs.timeframe2, sr_inputs.timeframe2Enabled),
            (3, sr_inputs.timeframe3, sr_inputs.timeframe3Enabled),
        ]
        self.sr_cluster_cache: Dict[str, List[SupportResistanceLevel]] = {}
        self.sr_pivot_store: Dict[str, Dict[str, List[CustomPoint]]] = {}

        # Candle pattern metrics ----------------------------------------------
        body = abs(close - open_) if not math.isnan(close) and not math.isnan(open_) else 0.0
        self.candle_body_avg = body
        prev_black = (not math.isnan(open_) and not math.isnan(close) and open_ > close)
        prev_white = (not math.isnan(open_) and not math.isnan(close) and open_ < close)
        self.candle_black_body_history: List[bool] = [prev_black]
        self.candle_white_body_history: List[bool] = [prev_white]
        self.candle_small_body_history: List[bool] = [False]

        # Structure confirmation ----------------------------------------------
        self.mnStrc: Optional[bool] = None
        self.prevMnStrc: Optional[bool] = None
        self.isPrevBos: Optional[bool] = None
        self.findIDM = False
        self.isBosUp = False
        self.isBosDn = False
        self.isCocUp = True
        self.isCocDn = True

        # POI storage ---------------------------------------------------------
        self.isSweepOBS = False
        self.current_OBS: Optional[int] = None
        self.high_MOBS: Optional[float] = None
        self.low_MOBS: Optional[float] = None
        self.isSweepOBD = False
        self.current_OBD: Optional[int] = None
        self.low_MOBD: Optional[float] = None
        self.high_MOBD: Optional[float] = None

        # Arrays --------------------------------------------------------------
        self.arrTopBotBar = PineArray([time_val])
        self.arrTop = PineArray([high])
        self.arrBot = PineArray([low])
        self.arrPbHBar = PineArray()
        self.arrPbHigh = PineArray()
        self.arrPbLBar = PineArray()
        self.arrPbLow = PineArray()
        self.demandZone = PineArray()
        self.supplyZone = PineArray()
        self.supplyZoneIsMit = PineArray()
        self.demandZoneIsMit = PineArray()
        self.hist_idm_boxes = PineArray()
        self.hist_ext_boxes = PineArray()
        self.arrIdmHigh = PineArray()
        self.arrIdmLow = PineArray()
        self.arrIdmHBar = PineArray()
        self.arrIdmLBar = PineArray()
        self.arrLastH = PineArray()
        self.arrLastHBar = PineArray()
        self.arrLastL = PineArray()
        self.arrLastLBar = PineArray()
        self.arrIdmLine = PineArray()
        self.arrIdmLabel = PineArray()
        self.arrBCLine = PineArray()
        self.arrBCLabel = PineArray()
        self.arrHLLabel = PineArray()
        self.arrHLCircle = PineArray()
        self.arrPrevPrsMin = PineArray([0.0])
        self.arrPrevIdxMin = PineArray([0])
        self.arrlstHigh = PineArray([0.0])
        self.arrlstLow = PineArray([0.0])
        self.arrOBTstdo = PineArray()
        self.arrOBTstd = PineArray()
        self.arrOBTstdTy = PineArray()
        self.arrOBBullm = PineArray()
        self.arrOBBearm = PineArray()
        self.arrOBBullisVm = PineArray()
        self.arrOBBearisVm = PineArray()
        self.arrOBBulls = PineArray()
        self.arrOBBears = PineArray()
        self.arrOBBullisVs = PineArray()
        self.arrOBBearisVs = PineArray()
        self.arrPrevPrs = PineArray([0.0])
        self.arrPrevIdx = PineArray([0])

        self.highLineArrayHTF = PineArray()
        self.lowLineArrayHTF = PineArray()
        self.highBoxArrayHTF = PineArray()
        self.lowBoxArrayHTF = PineArray()

        self.ob_top = PineArray()
        self.ob_btm = PineArray()
        self.ob_left = PineArray()
        self.ob_type = PineArray()
        self.ob_vol = PineArray()
        self.ob_buy_vol = PineArray()
        self.ob_sell_vol = PineArray()

        self.ob_top_mtf = PineArray()
        self.ob_btm_mtf = PineArray()
        self.ob_left_mtf = PineArray()
        self.ob_type_mtf = PineArray()
        self.ob_vol_mtf = PineArray()
        self.ob_buy_vol_mtf = PineArray()
        self.ob_sell_vol_mtf = PineArray()

        self.ob_boxes = PineArray()
        self.ob_lines = PineArray()
        self.ob_boxes_mtf = PineArray()
        self.ob_lines_mtf = PineArray()

        self.bullish_gap_holder = PineArray()
        self.bullish_gap_fill_holder = PineArray()
        self.bullish_mid_holder = PineArray()
        self.bullish_high_holder = PineArray()
        self.bullish_low_holder = PineArray()
        self.bullish_label_holder = PineArray()

        self.bearish_gap_holder = PineArray()
        self.bearish_gap_fill_holder = PineArray()
        self.bearish_mid_holder = PineArray()
        self.bearish_high_holder = PineArray()
        self.bearish_low_holder = PineArray()
        self.bearish_label_holder = PineArray()

        self.liquidity_high_lines = PineArray()
        self.liquidity_low_lines = PineArray()
        self.liquidity_high_boxes = PineArray()
        self.liquidity_low_boxes = PineArray()

        self.ict_prev_state: Dict[int, int] = {}
        self.ict_prev_state_prev: Dict[int, int] = {}
        self.t_MS = 0
        self.int_t_MS = 0
        self.internal_y_up = NA
        self.internal_x_up = time_val
        self.internal_y_dn = NA
        self.internal_x_dn = time_val
        self.y_up = NA
        self.x_up = time_val
        self.y_dn = NA
        self.x_dn = time_val
        self.crossed_up = True
        self.crossed_down = True
        self.internal_up_broke = True
        self.internal_dn_broke = True
        self.up_trailing = high
        self.down_trailing = low
        self.up_trailing_x = time_val
        self.down_trailing_x = time_val
        self.high_eqh_pre = NA
        self.low_eqh_pre = NA
        self.eq_top_x = time_val
        self.eq_btm_x = time_val
        self.eqh_lines = PineArray()
        self.eqh_labels = PineArray()
        self.eql_lines = PineArray()
        self.eql_labels = PineArray()
        self.prevHigh_s = NA
        self.prevLow_s = NA
        self.prevHighIndex_s: Optional[int] = None
        self.prevLowIndex_s: Optional[int] = None
        self.prevSwing_s = 0

        self.key_level_objects: Dict[str, Tuple[Optional[Line], Optional[Label]]] = {}
        self.untested_monday = False
        self.monday_time = time_val
        self.monday_high = high
        self.monday_low = low
        self.monday_mid = (high + low) / 2.0
        self.weekly_time_marker: Optional[int] = None

        def _session_template() -> Dict[str, Any]:
            return {
                "active": False,
                "high": 0.0,
                "low": close,
                "open": close,
                "start": time_val,
                "final_high": 0.0,
                "final_low": 0.0,
                "final_open": 0.0,
            }

        self.session_states: Dict[str, Dict[str, Any]] = {
            "london": _session_template(),
            "us": _session_template(),
            "asia": _session_template(),
        }

        self.session_objects: Dict[str, Tuple[Optional[Line], Optional[Label]]] = {}

        self.arrmitOBBulla = PineArray()
        self.arrmitOBBull = PineArray()
        self.arrmitOBBeara = PineArray()
        self.arrmitOBBear = PineArray()

        self.lstHlPrs: float = NA
        self.lstHlPrsIdm: float = NA
        self.lstBxIdm: Optional[Box] = None
        self.lstBx: Optional[Box] = None

        self.lstHlPrs_history: List[float] = []

        self.mergeRatio = 0.1
        self.maxBarHistory = 2000
        self.dayTf = 24 * 60 * 60 * 1000
        diff = abs(time_val - prev_time) if prev_time else 0
        self.curTf = diff if diff > 0 else 60 * 60 * 1000
        self.i_loop = max(int((2 * self.dayTf) / max(self.curTf, 1)), 1)
        self.len = max(self.curTf, 1)
        self.colorTP = "color.new(color.purple,0)"
        self.current_day = (time_val // self.dayTf) if time_val else None
        self.day_high = high
        self.day_low = low
        self.prev_day_high = high
        self.prev_day_low = low
        self.pdh = high
        self.pdl = low

        if self.base_tf_seconds is None:
            if prev_time and time_val:
                diff_seconds = int(abs(time_val - prev_time) // 1000)
                self.base_tf_seconds = diff_seconds if diff_seconds > 0 else 60
            else:
                self.base_tf_seconds = 60

        self.idm_label: Optional[Label] = None
        self.idm_line: Optional[Line] = None
        self.choch_label: Optional[Label] = None
        self.choch_line: Optional[Line] = None
        self.bos_label: Optional[Label] = None
        self.bos_line: Optional[Line] = None
        self.pdh_line: Optional[Line] = None
        self.pdh_label: Optional[Label] = None
        self.pdl_line: Optional[Line] = None
        self.pdl_label: Optional[Label] = None
        self.mid_line: Optional[Line] = None
        self.mid_label: Optional[Label] = None
        self.mid_line1: Optional[Line] = None
        self.mid_label1: Optional[Label] = None
        self.mid_line2: Optional[Line] = None
        self.mid_label2: Optional[Label] = None

        self.transp = "color.new(color.gray,100)"
        self.bxf: Optional[Box] = None
        self.bxty = 0
        self.prev_oi1: float = NA

        self.motherHigh_history: List[float] = [self.motherHigh]
        self.motherLow_history: List[float] = [self.motherLow]
        self.motherBar_history: List[int] = [self.motherBar]
        self.isb_history: List[bool] = []

        self.initialised = True
        self.time_history = [time_val]
        self.timediff_value = float(self.curTf)
        self.htfH = self.series.get("close")
        self.htfL = self.series.get("close")
        self.last_liq_high_time = None
        self.last_liq_low_time = None
        self.bullish_OB_Break = False
        self.bearish_OB_Break = False
        self.prev_close = close
        self._sync_state_mirrors()

    # ------------------------------------------------------------------
    # Helper retrieval functions (1:1 with Pine)
    # ------------------------------------------------------------------
    def getDirection(self, trend: bool, HBar: int, LBar: int, H: float, L: float) -> Tuple[int, float]:
        x = HBar if trend else LBar
        y = H if trend else L
        return x, y

    def getTextLabel(self, current: float, last: float, same: str, diff: str) -> str:
        return same if current > last else diff

    def getStyleLabel(self, trend: bool) -> str:
        return "label.style_label_down" if trend else "label.style_label_up"

    def getStyleArrow(self, trend: bool) -> str:
        return "label.style_arrowdown" if trend else "label.style_arrowup"

    def getYloc(self, trend: bool) -> str:
        return "yloc.abovebar" if trend else "yloc.belowbar"

    def textCenter(self, left: int, right: int) -> int:
        return int((left + right) / 2)

    def isGreenBar(self, offset: int = 0) -> bool:
        return self.series.get("close", offset) > self.series.get("open", offset)

    def _map_line_style(self, style: str) -> str:
        if style == "Dashed":
            return "line.style_dashed"
        if style == "Dotted":
            return "line.style_dotted"
        return "line.style_solid"

    def _map_label_size(self, setting: str) -> str:
        mapping = {
            "Small": "size.tiny",
            "Medium": "size.small",
            "Large": "size.normal",
            "Medium2": "size.normal",
            "Large2": "size.large",
        }
        return mapping.get(setting, "size.huge")

    def _key_label_size(self, setting: str) -> str:
        mapping = {
            "Small": "size.small",
            "Medium": "size.normal",
            "Large": "size.large",
        }
        return mapping.get(setting, "size.small")

    def _line_width_from_setting(self, setting: str) -> int:
        if setting == "Medium":
            return 2
        if setting == "Large":
            return 3
        return 1

    def _true_range_at(self, offset: int) -> float:
        high = self.series.get("high", offset)
        low = self.series.get("low", offset)
        prev_close = self.series.get("close", offset + 1)
        if math.isnan(high) or math.isnan(low):
            return 0.0
        if math.isnan(prev_close):
            return high - low
        return max(high - low, abs(high - prev_close), abs(low - prev_close))

    def _sr_timeframe_key(self, timeframe: str) -> str:
        return timeframe if timeframe not in (None, "") else "__base__"

    def _sr_format_timeframe(self, timeframe: str) -> str:
        if timeframe in (None, "", "__base__"):
            return self.base_timeframe or "Current"
        tf = str(timeframe).strip().upper()
        if not tf:
            return self.base_timeframe or "Current"

        def _num(part: str) -> Optional[float]:
            part = (part or "").strip()
            if not part:
                return None
            try:
                return float(part)
            except (TypeError, ValueError):
                return None

        if tf.endswith("H"):
            hours = _num(tf[:-1])
            if hours is None:
                return self.base_timeframe or "Current"
            if hours.is_integer():
                return f"{int(hours)} Hour" + ("s" if hours != 1 else "")
            return f"{hours} Hour"
        if tf.endswith("D"):
            days = _num(tf[:-1])
            if days is None:
                return self.base_timeframe or "Current"
            if days.is_integer():
                return f"{int(days)} Day" + ("s" if days != 1 else "")
            return f"{days} Day"
        if tf.endswith("W"):
            weeks = _num(tf[:-1])
            if weeks is None:
                return self.base_timeframe or "Current"
            if weeks.is_integer():
                return f"{int(weeks)} Week" + ("s" if weeks != 1 else "")
            return f"{weeks} Week"
        if tf.endswith("M"):
            months = _num(tf[:-1])
            if months is None:
                return self.base_timeframe or "Current"
            if months.is_integer():
                return f"{int(months)} Month" + ("s" if months != 1 else "")
            return f"{months} Month"
        if tf.endswith("S"):
            seconds = _num(tf[:-1])
            if seconds is None:
                return self.base_timeframe or "Current"
            if seconds >= 60:
                minutes = seconds / 60.0
                if minutes.is_integer():
                    return f"{int(minutes)} Min"
                return f"{minutes} Min"
            return f"{int(seconds)} Sec"
        if tf.isdigit():
            return f"{int(tf)} Min"
        return str(timeframe)

    def _sr_map_line_style(self, setting: str) -> str:
        if setting == "----":
            return "line.style_dashed"
        if setting == "....":
            return "line.style_dotted"
        return "line.style_solid"

    def _sr_zone_width_percent(self, price: float, sr: SupportResistanceInputs) -> float:
        if sr.zoneWidthType == "Dynamic":
            atr_val = self._atr(30)
            if price == 0:
                return 0.0
            return ((atr_val) / price) * 100.0 / 3.0
        mapping = {1: 0.05, 2: 0.06, 3: 0.075}
        return mapping.get(sr.zoneWidth, 0.05)

    def _sr_zone_bounds(self, price: float, sr: SupportResistanceInputs) -> Tuple[float, float]:
        percent = self._sr_zone_width_percent(price, sr)
        top = price * (1.0 + percent / 2.0 / 100.0)
        bottom = price * (1.0 - percent / 2.0 / 100.0)
        return top, bottom

    def _sr_collect_base_pivots(self, pivot_range: int) -> Dict[str, List[CustomPoint]]:
        highs: List[CustomPoint] = []
        lows: List[CustomPoint] = []
        total = len(self.series.high)
        if total < pivot_range * 2 + 1:
            return {"high": highs, "low": lows}
        for idx in range(pivot_range, total - pivot_range):
            price_high = self.series.high[idx]
            price_low = self.series.low[idx]
            is_high_pivot = True
            is_low_pivot = True
            for look in range(1, pivot_range + 1):
                if self.series.high[idx - look] >= price_high or self.series.high[idx + look] > price_high:
                    is_high_pivot = False
                if self.series.low[idx - look] <= price_low or self.series.low[idx + look] < price_low:
                    is_low_pivot = False
                if not is_high_pivot and not is_low_pivot:
                    break
            time_val = int(self.series.time[idx])
            prev_close = self.series.close[idx - 1] if idx > 0 else self.series.close[idx]
            tr_val = max(
                self.series.high[idx] - self.series.low[idx],
                abs(self.series.high[idx] - prev_close),
                abs(self.series.low[idx] - prev_close),
            )
            if is_high_pivot:
                highs.append(CustomPoint(time_val, price_high, tr_val))
            if is_low_pivot:
                lows.append(CustomPoint(time_val, price_low, tr_val))
        highs = highs[-self.sr_max_pivots_allowed :]
        lows = lows[-self.sr_max_pivots_allowed :]
        return {"high": highs, "low": lows}

    def _sr_collect_timeframe_pivots(
        self, timeframe: str, pivot_range: int
    ) -> Dict[str, List[CustomPoint]]:
        key = self._sr_timeframe_key(timeframe)
        feed = self._ensure_security_feed(timeframe)
        if feed is None:
            return self._sr_collect_base_pivots(pivot_range)
        highs: List[CustomPoint] = []
        lows: List[CustomPoint] = []
        length = len(feed.final_time)
        if length < pivot_range * 2 + 1:
            return {"high": highs, "low": lows}
        for idx in range(pivot_range, length - pivot_range):
            price_high = feed.final_high[idx]
            price_low = feed.final_low[idx]
            is_high_pivot = True
            is_low_pivot = True
            for look in range(1, pivot_range + 1):
                if feed.final_high[idx - look] >= price_high or feed.final_high[idx + look] > price_high:
                    is_high_pivot = False
                if feed.final_low[idx - look] <= price_low or feed.final_low[idx + look] < price_low:
                    is_low_pivot = False
                if not is_high_pivot and not is_low_pivot:
                    break
            prev_close = feed.final_close[idx - 1] if idx > 0 else feed.final_close[idx]
            tr_val = max(
                feed.final_high[idx] - feed.final_low[idx],
                abs(feed.final_high[idx] - prev_close),
                abs(feed.final_low[idx] - prev_close),
            )
            time_val = int(feed.final_time[idx])
            if is_high_pivot:
                highs.append(CustomPoint(time_val, price_high, tr_val))
            if is_low_pivot:
                lows.append(CustomPoint(time_val, price_low, tr_val))
        highs = highs[-self.sr_max_pivots_allowed :]
        lows = lows[-self.sr_max_pivots_allowed :]
        store = {"high": highs, "low": lows}
        self.sr_pivot_store[key] = store
        return store

    def _sr_cluster_points(
        self, points: List[CustomPoint], sr: SupportResistanceInputs
    ) -> List[Dict[str, Any]]:
        clusters: List[Dict[str, Any]] = []
        for point in sorted(points, key=lambda p: p.time):
            placed = False
            for cluster in clusters:
                tolerance = max(cluster["avg_tr"], point.tr) * self.sr_touch_atr_ratio
                if abs(point.price - cluster["price"]) <= tolerance:
                    cluster["points"].append(point)
                    cluster["sum_price"] += point.price
                    cluster["price"] = cluster["sum_price"] / len(cluster["points"])
                    count = len(cluster["points"])
                    cluster["avg_tr"] = (cluster["avg_tr"] * (count - 1) + point.tr) / count
                    placed = True
                    break
            if not placed:
                clusters.append(
                    {
                        "price": point.price,
                        "points": [point],
                        "sum_price": point.price,
                        "avg_tr": point.tr,
                    }
                )
        valid = [c for c in clusters if len(c["points"]) >= sr.strength]
        valid.sort(key=lambda c: c["points"][-1].time, reverse=True)
        return valid[: sr.resistanceSupportCount]

    def _sr_dispose_level(self, level: SupportResistanceLevel) -> None:
        if level.line is not None:
            self._delete_line(level.line)
            level.line = None
        if level.box is not None:
            self._delete_box(level.box)
            level.box = None
        if level.price_label is not None:
            self._delete_label(level.price_label)
            level.price_label = None
        if level.break_label is not None:
            self._delete_label(level.break_label)
            level.break_label = None
        if level.break_line is not None:
            self._delete_line(level.break_line)
            level.break_line = None
        if level.break_box is not None:
            self._delete_box(level.break_box)
            level.break_box = None
        if level.retest_labels:
            for lbl in level.retest_labels:
                self._delete_label(lbl)
            level.retest_labels = []
        if level in self.sr_levels:
            self.sr_levels.remove(level)
        self.sr_history.append(level)

    def _sr_update_level_visuals(
        self,
        level: SupportResistanceLevel,
        sr: SupportResistanceInputs,
        cluster: Dict[str, Any],
    ) -> None:
        color = sr.resistanceColor if level.rs_type == "Resistance" else sr.supportColor
        start_time = min(point.time for point in level.points)
        end_time = self.series.get_time()
        label_text = f"{self._sr_format_timeframe(level.timeframe)} | {level.price:.5f}"
        if sr.enableZones:
            top, bottom = self._sr_zone_bounds(level.price, sr)
            if level.box is None:
                level.box = self.box_new(start_time, end_time, top, bottom, color)
            level.box.set_lefttop(start_time, top)
            level.box.set_rightbottom(end_time, bottom)
            level.box.set_bgcolor(color)
            level.box.set_border_color(color)
            level.box.set_extend("extend.both" if sr.expandLines else "extend.right")
            level.box.set_text(label_text if not level.is_broken else "")
            level.box.set_text_color(sr.textColor)
        else:
            style = self._sr_map_line_style(sr.lineStyle_)
            if level.line is None:
                level.line = self.line_new(start_time, level.price, end_time, level.price, "xloc.bar_time", color, style)
            level.line.set_color(color)
            level.line.set_style(style)
            level.line.set_width(sr.lineWidth)
            level.line.set_xy1(start_time, level.price)
            level.line.set_xy2(end_time, level.price)
            level.line.set_extend("extend.both" if sr.expandLines else "extend.right")
        label_size = self._key_label_size(sr.labelsize)
        if sr.labelsAlign == "Center":
            label_x = int((start_time + end_time) / 2)
        else:
            label_x = self._extend_time(self.sr_label_offset_x)
        if level.price_label is None:
            level.price_label = self.label_new(
                label_x,
                level.price,
                "" if sr.enableZones else label_text,
                "xloc.bar_time",
                "yloc.price",
                "#00000000",
                "label.style_none",
                label_size,
                sr.textColor,
            )
        level.price_label.set_xy(label_x, level.price)
        level.price_label.set_text("" if sr.enableZones else label_text)
        level.price_label.set_size(label_size)
        level.price_label.set_textcolor(sr.textColor)
        level.price_label.set_color("#00000000")
        level.price_label.set_xloc(label_x, "xloc.bar_time")

    def _sr_handle_breaks_and_retests(
        self, level: SupportResistanceLevel, sr: SupportResistanceInputs
    ) -> None:
        if not level.points:
            return
        current_time = self.series.get_time()
        close = self.series.get("close")
        prev_close = self.series.get("close", 1)
        high = self.series.get("high")
        low = self.series.get("low")
        tr = level.points[-1].tr if level.points else 0.0
        tolerance = tr * self.sr_touch_atr_ratio
        bar_index = self.series.length() - 1
        if not level.is_broken:
            is_break = False
            if level.rs_type == "Resistance":
                is_break = (
                    not math.isnan(prev_close)
                    and not math.isnan(close)
                    and prev_close <= level.price
                    and close > level.price
                )
            else:
                is_break = (
                    not math.isnan(prev_close)
                    and not math.isnan(close)
                    and prev_close >= level.price
                    and close < level.price
                )
            if is_break:
                level.is_broken = True
                level.broken_time = current_time
                level.break_level = close
                level.break_tr = high - low if not (math.isnan(high) or math.isnan(low)) else 0.0
                if sr.showBreaks:
                    color = sr.resistanceColor if level.rs_type == "Resistance" else sr.supportColor
                    delta = level.break_tr / 1.5 if level.break_tr != 0 else tr / 1.5
                    y = level.price + (delta if level.rs_type == "Resistance" else -delta)
                    style = "label.style_label_up" if level.rs_type == "Resistance" else "label.style_label_down"
                    level.break_label = self.label_new(
                        current_time,
                        y,
                        "B",
                        "xloc.bar_time",
                        "yloc.price",
                        color,
                        style,
                        "size.tiny",
                        "color.white",
                    )
                new_color = sr.supportColor if level.rs_type == "Resistance" else sr.resistanceColor
                if not sr.inverseBrokenLineColor:
                    new_color = sr.resistanceColor if level.rs_type == "Resistance" else sr.supportColor
                if sr.enableZones and level.box is not None:
                    level.box.set_bgcolor(new_color)
                    level.box.set_border_color(new_color)
                if level.line is not None:
                    level.line.set_color(new_color)
                if sr.enableBreakAlerts and level.last_break_alert_time != current_time:
                    level.last_break_alert_time = current_time
            else:
                touch = False
                if level.rs_type == "Resistance":
                    touch = (
                        (not math.isnan(close) and abs(close - level.price) <= tolerance)
                        or (not math.isnan(high) and abs(high - level.price) <= tolerance)
                    )
                else:
                    touch = (
                        (not math.isnan(close) and abs(close - level.price) <= tolerance)
                        or (not math.isnan(low) and abs(low - level.price) <= tolerance)
                    )
                if touch and sr.showRetests:
                    if level.last_retest_bar is None or bar_index - level.last_retest_bar >= self.sr_retest_spacing:
                        delta = level.points[-1].tr / 1.5 if level.points[-1].tr != 0 else tr / 1.5
                        y = level.price + (delta if level.rs_type == "Resistance" else -delta)
                        style = "label.style_label_up" if level.rs_type == "Resistance" else "label.style_label_down"
                        color = sr.resistanceColor if level.rs_type == "Resistance" else sr.supportColor
                        lbl = self.label_new(
                            current_time,
                            y,
                            "R",
                            "xloc.bar_time",
                            "yloc.price",
                            color,
                            style,
                            "size.tiny",
                            "color.white",
                        )
                        level.retest_labels.append(lbl)
                        if len(level.retest_labels) > self.sr_max_retest_labels:
                            old = level.retest_labels.pop(0)
                            self._delete_label(old)
                        level.last_retest_bar = bar_index
                        level.last_retest_time = current_time
                        if sr.enableRetestAlerts and level.last_retest_alert_time != current_time:
                            level.last_retest_alert_time = current_time
        else:
            if sr.expandLines:
                if sr.enableZones and level.box is not None:
                    level.box.set_right(self.series.get_time())
                    level.box.set_extend("extend.both")
                if level.line is not None:
                    level.line.set_x2(self.series.get_time())
                    level.line.set_extend("extend.both")

    def _sr_refresh_levels(
        self,
        timeframe_key: str,
        rs_type: str,
        points: List[CustomPoint],
        sr: SupportResistanceInputs,
    ) -> None:
        clusters = self._sr_cluster_points(points, sr)
        existing = [lvl for lvl in self.sr_levels if lvl.timeframe == timeframe_key and lvl.rs_type == rs_type]
        updated: List[SupportResistanceLevel] = []
        for cluster in clusters:
            level: Optional[SupportResistanceLevel] = None
            for candidate in existing:
                candidate_tr = candidate.points[-1].tr if candidate.points else cluster["avg_tr"]
                tolerance = max(candidate_tr, cluster["avg_tr"]) * self.sr_touch_atr_ratio
                if abs(candidate.price - cluster["price"]) <= tolerance:
                    level = candidate
                    break
            if level is None:
                level = SupportResistanceLevel(rs_type=rs_type, timeframe=timeframe_key, price=cluster["price"])
                self.sr_levels.append(level)
            level.price = cluster["price"]
            level.points = list(cluster["points"])
            self._sr_update_level_visuals(level, sr, cluster)
            self._sr_handle_breaks_and_retests(level, sr)
            updated.append(level)
        for candidate in existing:
            if candidate not in updated:
                self._sr_dispose_level(candidate)

    def _sr_cleanup_levels(self, sr: SupportResistanceInputs) -> None:
        # Remove excessive history entries
        if len(self.sr_history) > sr.debug_maxHistoryRecords:
            overflow = len(self.sr_history) - sr.debug_maxHistoryRecords
            for _ in range(overflow):
                level = self.sr_history.pop(0)
                # already disposed


    def _pivot_base_series(self, left: int, right: int, is_high: bool) -> Optional[Tuple[int, float]]:
        if left + right + 1 > self.series.length():
            return None
        value = self.series.get("high" if is_high else "low", right)
        if math.isnan(value):
            return None
        for i in range(1, left + 1):
            comp = self.series.get("high" if is_high else "low", right + i)
            if math.isnan(comp):
                return None
            if (is_high and comp >= value) or ((not is_high) and comp <= value):
                return None
        for i in range(1, right + 1):
            comp = self.series.get("high" if is_high else "low", right - i)
            if math.isnan(comp):
                return None
            if (is_high and comp > value) or ((not is_high) and comp < value):
                return None
        return self.series.get_time(right), value

    def _pivot_point(self, left: int, right: int, is_low: bool) -> Optional[Tuple[int, float]]:
        if left + right + 1 > self.series.length():
            return None
        series_name = "low" if is_low else "high"
        pivot_value = self.series.get(series_name, right)
        if math.isnan(pivot_value):
            return None
        for i in range(1, left + 1):
            comp = self.series.get(series_name, right + i)
            if math.isnan(comp):
                return None
            if is_low:
                if comp <= pivot_value:
                    return None
            else:
                if comp >= pivot_value:
                    return None
        for i in range(1, right + 1):
            comp = self.series.get(series_name, right - i)
            if math.isnan(comp):
                return None
            if is_low:
                if comp < pivot_value:
                    return None
            else:
                if comp > pivot_value:
                    return None
        return self.series.get_time(right), pivot_value

    def _crossover(self, prev_value: float, current_value: float, target: float) -> bool:
        if math.isnan(target):
            return False
        return prev_value <= target and current_value > target

    def _crossunder(self, prev_value: float, current_value: float, target: float) -> bool:
        if math.isnan(target):
            return False
        return prev_value >= target and current_value < target

    def _combine_levels(
        self,
        prices: List[float],
        labels: List[Label],
        price: float,
        label: Label,
        color: str,
    ) -> None:
        for idx, existing_price in enumerate(prices):
            if math.isclose(existing_price, price, rel_tol=1e-9, abs_tol=1e-9):
                existing_label = labels[idx]
                new_text = label.text
                if new_text:
                    if existing_label.text:
                        existing_label.text = f"{new_text} / {existing_label.text}"
                    else:
                        existing_label.text = new_text
                existing_label.textcolor = color
                label.text = ""
                return
        prices.append(price)
        labels.append(label)

    def _update_level_visual(
        self,
        key: str,
        x1: int,
        x2: int,
        y: float,
        color: str,
        style: str,
        width: int,
        label_text: str,
        label_color: str,
        label_size: str,
    ) -> Optional[Label]:
        line_obj: Optional[Line]
        label_obj: Optional[Label]
        line_obj, label_obj = self.key_level_objects.get(key, (None, None))
        if line_obj is None:
            line_obj = self.line_new(x1, y, x2, y, "xloc.bar_time", color, style)
        line_obj.set_x1(x1)
        line_obj.set_x2(x2)
        line_obj.set_y1(y)
        line_obj.set_y2(y)
        line_obj.set_color(color)
        line_obj.set_style(style)
        line_obj.set_width(width)
        line_obj.set_extend("extend.none")

        if label_obj is None:
            label_obj = self.label_new(
                x2,
                y,
                label_text,
                "xloc.bar_time",
                "yloc.price",
                "#00000000",
                "label.style_label_left",
                label_size,
                label_color,
            )
        label_obj.x = x2
        label_obj.y = y
        label_obj.text = label_text
        label_obj.textcolor = label_color
        label_obj.size = label_size
        label_obj.color = "#00000000"
        label_obj.style = "label.style_none"
        self.key_level_objects[key] = (line_obj, label_obj)
        return label_obj

    def _parse_session(self, session: str) -> Tuple[int, int]:
        if "-" not in session:
            return (0, 0)
        start_str, end_str = session.split("-", 1)
        start_str = start_str.strip()
        end_str = end_str.strip()

        def _to_minutes(value: str) -> int:
            value = value.replace(":", "")
            if len(value) < 4:
                value = value.rjust(4, "0")
            hour = int(value[:2])
            minute = int(value[2:])
            return hour * 60 + minute

        return _to_minutes(start_str), _to_minutes(end_str)

    def _time_minutes(self, time_val: int) -> int:
        tm = time.gmtime(max(time_val, 0) // 1000)
        return tm.tm_hour * 60 + tm.tm_min

    def _time_in_session(self, time_val: int, session: str) -> bool:
        start_min, end_min = self._parse_session(session)
        current_min = self._time_minutes(time_val)
        if end_min >= start_min:
            return start_min <= current_min < end_min
        return current_min >= start_min or current_min < end_min

    def _show_ms(
        self,
        x: int,
        y: float,
        text: str,
        color: str,
        dashed: bool,
        down: bool,
        label_size: str,
    ) -> None:
        target_time = self.series.get_time()
        mid_x = int((x + target_time) / 2)
        lbl = self.label_new(
            mid_x,
            y,
            text,
            "xloc.bar_time",
            "yloc.price",
            "#00000000",
            "label.style_label_down" if down else "label.style_label_up",
            label_size,
            color,
        )
        lbl.color = "#00000000"
        ln = self.line_new(
            x,
            y,
            target_time,
            y,
            "xloc.bar_time",
            color,
            "line.style_dashed" if dashed else "line.style_solid",
        )
        ln.set_style("line.style_dashed" if dashed else "line.style_solid")
        ln.set_color(color)

    def _update_timediff(self, time_val: int) -> None:
        self.time_history.append(time_val)
        if len(self.time_history) > 150:
            self.time_history.pop(0)
        if len(self.time_history) > 100:
            diff = self.time_history[-1] - self.time_history[-101]
            if diff > 0:
                self.timediff_value = diff / 100.0
        elif len(self.time_history) > 1:
            diff = self.time_history[-1] - self.time_history[-2]
            if diff > 0:
                self.timediff_value = float(diff)
        if self.timediff_value == 0.0:
            self.timediff_value = float(self.curTf)

    def _timediff(self) -> float:
        return self.timediff_value if self.timediff_value > 0 else float(self.curTf)

    def _extend_time(self, length: int) -> int:
        return int(self.series.get_time() + self._timediff() * length)

    def _timeframe_bucket(self, timeframe: str) -> Optional[int]:
        seconds = _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds)
        if seconds is None or seconds <= 0:
            return None
        return int(self.series.get_time() // (seconds * 1000))

    def _is_newbar(self, timeframe: str) -> bool:
        bucket = self._timeframe_bucket(timeframe)
        key = timeframe or "__base"
        prev = self.security_bucket_tracker.get(key)
        self.security_bucket_tracker[key] = bucket
        if bucket is None:
            return False
        return prev is None or bucket != prev

    def getPdhlBar(self, value: float) -> int:
        x = 0
        loop_end = max(self.i_loop, 1)
        if math.isclose(value, getattr(self, "pdh", NA), rel_tol=1e-9, abs_tol=1e-9):
            for i in range(loop_end, 0, -1):
                if math.isclose(self.series.get("high", i), value):
                    x = self.series.get_time(i)
                    break
        else:
            for i in range(loop_end, 0, -1):
                if math.isclose(self.series.get("low", i), getattr(self, "pdl", NA)):
                    x = self.series.get_time(i)
                    break
        return x

    def _series_highest(self, series: SecuritySeries, name: str, length: int) -> float:
        best = -math.inf
        found = False
        for i in range(min(length, series.length())):
            value = series.get(name, i)
            if math.isnan(value):
                continue
            found = True
            if value > best:
                best = value
        return best if found else NA

    def _series_lowest(self, series: SecuritySeries, name: str, length: int) -> float:
        best = math.inf
        found = False
        for i in range(min(length, series.length())):
            value = series.get(name, i)
            if math.isnan(value):
                continue
            found = True
            if value < best:
                best = value
        return best if found else NA

    def _ema(self, prev: float, value: float, length: int) -> float:
        if math.isnan(value):
            return value
        if math.isnan(prev):
            return value
        if length <= 0:
            return value
        alpha = 2.0 / (length + 1.0)
        return prev + alpha * (value - prev)

    def _sma(self, series: str, length: int) -> float:
        if length <= 0:
            return math.nan
        values = []
        for i in range(length):
            value = self.series.get(series, i)
            if math.isnan(value):
                return math.nan
            values.append(value)
        if not values:
            return math.nan
        return sum(values) / len(values)

    def _atr(self, length: int) -> float:
        total = 0.0
        count = 0
        bars = min(length, self.series.length() - 1)
        for i in range(bars):
            high = self.series.get("high", i)
            low = self.series.get("low", i)
            prev_close = self.series.get("close", i + 1)
            if math.isnan(high) or math.isnan(low) or math.isnan(prev_close):
                continue
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            total += tr
            count += 1
        return total / count if count else 0.0

    def _tf_multi(self, timeframe: str) -> float:
        seconds = _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds)
        base = self.base_tf_seconds or 60
        if seconds is None or seconds <= 0:
            return 1.0
        return max(seconds / max(base, 1), 1.0)

    def _liquidity_display_limit(self, array: PineArray, limit: int, is_line: bool) -> None:
        while array.size() > max(limit // 2, 1):
            obj = array.remove(0)
            if is_line and isinstance(obj, Line):
                self._delete_line(obj)
            elif (not is_line) and isinstance(obj, Box):
                self._delete_box(obj)

    def _liquidity_remove_mitigated_lines(
        self,
        array: PineArray,
        is_high: bool,
        liq: LiquidityInputs,
    ) -> bool:
        mitigated = False
        reference = self.series.get("close", 1) if liq._candleType == "Close" else (
            self.series.get("high") if is_high else self.series.get("low")
        )
        for i in range(array.size() - 1, -1, -1):
            line_obj: Line = array.get(i)
            trigger = line_obj.get_y1()
            if (is_high and reference > trigger) or ((not is_high) and reference < trigger):
                array.remove(i)
                self._delete_line(line_obj)
                mitigated = True
                if liq.mitiOptions == "Show":
                    color = liq.highLineColorHTF if is_high else liq.lowLineColorHTF
                    style = self._map_line_style(liq._highLineStyleHTF)
                    self.line_new(
                        line_obj.x1,
                        trigger,
                        self.series.get_time(),
                        trigger,
                        "xloc.bar_time",
                        color,
                        style,
                    )
        self._liquidity_display_limit(array, liq.displayLimit, True)
        return mitigated

    def _liquidity_remove_mitigated_boxes(
        self,
        array: PineArray,
        is_high: bool,
        liq: LiquidityInputs,
    ) -> bool:
        mitigated = False
        reference = self.series.get("close", 1) if liq._candleType == "Close" else (
            self.series.get("high") if is_high else self.series.get("low")
        )
        for i in range(array.size() - 1, -1, -1):
            box_obj: Box = array.get(i)
            trigger = box_obj.get_top() if is_high else box_obj.get_bottom()
            condition = reference > trigger if is_high else reference < trigger
            if condition:
                array.remove(i)
                self._delete_box(box_obj)
                mitigated = True
                if liq.mitiOptions == "Show":
                    color = liq.highBoxBorderColorHTF if is_high else liq.lowBoxBorderColorHTF
                    bgcolor = liq.highLineColorHTF if is_high else liq.lowLineColorHTF
                    new_box = self.box_new(
                        box_obj.get_left(),
                        self.series.get_time(),
                        box_obj.get_top(),
                        box_obj.get_bottom(),
                        bgcolor,
                    )
                    new_box.set_border_color(color)
                    new_box.set_border_style(self._map_line_style(liq._highLineStyleHTF))
        self._liquidity_display_limit(array, liq.displayLimit, False)
        return mitigated

    def _liquidity_extend_lines(self, array: PineArray, extend_time: int) -> None:
        for i in range(array.size()):
            line_obj: Line = array.get(i)
            line_obj.set_x2(extend_time)

    def _liquidity_extend_boxes(self, array: PineArray, extend_time: int) -> None:
        for i in range(array.size()):
            box_obj: Box = array.get(i)
            box_obj.set_right(extend_time)

    def _calculate_swing_points(
        self, length: int
    ) -> Tuple[Optional[Tuple[int, float]], Optional[Tuple[int, float]]]:
        prev = self.ict_prev_state.get(length, 0)
        prev_prev = self.ict_prev_state_prev.get(length, 0)
        high_pivot = self._pivot_base_series(length, length, True)
        low_pivot = self._pivot_base_series(length, length, False)
        current_state = prev
        top: Optional[Tuple[int, float]] = None
        bottom: Optional[Tuple[int, float]] = None
        if high_pivot is not None:
            current_state = 0
            if prev_prev != 0:
                top = high_pivot
        elif low_pivot is not None:
            current_state = 1
            if prev_prev != 1:
                bottom = low_pivot
        self.ict_prev_state_prev[length] = prev
        self.ict_prev_state[length] = current_state
        return top, bottom

    def _update_ict_market_structure(self, high: float, low: float, close: float) -> None:
        ict = self.inputs.ict_structure
        if not (ict.showms or ict.show_equal_highlow):
            return

        length = 50
        internal_length = ict.swingSize
        top_bottom = self._calculate_swing_points(length)
        internal = self._calculate_swing_points(internal_length)

        if top_bottom[0] is not None:
            self.crossed_up = True
            self.y_up = top_bottom[0][1]
            self.x_up = top_bottom[0][0]
        if top_bottom[1] is not None:
            self.crossed_down = True
            self.y_dn = top_bottom[1][1]
            self.x_dn = top_bottom[1][0]

        if internal[0] is not None:
            self.internal_up_broke = True
            self.internal_y_up = internal[0][1]
            self.internal_x_up = internal[0][0]
            if math.isnan(self.prevHigh_s) or internal[0][1] >= self.prevHigh_s:
                self.prevSwing_s = 2
            else:
                self.prevSwing_s = 1
            self.prevHigh_s = internal[0][1]
            self.prevHighIndex_s = internal[0][0]
        if internal[1] is not None:
            self.internal_dn_broke = True
            self.internal_y_dn = internal[1][1]
            self.internal_x_dn = internal[1][0]
            if math.isnan(self.prevLow_s) or internal[1][1] >= self.prevLow_s:
                self.prevSwing_s = -1
            else:
                self.prevSwing_s = -2
            self.prevLow_s = internal[1][1]
            self.prevLowIndex_s = internal[1][0]

        label_size = self._map_label_size(ict.label_sizes_s)
        bull_color = ict.bosColor1
        bear_color = ict.bosColor2

        bull_mss = False
        bear_mss = False
        bull_bos = False
        bear_bos = False
        bull_mss_ext = False
        bear_mss_ext = False
        bull_bos_ext = False
        bear_bos_ext = False

        prev_close = self.prev_close

        if (
            not math.isnan(self.internal_y_up)
            and self.internal_up_broke
            and self._crossover(prev_close, close, self.internal_y_up)
            and (math.isnan(self.y_up) or not math.isclose(self.y_up, self.internal_y_up, rel_tol=1e-9, abs_tol=1e-9))
        ):
            MSS = self.int_t_MS < 0
            self.internal_up_broke = False
            self.int_t_MS = 1
            bull_mss = MSS
            bull_bos = not MSS
            if ict.showms and (ict.ms_type in ("All", "Internal")):
                self._show_ms(self.internal_x_up, self.internal_y_up, "MSS" if MSS else "BOS", bull_color, True, True, label_size)

        if (
            not math.isnan(self.internal_y_dn)
            and self.internal_dn_broke
            and self._crossunder(prev_close, close, self.internal_y_dn)
            and (math.isnan(self.y_dn) or not math.isclose(self.y_dn, self.internal_y_dn, rel_tol=1e-9, abs_tol=1e-9))
        ):
            MSS = self.int_t_MS > 0
            self.internal_dn_broke = False
            self.int_t_MS = -1
            bear_mss = MSS
            bear_bos = not MSS
            if ict.showms and (ict.ms_type in ("All", "Internal")):
                self._show_ms(self.internal_x_dn, self.internal_y_dn, "MSS" if MSS else "BOS", bear_color, True, False, label_size)

        if (
            not math.isnan(self.y_up)
            and self.crossed_up
            and self._crossover(prev_close, close, self.y_up)
        ):
            MSS = self.t_MS < 0
            self.crossed_up = False
            self.t_MS = 1
            bull_mss_ext = MSS
            bull_bos_ext = not MSS
            if ict.showms or ict.ms_type in ("All", "External"):
                self._show_ms(self.x_up, self.y_up, "MSS+" if MSS else "BOS+", bull_color, False, True, label_size)

        if (
            not math.isnan(self.y_dn)
            and self.crossed_down
            and self._crossunder(prev_close, close, self.y_dn)
        ):
            MSS = self.t_MS > 0
            self.crossed_down = False
            self.t_MS = -1
            bear_mss_ext = MSS
            bear_bos_ext = not MSS
            if ict.showms and (ict.ms_type in ("All", "External")):
                self._show_ms(self.x_dn, self.y_dn, "MSS+" if MSS else "BOS+", bear_color, False, False, label_size)

        if ict.show_equal_highlow:
            atr = self._atr(200)
            eq_length = 3
            eq_low = self._pivot_base_series(eq_length, eq_length, False)
            if eq_low is not None:
                low_value = eq_low[1]
                if not math.isnan(self.low_eqh_pre):
                    threshold = atr * ict.eq_threshold
                    if min(low_value, self.low_eqh_pre) > max(low_value, self.low_eqh_pre) - threshold:
                        line_obj = self.line_new(
                            self.eq_btm_x,
                            self.low_eqh_pre,
                            eq_low[0],
                            low_value,
                            "xloc.bar_time",
                            ict.eq_bull_color,
                            "line.style_dotted",
                        )
                        self.eql_lines.push(line_obj)
                        label = self.label_new(
                            int((self.eq_btm_x + eq_low[0]) / 2),
                            low_value,
                            "EQL",
                            "xloc.bar_time",
                            "yloc.price",
                            "#00000000",
                            "label.style_label_up",
                            label_size,
                            ict.eq_bull_color,
                        )
                        self.eql_labels.push(label)
                self.low_eqh_pre = low_value
                self.eq_btm_x = eq_low[0]

            eq_high = self._pivot_base_series(eq_length, eq_length, True)
            if eq_high is not None:
                high_value = eq_high[1]
                if not math.isnan(self.high_eqh_pre):
                    threshold = atr * ict.eq_threshold
                    if max(high_value, self.high_eqh_pre) < min(high_value, self.high_eqh_pre) + threshold:
                        line_obj = self.line_new(
                            self.eq_top_x,
                            self.high_eqh_pre,
                            eq_high[0],
                            high_value,
                            "xloc.bar_time",
                            ict.eq_bear_color,
                            "line.style_dotted",
                        )
                        self.eqh_lines.push(line_obj)
                        label = self.label_new(
                            int((self.eq_top_x + eq_high[0]) / 2),
                            high_value,
                            "EQH",
                            "xloc.bar_time",
                            "yloc.price",
                            "#00000000",
                            "label.style_label_down",
                            label_size,
                            ict.eq_bear_color,
                        )
                        self.eqh_labels.push(label)
                self.high_eqh_pre = high_value
                self.eq_top_x = eq_high[0]


    def _update_key_levels(self, open_: float, high: float, low: float) -> None:
        kl = self.inputs.key_levels
        if not (
            kl.Show_4H_Levels
            or kl.Show_Daily_Levels
            or kl.Show_Monday_Levels
            or kl.Show_Weekly_Levels
            or kl.Show_Monthly_Levels
            or kl.Show_Quaterly_Levels
            or kl.Show_Yearly_Levels
        ):
            return

        prices: List[float] = []
        labels: List[Label] = []
        line_width = self._line_width_from_setting(kl.linesize)
        label_size = self._key_label_size(kl.labelsize)

        fourh_feed = self._ensure_security_feed("240")
        daily_feed = self._ensure_security_feed("D")
        weekly_feed = self._ensure_security_feed("W")
        monthly_feed = self._ensure_security_feed("M")
        quarterly_feed = self._ensure_security_feed("3M")
        yearly_feed = self._ensure_security_feed("12M")

        def tf_time(feed: Optional[SecuritySeries], offset: int) -> int:
            if feed and feed.length() > offset:
                return feed.get_time(offset)
            return self.series.get_time()

        def tf_value(feed: Optional[SecuritySeries], series_name: str, offset: int, fallback: float) -> float:
            if feed and feed.length() > offset:
                value = feed.get(series_name, offset)
                if not math.isnan(value):
                    return value
            return fallback

        intra_time = tf_time(fourh_feed, 0)
        intra_open = tf_value(fourh_feed, "open", 0, open_)
        intrah_time = tf_time(fourh_feed, 1)
        intrah_open = tf_value(fourh_feed, "high", 1, high)
        intral_time = tf_time(fourh_feed, 1)
        intral_open = tf_value(fourh_feed, "low", 1, low)

        daily_time = tf_time(daily_feed, 0)
        daily_open = tf_value(daily_feed, "open", 0, open_)
        dailyh_time = tf_time(daily_feed, 1)
        dailyh_open = tf_value(daily_feed, "high", 1, high)
        dailyl_time = tf_time(daily_feed, 1)
        dailyl_open = tf_value(daily_feed, "low", 1, low)
        cdailyh_open = tf_value(daily_feed, "high", 0, high)
        cdailyl_open = tf_value(daily_feed, "low", 0, low)

        weekly_time = tf_time(weekly_feed, 0)
        weekly_open = tf_value(weekly_feed, "open", 0, open_)
        weeklyh_time = tf_time(weekly_feed, 1)
        weeklyh_open = tf_value(weekly_feed, "high", 1, high)
        weeklyl_time = tf_time(weekly_feed, 1)
        weeklyl_open = tf_value(weekly_feed, "low", 1, low)

        monthly_time = tf_time(monthly_feed, 0)
        monthly_open = tf_value(monthly_feed, "open", 0, open_)
        monthlyh_time = tf_time(monthly_feed, 1)
        monthlyh_open = tf_value(monthly_feed, "high", 1, high)
        monthlyl_time = tf_time(monthly_feed, 1)
        monthlyl_open = tf_value(monthly_feed, "low", 1, low)

        quarterly_time = tf_time(quarterly_feed, 0)
        quarterly_open = tf_value(quarterly_feed, "open", 0, open_)
        quarterlyh_time = tf_time(quarterly_feed, 1)
        quarterlyh_open = tf_value(quarterly_feed, "high", 1, high)
        quarterlyl_time = tf_time(quarterly_feed, 1)
        quarterlyl_open = tf_value(quarterly_feed, "low", 1, low)

        yearly_time = tf_time(yearly_feed, 0)
        yearly_open = tf_value(yearly_feed, "open", 0, open_)
        yearlyh_time = tf_time(yearly_feed, 1)
        yearlyh_open = tf_value(yearly_feed, "high", 1, high)
        yearlyl_time = tf_time(yearly_feed, 1)
        yearlyl_open = tf_value(yearly_feed, "low", 1, low)

        if weekly_time != self.weekly_time_marker:
            self.weekly_time_marker = weekly_time
            self.untested_monday = False
        if kl.Show_Monday_Levels and not self.untested_monday:
            if not math.isnan(cdailyh_open) and not math.isnan(cdailyl_open):
                self.untested_monday = True
                self.monday_time = daily_time
                self.monday_high = cdailyh_open
                self.monday_low = cdailyl_open
                self.monday_mid = (self.monday_high + self.monday_low) / 2.0

        def extend_to_current() -> int:
            return self._extend_time(kl.distanceright)

        iotext = "4H-O" if kl.Text_4H_Levels else "4H Open"
        pihtext = "P-4H-H" if kl.Text_4H_Levels else "Prev 4H High"
        piltext = "P-4H-L" if kl.Text_4H_Levels else "Prev 4H Low"
        pmonhtext = "MDAY-H" if kl.Text_Monday_Levels else "Monday High"
        pmonltext = "MDAY-L" if kl.Text_Monday_Levels else "Monday Low"
        pmonmtext = "MDAY-M" if kl.Text_Monday_Levels else "Monday Mid"
        dotext = "DO" if kl.Text_Daily_Levels else "Daily Open"
        pdhtext = "PDH" if kl.Text_Daily_Levels else "Prev Day High"
        pdltext = "PDL" if kl.Text_Daily_Levels else "Prev Day Low"
        wotext = "WO" if kl.WeeklyTextType else "Weekly Open"
        pwhtext = "PWH" if kl.WeeklyTextType else "Prev Week High"
        pwltext = "PWL" if kl.WeeklyTextType else "Prev Week Low"
        motext = "MO" if kl.MonthlyTextType else "Monthly Open"
        pmhtext = "PMH" if kl.MonthlyTextType else "Prev Month High"
        pmltext = "PML" if kl.MonthlyTextType else "Prev Month Low"
        pqmtext = "PQM" if kl.QuarterlyTextType else "Prev Quarter Mid"
        pqhtext = "PQH" if kl.QuarterlyTextType else "Prev Quarter High"
        pqltext = "PQL" if kl.QuarterlyTextType else "Prev Quarter Low"
        qotext = "QO" if kl.QuarterlyTextType else "Quarterly Open"
        yotext = "YO" if kl.YearlyTextType else "Yearly Open"
        cyhtext = "CYH" if kl.YearlyTextType else "Current Year High"
        cyltext = "CYL" if kl.YearlyTextType else "Current Year Low"

        if kl.Show_4H_Levels and not math.isnan(intra_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "4h_open",
                intra_time,
                right,
                intra_open,
                kl.Color_4H_Levels,
                self._map_line_style(kl.Style_4H_Levels),
                line_width,
                iotext,
                kl.Color_4H_Levels,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, intra_open, label, kl.Color_4H_Levels)
            if not math.isnan(intrah_open):
                label = self._update_level_visual(
                    "4h_high",
                    intrah_time,
                    right,
                    intrah_open,
                    kl.Color_4H_Levels,
                    self._map_line_style(kl.Style_4H_Levels),
                    line_width,
                    pihtext,
                    kl.Color_4H_Levels,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, intrah_open, label, kl.Color_4H_Levels)
            if not math.isnan(intral_open):
                label = self._update_level_visual(
                    "4h_low",
                    intral_time,
                    right,
                    intral_open,
                    kl.Color_4H_Levels,
                    self._map_line_style(kl.Style_4H_Levels),
                    line_width,
                    piltext,
                    kl.Color_4H_Levels,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, intral_open, label, kl.Color_4H_Levels)

        if kl.Show_Monday_Levels and not math.isnan(self.monday_high) and not math.isnan(self.monday_low):
            right = extend_to_current()
            label = self._update_level_visual(
                "monday_high",
                self.monday_time,
                right,
                self.monday_high,
                kl.Color_Monday_Levels,
                self._map_line_style(kl.Style_Monday_Levels),
                line_width,
                pmonhtext,
                kl.Color_Monday_Levels,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, self.monday_high, label, kl.Color_Monday_Levels)
            label = self._update_level_visual(
                "monday_low",
                self.monday_time,
                right,
                self.monday_low,
                kl.Color_Monday_Levels,
                self._map_line_style(kl.Style_Monday_Levels),
                line_width,
                pmonltext,
                kl.Color_Monday_Levels,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, self.monday_low, label, kl.Color_Monday_Levels)
            label = self._update_level_visual(
                "monday_mid",
                self.monday_time,
                right,
                self.monday_mid,
                kl.Color_Monday_Levels,
                self._map_line_style(kl.Style_Monday_Levels),
                line_width,
                pmonmtext,
                kl.Color_Monday_Levels,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, self.monday_mid, label, kl.Color_Monday_Levels)

        if kl.Show_Daily_Levels and not math.isnan(daily_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "daily_open",
                daily_time,
                right,
                daily_open,
                kl.Color_Daily_Levels,
                self._map_line_style(kl.Style_Daily_Levels),
                line_width,
                dotext,
                kl.Color_Daily_Levels,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, daily_open, label, kl.Color_Daily_Levels)
            if not math.isnan(dailyh_open):
                label = self._update_level_visual(
                    "daily_high",
                    dailyh_time,
                    right,
                    dailyh_open,
                    kl.Color_Daily_Levels,
                    self._map_line_style(kl.Style_Daily_Levels),
                    line_width,
                    pdhtext,
                    kl.Color_Daily_Levels,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, dailyh_open, label, kl.Color_Daily_Levels)
            if not math.isnan(dailyl_open):
                label = self._update_level_visual(
                    "daily_low",
                    dailyl_time,
                    right,
                    dailyl_open,
                    kl.Color_Daily_Levels,
                    self._map_line_style(kl.Style_Daily_Levels),
                    line_width,
                    pdltext,
                    kl.Color_Daily_Levels,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, dailyl_open, label, kl.Color_Daily_Levels)

        if kl.Show_Weekly_Levels and not math.isnan(weekly_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "weekly_open",
                weekly_time,
                right,
                weekly_open,
                kl.WeeklyColor,
                self._map_line_style(kl.Weekly_style),
                line_width,
                wotext,
                kl.WeeklyColor,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, weekly_open, label, kl.WeeklyColor)
            if not math.isnan(weeklyh_open):
                label = self._update_level_visual(
                    "weekly_high",
                    weeklyh_time,
                    right,
                    weeklyh_open,
                    kl.WeeklyColor,
                    self._map_line_style(kl.Weekly_style),
                    line_width,
                    pwhtext,
                    kl.WeeklyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, weeklyh_open, label, kl.WeeklyColor)
            if not math.isnan(weeklyl_open):
                label = self._update_level_visual(
                    "weekly_low",
                    weeklyl_time,
                    right,
                    weeklyl_open,
                    kl.WeeklyColor,
                    self._map_line_style(kl.Weekly_style),
                    line_width,
                    pwltext,
                    kl.WeeklyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, weeklyl_open, label, kl.WeeklyColor)

        if kl.Show_Monthly_Levels and not math.isnan(monthly_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "monthly_open",
                monthly_time,
                right,
                monthly_open,
                kl.MonthlyColor,
                self._map_line_style(kl.Monthly_style),
                line_width,
                motext,
                kl.MonthlyColor,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, monthly_open, label, kl.MonthlyColor)
            if not math.isnan(monthlyh_open):
                label = self._update_level_visual(
                    "monthly_high",
                    monthlyh_time,
                    right,
                    monthlyh_open,
                    kl.MonthlyColor,
                    self._map_line_style(kl.Monthly_style),
                    line_width,
                    pmhtext,
                    kl.MonthlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, monthlyh_open, label, kl.MonthlyColor)
            if not math.isnan(monthlyl_open):
                label = self._update_level_visual(
                    "monthly_low",
                    monthlyl_time,
                    right,
                    monthlyl_open,
                    kl.MonthlyColor,
                    self._map_line_style(kl.Monthly_style),
                    line_width,
                    pmltext,
                    kl.MonthlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, monthlyl_open, label, kl.MonthlyColor)

        if kl.Show_Quaterly_Levels and not math.isnan(quarterly_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "quarterly_open",
                quarterly_time,
                right,
                quarterly_open,
                kl.quarterlyColor,
                self._map_line_style(kl.Quaterly_style),
                line_width,
                qotext,
                kl.quarterlyColor,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, quarterly_open, label, kl.quarterlyColor)
            if not math.isnan(quarterlyh_open):
                label = self._update_level_visual(
                    "quarterly_high",
                    quarterlyh_time,
                    right,
                    quarterlyh_open,
                    kl.quarterlyColor,
                    self._map_line_style(kl.Quaterly_style),
                    line_width,
                    pqhtext,
                    kl.quarterlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, quarterlyh_open, label, kl.quarterlyColor)
            if not math.isnan(quarterlyl_open):
                label = self._update_level_visual(
                    "quarterly_low",
                    quarterlyl_time,
                    right,
                    quarterlyl_open,
                    kl.quarterlyColor,
                    self._map_line_style(kl.Quaterly_style),
                    line_width,
                    pqltext,
                    kl.quarterlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, quarterlyl_open, label, kl.quarterlyColor)

        if kl.Show_Yearly_Levels and not math.isnan(yearly_open):
            right = extend_to_current()
            label = self._update_level_visual(
                "yearly_open",
                yearly_time,
                right,
                yearly_open,
                kl.YearlyColor,
                self._map_line_style(kl.Yearly_style),
                line_width,
                yotext,
                kl.YearlyColor,
                label_size,
            )
            if label:
                self._combine_levels(prices, labels, yearly_open, label, kl.YearlyColor)
            if not math.isnan(yearlyh_open):
                label = self._update_level_visual(
                    "yearly_high",
                    yearlyh_time,
                    right,
                    yearlyh_open,
                    kl.YearlyColor,
                    self._map_line_style(kl.Yearly_style),
                    line_width,
                    cyhtext,
                    kl.YearlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, yearlyh_open, label, kl.YearlyColor)
            if not math.isnan(yearlyl_open):
                label = self._update_level_visual(
                    "yearly_low",
                    yearlyl_time,
                    right,
                    yearlyl_open,
                    kl.YearlyColor,
                    self._map_line_style(kl.Yearly_style),
                    line_width,
                    cyltext,
                    kl.YearlyColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, yearlyl_open, label, kl.YearlyColor)

    def _update_support_resistance(
        self, open_: float, high: float, low: float, close: float, volume: float
    ) -> None:
        sr = self.inputs.support_resistance
        if sr.resistanceSupportCount <= 0:
            return
        pivot_range = sr.pivotRange
        base_store = self._sr_collect_base_pivots(pivot_range)
        self.sr_pivot_store[self._sr_timeframe_key("")] = base_store
        base_seconds = self.base_tf_seconds or 60
        for _, timeframe, enabled in self.sr_timeframes:
            if not enabled:
                continue
            tf_seconds = _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds)
            if timeframe in (None, "") or tf_seconds == base_seconds:
                store = base_store
            else:
                store = self._sr_collect_timeframe_pivots(timeframe, pivot_range)
            self._sr_refresh_levels(
                self._sr_timeframe_key(timeframe),
                "Resistance",
                store.get("high", []),
                sr,
            )
            self._sr_refresh_levels(
                self._sr_timeframe_key(timeframe),
                "Support",
                store.get("low", []),
                sr,
            )
        self._sr_cleanup_levels(sr)

    def _update_sessions(self, open_: float, high: float, low: float, time_val: int) -> None:
        sessions = self.inputs.sessions
        if not (
            sessions.is_londonrange_enabled
            or sessions.is_usrange_enabled
            or sessions.is_tokyorange_enabled
        ):
            return

        kl = self.inputs.key_levels
        line_width = self._line_width_from_setting(kl.linesize)
        label_size = self._key_label_size(kl.labelsize)
        prices: List[float] = []
        labels: List[Label] = []

        def update_state(name: str, enabled: bool, session_str: str) -> None:
            state = self.session_states[name]
            if not enabled:
                state["active"] = False
                return
            active = self._time_in_session(time_val, session_str)
            if active:
                if not state["active"]:
                    state["open"] = open_
                    state["start"] = time_val
                    state["high"] = high
                    state["low"] = low
                else:
                    state["high"] = max(state["high"], high)
                    state["low"] = min(state["low"], low)
                state["final_high"] = state["high"]
                state["final_low"] = state["low"]
                state["final_open"] = state["open"]
            else:
                if state["active"]:
                    state["final_high"] = state["high"]
                    state["final_low"] = state["low"]
                    state["final_open"] = state["open"]
            state["active"] = active

        update_state("london", sessions.is_londonrange_enabled, sessions.Londont)
        update_state("us", sessions.is_usrange_enabled, sessions.USt)
        update_state("asia", sessions.is_tokyorange_enabled, sessions.Asiat)

        def session_text(short: bool, long_text: str, short_text: str) -> str:
            return short_text if short else long_text

        if sessions.is_londonrange_enabled:
            state = self.session_states["london"]
            start = state["start"]
            high_val = state["final_high"]
            low_val = state["final_low"]
            open_val = state["final_open"]
            right = self._extend_time(kl.distanceright)
            high_text = session_text(sessions.Short_text_London, "London High", "Lon-H")
            low_text = session_text(sessions.Short_text_London, "London Low", "Lon-L")
            open_text = session_text(sessions.Short_text_London, "London Open", "Lon-O")
            if not math.isnan(high_val) and sessions.london_HL:
                label = self._update_level_visual(
                    "session_london_high",
                    start,
                    right,
                    high_val,
                    sessions.LondonColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    high_text,
                    sessions.LondonColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, high_val, label, sessions.LondonColor)
            if not math.isnan(low_val) and sessions.london_HL:
                label = self._update_level_visual(
                    "session_london_low",
                    start,
                    right,
                    low_val,
                    sessions.LondonColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    low_text,
                    sessions.LondonColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, low_val, label, sessions.LondonColor)
            if not math.isnan(open_val) and sessions.london_OC:
                label = self._update_level_visual(
                    "session_london_open",
                    start,
                    right,
                    open_val,
                    sessions.LondonColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    open_text,
                    sessions.LondonColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, open_val, label, sessions.LondonColor)

        if sessions.is_usrange_enabled:
            state = self.session_states["us"]
            start = state["start"]
            high_val = state["final_high"]
            low_val = state["final_low"]
            open_val = state["final_open"]
            right = self._extend_time(kl.distanceright)
            high_text = session_text(sessions.Short_text_NY, "New York High", "NY-H")
            low_text = session_text(sessions.Short_text_NY, "New York Low", "NY-L")
            open_text = session_text(sessions.Short_text_NY, "New York Open", "NY-O")
            if not math.isnan(high_val) and sessions.us_HL:
                label = self._update_level_visual(
                    "session_us_high",
                    start,
                    right,
                    high_val,
                    sessions.USColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    high_text,
                    sessions.USColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, high_val, label, sessions.USColor)
            if not math.isnan(low_val) and sessions.us_HL:
                label = self._update_level_visual(
                    "session_us_low",
                    start,
                    right,
                    low_val,
                    sessions.USColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    low_text,
                    sessions.USColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, low_val, label, sessions.USColor)
            if not math.isnan(open_val) and sessions.us_OC:
                label = self._update_level_visual(
                    "session_us_open",
                    start,
                    right,
                    open_val,
                    sessions.USColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    open_text,
                    sessions.USColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, open_val, label, sessions.USColor)

        if sessions.is_tokyorange_enabled:
            state = self.session_states["asia"]
            start = state["start"]
            high_val = state["final_high"]
            low_val = state["final_low"]
            open_val = state["final_open"]
            right = self._extend_time(kl.distanceright)
            high_text = session_text(sessions.Short_text_TKY, "Tokyo High", "TK-H")
            low_text = session_text(sessions.Short_text_TKY, "Tokyo Low", "TK-L")
            open_text = session_text(sessions.Short_text_TKY, "Tokyo Open", "TK-O")
            if not math.isnan(high_val) and sessions.asia_HL:
                label = self._update_level_visual(
                    "session_asia_high",
                    start,
                    right,
                    high_val,
                    sessions.AsiaColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    high_text,
                    sessions.AsiaColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, high_val, label, sessions.AsiaColor)
            if not math.isnan(low_val) and sessions.asia_HL:
                label = self._update_level_visual(
                    "session_asia_low",
                    start,
                    right,
                    low_val,
                    sessions.AsiaColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    low_text,
                    sessions.AsiaColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, low_val, label, sessions.AsiaColor)
            if not math.isnan(open_val) and sessions.asia_OC:
                label = self._update_level_visual(
                    "session_asia_open",
                    start,
                    right,
                    open_val,
                    sessions.AsiaColor,
                    self._map_line_style(kl.linestyle),
                    line_width,
                    open_text,
                    sessions.AsiaColor,
                    label_size,
                )
                if label:
                    self._combine_levels(prices, labels, open_val, label, sessions.AsiaColor)
    def _liquidity_pivot(
        self,
        series: SecuritySeries,
        left: int,
        right: int,
        is_high: bool,
    ) -> Optional[Tuple[int, float]]:
        if left + right + 1 > series.length():
            return None
        value = series.get("high" if is_high else "low", left)
        if math.isnan(value):
            return None
        for i in range(1, left + 1):
            comp = series.get("high" if is_high else "low", left - i)
            if math.isnan(comp):
                return None
            if (is_high and comp >= value) or ((not is_high) and comp <= value):
                return None
        for i in range(1, right + 1):
            comp = series.get("high" if is_high else "low", left + i)
            if math.isnan(comp):
                return None
            if (is_high and comp > value) or ((not is_high) and comp < value):
                return None
        return series.get_time(left), value

    def updateTopBotValue(self) -> None:
        self.arrTop.push(self.series.get("high"))
        self.arrBot.push(self.series.get("low"))
        self.arrTopBotBar.push(self.series.get_time())

    def updateLastHLValue(self) -> None:
        self.arrLastH.push(self.lastH)
        self.arrLastHBar.push(self.lastHBar)
        self.arrLastL.push(self.lastL)
        self.arrLastLBar.push(self.lastLBar)

    def updateIdmHigh(self) -> None:
        self.arrIdmHigh.push(self.puHigh)
        self.arrIdmHBar.push(self.puHBar)

    def updateIdmLow(self) -> None:
        self.arrIdmLow.push(self.puLow)
        self.arrIdmLBar.push(self.puLBar)

    def getNLastValue(self, arr: PineArray, n: int) -> Any:
        if arr.size() > n - 1:
            return arr.get(arr.size() - n)
        return NA

    def _history_get(self, history: Sequence[Any], offset: int, default: Any = NA) -> Any:
        idx = len(history) - 1 - offset
        if idx < 0:
            return default
        return history[idx]

    def _security_key(self, timeframe: str, seconds: Optional[int]) -> str:
        return timeframe or f"__base_{seconds}"

    def _ensure_security_feed(self, timeframe: str) -> Optional[SecuritySeries]:
        seconds = _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds)
        if seconds is None:
            return None
        key = self._security_key(timeframe, seconds)
        feed = self.security_series.get(key)
        if feed is None:
            feed = SecuritySeries(seconds)
            self.security_series[key] = feed
        return feed

    def _update_security_context(
        self, time_val: int, open_: float, high: float, low: float, close: float, volume: float
    ) -> None:
        for feed in self.security_series.values():
            feed.update(time_val, open_, high, low, close, volume)

    def _delete_line(self, line_obj: Line) -> None:
        if line_obj in self.lines:
            self.lines.remove(line_obj)

    def _delete_box(self, box_obj: Box) -> None:
        if box_obj in self.boxes:
            self.boxes.remove(box_obj)

    def _delete_label(self, label_obj: Label) -> None:
        if label_obj in self.labels:
            self.labels.remove(label_obj)

    def _record_ob_volume(self, key: str, value: float) -> None:
        arr = self.ob_volume_history.setdefault(key, PineArray())
        arr.unshift(value)
        if arr.size() > 300:
            arr.pop()

    def _highest_ob_volume(self, key: str, fallback: float) -> float:
        arr = self.ob_volume_history.get(key)
        if arr is None or arr.size() == 0:
            return fallback
        return max(float(v) for v in arr.values)

    def _ensure_ob_visual_capacity(self, boxes: PineArray, lines: PineArray, max_count: int) -> None:
        current_time = self.series.get_time()
        while boxes.size() < max_count:
            boxes.push(
                self.box_new(
                    current_time,
                    current_time,
                    0.0,
                    0.0,
                    self.inputs.demand_supply.ibull_ob_css,
                )
            )
            lines.push(
                self.line_new(
                    current_time,
                    0.0,
                    current_time,
                    0.0,
                    "xloc.bar_time",
                    "color.gray",
                    "line.style_solid",
                )
            )

    def _render_order_blocks(
        self,
        boxes: PineArray,
        lines: PineArray,
        top_arr: PineArray,
        btm_arr: PineArray,
        left_arr: PineArray,
        type_arr: PineArray,
        vol_arr: PineArray,
        color_demand: str,
        color_supply: str,
        text_size: str,
        text_color: str,
        length_extend: int,
        extend_right: bool,
        volume_text: bool,
        percent_text: bool,
        show_line: bool,
        line_style: str,
    ) -> None:
        size = top_arr.size()
        self._ensure_ob_visual_capacity(boxes, lines, max(size, boxes.size()))
        time_now = self.series.get_time()
        extend_delta = self.curTf if hasattr(self, "curTf") else 0
        volume_sum = sum(float(vol_arr.get(i)) for i in range(size)) if size else 0.0
        for idx in range(boxes.size()):
            box = boxes.get(idx)
            line = lines.get(idx)
            if idx >= size:
                box.set_lefttop(time_now, 0.0)
                box.set_rightbottom(time_now, 0.0)
                box.set_bgcolor("color.new(#000000,100)")
                line.set_color("color.new(#000000,100)")
                continue
            top_val = float(top_arr.get(idx))
            btm_val = float(btm_arr.get(idx))
            left_val = int(left_arr.get(idx))
            type_val = int(type_arr.get(idx))
            vol_val = float(vol_arr.get(idx))
            box.set_lefttop(left_val, top_val)
            box.set_rightbottom(time_now + extend_delta * length_extend, btm_val)
            unit = ""
            volume_display = vol_val
            if vol_val > 100000000:
                volume_display = vol_val / 100000000.0
                unit = " B"
            elif vol_val > 1000000:
                volume_display = vol_val / 1000000.0
                unit = " M"
            else:
                volume_display = vol_val / 1000.0
                unit = " K"
            percent = (vol_val / volume_sum * 100.0) if volume_sum else 0.0
            text_parts: List[str] = []
            if volume_text:
                text_parts.append(f"{volume_display:.2f}{unit}")
            if percent_text:
                text_parts.append(f"{percent:.2f}%")
            box.set_text(" ".join(text_parts) if text_parts else "")
            box.set_text_color(text_color)
            box.set_text_halign("text.align_right")
            box.set_text_valign("text.align_center")
            box.set_text_size(text_size)
            box.set_border_width(2)
            box.set_extend("extend.right" if extend_right else "extend.none")
            css = color_demand if type_val == -1 else color_supply
            box.set_border_color(css)
            box.set_bgcolor(css)
            line.set_extend("extend.right" if extend_right else "extend.none")
            line.set_style(line_style)
            line.set_xy1(left_val, (top_val + btm_val) / 2.0)
            line.set_xy2(time_now + extend_delta * length_extend, (top_val + btm_val) / 2.0)
            line.set_color("color.gray" if show_line else "color.new(#000000,100)")

    def _handle_ob_detection(
        self,
        timeframe: str,
        result: Tuple[bool, float, int, int, float, float, int, int, str],
        top_arr: PineArray,
        btm_arr: PineArray,
        left_arr: PineArray,
        type_arr: PineArray,
        vol_arr: PineArray,
        buy_arr: PineArray,
        sell_arr: PineArray,
        boxes: PineArray,
        lines: PineArray,
        max_obs: int,
        color_demand: str,
        color_supply: str,
        text_size: str,
        text_color: str,
        length_extend: int,
        extend_right: bool,
        volume_text: bool,
        percent_text: bool,
        show_line: bool,
        line_style: str,
    ) -> None:
        seconds = _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds)
        key = self._security_key(timeframe, seconds)
        valid, volume_, b_volume, s_volume, top_val, bottom_val, left_val, type_val, _type = result
        prev_valid = self.ob_valid_history.get(key, False)
        self.ob_valid_history[key] = valid
        if valid and not prev_valid:
            top_arr.unshift(top_val)
            btm_arr.unshift(bottom_val)
            left_arr.unshift(left_val)
            type_arr.unshift(type_val)
            vol_arr.unshift(volume_)
            buy_arr.unshift(b_volume)
            sell_arr.unshift(s_volume)
            block_label = EVENT_PRINT_LABELS.get("ORDER_BLOCK", "Order Block")
            block_type_text = f"{block_label} ({_type})" if _type and _type != "none" else block_label
            direction = self._direction_from_ob_type(type_val)
            self._record_range_event(
                "ORDER_BLOCK",
                text=block_type_text,
                bottom=float(bottom_val),
                top=float(top_val),
                timestamp=self.series.get_time(),
                status="new",
                direction=direction,
            )
            if _type and "Internal" in _type:
                propulsion_label = EVENT_PRINT_LABELS.get("PROPULSION_BLOCK", "Propulsion Block")
                self._record_range_event(
                    "PROPULSION_BLOCK",
                    text=f"{propulsion_label} ({_type})",
                    bottom=float(bottom_val),
                    top=float(top_val),
                    timestamp=self.series.get_time(),
                    status="propulsion",
                    direction=direction,
                )
        if top_arr.size() > max_obs:
            top_arr.pop()
            btm_arr.pop()
            left_arr.pop()
            type_arr.pop()
            vol_arr.pop()
            buy_arr.pop()
            sell_arr.pop()
        self._render_order_blocks(
            boxes,
            lines,
            top_arr,
            btm_arr,
            left_arr,
            type_arr,
            vol_arr,
            color_demand,
            color_supply,
            text_size,
            text_color,
            length_extend,
            extend_right,
            volume_text,
            percent_text,
            show_line,
            line_style,
        )

    def _update_demand_supply_zones(self) -> None:
        ds = self.inputs.demand_supply
        show_base = ds.show_order_blocks
        show_mtf = ds.show_order_blocks_mtf
        if not (show_base or show_mtf):
            return

        show_iob = ds.ob_type__ in ("All", "Internal")
        show_ob = ds.ob_type__ in ("All", "External")
        feed_base = self._ensure_security_feed(ds.i_tf_ob)
        if feed_base:
            result_base = self.ob_found(feed_base, ds.i_tf_ob, show_ob, show_iob)
            self._handle_ob_detection(
                ds.i_tf_ob,
                result_base,
                self.ob_top,
                self.ob_btm,
                self.ob_left,
                self.ob_type,
                self.ob_vol,
                self.ob_buy_vol,
                self.ob_sell_vol,
                self.ob_boxes,
                self.ob_lines,
                ds.max_obs,
                ds.ibull_ob_css,
                ds.ibear_ob_css,
                ds.text_size_ob_,
                ds.ob_text_color_1,
                ds.length_extend_ob,
                ds.ob_extend,
                ds.volume_text,
                ds.percent_text,
                ds.show_line_ob_1,
                ds.line_style_ob_1,
            )

        if show_mtf:
            show_iob_mtf = ds.ob_type__mtf in ("All", "Internal")
            show_ob_mtf = ds.ob_type__mtf in ("All", "External")
            feed_mtf = self._ensure_security_feed(ds.i_tf_ob_mtf)
            if feed_mtf:
                result_mtf = self.ob_found(feed_mtf, ds.i_tf_ob_mtf, show_ob_mtf, show_iob_mtf)
                self._handle_ob_detection(
                    ds.i_tf_ob_mtf,
                    result_mtf,
                    self.ob_top_mtf,
                    self.ob_btm_mtf,
                    self.ob_left_mtf,
                    self.ob_type_mtf,
                    self.ob_vol_mtf,
                    self.ob_buy_vol_mtf,
                    self.ob_sell_vol_mtf,
                    self.ob_boxes_mtf,
                    self.ob_lines_mtf,
                    ds.max_obs_mtf,
                    ds.ibull_ob_css_2,
                    ds.ibear_ob_css_2,
                    ds.text_size_ob_2,
                    ds.ob_text_color_2,
                    ds.length_extend_ob_mtf,
                    ds.ob_extend_mtf,
                    ds.volume_text_2,
                    ds.percent_text_2,
                    ds.show_line_ob_2,
                    ds.line_style_ob_2,
                )

        self._apply_order_block_filters()

    def _filter_order_blocks(
        self,
        top_arr: PineArray,
        btm_arr: PineArray,
        left_arr: PineArray,
        type_arr: PineArray,
        vol_arr: PineArray,
        buy_arr: PineArray,
        sell_arr: PineArray,
        mittigation: str,
        overlapping: bool,
    ) -> Tuple[bool, bool]:
        bullish_break = False
        bearish_break = False
        mittigation = self._canonical_mitigation(mittigation)
        if overlapping and top_arr.size() > 1:
            remove_indices: List[int] = []
            for i in range(top_arr.size()):
                top_i = float(top_arr.get(i))
                btm_i = float(btm_arr.get(i))
                for j in range(i):
                    top_j = float(top_arr.get(j))
                    btm_j = float(btm_arr.get(j))
                    if (btm_i <= top_j and top_i >= btm_j) or (btm_j <= top_i and top_j >= btm_i):
                        remove_indices.append(i)
                        break
            for idx in sorted(set(remove_indices), reverse=True):
                top_arr.remove(idx)
                btm_arr.remove(idx)
                left_arr.remove(idx)
                type_arr.remove(idx)
                vol_arr.remove(idx)
                buy_arr.remove(idx)
                sell_arr.remove(idx)

        for i in range(top_arr.size() - 1, -1, -1):
            zone_top = float(top_arr.get(i))
            zone_bottom = float(btm_arr.get(i))
            zone_type = int(type_arr.get(i))
            if mittigation in ("Wicks", "Touch"):
                src_low = self.series.get("low")
                src_low_prev = self.series.get("low", 1)
                src_low_prev2 = self.series.get("low", 2)
                src_high = self.series.get("high")
                src_high_prev = self.series.get("high", 1)
                src_high_prev2 = self.series.get("high", 2)
            elif mittigation == "Close":
                src_low = self.series.get("close")
                src_low_prev = self.series.get("close", 1)
                src_low_prev2 = self.series.get("close", 2)
                src_high = self.series.get("close")
                src_high_prev = self.series.get("close", 1)
                src_high_prev2 = self.series.get("close", 2)
            else:
                src_low = self.series.get("low")
                src_low_prev = self.series.get("low", 1)
                src_low_prev2 = self.series.get("low", 2)
                src_high = self.series.get("high")
                src_high_prev = self.series.get("high", 1)
                src_high_prev2 = self.series.get("high", 2)

            threshold_up = zone_top
            threshold_dn = zone_bottom
            if mittigation == "Average":
                mid = zone_top - (zone_top - zone_bottom) / 2.0
                threshold_up = mid
                threshold_dn = mid
            elif mittigation not in ("Touch", "Average"):
                threshold_up = zone_bottom
                threshold_dn = zone_top

            remove_zone = False
            if zone_type == 1:
                checks = [src_low, src_low_prev]
                if mittigation != "Touch":
                    checks.append(src_low_prev2)
                if any(not math.isnan(val) and val < threshold_up for val in checks):
                    bullish_break = True
                    remove_zone = True
            elif zone_type == -1:
                checks = [src_high, src_high_prev]
                if mittigation != "Touch":
                    checks.append(src_high_prev2)
                if any(not math.isnan(val) and val > threshold_dn for val in checks):
                    bearish_break = True
                    remove_zone = True

            if remove_zone:
                direction = self._direction_from_ob_type(zone_type)
                breaker_label = EVENT_PRINT_LABELS.get("BREAKER_BLOCK", "Breaker Block")
                self._record_range_event(
                    "BREAKER_BLOCK",
                    text=breaker_label,
                    bottom=zone_bottom,
                    top=zone_top,
                    timestamp=self.series.get_time(),
                    status="breaker",
                    direction=direction,
                )
                top_arr.remove(i)
                btm_arr.remove(i)
                left_arr.remove(i)
                type_arr.remove(i)
                vol_arr.remove(i)
                buy_arr.remove(i)
                sell_arr.remove(i)

        return bullish_break, bearish_break

    def _apply_order_block_filters(self) -> None:
        ds = self.inputs.demand_supply
        self._track_order_block_touches(self.ob_top, self.ob_btm, self.ob_type)
        self._track_order_block_touches(self.ob_top_mtf, self.ob_btm_mtf, self.ob_type_mtf)
        bull_base, bear_base = self._filter_order_blocks(
            self.ob_top,
            self.ob_btm,
            self.ob_left,
            self.ob_type,
            self.ob_vol,
            self.ob_buy_vol,
            self.ob_sell_vol,
            ds.mittigation_filt,
            ds.overlapping_filt,
        )
        bull_mtf, bear_mtf = self._filter_order_blocks(
            self.ob_top_mtf,
            self.ob_btm_mtf,
            self.ob_left_mtf,
            self.ob_type_mtf,
            self.ob_vol_mtf,
            self.ob_buy_vol_mtf,
            self.ob_sell_vol_mtf,
            ds.mittigation_filt_mtf,
            ds.overlapping_filt_mtf,
        )

        self._render_order_blocks(
            self.ob_boxes,
            self.ob_lines,
            self.ob_top,
            self.ob_btm,
            self.ob_left,
            self.ob_type,
            self.ob_vol,
            ds.ibull_ob_css,
            ds.ibear_ob_css,
            ds.text_size_ob_,
            ds.ob_text_color_1,
            ds.length_extend_ob,
            ds.ob_extend,
            ds.volume_text,
            ds.percent_text,
            ds.show_line_ob_1,
            ds.line_style_ob_1,
        )
        self._render_order_blocks(
            self.ob_boxes_mtf,
            self.ob_lines_mtf,
            self.ob_top_mtf,
            self.ob_btm_mtf,
            self.ob_left_mtf,
            self.ob_type_mtf,
            self.ob_vol_mtf,
            ds.ibull_ob_css_2,
            ds.ibear_ob_css_2,
            ds.text_size_ob_2,
            ds.ob_text_color_2,
            ds.length_extend_ob_mtf,
            ds.ob_extend_mtf,
            ds.volume_text_2,
            ds.percent_text_2,
            ds.show_line_ob_2,
            ds.line_style_ob_2,
        )

        self.bullish_OB_Break = bull_base or bull_mtf
        self.bearish_OB_Break = bear_base or bear_mtf

    def _track_order_block_touches(
        self,
        top_arr: PineArray,
        btm_arr: PineArray,
        type_arr: PineArray,
    ) -> None:
        if top_arr.size() == 0:
            return
        high = self.series.get("high")
        low = self.series.get("low")
        if math.isnan(high) or math.isnan(low):
            return
        timestamp = self.series.get_time()
        mitigation_label = EVENT_PRINT_LABELS.get("MITIGATION_BLOCK", "Mitigation Block")
        for i in range(top_arr.size()):
            top_val = float(top_arr.get(i))
            bottom_val = float(btm_arr.get(i))
            if low > top_val or high < bottom_val:
                continue
            type_val = int(type_arr.get(i))
            token = f"{type_val}:{_fmt_price_key(bottom_val)}:{_fmt_price_key(top_val)}"
            if token in self.order_block_touch_seen:
                continue
            self.order_block_touch_seen.add(token)
            self._record_range_event(
                "MITIGATION_BLOCK",
                text=mitigation_label,
                bottom=bottom_val,
                top=top_val,
                timestamp=timestamp,
                status="touched",
                direction=self._direction_from_ob_type(type_val),
            )

    @staticmethod
    def _canonical_mitigation(value: str) -> str:
        if not isinstance(value, str):
            return value
        lookup = {
            "wick": "Wicks",
            "wicks": "Wicks",
            "touch": "Touch",
            "close": "Close",
            "average": "Average",
        }
        return lookup.get(value.lower(), value)

    def _fvg_create(
        self,
        upper: float,
        lower: float,
        mid: float,
        bar_time: int,
        holder: PineArray,
        holder_fill: PineArray,
        midholder: PineArray,
        highholder: PineArray,
        lowholder: PineArray,
        labelholder: PineArray,
        box_color: str,
        mtf_color: str,
        use_htf: bool,
    ) -> None:
        fvg = self.inputs.fvg
        extend_target = self._extend_time(fvg.length_extend)
        color_border = mtf_color if use_htf else box_color
        fill_color = mtf_color if use_htf else box_color
        base_color = fill_color if fvg.fvg_color_fill else "na"
        box_obj = self.box_new(bar_time, extend_target, upper, lower, color_border, text="")
        box_obj.set_border_color(color_border)
        box_obj.set_bgcolor(base_color)
        box_obj.set_extend("extend.right" if fvg.fvg_extend else "extend.none")
        box_obj.set_text_color("#787b86")
        box_obj.set_text_halign("text.align_right")
        box_obj.set_text_size("size.small")
        holder.unshift(box_obj)

        box_fill = self.box_new(bar_time, extend_target, upper, lower, color_border)
        box_fill.set_border_color(color_border if fvg.fvg_color_fill else "na")
        box_fill.set_bgcolor(base_color)
        box_fill.set_extend("extend.right" if fvg.fvg_extend else "extend.none")
        holder_fill.unshift(box_fill)

        mid_line = self.line_new(
            bar_time,
            (lower + upper) / 2.0,
            extend_target,
            mid,
            "xloc.bar_time",
            fvg.i_midPointColor,
            self._map_line_style(fvg.mid_style),
        )
        mid_line.set_extend("extend.right" if fvg.fvg_extend else "extend.none")
        midholder.unshift(mid_line)

        low_color = mtf_color if use_htf else box_color
        low_line = self.line_new(
            bar_time,
            lower,
            extend_target,
            lower,
            "xloc.bar_time",
            low_color if fvg.i_fillByMid else "na",
            "line.style_solid",
        )
        low_line.set_extend("extend.right" if fvg.fvg_extend else "extend.none")
        lowholder.unshift(low_line)

        high_color = mtf_color if use_htf else box_color
        high_line = self.line_new(
            bar_time,
            upper,
            extend_target,
            upper,
            "xloc.bar_time",
            high_color if fvg.i_fillByMid else "na",
            "line.style_solid",
        )
        high_line.set_extend("extend.right" if fvg.fvg_extend else "extend.none")
        highholder.unshift(high_line)

        label_text = fvg.i_tf if use_htf else "Current"
        label_offset = fvg.i_mtfos if use_htf else fvg.i_tfos
        label = self.label_new(
            bar_time + int(self._timediff() * label_offset),
            (upper + lower) / 2.0,
            label_text,
            "xloc.bar_time",
            "yloc.price",
            "color.new(#000000,100)",
            "label.style_label_left",
            "size.small",
            fvg.i_textColor,
        )
        labelholder.unshift(label)
        self._record_console_event(
            "FVG",
            {
                "text": "FVG",
                "price": (lower, upper),
                "time": bar_time,
                "time_display": format_timestamp(bar_time),
                "display": f"FVG {format_price(lower)} → {format_price(upper)}",
                "fingerprint": f"FVG:{_fmt_price_key(lower)}:{_fmt_price_key(upper)}",
            },
        )

    def _fvg_delete(
        self,
        index: int,
        holder: PineArray,
        holder_fill: PineArray,
        midholder: PineArray,
        highholder: PineArray,
        lowholder: PineArray,
        labelholder: PineArray,
        delete_objects: bool,
    ) -> None:
        gap_box: Box = holder.remove(index)
        fill_box: Box = holder_fill.remove(index)
        mid_line: Line = midholder.remove(index)
        high_line: Line = highholder.remove(index)
        low_line: Line = lowholder.remove(index)
        label_obj: Optional[Label] = None
        if labelholder.size() > index:
            label_obj = labelholder.remove(index)
        if delete_objects:
            self._delete_box(gap_box)
            self._delete_box(fill_box)
            self._delete_line(mid_line)
            self._delete_line(high_line)
            self._delete_line(low_line)
            if label_obj:
                self._delete_label(label_obj)
        else:
            current_time = self.series.get_time()
            gap_box.set_extend("extend.none")
            gap_box.set_right(current_time)
            fill_box.set_extend("extend.none")
            fill_box.set_right(current_time)
            mid_line.set_extend("extend.none")
            mid_line.set_x2(current_time)
            high_line.set_extend("extend.none")
            high_line.set_x2(current_time)
            low_line.set_extend("extend.none")
            low_line.set_x2(current_time)
            if label_obj:
                label_obj.x = current_time

    def _fvg_trim(self, arr: PineArray, max_count: int) -> None:
        while arr.size() > max_count:
            obj = arr.pop()
            if isinstance(obj, Box):
                self._delete_box(obj)
            elif isinstance(obj, Line):
                self._delete_line(obj)
            elif isinstance(obj, Label):
                self._delete_label(obj)

    def _fvg_validate_side(
        self,
        high: float,
        low: float,
        close: float,
        is_bullish: bool,
    ) -> int:
        fvg = self.inputs.fvg
        removed_flag = 0
        holder = self.bullish_gap_holder if is_bullish else self.bearish_gap_holder
        holder_fill = self.bullish_gap_fill_holder if is_bullish else self.bearish_gap_fill_holder
        midholder = self.bullish_mid_holder if is_bullish else self.bearish_mid_holder
        highholder = self.bullish_high_holder if is_bullish else self.bearish_high_holder
        lowholder = self.bullish_low_holder if is_bullish else self.bearish_low_holder
        labelholder = self.bullish_label_holder if is_bullish else self.bearish_label_holder
        size = holder.size()
        if size == 0:
            return 0
        extend_target = self._extend_time(fvg.length_extend)
        for i in range(size - 1, -1, -1):
            box_obj: Box = holder.get(i)
            fill_obj: Box = holder_fill.get(i)
            mid_line: Line = midholder.get(i)
            high_line: Line = highholder.get(i)
            low_line: Line = lowholder.get(i)
            if fvg.fvg_extend_B:
                box_obj.set_right(extend_target)
                fill_obj.set_right(extend_target)
                mid_line.set_x2(extend_target)
                high_line.set_x2(extend_target)
                low_line.set_x2(extend_target)
            if is_bullish:
                trigger_top = box_obj.get_top()
                trigger_bottom = fill_obj.get_top()
                trigger_mid = mid_line.get_y1()
                condition_touch = high > trigger_top
                condition_close = close > trigger_top
                condition_average = high > trigger_mid
                condition_wicks = high > trigger_top
                if fvg.fvg_shade_fill and high > trigger_bottom:
                    fill_obj.set_bottom(max(fill_obj.get_bottom(), high))
                    fill_obj.set_bgcolor("#787b865e")
                triggered = False
                if fvg.mittigation_filt_fvg == "Touch" and condition_touch:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Wicks" and condition_wicks:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Close" and condition_close:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Average" and condition_average:
                    triggered = True
                if triggered:
                    removed_flag = 1
                    self._fvg_delete(
                        i,
                        holder,
                        holder_fill,
                        midholder,
                        highholder,
                        lowholder,
                        labelholder,
                        fvg.i_deleteonfill,
                    )
            else:
                trigger_top = fill_obj.get_top()
                trigger_bottom = box_obj.get_bottom()
                trigger_mid = mid_line.get_y1()
                condition_touch = low < trigger_top
                condition_close = close < trigger_top
                condition_average = low < trigger_mid
                condition_wicks = low < trigger_top
                if fvg.fvg_shade_fill and low < trigger_bottom:
                    fill_obj.set_bottom(min(fill_obj.get_bottom(), low))
                    fill_obj.set_bgcolor("#787b865e")
                triggered = False
                if fvg.mittigation_filt_fvg == "Touch" and condition_touch:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Wicks" and condition_wicks:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Close" and condition_close:
                    triggered = True
                elif fvg.mittigation_filt_fvg == "Average" and condition_average:
                    triggered = True
                if triggered:
                    removed_flag = -1
                    self._fvg_delete(
                        i,
                        holder,
                        holder_fill,
                        midholder,
                        highholder,
                        lowholder,
                        labelholder,
                        fvg.i_deleteonfill,
                    )
        return removed_flag

    def _update_fvg(self) -> None:
        fvg = self.inputs.fvg
        if not fvg.show_fvg:
            for arr in [
                self.bullish_gap_holder,
                self.bullish_gap_fill_holder,
                self.bearish_gap_holder,
                self.bearish_gap_fill_holder,
            ]:
                for i in range(arr.size()):
                    box = arr.get(i)
                    box.set_bgcolor("color.new(#000000,100)")
            return
        if self.series.length() < 3:
            return
        self.fvg_gap = 0
        timeframe = fvg.i_tf
        use_htf = fvg.i_mtf in ("Current + HTF", "HTF") and timeframe != ""
        if use_htf and self._is_newbar(timeframe):
            self.htfH = self.series.get("high")
            self.htfL = self.series.get("low")
            feed = self._ensure_security_feed(timeframe)
            if feed and feed.length() >= 3:
                close1 = feed.get("close", 1)
                high2 = feed.get("high", 2)
                low2 = feed.get("low", 2)
                high0 = self.htfH
                low0 = self.htfL
                open1 = feed.get("open", 1)
                if not (math.isnan(close1) or math.isnan(high2) or math.isnan(low2)):
                    range_high = self._series_highest(feed, "high", 300)
                    range_low = self._series_lowest(feed, "low", 300)
                    if not math.isnan(range_high) and not math.isnan(range_low):
                        thold = (range_high - range_low) * max(fvg.max_width_fvg, 0.1) / 100.0
                    else:
                        thold = 0.0
                    if open1 > close1 and low0 > high2:
                        if (not fvg.remove_small) or abs(low0 - high2) > thold:
                            mid = low0 - (low0 - high2) / 2.0
                            self._fvg_create(
                                low0,
                                high2,
                                mid,
                                self.series.get_time(0),
                                self.bullish_gap_holder,
                                self.bullish_gap_fill_holder,
                                self.bullish_mid_holder,
                                self.bullish_high_holder,
                                self.bullish_low_holder,
                                self.bullish_label_holder,
                                fvg.i_bullishfvgcolor,
                                fvg.i_mtfbullishfvgcolor,
                                True,
                            )
                            self.fvg_gap = 1
                    elif open1 < close1 and high0 < low2:
                        if (not fvg.remove_small) or abs(low2 - high0) > thold:
                            mid = high0 + (low2 - high0) / 2.0
                            self._fvg_create(
                                low2,
                                high0,
                                mid,
                                self.series.get_time(0),
                                self.bearish_gap_holder,
                                self.bearish_gap_fill_holder,
                                self.bearish_mid_holder,
                                self.bearish_high_holder,
                                self.bearish_low_holder,
                                self.bearish_label_holder,
                                fvg.i_bearishfvgcolor,
                                fvg.i_mtfbearishfvgcolor,
                                True,
                            )
                            self.fvg_gap = -1

        high = self.series.get("high")
        low = self.series.get("low")
        close = self.series.get("close")
        self.fvg_removed = 0
        removed_bull = self._fvg_validate_side(high, low, close, True)
        removed_bear = self._fvg_validate_side(high, low, close, False)
        if removed_bull == 1:
            self.fvg_removed = 1
        if removed_bear == -1:
            self.fvg_removed = -1 if self.fvg_removed == 0 else self.fvg_removed

        self._fvg_trim(self.bullish_gap_holder, fvg.max_fvg)
        self._fvg_trim(self.bullish_gap_fill_holder, fvg.max_fvg)
        self._fvg_trim(self.bullish_mid_holder, fvg.max_fvg)
        self._fvg_trim(self.bullish_high_holder, fvg.max_fvg)
        self._fvg_trim(self.bullish_low_holder, fvg.max_fvg)
        self._fvg_trim(self.bullish_label_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_gap_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_gap_fill_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_mid_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_high_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_low_holder, fvg.max_fvg)
        self._fvg_trim(self.bearish_label_holder, fvg.max_fvg)


    def _update_liquidity(self) -> None:
        liq = self.inputs.liquidity
        if not liq.currentTF:
            return
        feed = self._ensure_security_feed(liq.htfTF)
        if feed is None or feed.length() < 3:
            return
        ratio = self._tf_multi(liq.htfTF)
        left = max(int(liq.leftBars * ratio), 1)
        right = max(int(liq.leftBars + ratio), 1)
        pivot_high = self._liquidity_pivot(feed, left, right, True)
        pivot_low = self._liquidity_pivot(feed, left, right, False)
        atr_val = self._atr(300)
        thold_liq = atr_val * (liq.box_width / 10.0)
        extend_time = self._extend_time(liq.length_extend_liq)
        style = self._map_line_style(liq._highLineStyleHTF)

        created_high = False
        created_low = False

        if pivot_high is not None:
            time_ref, price = pivot_high
            if self.last_liq_high_time != time_ref:
                if liq.displayStyle_liq == "Lines":
                    line_obj = self.line_new(
                        time_ref,
                        price,
                        extend_time,
                        price,
                        "xloc.bar_time",
                        liq.highLineColorHTF,
                        style,
                    )
                    if liq.extentionMax:
                        line_obj.set_extend("extend.right")
                    self.highLineArrayHTF.push(line_obj)
                else:
                    top = price
                    bottom = price - thold_liq
                    box_obj = self.box_new(
                        time_ref,
                        extend_time,
                        top,
                        bottom,
                        liq.highLineColorHTF,
                        text="$$$",
                        text_color=liq.liquidity_text_color,
                    )
                    box_obj.set_border_color(liq.highBoxBorderColorHTF)
                    box_obj.set_border_style(style)
                    if liq.extentionMax:
                        box_obj.set_extend("extend.right")
                    self.highBoxArrayHTF.push(box_obj)
                self.last_liq_high_time = time_ref
                created_high = True
                self._record_console_event(
                    "LIQUIDITY_LEVELS",
                    {
                        "text": "Liquidity High",
                        "price": price,
                        "time": time_ref,
                        "time_display": format_timestamp(time_ref),
                        "display": f"Liquidity High @ {format_price(price)}",
                        "direction": "bearish",
                        "fingerprint": f"LIQ_HIGH:{_fmt_price_key(price)}",
                    },
                )

        if pivot_low is not None:
            time_ref, price = pivot_low
            if self.last_liq_low_time != time_ref:
                if liq.displayStyle_liq == "Lines":
                    line_obj = self.line_new(
                        time_ref,
                        price,
                        extend_time,
                        price,
                        "xloc.bar_time",
                        liq.lowLineColorHTF,
                        style,
                    )
                    if liq.extentionMax:
                        line_obj.set_extend("extend.right")
                    self.lowLineArrayHTF.push(line_obj)
                else:
                    bottom = price
                    top = price + thold_liq
                    box_obj = self.box_new(
                        time_ref,
                        extend_time,
                        top,
                        bottom,
                        liq.lowLineColorHTF,
                        text="$$$",
                        text_color=liq.liquidity_text_color,
                    )
                    box_obj.set_border_color(liq.lowBoxBorderColorHTF)
                    box_obj.set_border_style(style)
                    if liq.extentionMax:
                        box_obj.set_extend("extend.right")
                    self.lowBoxArrayHTF.push(box_obj)
                self.last_liq_low_time = time_ref
                created_low = True
                self._record_console_event(
                    "LIQUIDITY_LEVELS",
                    {
                        "text": "Liquidity Low",
                        "price": price,
                        "time": time_ref,
                        "time_display": format_timestamp(time_ref),
                        "display": f"Liquidity Low @ {format_price(price)}",
                        "direction": "bullish",
                        "fingerprint": f"LIQ_LOW:{_fmt_price_key(price)}",
                    },
                )

        high_line_alert = self._liquidity_remove_mitigated_lines(self.highLineArrayHTF, True, liq)
        low_line_alert = self._liquidity_remove_mitigated_lines(self.lowLineArrayHTF, False, liq)
        high_box_alert = self._liquidity_remove_mitigated_boxes(self.highBoxArrayHTF, True, liq)
        low_box_alert = self._liquidity_remove_mitigated_boxes(self.lowBoxArrayHTF, False, liq)
        if high_line_alert or high_box_alert:
            price = self.series.get("high")
            self._record_console_event(
                "LIQUIDITY_TOUCH",
                {
                    "text": "Liquidity Sweep High",
                    "price": price,
                    "time": self.series.get_time(),
                    "time_display": format_timestamp(self.series.get_time()),
                    "display": f"Liquidity Sweep High @ {format_price(price)}",
                    "direction": "bearish",
                    "direction_display": "هابط",
                    "fingerprint": f"LIQ_SWEEP_HIGH:{_fmt_price_key(price)}",
                },
            )
        if low_line_alert or low_box_alert:
            price = self.series.get("low")
            self._record_console_event(
                "LIQUIDITY_TOUCH",
                {
                    "text": "Liquidity Sweep Low",
                    "price": price,
                    "time": self.series.get_time(),
                    "time_display": format_timestamp(self.series.get_time()),
                    "display": f"Liquidity Sweep Low @ {format_price(price)}",
                    "direction": "bullish",
                    "direction_display": "صاعد",
                    "fingerprint": f"LIQ_SWEEP_LOW:{_fmt_price_key(price)}",
                },
            )

        self._liquidity_extend_lines(self.highLineArrayHTF, extend_time)
        self._liquidity_extend_lines(self.lowLineArrayHTF, extend_time)
        self._liquidity_extend_boxes(self.highBoxArrayHTF, extend_time)
        self._liquidity_extend_boxes(self.lowBoxArrayHTF, extend_time)


    def _update_swing_detection(self) -> None:
        inputs = self.inputs.swing_detection
        lbLeft = 20
        lbRight = 20
        length = self.series.length()
        if length <= lbRight:
            self.bullishSFP_history.append(False)
            self.bearishSFP_history.append(False)
            if len(self.bullishSFP_history) > 50:
                self.bullishSFP_history.pop(0)
            if len(self.bearishSFP_history) > 50:
                self.bearishSFP_history.pop(0)
            self.pLowVal_history.append(self.pLowVal_history[-1] if self.pLowVal_history else NA)
            self.pHighVal_history.append(self.pHighVal_history[-1] if self.pHighVal_history else NA)
            return

        low = self.series.get("low")
        high = self.series.get("high")
        close = self.series.get("close")
        open_ = self.series.get("open")
        prev_close = self.series.get("close", 1)
        prev_close2 = self.series.get("close", 2)
        bar_index = length - 1
        time_val = self.series.get_time()

        pivot_low = self._pivot_point(lbLeft, lbRight, True)
        pivot_high = self._pivot_point(lbLeft, lbRight, False)

        if pivot_low is not None:
            current_p_low = pivot_low[1]
            prevLowIndex = pivot_low[0]
        else:
            current_p_low = self.pLowVal_history[-1] if self.pLowVal_history else NA
            prevLowIndex = self.series.get_time(lbRight)
        if pivot_high is not None:
            current_p_high = pivot_high[1]
            prevHighIndex = pivot_high[0]
        else:
            current_p_high = self.pHighVal_history[-1] if self.pHighVal_history else NA
            prevHighIndex = self.series.get_time(lbRight)

        self.pLowVal_history.append(current_p_low)
        self.pHighVal_history.append(current_p_high)
        if len(self.pLowVal_history) > 100:
            self.pLowVal_history.pop(0)
        if len(self.pHighVal_history) > 100:
            self.pHighVal_history.pop(0)

        def _lowest(series: str, count: int) -> float:
            values = [self.series.get(series, i) for i in range(count)]
            filtered = [v for v in values if not math.isnan(v)]
            return min(filtered) if filtered else math.nan

        def _highest(series: str, count: int) -> float:
            values = [self.series.get(series, i) for i in range(count)]
            filtered = [v for v in values if not math.isnan(v)]
            return max(filtered) if filtered else math.nan

        lp = _lowest("low", lbLeft)
        hp = _highest("high", lbLeft)
        highestClose = _highest("close", lbLeft)
        lowestClose = _lowest("close", lbLeft)

        bullishSFP = (
            pivot_low is not None
            and not math.isnan(current_p_low)
            and not math.isnan(lp)
            and not math.isnan(lowestClose)
            and low < current_p_low
            and close > current_p_low
            and open_ > current_p_low
            and math.isclose(low, lp, rel_tol=1e-9, abs_tol=1e-9)
            and lowestClose >= current_p_low
        )

        bearishSFP = (
            pivot_high is not None
            and not math.isnan(current_p_high)
            and not math.isnan(hp)
            and not math.isnan(highestClose)
            and high > current_p_high
            and close < current_p_high
            and open_ < current_p_high
            and math.isclose(high, hp, rel_tol=1e-9, abs_tol=1e-9)
            and highestClose <= current_p_high
        )

        self.bullishSFP_history.append(bullishSFP)
        self.bearishSFP_history.append(bearishSFP)
        if len(self.bullishSFP_history) > 100:
            self.bullishSFP_history.pop(0)
        if len(self.bearishSFP_history) > 100:
            self.bearishSFP_history.pop(0)

        prev_bull = self.bullishSFP_history[-4] if len(self.bullishSFP_history) >= 4 else False
        prev_bear = self.bearishSFP_history[-4] if len(self.bearishSFP_history) >= 4 else False

        prev_p_low1 = self.pLowVal_history[-2] if len(self.pLowVal_history) >= 2 else NA
        prev_p_low2 = self.pLowVal_history[-3] if len(self.pLowVal_history) >= 3 else NA
        prev_p_high1 = self.pHighVal_history[-2] if len(self.pHighVal_history) >= 2 else NA
        prev_p_high2 = self.pHighVal_history[-3] if len(self.pHighVal_history) >= 3 else NA

        bullCond = (
            prev_bull
            and not math.isnan(current_p_low)
            and not math.isnan(prev_p_low1)
            and not math.isnan(prev_p_low2)
            and close > current_p_low
            and prev_close > prev_p_low1
            and prev_close2 > prev_p_low2
            and bar_index >= self.bullSignalIndex + inputs.cooldownPeriod
        )

        bearCond = (
            prev_bear
            and not math.isnan(current_p_high)
            and not math.isnan(prev_p_high1)
            and not math.isnan(prev_p_high2)
            and close < current_p_high
            and prev_close < prev_p_high1
            and prev_close2 < prev_p_high2
            and bar_index >= self.bearSignalIndex + inputs.cooldownPeriod
        )

        display_third = inputs.display_third

        if bullCond and display_third:
            self.bullSignalIndex = bar_index
            if self.bullLine is not None:
                self._delete_line(self.bullLine)
            end_time = self.series.get_time(3) if length > 3 else time_val
            self.bullLine = self.line_new(
                prevLowIndex,
                current_p_low,
                end_time,
                current_p_low,
                "xloc.bar_time",
                inputs.bullColor,
                self._map_line_style(inputs.bullStyle),
            )
            self.bullLine.set_width(inputs.bullWidth)

        if bearCond and display_third:
            self.bearSignalIndex = bar_index
            if self.bearLine is not None:
                self._delete_line(self.bearLine)
            end_time = self.series.get_time(3) if length > 3 else time_val
            self.bearLine = self.line_new(
                prevHighIndex,
                current_p_high,
                end_time,
                current_p_high,
                "xloc.bar_time",
                inputs.bearColor,
                self._map_line_style(inputs.bearStyle),
            )
            self.bearLine.set_width(inputs.bearWidth)

        if inputs.showSwing_ and display_third:
            if not self.stopPrintingHigh and self.highLine is not None:
                self.highLine.set_x2(time_val + 5 * max(self.curTf, 1))
            if not self.stopPrintingLow and self.lowLine is not None:
                self.lowLine.set_x2(time_val + 5 * max(self.curTf, 1))

            if pivot_high is not None and not bearishSFP:
                self.stopPrintingHigh = False
                self.swingHighVal = current_p_high
                if self.highLine is not None:
                    self._delete_line(self.highLine)
                self.highLine = self.line_new(
                    pivot_high[0],
                    current_p_high,
                    time_val + 10 * max(self.curTf, 1),
                    current_p_high,
                    "xloc.bar_time",
                    inputs.swingClr,
                    "line.style_solid",
                )
                self.highLine.set_width(2)
                if self.swingHighLbl is not None:
                    self._delete_label(self.swingHighLbl)
                if self.swingHighLblTxt is not None:
                    self._delete_label(self.swingHighLblTxt)
                self.swingHighLbl = self.label_new(
                    pivot_high[0],
                    current_p_high,
                    "",
                    "xloc.bar_time",
                    "yloc.abovebar",
                    inputs.swingClr,
                    "label.style_triangledown",
                    "size.auto",
                    inputs.swingClr,
                )
                self.swingHighLblTxt = self.label_new(
                    pivot_high[0],
                    current_p_high,
                    "Swing\nH",
                    "xloc.bar_time",
                    "yloc.abovebar",
                    inputs.swingClr,
                    "label.style_none",
                    "size.small",
                    inputs.swingClr,
                )
                self.swingHighArr.push(self.swingHighLbl)
                self.swingHighTextArr.push(self.swingHighLblTxt)

            if pivot_low is not None and not bullishSFP:
                self.stopPrintingLow = False
                self.swingLowVal = current_p_low
                if self.lowLine is not None:
                    self._delete_line(self.lowLine)
                self.lowLine = self.line_new(
                    pivot_low[0],
                    current_p_low,
                    time_val + 10 * max(self.curTf, 1),
                    current_p_low,
                    "xloc.bar_time",
                    inputs.swingClr,
                    "line.style_solid",
                )
                self.lowLine.set_width(2)
                if self.swingLowLbl is not None:
                    self._delete_label(self.swingLowLbl)
                if self.swingLowLblTxt is not None:
                    self._delete_label(self.swingLowLblTxt)
                self.swingLowLbl = self.label_new(
                    pivot_low[0],
                    current_p_low,
                    "",
                    "xloc.bar_time",
                    "yloc.belowbar",
                    inputs.swingClr,
                    "label.style_triangleup",
                    "size.auto",
                    inputs.swingClr,
                )
                self.swingLowLblTxt = self.label_new(
                    pivot_low[0],
                    current_p_low,
                    "Swing\nL",
                    "xloc.bar_time",
                    "yloc.belowbar",
                    inputs.swingClr,
                    "label.style_none",
                    "size.small",
                    inputs.swingClr,
                )
                self.swingLowArr.push(self.swingLowLbl)
                self.swingLowTextArr.push(self.swingLowLblTxt)

        if self.swingLowArr.size() >= 3:
            lbl = self.swingLowArr.remove(0)
            txt = self.swingLowTextArr.remove(0)
            if lbl:
                self._delete_label(lbl)
            if txt:
                self._delete_label(txt)
        if self.swingHighArr.size() >= 3:
            lbl = self.swingHighArr.remove(0)
            txt = self.swingHighTextArr.remove(0)
            if lbl:
                self._delete_label(lbl)
            if txt:
                self._delete_label(txt)

        if not math.isnan(self.swingLowVal):
            if self.isSwingLowCheck and high < self.swingLowVal:
                self.swingLowCounter += 1
            if self._crossunder(prev_close, close, self.swingLowVal) and not self.isSwingLowCheck:
                self.isSwingLowCheck = True
                self.swingLowCounter = 1
            if self.swingLowCounter >= 5 and self.isSwingLowCheck and self.lowLine is not None:
                self.stopPrintingLow = True
                self.isSwingLowCheck = False
                x2_time = self.series.get_time(4) if length > 4 else time_val
                self.lowLine.set_x2(x2_time)

        if not math.isnan(self.swingHighVal):
            if self.isSwingHighCheck and low > self.swingHighVal:
                self.swingHighCounter += 1
            if self._crossover(prev_close, close, self.swingHighVal) and not self.isSwingHighCheck:
                self.isSwingHighCheck = True
                self.swingHighCounter = 1
            if self.swingHighCounter >= 5 and self.isSwingHighCheck and self.highLine is not None:
                self.stopPrintingHigh = True
                self.isSwingHighCheck = False
                x2_time = self.series.get_time(4) if length > 4 else time_val
                self.highLine.set_x2(x2_time)


    def _update_candlestick_patterns(self) -> None:
        if self.series.length() < 2:
            return

        inputs = self.inputs.candle
        open_ = self.series.get("open")
        close = self.series.get("close")
        high = self.series.get("high")
        low = self.series.get("low")
        prev_open = self.series.get("open", 1)
        prev_close = self.series.get("close", 1)

        if any(math.isnan(v) for v in (open_, close, prev_open, prev_close, high, low)):
            self.candle_black_body_history.append(False)
            self.candle_white_body_history.append(False)
            self.candle_small_body_history.append(False)
            for history in (
                self.candle_black_body_history,
                self.candle_white_body_history,
                self.candle_small_body_history,
            ):
                if len(history) > 50:
                    history.pop(0)
            return

        body_hi = max(close, open_)
        body_lo = min(close, open_)
        body = body_hi - body_lo
        self.candle_body_avg = self._ema(self.candle_body_avg, body, 14)
        small_body = body < self.candle_body_avg if not math.isnan(self.candle_body_avg) else False
        long_body = body > self.candle_body_avg if not math.isnan(self.candle_body_avg) else False
        white_body = open_ < close
        black_body = open_ > close

        down_trend = True
        up_trend = True
        trend_rule = inputs.trendRule
        if trend_rule == "SMA50":
            price_avg = self._sma("close", 50)
            if math.isnan(price_avg):
                down_trend = False
                up_trend = False
            else:
                down_trend = close < price_avg
                up_trend = close > price_avg
        elif trend_rule == "SMA50, SMA200":
            sma50 = self._sma("close", 50)
            sma200 = self._sma("close", 200)
            if math.isnan(sma50) or math.isnan(sma200):
                down_trend = False
                up_trend = False
            else:
                down_trend = close < sma50 and sma50 < sma200
                up_trend = close > sma50 and sma50 > sma200

        prev_black = self.candle_black_body_history[-1] if self.candle_black_body_history else False
        prev_white = self.candle_white_body_history[-1] if self.candle_white_body_history else False
        prev_small = self.candle_small_body_history[-1] if self.candle_small_body_history else False

        bullish_engulfing = (
            down_trend
            and white_body
            and long_body
            and prev_black
            and prev_small
            and close >= prev_open
            and open_ <= prev_close
            and (close > prev_open or open_ < prev_close)
        )

        bearish_engulfing = (
            up_trend
            and black_body
            and long_body
            and prev_white
            and prev_small
            and close <= prev_open
            and open_ >= prev_close
            and (close < prev_open or open_ > prev_close)
        )

        atr30 = self._atr(30)
        patternLabelPosLow = low - (atr30 * 0.6)
        patternLabelPosHigh = high + (atr30 * 0.6)
        display_third = self.inputs.swing_detection.display_third

        if bullish_engulfing and display_third:
            tooltip = (
                "Engulfing\nAt the end of a given downward trend, there will most likely be a reversal pattern. "
                "To distinguish the first day, this candlestick pattern uses a small body, followed by a day where the "
                "candle body fully overtakes the body from the day before, and closes in the trend’s opposite direction. "
                "Although similar to the outside reversal chart pattern, it is not essential for this pattern to completely "
                "overtake the range (high to low), rather only the open and the close."
            )
            self.label_new(
                self.series.get_time(),
                patternLabelPosLow,
                "BE",
                "xloc.bar_time",
                "yloc.belowbar",
                inputs.label_color_bullish,
                "label.style_label_up",
                "size.auto",
                "color.white",
                tooltip,
            )

        if bearish_engulfing and display_third:
            tooltip = (
                "Engulfing\nAt the end of a given uptrend, a reversal pattern will most likely appear. During the first day, "
                "this candlestick pattern uses a small body. It is then followed by a day where the candle body fully overtakes "
                "the body from the day before it and closes in the trend’s opposite direction. Although similar to the outside "
                "reversal chart pattern, it is not essential for this pattern to fully overtake the range (high to low), rather "
                "only the open and the close."
            )
            self.label_new(
                self.series.get_time(),
                patternLabelPosHigh,
                "BE",
                "xloc.bar_time",
                "yloc.abovebar",
                inputs.label_color_bearish,
                "label.style_label_down",
                "size.auto",
                "color.white",
                tooltip,
            )

        self.candle_black_body_history.append(black_body)
        self.candle_white_body_history.append(white_body)
        self.candle_small_body_history.append(small_body)
        for history in (
            self.candle_black_body_history,
            self.candle_white_body_history,
            self.candle_small_body_history,
        ):
            if len(history) > 50:
                history.pop(0)

    def _update_daily_levels(self, high: float, low: float, time_val: int) -> None:
        if time_val == 0:
            return
        current_day = time_val // self.dayTf
        if self.current_day is None:
            self.current_day = current_day
            self.day_high = high
            self.day_low = low
            self.prev_day_high = high
            self.prev_day_low = low
        elif current_day != self.current_day:
            self.prev_day_high = self.day_high
            self.prev_day_low = self.day_low
            self.current_day = current_day
            self.day_high = high
            self.day_low = low
        else:
            self.day_high = max(self.day_high, high)
            self.day_low = min(self.day_low, low)
        self.pdh = self.prev_day_high
        self.pdl = self.prev_day_low

    def removeNLastLabel(self, arr: PineArray, n: int) -> None:
        if arr.size() > n - 1:
            label_obj = arr.get(arr.size() - n)
            if label_obj in self.labels:
                self.labels.remove(label_obj)

    def removeNLastLine(self, arr: PineArray, n: int) -> None:
        if arr.size() > n - 1:
            line_obj = arr.get(arr.size() - n)
            if line_obj in self.lines:
                self.lines.remove(line_obj)

    def removeLastLabel(self, arr: PineArray, n: int) -> None:
        if arr.size() > n - 1:
            for i in range(1, n + 1):
                label_obj = arr.get(arr.size() - i)
                if label_obj in self.labels:
                    self.labels.remove(label_obj)

    def removeLastLine(self, arr: PineArray, n: int) -> None:
        if arr.size() > n - 1:
            for i in range(1, n + 1):
                line_obj = arr.get(arr.size() - i)
                if line_obj in self.lines:
                    self.lines.remove(line_obj)

    def removeZone(self, zoneArray: PineArray, zone: Box, zoneArrayisMit: PineArray, isBull: bool) -> None:
        index = zoneArray.indexof(zone)
        if index == -1:
            return
        if not self.inputs.order_block.showBrkob:
            if zone in self.boxes:
                self.boxes.remove(zone)
        else:
            zone.set_right(self.series.get_time())
            zone.set_extend("extend.none")
            if not isBull:
                self.arrmitOBBull.unshift(zone)
                self.arrmitOBBulla.unshift(False)
            else:
                self.arrmitOBBear.unshift(zone)
                self.arrmitOBBeara.unshift(False)
        zoneArray.remove(index)
        zoneArrayisMit.remove(index)

    def ob_found(
        self,
        series: SecuritySeries,
        timeframe: str,
        show_ob: bool,
        show_iob: bool,
    ) -> Tuple[bool, float, int, int, float, float, int, int, str]:
        if series is None or series.length() < 6:
            return (False, 0.0, 0, 0, NA, NA, 0, 0, "none")

        def g(name: str, offset: int) -> float:
            return series.get(name, offset)

        def t(offset: int) -> int:
            return series.get_time(offset)

        open0 = g("open", 0)
        close0 = g("close", 0)
        high0 = g("high", 0)
        low0 = g("low", 0)
        open4 = g("open", 4)
        close4 = g("close", 4)
        high4 = g("high", 4)
        low4 = g("low", 4)
        open5 = g("open", 5)
        close5 = g("close", 5)
        high5 = g("high", 5)
        low5 = g("low", 5)
        high1 = g("high", 1)
        low1 = g("low", 1)
        volume5 = g("volume", 5)
        volume4 = g("volume", 4)

        required = [open5, close5, high5, low5, open4, close4, high4, low4]
        if any(math.isnan(val) for val in required):
            return (False, 0.0, 0, 0, NA, NA, 0, 0, "none")

        type_obs = "none"
        valid = False
        H = high0
        L = low0
        O = open0
        C = close0
        V = g("volume", 0)
        idx = series.get_time(0)
        use_max = False

        close3 = g("close", 3)
        if (
            open5 > close5
            and close4 >= open5
            and low1 > high5
            and low0 > high5
            and show_iob
        ):
            if low5 > low4:
                type_obs = "Internal Bearish"
                H = min(high4, high5)
                L = low4
                O = open4
                C = close4
                V = volume4
                idx = t(4)
                valid = True
                use_max = False
            else:
                type_obs = "Internal Bearish"
                H = high5
                L = low5
                O = open5
                C = close5
                V = volume5
                idx = t(5)
                valid = True
                use_max = False
        elif (
            open5 < close5
            and close4 <= open5
            and high1 < low5
            and high0 < low5
            and show_iob
        ):
            if high4 > high5:
                type_obs = "Internal Bullish"
                H = high4
                L = max(low4, low5)
                O = open4
                C = close4
                V = volume4
                idx = t(4)
                valid = True
                use_max = True
            else:
                type_obs = "Internal Bullish"
                H = high5
                L = low5
                O = open5
                C = close5
                V = volume5
                idx = t(5)
                valid = True
                use_max = True
        elif (
            open5 > close5
            and close4 > close5
            and not math.isnan(close3)
            and close3 >= open5
            and low0 > high5
            and show_iob
        ):
            if low5 > low4:
                type_obs = "Internal Bearish"
                H = min(high4, high5)
                L = low4
                O = open4
                C = close4
                V = volume4
                idx = t(4)
                valid = True
                use_max = False
            else:
                type_obs = "Internal Bearish"
                H = high5
                L = low5
                O = open5
                C = close5
                V = volume5
                idx = t(5)
                valid = True
                use_max = False
        elif (
            open5 < close5
            and close4 < close5
            and not math.isnan(close3)
            and close3 <= open5
            and high0 < low5
            and show_iob
        ):
            if high4 > high5:
                type_obs = "Internal Bullish"
                H = high4
                L = max(low4, low5)
                O = open4
                C = close4
                V = volume4
                idx = t(4)
                valid = True
                use_max = True
            else:
                type_obs = "Internal Bullish"
                H = high5
                L = low5
                O = open5
                C = close5
                V = volume5
                idx = t(5)
                valid = True
                use_max = True
        else:
            # External order blocks
            open1 = g("open", 1)
            close1 = g("close", 1)
            high1c = g("high", 1)
            low1c = g("low", 1)
            high2 = g("high", 2)
            low2 = g("low", 2)
            high3 = g("high", 3)
            low3 = g("low", 3)
            if any(math.isnan(val) for val in [open1, close1, high1c, low1c, high2, low2, high3, low3]):
                pass
            else:
                if (
                    open1 > close1
                    and close0 > close1
                    and close0 >= open1
                    and low0 > high1c
                    and show_ob
                ):
                    type_obs = "External Bearish"
                    H = high1c
                    L = low1c
                    O = open1
                    C = close1
                    V = g("volume", 1)
                    idx = t(1)
                    valid = True
                    use_max = False
                elif (
                    open1 < close1
                    and close0 < close1
                    and close0 <= open1
                    and high0 < low1c
                    and show_ob
                ):
                    type_obs = "External Bullish"
                    H = high1c
                    L = low1c
                    O = open1
                    C = close1
                    V = g("volume", 1)
                    idx = t(1)
                    valid = True
                    use_max = True

        if not valid:
            return (False, 0.0, 0, 0, NA, NA, 0, 0, "none")

        range_ = H - L
        if math.isclose(range_, 0.0):
            range_ = 1e-9
        buyingVolume = round(V * (C - L) / range_)
        sellingVolume = round(V * (H - C) / range_)
        t_volume = (buyingVolume + sellingVolume) / 2.0
        key = self._security_key(timeframe, _parse_timeframe_to_seconds(timeframe, self.base_tf_seconds))
        self._record_ob_volume(key, t_volume)
        highest_tv = self._highest_ob_volume(key, t_volume if t_volume != 0 else 1.0)
        if math.isclose(highest_tv, 0.0):
            highest_tv = 1.0
        b_volume = int((buyingVolume / highest_tv) * 100)
        s_volume = int((sellingVolume / highest_tv) * 100)
        volume_ = V

        width_ratio = self.inputs.demand_supply.max_width_ob
        if math.isclose(width_ratio, 3.0):
            width_ratio = 20.0
        thold = (
            (self._series_highest(series, "high", 300) - self._series_lowest(series, "low", 300))
            * (width_ratio / 2.0)
            / 100.0
        )
        thold = 0.0 if math.isnan(thold) else thold

        if use_max:
            max_val = H
            min_val = max(L, max_val - thold)
        else:
            max_val_candidate = H
            min_val = L
            max_val = min(max_val_candidate, min_val + thold)

        return (True, float(volume_), b_volume, s_volume, float(max_val), float(min_val), idx, -1 if use_max else 1, type_obs)

    def fixStrcAfterBos(self) -> None:
        self.removeLastLabel(self.arrBCLabel, 1)
        self.removeLastLine(self.arrBCLine, 1)
        self.removeLastLabel(self.arrIdmLabel, 1)
        self.removeLastLine(self.arrIdmLine, 1)
        self.removeLastLabel(self.arrHLLabel, 2)
        self.removeLastLabel(self.arrHLCircle, 2)

    def fixStrcAfterChoch(self) -> None:
        self.removeLastLabel(self.arrBCLabel, 2)
        self.removeLastLine(self.arrBCLine, 2)
        self.removeNLastLabel(self.arrHLLabel, 2)
        self.removeNLastLabel(self.arrHLLabel, 3)
        self.removeNLastLabel(self.arrHLCircle, 2)
        self.removeNLastLabel(self.arrHLCircle, 3)
        self.removeNLastLabel(self.arrIdmLabel, 2)
        self.removeNLastLine(self.arrIdmLine, 2)

    def sweepHL(self, trend: bool) -> None:
        if not self.inputs.structure_util.showSw:
            return
        x, y = self.getDirection(trend, self.lastHBar, self.lastLBar, self.lastH, self.lastL)
        ln = self.line_new(
            x,
            y,
            self.series.get_time(),
            y,
            "xloc.bar_time",
            self.inputs.structure_util.colorSweep,
            "line.style_dotted",
        )
        if self.inputs.structure_util.markX:
            self.label_new(
                self.textCenter(self.series.get_time(), x),
                y,
                "X",
                "xloc.bar_time",
                self.getYloc(trend),
                self.transp,
                self.getStyleLabel(trend),
                "size.small",
                self.inputs.structure_util.colorSweep,
            )
        self.arrBCLine.push(ln)
        self._record_console_event(
            "SWING_SWEEP",
            {
                "text": "Swing Sweep",
                "price": y,
                "time": self.series.get_time(),
                "time_display": format_timestamp(self.series.get_time()),
                "display": f"Swing Sweep @ {format_price(y)}",
                "fingerprint": f"SWING_SWEEP:{_fmt_price_key(y)}",
            },
        )

    def TP(self, H: float, L: float) -> None:
        target = (self.series.get("high") + abs(H - L)) if self.isCocUp else (self.series.get("low") - abs(H - L))
        if target < 0:
            target = 0
        if self.inputs.structure_util.showTP:
            self.line_new(
                self.series.get_time(),
                self.series.get("high") if self.isCocUp else self.series.get("low"),
                self.series.get_time(),
                target,
                "xloc.bar_time",
                self.colorTP,
                "line.style_arrow_right",
            )

    def _color_new_expr(self, base_color: str) -> str:
        if base_color.startswith("color.new("):
            return base_color
        if base_color.startswith("#"):
            return f"color.new({base_color}, 20)"
        if base_color.startswith("color.") or base_color.startswith("color.rgb"):
            return f"color.new({base_color}, 20)"
        return base_color

    def createBox(
        self,
        left: int,
        right: int,
        top: float,
        bottom: float,
        color: str,
        *,
        text: str = "",
        text_size: Optional[str] = None,
        text_color: Optional[str] = None,
    ) -> Box:
        box_obj = self.box_new(left, right, top, bottom, color, text=text)
        box_obj.set_text_color(self._color_new_expr(text_color or color))
        box_obj.set_text_size(text_size or self.inputs.order_block.txtsiz)
        box_obj.set_text_halign("text.align_center")
        box_obj.set_text_valign("text.align_center")
        box_obj.set_extend("extend.none")
        return box_obj

    def marginZone(self, zone: Optional[Box]) -> Tuple[float, float, int]:
        if zone is None:
            return NA, NA, 0
        return zone.top, zone.bottom, zone.left

    def drawLiveStrc(
        self,
        condition: bool,
        direction: bool,
        color1: str,
        color2: str,
        txt: str,
        length: int,
        label_attr: str,
        line_attr: str,
    ) -> None:
        current_line: Optional[Line] = getattr(self, line_attr)
        current_label: Optional[Label] = getattr(self, label_attr)
        if current_line and current_line in self.lines:
            self.lines.remove(current_line)
        if current_label and current_label in self.labels:
            self.labels.remove(current_label)
        new_line: Optional[Line] = None
        new_label: Optional[Label] = None
        if condition:
            color_text = color1 if direction else color2
            if txt == self.IDM_TEXT:
                x, y = self.getDirection(direction, self.idmHBar, self.idmLBar, self.idmHigh, self.idmLow)
                line_color = self.inputs.structure.colorIDM
                style = "line.style_dotted"
            else:
                x, y = self.getDirection(direction, self.lastHBar, self.lastLBar, self.lastH, self.lastL)
                line_color = color_text
                style = "line.style_dotted"
            x2 = self.series.get_time() + int(self.len * length)
            new_line = self.line_new(x, y, x2, y, "xloc.bar_time", line_color, style)
            label_text = f"{txt} - {y}" if txt else f"{y}"
            new_label = self.label_new(
                x2,
                y,
                label_text,
                "xloc.bar_time",
                "yloc.price",
                self.transp,
                "label.style_label_left",
                "size.small",
                color_text,
            )
            self._trace(
                "structure",
                "drawLiveStrc",
                timestamp=self.series.get_time(),
                text=txt,
                x=x2,
                y=y,
                direction="up" if direction else "down",
            )
        setattr(self, line_attr, new_line)
        setattr(self, label_attr, new_label)

    def fibo_limit(self, ratio: float, range_high: float, range_low: float) -> float:
        range_1 = range_high - range_low
        return range_high - range_1 * ratio

    def drawPrevStrc(
        self,
        condition: bool,
        txt: str,
        label_attr: str,
        line_attr: str,
        ote: float,
    ) -> Tuple[float, Optional[int], bool]:
        val = NA
        valiIdx: Optional[int] = None
        current_line: Optional[Line] = getattr(self, line_attr)
        current_label: Optional[Label] = getattr(self, label_attr)
        if current_line and current_line in self.lines:
            self.lines.remove(current_line)
        if current_label and current_label in self.labels:
            self.labels.remove(current_label)
        idDirUP = self.lastLBar < self.lastHBar
        if condition:
            if txt == self.PDH_TEXT:
                x = self.getPdhlBar(self.pdh)
                y = self.pdh
                color = self.inputs.structure.bull
                length = self.inputs.structure_util.lengPdh
                style = "line.style_solid"
            elif txt == self.PDL_TEXT:
                x = self.getPdhlBar(self.pdl)
                y = self.pdl
                color = self.inputs.structure.bear
                length = self.inputs.structure_util.lengPdl
                style = "line.style_solid"
            elif txt == self.MID_TEXT:
                x = min(self.lastLBar, self.lastHBar)
                y = pine_avg(self.lastL, self.lastH)
                color = self.inputs.structure.colorIDM
                length = self.inputs.structure_util.lengMid
                style = "line.style_dotted"
            else:
                x = min(self.lastLBar, self.lastHBar)
                y = self.fibo_limit(ote, self.lastH, self.lastL) if idDirUP else self.fibo_limit(ote, self.lastL, self.lastH)
                color = self.inputs.structure.colorIDM
                length = self.inputs.structure_util.lengMid
                style = "line.style_dotted"
            if not math.isnan(y):
                val = y
                valiIdx = x
                x2 = self.series.get_time() + int(self.len * length)
                new_line = self.line_new(x, y, x2, y, "xloc.bar_time", color, style)
                new_label = None
                if txt:
                    new_label = self.label_new(
                        x2,
                        y,
                        f"{txt} - {y}",
                        "xloc.bar_time",
                        "yloc.price",
                        self.transp,
                        "label.style_label_left",
                        "size.small",
                        color,
                    )
                if txt == self.MID_TEXT:
                    high = self.series.get("high")
                    low = self.series.get("low")
                    if not math.isnan(high) and not math.isnan(low) and low <= y <= high:
                        touch_time = self.series.get_time()
                        self._record_console_event(
                            "EQUILIBRIUM",
                            {
                                "text": "0.5",
                                "price": y,
                                "time": touch_time,
                                "time_display": format_timestamp(touch_time),
                                "display": f"0.5 @ {format_price(y)}",
                                "fingerprint": f"EQUILIBRIUM:{_fmt_price_key(y)}:{touch_time}",
                            },
                        )
                setattr(self, line_attr, new_line)
                setattr(self, label_attr, new_label)
        return val, valiIdx, idDirUP

    # ------------------------------------------------------------------
    # High level drawing helpers
    # ------------------------------------------------------------------
    def drawIDM(self, trend: bool) -> Optional[Box]:
        x, y = self.getDirection(trend, self.idmLBar, self.idmHBar, self.idmLow, self.idmHigh)
        lstBx_: Optional[Box] = None
        if trend:
            idx = -1
            lstPrs: Optional[float] = None
            for i in range(self.demandZone.size()):
                bx = self.demandZone.get(i)
                if (
                    self.demandZoneIsMit.get(i) == 0
                    and (lstPrs is None or bx.top <= lstPrs)
                    and bx.top <= y
                    and bx.bottom >= (self.lstHlPrsIdm if not math.isnan(self.lstHlPrsIdm) else -math.inf)
                ):
                    idx = i
                    lstPrs = bx.top
            if idx != -1:
                self._archive_box(self.lstBxIdm, "Hist IDM OB", self.hist_idm_boxes)
                self.lstBxIdm = self.demandZone.get(idx)
                if self.inputs.order_block.showIdmob:
                    zone = self.demandZone.get(idx)
                    zone.set_text("IDM OB")
                    zone.set_text_color(self.inputs.order_block.clrtxtextbulliem)
                    zone.set_bgcolor(self.inputs.order_block.clrtxtextbulliembg)
                    self._register_box_event(zone, status="new")
                    self.demandZoneIsMit.set(idx, 1)
                else:
                    self.removeZone(self.demandZone, self.demandZone.get(idx), self.demandZoneIsMit, True)
                lstBx_ = self.demandZone.get(idx) if idx != -1 else None
        else:
            idx = -1
            lstPrs = None
            for i in range(self.supplyZone.size()):
                bx = self.supplyZone.get(i)
                if (
                    self.supplyZoneIsMit.get(i) == 0
                    and (lstPrs is None or bx.bottom >= lstPrs)
                    and bx.bottom >= y
                    and bx.top <= (self.lstHlPrsIdm if not math.isnan(self.lstHlPrsIdm) else math.inf)
                ):
                    idx = i
                    lstPrs = bx.top
            if idx != -1:
                self._archive_box(self.lstBxIdm, "Hist IDM OB", self.hist_idm_boxes)
                self.lstBxIdm = self.supplyZone.get(idx)
                if self.inputs.order_block.showIdmob:
                    zone = self.supplyZone.get(idx)
                    zone.set_text("IDM OB")
                    zone.set_text_color(self.inputs.order_block.clrtxtextbeariem)
                    zone.set_bgcolor(self.inputs.order_block.clrtxtextbeariembg)
                    self._register_box_event(zone, status="new")
                    self.supplyZoneIsMit.set(idx, 1)
                else:
                    self.removeZone(self.supplyZone, self.supplyZone.get(idx), self.supplyZoneIsMit, False)
                lstBx_ = self.supplyZone.get(idx) if idx != -1 else None

        colorText = (
            self.inputs.structure.bear
            if (trend and self.H_lastH > self.L_lastHH) or (not trend and self.H_lastLL > self.L_lastL)
            else self.inputs.structure.colorIDM
        )
        if self.inputs.structure.showSMC:
            ln = self.line_new(x, y, self.series.get_time(), y, "xloc.bar_time", self.inputs.structure.colorIDM, "line.style_dotted")
            lbl = self.label_new(
                (self.series.get_time() + x) // 2,
                y,
                self.IDM_TEXT,
                "xloc.bar_time",
                "yloc.price",
                self.transp,
                "label.style_label_down" if trend else "label.style_label_up",
                "size.small",
                colorText,
            )
            self.arrIdmLine.push(ln)
            self.arrIdmLabel.push(lbl)
        self.arrIdmLow.clear()
        self.arrIdmHigh.clear()
        self.arrIdmLBar.clear()
        self.arrIdmHBar.clear()
        return lstBx_

    def drawStructure(self, name: str, trend: bool) -> Tuple[float, Optional[Box]]:
        x, y = self.getDirection(trend, self.lastHBar, self.lastLBar, self.lastH, self.lastL)
        lstBx_: Optional[Box] = None
        if trend:
            idx = -1
            lstPrs: Optional[float] = None
            for i in range(self.demandZone.size()):
                bx = self.demandZone.get(i)
                cond = (
                    self.demandZoneIsMit.get(i) == 0
                    and (lstPrs is None or bx.top <= lstPrs)
                    and bx.top <= y
                    and bx.bottom >= (self.lstHlPrs if not math.isnan(self.lstHlPrs) else -math.inf)
                )
                if cond:
                    idx = i
                    lstPrs = bx.top
            if idx != -1:
                self._archive_box(self.lstBx, "Hist EXT OB", self.hist_ext_boxes)
                lstBx_ = self.demandZone.get(idx)
                if self.inputs.order_block.showExob:
                    zone = self.demandZone.get(idx)
                    zone.set_text("EXT OB")
                    zone.set_text_color(self.inputs.order_block.clrtxtextbull)
                    zone.set_bgcolor(self.inputs.order_block.clrtxtextbullbg)
                    self._register_box_event(zone, status="new")
                    self.demandZoneIsMit.set(idx, 1)
                else:
                    self.removeZone(self.demandZone, self.demandZone.get(idx), self.demandZoneIsMit, True)
        else:
            idx = -1
            lstPrs = None
            for i in range(self.supplyZone.size()):
                bx = self.supplyZone.get(i)
                cond = (
                    self.supplyZoneIsMit.get(i) == 0
                    and (lstPrs is None or bx.top >= lstPrs)
                    and bx.bottom >= y
                    and bx.top <= (self.lstHlPrs if not math.isnan(self.lstHlPrs) else math.inf)
                )
                if cond:
                    idx = i
                    lstPrs = bx.top
            if idx != -1:
                self._archive_box(self.lstBx, "Hist EXT OB", self.hist_ext_boxes)
                lstBx_ = self.supplyZone.get(idx)
                if self.inputs.order_block.showExob:
                    zone = self.supplyZone.get(idx)
                    zone.set_text("EXT OB")
                    zone.set_text_color(self.inputs.order_block.clrtxtextbear)
                    zone.set_bgcolor(self.inputs.order_block.clrtxtextbearbg)
                    self._register_box_event(zone, status="new")
                    self.supplyZoneIsMit.set(idx, 1)
                else:
                    self.removeZone(self.supplyZone, self.supplyZone.get(idx), self.supplyZoneIsMit, False)
        color = self.inputs.structure.bull if trend else self.inputs.structure.bear
        event_time = self.series.get_time()
        if not math.isnan(y):
            if name == "BOS":
                self._register_structure_break_event("BOS", y, event_time, bullish=trend)
            elif name == "ChoCh":
                self._register_structure_break_event("CHOCH", y, event_time, bullish=trend)
        if name == "BOS" and self.inputs.structure.showSMC:
            ln = self.line_new(x, y, self.series.get_time(), y, "xloc.bar_time", color, "line.style_dashed")
            lbl = self.label_new(
                (self.series.get_time() + x) // 2,
                y,
                self.BOS_TEXT,
                "xloc.bar_time",
                "yloc.price",
                self.transp,
                "label.style_label_down" if trend else "label.style_label_up",
                "size.small",
                color,
            )
            self.arrBCLine.push(ln)
            self.arrBCLabel.push(lbl)
        if name == "ChoCh" and self.inputs.structure.showSMC:
            ln = self.line_new(x, y, self.series.get_time(), y, "xloc.bar_time", color, "line.style_dashed")
            lbl = self.label_new(
                (self.series.get_time() + x) // 2,
                y,
                self.CHOCH_TEXT,
                "xloc.bar_time",
                "yloc.price",
                self.transp,
                "label.style_label_down" if trend else "label.style_label_up",
                "size.small",
                color,
            )
            self.arrBCLine.push(ln)
            self.arrBCLabel.push(lbl)
        return self.lstHlPrs, lstBx_

    # ------------------------------------------------------------------
    def labelMn(self, trend: bool) -> None:
        x, y = self.getDirection(trend, self.puHBar, self.puLBar, self.puHigh, self.puLow)
        color = self.inputs.structure.bear if trend else self.inputs.structure.bull
        txt = (
            self.getTextLabel(self.puHigh, self.arrlstHigh.get(0), "HH", "LH")
            if trend
            else self.getTextLabel(self.puLow, self.arrlstLow.get(0), "HL", "LL")
        )
        if self.inputs.pullback.showMn:
            self.label_new(
                x,
                y,
                "",
                "xloc.bar_time",
                self.getYloc(trend),
                color,
                self.getStyleArrow(trend),
                "size.tiny",
                "color.red",
            )

        if self.inputs.order_flow.showISOB:
            if txt in ("HH", "LL"):
                self.arrPrevPrsMin.set(0, y)
                self.arrPrevIdxMin.set(0, x)
            if txt in ("HL", "LH") and self.arrPrevPrsMin.get(0) != 0:
                if txt == "HL":
                    bx = self.box_new(
                        self.arrPrevIdxMin.get(0),
                        x,
                        self.arrPrevPrsMin.get(0),
                        y,
                        self.inputs.order_flow.ClrMinorOFBull,
                    )
                    self._record_range_event(
                        "ORDER_FLOW_MINOR",
                        text=EVENT_PRINT_LABELS.get("ORDER_FLOW_MINOR", "Minor OF"),
                        bottom=bx.bottom,
                        top=bx.top,
                        timestamp=self.series.get_time(),
                        status="new",
                        direction="bullish",
                    )
                    self.arrOBBulls.unshift(bx)
                    self.arrOBBullisVs.unshift(False)
                    if self.arrOBBulls.size() > self.inputs.order_flow.showISOBMax:
                        old = self.arrOBBulls.pop()
                        if old in self.boxes:
                            self.boxes.remove(old)
                        self.arrOBBullisVs.pop()
                else:
                    bx = self.box_new(
                        x,
                        self.arrPrevIdxMin.get(0),
                        y,
                        self.arrPrevPrsMin.get(0),
                        self.inputs.order_flow.ClrMinorOFBear,
                    )
                    self._record_range_event(
                        "ORDER_FLOW_MINOR",
                        text=EVENT_PRINT_LABELS.get("ORDER_FLOW_MINOR", "Minor OF"),
                        bottom=bx.bottom,
                        top=bx.top,
                        timestamp=self.series.get_time(),
                        status="new",
                        direction="bearish",
                    )
                    self.arrOBBears.unshift(bx)
                    self.arrOBBearisVs.unshift(False)
                    if self.arrOBBears.size() > self.inputs.order_flow.showISOBMax:
                        old = self.arrOBBears.pop()
                        if old in self.boxes:
                            self.boxes.remove(old)
                        self.arrOBBearisVs.pop()
                self.arrPrevPrsMin.set(0, 0)
                self.arrPrevIdxMin.set(0, 0)

        if trend:
            self.arrlstHigh.set(0, y)
        else:
            self.arrlstLow.set(0, y)

    def labelHL(self, trend: bool) -> float:
        x, y = self.getDirection(trend, self.HBar, self.LBar, self.H, self.L)
        txt = (
            self.getTextLabel(self.H, self.getNLastValue(self.arrLastH, 1), "HH", "LH")
            if trend
            else self.getTextLabel(self.L, self.getNLastValue(self.arrLastL, 1), "HL", "LL")
        )
        if self.inputs.order_flow.showMajoinMiner:
            if txt in ("HH", "LL"):
                self.arrPrevPrs.set(0, y)
                self.arrPrevIdx.set(0, x)
            if txt in ("HL", "LH") and self.arrPrevPrs.get(0) != 0:
                if txt == "HL":
                    bx = self.box_new(
                        self.arrPrevIdx.get(0),
                        x,
                        self.arrPrevPrs.get(0),
                        y,
                        self.inputs.order_flow.ClrMajorOFBull,
                    )
                    self._record_range_event(
                        "ORDER_FLOW_MAJOR",
                        text=EVENT_PRINT_LABELS.get("ORDER_FLOW_MAJOR", "Major OF"),
                        bottom=bx.bottom,
                        top=bx.top,
                        timestamp=self.series.get_time(),
                        status="new",
                        direction="bullish",
                    )
                    self.arrOBBullm.unshift(bx)
                    self.arrOBBullisVm.unshift(False)
                    if self.arrOBBullm.size() > self.inputs.order_flow.showMajoinMinerMax:
                        old = self.arrOBBullm.pop()
                        if old in self.boxes:
                            self.boxes.remove(old)
                        self.arrOBBullisVm.pop()
                else:
                    bx = self.box_new(
                        x,
                        self.arrPrevIdx.get(0),
                        y,
                        self.arrPrevPrs.get(0),
                        self.inputs.order_flow.ClrMajorOFBear,
                    )
                    self._record_range_event(
                        "ORDER_FLOW_MAJOR",
                        text=EVENT_PRINT_LABELS.get("ORDER_FLOW_MAJOR", "Major OF"),
                        bottom=bx.bottom,
                        top=bx.top,
                        timestamp=self.series.get_time(),
                        status="new",
                        direction="bearish",
                    )
                    self.arrOBBearm.unshift(bx)
                    self.arrOBBearisVm.unshift(False)
                    if self.arrOBBearm.size() > self.inputs.order_flow.showMajoinMinerMax:
                        old = self.arrOBBearm.pop()
                        if old in self.boxes:
                            self.boxes.remove(old)
                        self.arrOBBearisVm.pop()
                self.arrPrevPrs.set(0, 0)
                self.arrPrevIdx.set(0, 0)

        if self.inputs.pullback.showHL:
            lbl = self.label_new(
                x,
                y,
                txt,
                "xloc.bar_time",
                "yloc.price",
                self.transp,
                "label.style_label_down" if trend else "label.style_label_up",
                "size.tiny",
                self.inputs.pullback.colorHL,
            )
            self.arrHLLabel.push(lbl)
        if self.inputs.structure.showCircleHL:
            lbl = self.label_new(
                x,
                y,
                "",
                "xloc.bar_time",
                "yloc.abovebar" if trend else "yloc.belowbar",
                self.inputs.structure.bull if trend else self.inputs.structure.bear,
                "label.style_circle",
                "size.tiny",
                self.inputs.structure.bull if trend else self.inputs.structure.bear,
            )
            self.arrHLCircle.push(lbl)
        return y

    # ------------------------------------------------------------------
    def getProcess(self, arrOBBull: PineArray, arrOBBear: PineArray, arrOBBullisV: PineArray, arrOBBearisV: PineArray) -> Tuple[bool, bool]:
        alertBullOf = False
        alertBearOf = False
        current_time = self.series.get_time()
        if arrOBBull.size() > 0:
            i = 0
            while i < arrOBBull.size():
                bx: Box = arrOBBull.get(i)
                bx.set_right(current_time)
                if not arrOBBullisV.get(i):
                    if self.series.get("low") < bx.bottom:
                        if bx in self.boxes:
                            self.boxes.remove(bx)
                        arrOBBull.remove(i)
                        arrOBBullisV.remove(i)
                        i -= 1
                    elif self.series.get("high") > bx.top:
                        arrOBBullisV.set(i, True)
                else:
                    if (
                        self.series.get("low") < bx.top
                        and self.series.get("low", 1) > bx.top
                    ):
                        alertBullOf = True
                    if self.series.get("low") < bx.top:
                        bx.set_bgcolor(self.inputs.order_flow.clrObBBTated)
                        bx.set_border_color(self.inputs.order_flow.clrObBBTated)
                        self.arrOBTstd.unshift(bx)
                        self.arrOBTstdTy.unshift(1)
                        arrOBBull.remove(i)
                        arrOBBullisV.remove(i)
                        i -= 1
                i += 1
        if arrOBBear.size() > 0:
            i = 0
            while i < arrOBBear.size():
                bx = arrOBBear.get(i)
                bx.set_right(current_time)
                if not arrOBBearisV.get(i):
                    if self.series.get("high") > bx.top:
                        if bx in self.boxes:
                            self.boxes.remove(bx)
                        arrOBBear.remove(i)
                        arrOBBearisV.remove(i)
                        i -= 1
                    elif self.series.get("low") < bx.bottom:
                        arrOBBearisV.set(i, True)
                else:
                    if (
                        self.series.get("high") > bx.bottom
                        and self.series.get("high", 1) < bx.bottom
                    ):
                        alertBearOf = True
                    if self.series.get("high") > bx.bottom:
                        bx.set_bgcolor(self.inputs.order_flow.clrObBBTated)
                        bx.set_border_color(self.inputs.order_flow.clrObBBTated)
                        self.arrOBTstd.unshift(bx)
                        self.arrOBTstdTy.unshift(-1)
                        arrOBBear.remove(i)
                        arrOBBearisV.remove(i)
                        i -= 1
                i += 1
        return alertBullOf, alertBearOf

    # ------------------------------------------------------------------
    def scob(self, zones: PineArray, isSupply: bool) -> Optional[str]:
        if zones.size() == 0:
            return None
        zone = self.getNLastValue(zones, 1)
        if not isinstance(zone, Box):
            return None
        topZone, botZone = zone.top, zone.bottom
        if pine_bool(self.inputs.order_block.showSCOB) and self.series.length() > 2:
            if not isSupply and self.series.get("low", 1) < self.series.get("low", 2) and self.series.get("low", 1) < self.series.get("low"):
                if self.series.get("close") > self.series.get("high", 1) and topZone >= self.series.get("low", 1) > botZone:
                    return self.inputs.order_block.scobUp
            if isSupply and self.series.get("high", 1) > self.series.get("high", 2) and self.series.get("high", 1) > self.series.get("high"):
                if self.series.get("close") < self.series.get("low", 1) and topZone >= self.series.get("high", 1) > botZone:
                    return self.inputs.order_block.scobDn
        return None

    # ------------------------------------------------------------------
    def handleZone(self, zoneArray: PineArray, zoneArrayisMit: PineArray, left: int, top: float, bot: float, color: str, isBull: bool) -> None:
        zone = self.getNLastValue(zoneArray, 1)
        should_create = True
        if isinstance(zone, Box):
            topZone, botZone, leftZone = zone.top, zone.bottom, zone.left
            denominator = max(topZone - botZone, 1e-9)
            rangeTop = abs(top - topZone) / denominator < self.mergeRatio
            rangeBot = abs(bot - botZone) / denominator < self.mergeRatio
            if (top >= topZone and bot <= botZone) or rangeTop or rangeBot:
                top = max(top, topZone)
                bot = min(bot, botZone)
                left = leftZone
                self.removeZone(zoneArray, zone, zoneArrayisMit, isBull)
            if top <= topZone and bot >= botZone:
                should_create = False
        if should_create:
            box_obj = self.createBox(
                left,
                self.series.get_time(),
                top,
                bot,
                color,
            )
            zoneArray.push(box_obj)
            zoneArrayisMit.push(0)
            key = "DEMAND_ZONE" if isBull else "SUPPLY_ZONE"
            direction = "bullish" if isBull else "bearish"
            self._record_range_event(
                key,
                text=EVENT_PRINT_LABELS.get(key, key),
                bottom=box_obj.bottom,
                top=box_obj.top,
                timestamp=self.series.get_time(),
                status="new",
                direction=direction,
            )

    # ------------------------------------------------------------------
    def processZones(self, zones: PineArray, isSupply: bool, zonesmit: PineArray) -> bool:
        isAlertextidm = False
        if zones.size() == 0:
            return False
        i = zones.size() - 1
        while i >= 0:
            zone: Box = zones.get(i)
            if zonesmit.get(i) in (0, 1):
                zone.set_right(self.series.get_time())
            topZone, botZone, leftZone = zone.top, zone.bottom, zone.left
            if isSupply and self.series.get("low") < botZone and self.series.get("close") > topZone:
                new_zone = self.createBox(
                        leftZone,
                        self.series.get_time(),
                        topZone,
                        botZone,
                        self.inputs.order_block.colorDemand,
                    )
                self.demandZone.push(new_zone)
                self.demandZoneIsMit.push(0)
                self._record_range_event(
                    "DEMAND_ZONE",
                    text=EVENT_PRINT_LABELS.get("DEMAND_ZONE", "Demand Zone"),
                    bottom=new_zone.bottom,
                    top=new_zone.top,
                    timestamp=self.series.get_time(),
                    status="new",
                    direction="bullish",
                )
            elif (not isSupply) and self.series.get("high") > topZone and self.series.get("close") < botZone:
                new_zone = self.createBox(
                        leftZone,
                        self.series.get_time(),
                        topZone,
                        botZone,
                        self.inputs.order_block.colorSupply,
                    )
                self.supplyZone.push(new_zone)
                self.supplyZoneIsMit.push(0)
                self._record_range_event(
                    "SUPPLY_ZONE",
                    text=EVENT_PRINT_LABELS.get("SUPPLY_ZONE", "Supply Zone"),
                    bottom=new_zone.bottom,
                    top=new_zone.top,
                    timestamp=self.series.get_time(),
                    status="new",
                    direction="bearish",
                )
            elif zonesmit.get(i) in (0, 1) and (
                (isSupply and self.series.get("high") >= botZone and self.series.get("high", 1) < botZone)
                or ((not isSupply) and self.series.get("low") <= topZone and self.series.get("low", 1) > topZone)
            ):
                prev_state = zonesmit.get(i)
                zone.set_right(self.series.get_time())
                zone.set_extend("extend.none")
                if self.inputs.order_block.extndBox and self.series.get("high") >= topZone and self.series.get("low") <= botZone:
                    if isSupply:
                        self.arrmitOBBull.unshift(zone)
                        self.arrmitOBBulla.unshift(False)
                    else:
                        self.arrmitOBBear.unshift(zone)
                        self.arrmitOBBeara.unshift(False)
                if isSupply:
                    if zonesmit.get(i) == 1:
                        isAlertextidm = True
                    if zonesmit.get(i) != 1:
                        zone.set_bgcolor(self.inputs.order_block.colorMitigated)
                        zone.set_border_color(self.inputs.order_block.colorMitigated)
                    zonesmit.set(i, 3 if zonesmit.get(i) == 1 else 2)
                else:
                    if zonesmit.get(i) == 1:
                        isAlertextidm = True
                    if zonesmit.get(i) != 1:
                        zone.set_bgcolor(self.inputs.order_block.colorMitigated)
                        zone.set_border_color(self.inputs.order_block.colorMitigated)
                    zonesmit.set(i, 3 if zonesmit.get(i) == 1 else 2)
                status = "retest" if prev_state == 1 else "touched"
                self._register_box_event(zone, status=status, event_time=self.series.get_time())
                key = "SUPPLY_ZONE" if isSupply else "DEMAND_ZONE"
                direction = "bearish" if isSupply else "bullish"
                self._record_range_event(
                    key,
                    text=EVENT_PRINT_LABELS.get(key, key),
                    bottom=zone.bottom,
                    top=zone.top,
                    timestamp=self.series.get_time(),
                    status=status,
                    direction=direction,
                )
                if self.inputs.order_block.showBrkob:
                    zones.remove(i)
                    zonesmit.remove(i)
            elif (
                (self.series.get_time() - leftZone > self.len * self.maxBarHistory)
                or (isSupply and self.series.get("high") >= topZone)
                or ((not isSupply) and self.series.get("low") <= botZone)
            ):
                self.removeZone(zones, zone, zonesmit, not isSupply)
            i -= 1
        return isAlertextidm

    def _update_bar(self) -> None:
        # Historical caches ---------------------------------------------------
        high = self.series.get("high")
        low = self.series.get("low")
        close = self.series.get("close")
        open_ = self.series.get("open")
        time_val = self.series.get_time()
        volume = self.series.get("volume")

        self._trace(
            "update_bar",
            "start",
            timestamp=time_val,
            high=high,
            low=low,
            close=close,
            open=open_,
            volume=volume,
        )

        self._update_security_context(time_val, open_, high, low, close, volume)
        self._update_timediff(time_val)
        if math.isnan(self.htfH) or close > self.htfH:
            self.htfH = close
        if math.isnan(self.htfL) or close < self.htfL:
            self.htfL = close

        self._update_daily_levels(high, low, time_val)
        self._update_ict_market_structure(high, low, close)
        self._update_key_levels(open_, high, low)
        self._update_support_resistance(open_, high, low, close, volume)
        self._update_sessions(open_, high, low, time_val)
        self._trace("update_bar", "post_core", timestamp=time_val)

        if self.inputs.order_block.extndBox:
            i = 0
            while i < self.arrmitOBBull.size():
                bx = self.arrmitOBBull.get(i)
                bx.set_right(time_val)
                if close > bx.get_top() and not self.arrmitOBBulla.get(i):
                    self.arrmitOBBulla.set(i, True)
                if low < bx.get_top() and self.arrmitOBBulla.get(i):
                    if bx in self.boxes:
                        self.boxes.remove(bx)
                    self.arrmitOBBull.remove(i)
                    self.arrmitOBBulla.remove(i)
                    i -= 1
                i += 1
            i = 0
            while i < self.arrmitOBBear.size():
                bx = self.arrmitOBBear.get(i)
                bx.set_right(time_val)
                if close < bx.get_bottom() and not self.arrmitOBBeara.get(i):
                    self.arrmitOBBeara.set(i, True)
                if high > bx.get_bottom() and self.arrmitOBBeara.get(i):
                    if bx in self.boxes:
                        self.boxes.remove(bx)
                    self.arrmitOBBear.remove(i)
                    self.arrmitOBBeara.remove(i)
                    i -= 1
                i += 1

        i = 0
        while i < self.arrOBTstd.size():
            bx = self.arrOBTstd.get(i)
            typ = self.arrOBTstdTy.get(i)
            remove_box = False
            if typ == 1 and low < bx.get_bottom():
                remove_box = True
            elif typ == -1 and high > bx.get_top():
                remove_box = True
            if remove_box:
                if self.inputs.order_flow.showTsted:
                    self.arrOBTstdo.unshift(bx)
                else:
                    if bx in self.boxes:
                        self.boxes.remove(bx)
                self.arrOBTstd.remove(i)
                self.arrOBTstdTy.remove(i)
                i -= 1
            i += 1
        while self.arrOBTstdo.size() > self.inputs.order_flow.maxTested:
            old = self.arrOBTstdo.pop()
            if old in self.boxes:
                self.boxes.remove(old)

        # Inside bar update ---------------------------------------------------
        motherHigh = self.motherHigh
        motherLow = self.motherLow
        isb = motherHigh > high and motherLow < low
        if isb:
            pass
        else:
            self.motherHigh = high
            self.motherLow = low
            self.motherBar = time_val
        self.motherHigh_history.append(self.motherHigh)
        self.motherLow_history.append(self.motherLow)
        self.motherBar_history.append(self.motherBar)
        self.isb_history.append(bool(isb))

        # Top/bottom history -------------------------------------------------
        top = self.getNLastValue(self.arrTop, 1)
        bot = self.getNLastValue(self.arrBot, 1)
        topBotBar = self.getNLastValue(self.arrTopBotBar, 1)
        top1 = self.getNLastValue(self.arrTop, 2)
        bot1 = self.getNLastValue(self.arrBot, 2)
        topBotBar1 = self.getNLastValue(self.arrTopBotBar, 2)

        # Minor structure detection -----------------------------------------
        if not math.isnan(top) and not math.isnan(bot):
            if high >= top and low <= bot:
                if self.mnStrc is not None:
                    self.prevMnStrc = True if self.mnStrc else False
                else:
                    if (
                        self.prevMnStrc
                        and self.isGreenBar(0)
                        and not self.isGreenBar(1)
                    ):
                        self.puHigh = top
                        self.puHigh_ = top
                        self.puHBar = topBotBar
                        self.labelMn(True)
                        self.labelMn(False)
                        if high > self.H:
                            self.updateIdmLow()
                    if (
                        (not self.prevMnStrc)
                        and (not self.isGreenBar(0))
                        and self.isGreenBar(1)
                    ):
                        self.puLow = bot
                        self.puLow_ = bot
                        self.puLBar = topBotBar
                        self.labelMn(True)
                        self.labelMn(False)
                        if low < self.L:
                            self.updateIdmHigh()
                if low < self.L and self.isGreenBar(0):
                    self.updateIdmHigh()
                if high > self.H and not self.isGreenBar(0):
                    self.updateIdmLow()
                self.puHigh = high
                self.puHigh_ = high
                self.puLow = low
                self.puLow_ = low
                self.puHBar = time_val
                self.puLBar = time_val
                self.mnStrc = None
            if high >= top and low > bot:
                if self.prevMnStrc and self.mnStrc is None:
                    self.puHigh = top1
                    self.puHigh_ = top1
                    self.puHBar = topBotBar1
                    self.labelMn(True)
                    self.labelMn(False)
                elif (not self.prevMnStrc and self.mnStrc is None) or not self.mnStrc:
                    self.labelMn(False)
                if high > self.H:
                    self.updateIdmLow()
                self.puHigh = high
                self.puHigh_ = high
                self.puHBar = time_val
                self.prevMnStrc = None
                self.mnStrc = True
            if high < top and low <= bot:
                if (not self.prevMnStrc) and self.mnStrc is None:
                    self.puLow = bot1
                    self.puLow_ = bot1
                    self.puLBar = topBotBar1
                    self.labelMn(False)
                    self.labelMn(True)
                elif (self.prevMnStrc and self.mnStrc is None) or self.mnStrc:
                    self.labelMn(True)
                if low < self.L:
                    self.updateIdmHigh()
                self.puLow = low
                self.puLow_ = low
                self.puLBar = time_val
                self.prevMnStrc = None
                self.mnStrc = False

        # Refresh top/bottom after updates ----------------------------------
        self.updateTopBotValue()

        osb = False
        if not math.isnan(top) and not math.isnan(bot):
            osb = high > top and low < bot

        if high >= self.H:
            self.H = high
            self.HBar = time_val
            self.L_lastHH = low
            idm_low = self.getNLastValue(self.arrIdmLow, 1)
            idm_lbar = self.getNLastValue(self.arrIdmLBar, 1)
            if not is_na(idm_low):
                self.idmLow = idm_low
            if not is_na(idm_lbar):
                self.idmLBar = int(idm_lbar)

        if low <= self.L:
            self.L = low
            self.LBar = time_val
            self.H_lastLL = high
            idm_high = self.getNLastValue(self.arrIdmHigh, 1)
            idm_hbar = self.getNLastValue(self.arrIdmHBar, 1)
            if not is_na(idm_high):
                self.idmHigh = idm_high
            if not is_na(idm_hbar):
                self.idmHBar = int(idm_hbar)

        structure_type = self.inputs.structure.structure_type

        if self._eval_condition(
            "findIDM_guard_up",
            "if findIDM and isCocUp and isCocUp and not na(idmLow)",
            lambda: self.findIDM and self.isCocUp and self.isCocUp and not is_na(self.idmLow),
        ):
            if self._eval_condition("low_breaks_idmLow", "if low < idmLow", lambda: low < self.idmLow):
                if self._eval_condition(
                    "fix_after_idmLow_touch",
                    "if structure_type == 'Choch with IDM' and idmLow == lastL",
                    lambda: structure_type == "Choch with IDM"
                    and math.isclose(self.idmLow, self.lastL, rel_tol=1e-9, abs_tol=1e-9),
                ):
                    if self._eval_condition("fix_after_bos", "if isPrevBos", lambda: self.isPrevBos):
                        self.fixStrcAfterBos()
                        lastL_prev = self.getNLastValue(self.arrLastL, 1)
                        lastLBar_prev = self.getNLastValue(self.arrLastLBar, 1)
                        if not is_na(lastL_prev):
                            self.lastL = lastL_prev
                        if not is_na(lastLBar_prev):
                            self.lastLBar = int(lastLBar_prev)
                    else:
                        self.fixStrcAfterChoch()
                self.findIDM = False
                self.isBosUp = False
                self.lastH = self.H
                self.lastHBar = self.HBar
                self.lstHlPrs = self.labelHL(True)
                lstBx_ = self.drawIDM(True)
                if lstBx_ is not None:
                    self.lstBxIdm = lstBx_
                self.updateLastHLValue()
                lastH_prev = self.getNLastValue(self.arrLastH, 1)
                if not is_na(lastH_prev):
                    self.H_lastH = lastH_prev
                self.L = low
                self.LBar = time_val

        if self._eval_condition(
            "findIDM_guard_down",
            "if findIDM and isCocDn and isBosDn and not na(idmHigh)",
            lambda: self.findIDM and self.isCocDn and self.isBosDn and not is_na(self.idmHigh),
        ):
            if self._eval_condition("high_breaks_idmHigh", "if high > idmHigh", lambda: high > self.idmHigh):
                if self._eval_condition(
                    "fix_after_idmHigh_touch",
                    "if structure_type == 'Choch with IDM' and idmHigh == lastH",
                    lambda: structure_type == "Choch with IDM"
                    and math.isclose(self.idmHigh, self.lastH, rel_tol=1e-9, abs_tol=1e-9),
                ):
                    if self._eval_condition("fix_after_bos_down", "if isPrevBos", lambda: self.isPrevBos):
                        self.fixStrcAfterBos()
                        lastH_prev = self.getNLastValue(self.arrLastH, 1)
                        lastHBar_prev = self.getNLastValue(self.arrLastHBar, 1)
                        if not is_na(lastH_prev):
                            self.lastH = lastH_prev
                        if not is_na(lastHBar_prev):
                            self.lastHBar = int(lastHBar_prev)
                    else:
                        self.fixStrcAfterChoch()
                self.findIDM = False
                self.isBosDn = False
                self.lastL = self.L
                self.lastLBar = self.LBar
                self.lstHlPrs = self.labelHL(False)
                lstBx_ = self.drawIDM(False)
                if lstBx_ is not None:
                    self.lstBxIdm = lstBx_
                self.updateLastHLValue()
                lastL_prev = self.getNLastValue(self.arrLastL, 1)
                if not is_na(lastL_prev):
                    self.L_lastL = lastL_prev
                self.H = high
                self.HBar = time_val

        if self._eval_condition(
            "choch_up_break_guard",
            "if isCocDn and high > lastH",
            lambda: self.isCocDn and high > self.lastH,
        ):
            if self._eval_condition(
                "remove_idm_on_close_above",
                "if structure_type == 'Choch without IDM' and idmHigh == lastH and close > idmHigh",
                lambda: structure_type == "Choch without IDM"
                and math.isclose(self.idmHigh, self.lastH, rel_tol=1e-9, abs_tol=1e-9)
                and close > self.idmHigh,
            ):
                self.removeLastLabel(self.arrIdmLabel, 1)
                self.removeLastLine(self.arrIdmLine, 1)
            if self._eval_condition("choch_up_confirm", "if close > lastH", lambda: close > self.lastH):
                lstHlPrsIdm_, lstBx_ = self.drawStructure("ChoCh", True)
                if not is_na(lstHlPrsIdm_):
                    self.lstHlPrsIdm = lstHlPrsIdm_
                if lstBx_ is not None:
                    self.lstBx = lstBx_
                self.findIDM = True
                self.isBosUp = True
                self.isCocUp = True
                self.isBosDn = False
                self.isCocDn = False
                self.isPrevBos = False
                lastL_prev = self.getNLastValue(self.arrLastL, 1)
                if not is_na(lastL_prev):
                    self.L_lastL = lastL_prev
                self.TP(self.lastH, self.lastL)
            else:
                if self._eval_condition(
                    "remove_idm_line_up",
                    "if idmHigh == lastH",
                    lambda: math.isclose(self.idmHigh, self.lastH, rel_tol=1e-9, abs_tol=1e-9),
                ):
                    self.removeLastLine(self.arrIdmLine, 1)
                self.sweepHL(True)

        if self._eval_condition(
            "choch_down_break_guard",
            "if isCocUp and low < lastL",
            lambda: self.isCocUp and low < self.lastL,
        ):
            if self._eval_condition(
                "remove_idm_on_close_below",
                "if structure_type == 'Choch without IDM' and idmLow == lastL and close < idmLow",
                lambda: structure_type == "Choch without IDM"
                and math.isclose(self.idmLow, self.lastL, rel_tol=1e-9, abs_tol=1e-9)
                and close < self.idmLow,
            ):
                self.removeLastLabel(self.arrIdmLabel, 1)
                self.removeLastLine(self.arrIdmLine, 1)
            if self._eval_condition("choch_down_confirm", "if close < lastL", lambda: close < self.lastL):
                lstHlPrsIdm_, lstBx_ = self.drawStructure("ChoCh", False)
                if not is_na(lstHlPrsIdm_):
                    self.lstHlPrsIdm = lstHlPrsIdm_
                if lstBx_ is not None:
                    self.lstBx = lstBx_
                self.findIDM = True
                self.isBosUp = False
                self.isCocUp = False
                self.isBosDn = True
                self.isCocDn = True
                self.isPrevBos = False
                lastH_prev = self.getNLastValue(self.arrLastH, 1)
                if not is_na(lastH_prev):
                    self.H_lastH = lastH_prev
                self.TP(self.lastH, self.lastL)
            else:
                if self._eval_condition(
                    "remove_idm_line_down",
                    "if idmLow == lastL",
                    lambda: math.isclose(self.idmLow, self.lastL, rel_tol=1e-9, abs_tol=1e-9),
                ):
                    self.removeLastLine(self.arrIdmLine, 1)
                self.sweepHL(False)

        if self._eval_condition(
            "bos_up_guard",
            "if not findIDM and not isBosUp and isCocUp and high > lastH",
            lambda: not self.findIDM and not self.isBosUp and self.isCocUp and high > self.lastH,
        ):
            if self._eval_condition("bos_up_confirm", "if close > lastH", lambda: close > self.lastH):
                self.findIDM = True
                self.isBosUp = True
                self.isCocUp = True
                self.isBosDn = False
                self.isCocDn = False
                self.isPrevBos = True
                self.lstHlPrs = self.labelHL(False)
                lstHlPrsIdm_, lstBx_ = self.drawStructure("BOS", True)
                if not is_na(lstHlPrsIdm_):
                    self.lstHlPrsIdm = lstHlPrsIdm_
                if lstBx_ is not None:
                    self.lstBx = lstBx_
                self.lastL = self.L
                self.lastLBar = self.LBar
                self.L_lastL = self.L
                self.TP(self.lastH, self.lastL)
            else:
                self.sweepHL(True)

        if self._eval_condition(
            "bos_down_guard",
            "if not findIDM and not isBosDn and isCocDn and low < lastL",
            lambda: not self.findIDM and not self.isBosDn and self.isCocDn and low < self.lastL,
        ):
            if self._eval_condition("bos_down_confirm", "if close < lastL", lambda: close < self.lastL):
                self.findIDM = True
                self.isBosUp = False
                self.isCocUp = False
                self.isBosDn = True
                self.isCocDn = True
                self.isPrevBos = True
                self.lstHlPrs = self.labelHL(True)
                lstHlPrsIdm_, lstBx_ = self.drawStructure("BOS", False)
                if not is_na(lstHlPrsIdm_):
                    self.lstHlPrsIdm = lstHlPrsIdm_
                if lstBx_ is not None:
                    self.lstBx = lstBx_
                self.lastH = self.H
                self.lastHBar = self.HBar
                self.H_lastH = self.H
                self.TP(self.lastH, self.lastL)
            else:
                self.sweepHL(False)

        if high > self.lastH:
            self.lastH = high
            self.lastHBar = time_val

        if low < self.lastL:
            self.lastL = low
            self.lastLBar = time_val

        # Order flow updates -------------------------------------------------
        self._update_demand_supply_zones()
        self._update_fvg()
        self._update_liquidity()
        self._update_swing_detection()
        self._update_candlestick_patterns()
        self.prev_close = close

        alertBullOfMajor, alertBearOfMajor = self.getProcess(
            self.arrOBBullm, self.arrOBBearm, self.arrOBBullisVm, self.arrOBBearisVm
        )
        alertBullOfMinor, alertBearOfMinor = self.getProcess(
            self.arrOBBulls, self.arrOBBears, self.arrOBBullisVs, self.arrOBBearisVs
        )
        if alertBullOfMajor or alertBearOfMajor:
            direction = "bullish" if alertBullOfMajor else "bearish"
            direction_text = "صاعد" if alertBullOfMajor else "هابط"
            self._record_console_event(
                "ORDER_FLOW_BREAK_MAJOR",
                {
                    "text": "Order Flow Break (Major)",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Order Flow Break (Major) @ {format_price(close)} ({direction_text})",
                    "direction": direction,
                    "direction_display": direction_text,
                    "fingerprint": f"ORDER_FLOW_BREAK_MAJOR:{_fmt_price_key(close)}:{time_val}",
                },
            )
        if alertBullOfMinor or alertBearOfMinor:
            direction = "bullish" if alertBullOfMinor else "bearish"
            direction_text = "صاعد" if alertBullOfMinor else "هابط"
            self._record_console_event(
                "ORDER_FLOW_BREAK_MINOR",
                {
                    "text": "Order Flow Break (Minor)",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Order Flow Break (Minor) @ {format_price(close)} ({direction_text})",
                    "direction": direction,
                    "direction_display": direction_text,
                    "fingerprint": f"ORDER_FLOW_BREAK_MINOR:{_fmt_price_key(close)}:{time_val}",
                },
            )

        # Order block zone processing ---------------------------------------
        isAlertextidmSell = self.processZones(self.supplyZone, True, self.supplyZoneIsMit)
        isAlertextidmBuy = self.processZones(self.demandZone, False, self.demandZoneIsMit)

        # POI sweeps ---------------------------------------------------------
        if self.inputs.order_block.showPOI and self.series.length() > 4:
            if not self.isSweepOBS:
                self.high_MOBS = self.series.get("high", 3)
                self.low_MOBS = self.series.get("low", 3)
                self.current_OBS = self.series.get_time(3)
                if (
                    not math.isnan(self.high_MOBS)
                    and not math.isnan(self.series.get("high", 4))
                    and not math.isnan(self.series.get("high", 2))
                    and self.high_MOBS > self.series.get("high", 4)
                    and self.high_MOBS > self.series.get("high", 2)
                ):
                    self.isSweepOBS = True
            else:
                if not math.isnan(self.low_MOBS) and self.low_MOBS > self.series.get("high", 1):
                    if self.current_OBS is not None and not math.isnan(self.high_MOBS) and not math.isnan(self.low_MOBS):
                        self.handleZone(
                            self.supplyZone,
                            self.supplyZoneIsMit,
                            self.current_OBS,
                            self.high_MOBS,
                            self.low_MOBS,
                            self.inputs.order_block.colorSupply,
                            False,
                        )
                    self.isSweepOBS = False
                else:
                    if (
                        self.inputs.order_block.poi_type == "Mother Bar"
                        and self.series.length() > 2
                        and self._history_get(self.isb_history, 2, False)
                    ):
                        mother_high = self._history_get(self.motherHigh_history, 2, self.motherHigh)
                        mother_low = self._history_get(self.motherLow_history, 2, self.motherLow)
                        mother_bar = self._history_get(self.motherBar_history, 2, self.motherBar)
                        self.high_MOBS = max(self.high_MOBS or -math.inf, mother_high)
                        self.low_MOBS = min(self.low_MOBS or math.inf, mother_low)
                        self.current_OBS = min(self.current_OBS or time_val, mother_bar)
                    else:
                        self.high_MOBS = self.series.get("high", 2)
                        self.low_MOBS = self.series.get("low", 2)
                        self.current_OBS = self.series.get_time(2)

            if not self.isSweepOBD:
                self.low_MOBD = self.series.get("low", 3)
                self.high_MOBD = self.series.get("high", 3)
                self.current_OBD = self.series.get_time(3)
                if (
                    not math.isnan(self.low_MOBD)
                    and not math.isnan(self.series.get("low", 4))
                    and not math.isnan(self.series.get("low", 2))
                    and self.low_MOBD < self.series.get("low", 4)
                    and self.low_MOBD < self.series.get("low", 2)
                ):
                    self.isSweepOBD = True
            else:
                if not math.isnan(self.high_MOBD) and self.high_MOBD < self.series.get("low", 1):
                    if self.current_OBD is not None and not math.isnan(self.high_MOBD) and not math.isnan(self.low_MOBD):
                        self.handleZone(
                            self.demandZone,
                            self.demandZoneIsMit,
                            self.current_OBD,
                            self.high_MOBD,
                            self.low_MOBD,
                            self.inputs.order_block.colorDemand,
                            True,
                        )
                    self.isSweepOBD = False
                else:
                    if (
                        self.inputs.order_block.poi_type == "Mother Bar"
                        and self.series.length() > 2
                        and self._history_get(self.isb_history, 2, False)
                    ):
                        mother_high = self._history_get(self.motherHigh_history, 2, self.motherHigh)
                        mother_low = self._history_get(self.motherLow_history, 2, self.motherLow)
                        mother_bar = self._history_get(self.motherBar_history, 2, self.motherBar)
                        self.high_MOBD = max(self.high_MOBD or -math.inf, mother_high)
                        self.low_MOBD = min(self.low_MOBD or math.inf, mother_low)
                        self.current_OBD = min(self.current_OBD or time_val, mother_bar)
                    else:
                        self.high_MOBD = self.series.get("high", 2)
                        self.low_MOBD = self.series.get("low", 2)
                        self.current_OBD = self.series.get_time(2)

        # SCOB and candle colouring -----------------------------------------
        scob_supply = self.scob(self.supplyZone, True)
        scob_demand = self.scob(self.demandZone, False)
        if scob_supply:
            self.bar_colors.append((time_val, scob_supply))
            self._record_console_event(
                "SCOB_BEARISH",
                {
                    "text": "Bearish SCOB",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Bearish SCOB @ {format_price(close)}",
                    "direction": "bearish",
                    "fingerprint": f"SCOB_BEARISH:{_fmt_price_key(close)}:{time_val}",
                },
            )
            self._record_console_event(
                "SCOB",
                {
                    "text": "SCOB",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"SCOB @ {format_price(close)} (هابط)",
                    "direction": "bearish",
                    "fingerprint": f"SCOB:{_fmt_price_key(close)}:{time_val}",
                },
            )
        if scob_demand:
            self.bar_colors.append((time_val, scob_demand))
            self._record_console_event(
                "SCOB_BULLISH",
                {
                    "text": "Bullish SCOB",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Bullish SCOB @ {format_price(close)}",
                    "direction": "bullish",
                    "fingerprint": f"SCOB_BULLISH:{_fmt_price_key(close)}:{time_val}",
                },
            )
            self._record_console_event(
                "SCOB",
                {
                    "text": "SCOB",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"SCOB @ {format_price(close)} (صاعد)",
                    "direction": "bullish",
                    "fingerprint": f"SCOB:{_fmt_price_key(close)}:{time_val}",
                },
            )
        if self.inputs.candle.showISB and isb:
            self.bar_colors.append((time_val, self.inputs.candle.colorISB))
            self._record_console_event(
                "INSIDE_BAR_CANDLE",
                {
                    "text": "Inside Bar Candle",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Inside Bar Candle @ {format_price(close)}",
                    "fingerprint": f"INSIDE_BAR_CANDLE:{_fmt_price_key(close)}:{time_val}",
                },
            )
            self._record_console_event(
                "INSIDE_BAR",
                {
                    "text": "Inside Bar",
                    "price": close,
                    "time": time_val,
                    "time_display": format_timestamp(time_val),
                    "display": f"Inside Bar @ {format_price(close)}",
                    "fingerprint": f"INSIDE_BAR:{_fmt_price_key(close)}:{time_val}",
                },
            )
        if self.inputs.candle.showOSB and osb:
            color = self.inputs.candle.colorOSB_up if self.isGreenBar(0) else self.inputs.candle.colorOSB_down
            self.bar_colors.append((time_val, color))

        self.drawLiveStrc(self.inputs.structure.showSMC and self.findIDM, not self.isCocUp, self.inputs.structure.colorIDM, self.inputs.structure.colorIDM, self.IDM_TEXT, self.inputs.structure.lengSMC, "idm_label", "idm_line")
        self.drawLiveStrc(self.inputs.structure.showSMC, not self.isCocUp, self.inputs.structure.bull, self.inputs.structure.bear, self.CHOCH_TEXT, self.inputs.structure.lengSMC, "choch_label", "choch_line")
        self.drawLiveStrc(self.inputs.structure.showSMC and not self.findIDM, self.isCocUp, self.inputs.structure.bull, self.inputs.structure.bear, self.BOS_TEXT, self.inputs.structure.lengSMC, "bos_label", "bos_line")

        self.drawPrevStrc(self.inputs.structure_util.showPdh, self.PDH_TEXT, "pdh_label", "pdh_line", 0.0)
        self.drawPrevStrc(self.inputs.structure_util.showPdl, self.PDL_TEXT, "pdl_label", "pdl_line", 0.0)
        self.drawPrevStrc(self.inputs.structure_util.showMid, self.MID_TEXT, "mid_label", "mid_line", 0.0)

        if self.inputs.structure_util.isOTE:
            if self.bxf is not None:
                self.bxf.set_right(time_val)
            ot, oi1, dir_up = self.drawPrevStrc(True, "", "mid_label1", "mid_line1", self.inputs.structure_util.ote1)
            ob, _, _ = self.drawPrevStrc(True, "", "mid_label2", "mid_line2", self.inputs.structure_util.ote2)
            if oi1 is not None:
                if self.bxf and self.bxf in self.boxes:
                    self.boxes.remove(self.bxf)
                top_val = ot if not math.isnan(ot) else self.series.get("high")
                bot_val = ob if not math.isnan(ob) else self.series.get("low")
                self.bxf = self.box_new(int(oi1), time_val, top_val, bot_val, self.inputs.structure_util.oteclr)
                self.bxf.set_text("Golden zone")
                self.bxf.set_text_color(self.inputs.structure_util.oteclr)
                self._register_box_event(self.bxf, status="new")
                self.bxty = 1 if dir_up else -1
                self.prev_oi1 = float(oi1)

        self._sync_state_mirrors()


# ----------------------------------------------------------------------------
# Report generation
# ----------------------------------------------------------------------------


@dataclass
class BinanceSymbolSelection:
    """Container describing the outcome of Binance symbol selection."""

    symbols: List[str]
    prioritized: List[str]
    used_height_filter: bool
    had_prioritization_data: bool


def _safe_symbol_metric(value: Any) -> Optional[float]:
    """Convert metric strings such as ``"12.5%"`` into floats safely."""

    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number):
            return None
        return number
    if isinstance(value, str):
        token = value.strip().rstrip("%")
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _ticker_metric_value(ticker: Dict[str, Any], metric: str) -> Optional[float]:
    """Pull a metric from the CCXT ticker payload regardless of vendor keys."""

    if not isinstance(ticker, dict):
        return None

    normalized = (metric or "").strip().lower()
    mapping: Dict[str, Tuple[str, ...]] = {
        "percentage": ("percentage", "priceChangePercent", "P"),
        "pricechange": ("change", "priceChange", "c"),
        "lastprice": ("last", "close", "price"),
    }
    probe_keys = mapping.get(normalized, mapping["percentage"])

    sources: List[Dict[str, Any]] = [ticker]
    info = ticker.get("info")
    if isinstance(info, dict):
        sources.append(info)

    for source in sources:
        for key in probe_keys:
            candidate = source.get(key)
            numeric = _safe_symbol_metric(candidate)
            if numeric is not None:
                return numeric
    return None


def _ohlcv_metric_value(
    candles: Sequence[Sequence[Any]],
    metric: str,
) -> Tuple[Optional[float], float]:
    """Convert a slice of OHLCV rows into the requested height metric."""

    if not candles:
        return None, 0.0

    first = candles[0]
    last = candles[-1]

    try:
        first_open = float(first[1])
    except (TypeError, ValueError):
        first_open = math.nan
    try:
        last_close = float(last[4])
    except (TypeError, ValueError):
        last_close = math.nan

    total_volume = 0.0
    for entry in candles:
        try:
            vol = float(entry[5])
        except (TypeError, ValueError):
            continue
        if math.isnan(vol):
            continue
        total_volume += vol

    if math.isnan(first_open) or math.isnan(last_close):
        return None, total_volume

    normalized = (metric or "").strip().lower()
    if normalized == "pricechange":
        return last_close - first_open, total_volume
    if normalized == "lastprice":
        return last_close, total_volume

    if first_open == 0:
        return None, total_volume
    return ((last_close - first_open) / first_open) * 100.0, total_volume


def _extract_quote_volume(ticker: Dict[str, Any]) -> Optional[float]:
    """Extract the quote volume used for secondary ranking."""

    if not isinstance(ticker, dict):
        return None

    candidates: List[Any] = []
    for key in ("quoteVolume", "volume", "turnover"):
        if key in ticker:
            candidates.append(ticker.get(key))
    info = ticker.get("info")
    if isinstance(info, dict):
        for key in ("quoteVolume", "volume", "turnover"):
            if key in info:
                candidates.append(info.get(key))

    for candidate in candidates:
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if not math.isnan(value):
            return value
    return None


def _binance_linear_symbol_id(symbol: str) -> Optional[str]:
    """Translate ``BTC/USDT:USDT`` into the REST identifier ``BTCUSDT``."""

    if not symbol or not isinstance(symbol, str):
        return None
    core = symbol.split(":", 1)[0].replace("/", "")
    if not core.endswith("USDT"):
        return None
    return core


def _binance_linear_symbol_from_id(symbol: str) -> Optional[str]:
    """Normalise Binance linear contract identifiers to ccxt symbols."""

    if not symbol or not isinstance(symbol, str):
        return None
    token = symbol.strip().upper()
    if "/" in token or ":" in token:
        return token
    if token.endswith("USDT") and len(token) > 4:
        base = token[:-4]
        return f"{base}/USDT:USDT"
    return token or None


def _bulk_fetch_recent_ohlcv(
    exchange: Any,
    symbols: Sequence[str],
    timeframe: str,
    candle_window: int,
) -> Dict[str, Sequence[Sequence[Any]]]:
    """Fetch OHLC candles in parallel when possible to speed up filtering."""

    if candle_window <= 0:
        return {symbol: [] for symbol in symbols}
    unique_symbols = list(dict.fromkeys(symbols))
    if not unique_symbols:
        return {}

    if requests is None:
        return {
            symbol: _fetch_recent_ohlcv(exchange, symbol, timeframe, candle_window)
            for symbol in unique_symbols
        }

    rest_mapping: Dict[str, Optional[str]] = {
        symbol: _binance_linear_symbol_id(symbol) for symbol in unique_symbols
    }

    results: Dict[str, Sequence[Sequence[Any]]] = {}
    endpoint = "https://fapi.binance.com/fapi/v1/klines"
    max_workers = min(8, max(1, len(unique_symbols)))
    timeout = (3.05, 10.0)

    session: Optional[requests.Session]
    if HTTPAdapter is not None and Retry is not None:
        retries = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=max_workers,
            pool_maxsize=max_workers,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    else:  # pragma: no cover - requests missing adapters
        session = requests.Session()

    def fetch(symbol: str, rest_symbol: Optional[str]) -> Sequence[Sequence[Any]]:
        if not rest_symbol:
            return _fetch_recent_ohlcv(exchange, symbol, timeframe, candle_window)
        params = {
            "symbol": rest_symbol,
            "interval": timeframe,
            "limit": candle_window,
        }
        try:
            response = session.get(endpoint, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # pragma: no cover - network variability
            print(
                f"تعذر جلب شموع {symbol} عبر واجهة Binance السريعة: {exc}",
                flush=True,
            )
            return _fetch_recent_ohlcv(exchange, symbol, timeframe, candle_window)

        if not isinstance(payload, list):
            return _fetch_recent_ohlcv(exchange, symbol, timeframe, candle_window)
        return payload[-candle_window:]

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {
                pool.submit(fetch, symbol, rest_mapping[symbol]): symbol
                for symbol in unique_symbols
            }
            for future in concurrent.futures.as_completed(future_map):
                symbol = future_map[future]
                try:
                    results[symbol] = future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    print(f"تعذر جلب شموع {symbol}: {exc}", flush=True)
                    results[symbol] = _fetch_recent_ohlcv(
                        exchange, symbol, timeframe, candle_window
                    )
    finally:
        session.close()

    return results


def _fetch_recent_ohlcv(
    exchange: Any,
    symbol: str,
    timeframe: str,
    candle_window: int,
) -> Sequence[Sequence[Any]]:
    """Fetch the most recent ``candle_window`` OHLC rows for ``symbol``."""

    if candle_window <= 0:
        return []
    try:
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=candle_window)
    except Exception as exc:
        print(f"تعذر جلب شموع {symbol} على إطار {timeframe}: {exc}", flush=True)
        return []


def _binance_pick_symbols(
    exchange: Any,
    limit: int,
    explicit: Optional[str],
    selector: BinanceSymbolSelectorConfig,
) -> BinanceSymbolSelection:
    """Return Binance USDT-M symbols prioritised by performance metrics."""

    if explicit:
        requested = [symbol.strip().upper() for symbol in explicit.split(",") if symbol.strip()]
        try:
            markets = exchange.load_markets()
        except Exception as exc:
            print(f"فشل تحميل الأسواق للتحقق من الرموز المحددة يدويًا: {exc}")
            return BinanceSymbolSelection([], [], False, False)
        valid: List[str] = []
        invalid: List[str] = []
        for symbol in requested:
            if symbol in markets:
                valid.append(symbol)
                continue
            canonical = _binance_linear_symbol_from_id(symbol)
            if canonical and canonical in markets:
                valid.append(canonical)
            else:
                invalid.append(symbol)
        if invalid:
            invalid_sorted = sorted(dict.fromkeys(invalid))
            print(f"تحذير: سيتم تجاهل الرموز غير الصحيحة: {', '.join(invalid_sorted)}")
        return BinanceSymbolSelection(valid, [], False, False)

    try:
        markets = exchange.load_markets()
    except Exception as exc:
        print(f"فشل تحميل أسواق Binance: {exc}")
        return BinanceSymbolSelection([], [], False, False)

    usdtm_markets: List[Dict[str, Any]] = [
        market
        for market in markets.values()
        if market.get("linear") and market.get("quote") == "USDT" and market.get("type") == "swap" and market.get("active")
    ]
    if not usdtm_markets:
        print("لم يتم العثور على عقود Binance USDT-M نشطة.")
        return BinanceSymbolSelection([], [], False, False)

    try:
        tickers = exchange.fetch_tickers()
    except Exception as exc:
        print(f"تعذر جلب بيانات التيكر، سيتم استخدام فرز افتراضي: {exc}")
        tickers = {}

    symbol_set = {
        market.get("symbol")
        for market in usdtm_markets
        if isinstance(market.get("symbol"), str)
    }
    tickers = {symbol: tickers.get(symbol, {}) for symbol in symbol_set}

    prioritized: List[Tuple[str, float, float]] = []
    try:
        threshold = float(selector.top_gainer_threshold) if selector.top_gainer_threshold is not None else None
    except (TypeError, ValueError):
        threshold = None
    metric = (selector.top_gainer_metric or "percentage").strip() or "percentage"
    scope = (selector.top_gainer_scope or "").strip()
    try:
        candle_window = (
            int(selector.top_gainer_candle_window)
            if selector.top_gainer_candle_window is not None
            else None
        )
    except (TypeError, ValueError):
        candle_window = None
    if candle_window is not None and candle_window <= 0:
        candle_window = None

    prioritized_symbols: List[str] = []
    have_ticker_data = bool(tickers)
    have_height_requirements = (
        selector.prioritize_top_gainers
        and threshold is not None
        and bool(scope)
        and candle_window is not None
    )
    used_height_filter = have_height_requirements

    if selector.prioritize_top_gainers and not have_height_requirements:
        print(
            "تعذر تشغيل فلتر الارتفاع: يرجى ضبط حد النسبة، الإطار الزمني، وعدد الشموع.",
            flush=True,
        )

    if have_height_requirements:
        timeframe_seconds = _parse_timeframe_to_seconds(scope, None)
        if timeframe_seconds is None:
            print(
                f"تعذر تفسير الإطار الزمني '{scope}' لفلتر الارتفاع؛ سيتم تجاهل التصفية.",
                flush=True,
            )
            have_height_requirements = False
            used_height_filter = False
        elif candle_window is None:
            have_height_requirements = False
            used_height_filter = False

    if have_height_requirements and candle_window:
        print(
            f"تحديد أولوية الرابحين الأعلى باستخدام المقياس '{metric}' وحد أدنى {threshold:.2f} على إطار {scope} مع {candle_window} شموع.",
            flush=True,
        )
        candles_map = _bulk_fetch_recent_ohlcv(
            exchange,
            [market.get("symbol") for market in usdtm_markets if isinstance(market.get("symbol"), str)],
            scope,
            candle_window,
        )
        for market in usdtm_markets:
            symbol = market.get("symbol")
            if not isinstance(symbol, str):
                continue
            candles = candles_map.get(symbol) or []
            metric_value, ohlcv_volume = _ohlcv_metric_value(candles[-candle_window:], metric)
            if metric_value is None or threshold is None or metric_value < threshold:
                continue
            ticker_volume = _extract_quote_volume(tickers.get(symbol, {})) or 0.0
            volume = ticker_volume or ohlcv_volume
            prioritized.append((symbol, metric_value, volume))
        prioritized.sort(key=lambda item: (item[1], item[2]), reverse=True)
        prioritized_symbols = [symbol for symbol, _, _ in prioritized]

        if prioritized_symbols:
            target_limit = limit if limit and limit > 0 else len(prioritized_symbols)
            limited_prioritized = prioritized_symbols[:target_limit]
            if len(limited_prioritized) < len(prioritized_symbols):
                print(
                    f"تم العثور على {len(prioritized_symbols)} رمزًا تجاوزت حد الارتفاع؛ سيتم مسح أول {len(limited_prioritized)} فقط.",
                    flush=True,
                )
            else:
                print(
                    f"تم العثور على {len(prioritized_symbols)} رمزًا متوافقة مع حد الارتفاع وسيتم مسحها فقط.",
                    flush=True,
                )
            return BinanceSymbolSelection(
                limited_prioritized,
                limited_prioritized,
                True,
                True,
            )

        print(
            "لم يتم العثور على رموز تتجاوز حد فلتر الارتفاع المحدد؛ لن يتم فحص أي رموز.",
            flush=True,
        )
        return BinanceSymbolSelection([], [], True, True)

    def volume_key(market_data: Dict[str, Any]) -> float:
        symbol = market_data.get("symbol")
        if not symbol:
            return 0.0
        vol = _extract_quote_volume(tickers.get(symbol, {}))
        if vol is not None:
            return vol
        base_vol = tickers.get(symbol, {}).get("baseVolume")
        try:
            return float(base_vol) if base_vol is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    usdtm_by_volume = sorted(usdtm_markets, key=volume_key, reverse=True)

    target_limit = limit if limit and limit > 0 else len(usdtm_by_volume)
    final: List[str] = []
    added: set[str] = set()

    for symbol, _, _ in prioritized:
        if len(final) >= target_limit:
            break
        if symbol not in added:
            final.append(symbol)
            added.add(symbol)

    for market in usdtm_by_volume:
        if len(final) >= target_limit:
            break
        symbol = market.get("symbol")
        if symbol and symbol not in added:
            final.append(symbol)
            added.add(symbol)

    prioritized_symbols = [symbol for symbol, _, _ in prioritized if symbol in added]
    return BinanceSymbolSelection(final, prioritized_symbols, used_height_filter, have_ticker_data)


def fetch_binance_usdtm_symbols(
    exchange: Any,
    *,
    limit: Optional[int] = None,
    explicit: Optional[str] = None,
    selector: Optional[BinanceSymbolSelectorConfig] = None,
) -> List[str]:
    """Load Binance USDT-M symbols prioritising momentum if requested."""

    selector_cfg = selector or DEFAULT_BINANCE_SYMBOL_SELECTOR
    selection = _binance_pick_symbols(exchange, limit or 0, explicit, selector_cfg)
    if selection.symbols:
        return selection.symbols
    if selection.used_height_filter and selection.had_prioritization_data:
        return []

    # Fallback to legacy behaviour when prioritisation fails
    markets = exchange.load_markets()
    symbols = [
        symbol
        for symbol, market in markets.items()
        if market.get("linear") and market.get("quote") == "USDT" and market.get("type") == "swap"
    ]
    symbols.sort()
    if limit and limit > 0:
        return symbols[:limit]
    return symbols


def _ensure_markets_loaded(exchange: Any) -> bool:
    """Load exchange markets once per client for faster repeated OHLCV calls."""

    markets = getattr(exchange, "markets", None)
    if markets:
        return True
    try:
        _call_with_retries(exchange.load_markets, retries=3)
    except Exception as exc:
        if _is_rate_limit_error(exc):
            print(f"تحذير: تعذر تحميل أسواق Binance بسبب معدل الطلبات: {exc}", flush=True)
        else:
            print(f"تحذير: تعذر تحميل أسواق Binance: {exc}", flush=True)
        return False
    return True


def _is_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if isinstance(status, int) and status in (418, 429):
        return True
    return any(
        token in message
        for token in (
            "too many requests",
            "rate limit",
            "rate-limit",
            "banned",
            "418",
            "429",
            "ip banned",
        )
    )


def _rate_limit_backoff(attempt: int) -> float:
    return min(10.0, 1.5 ** attempt)


def _extract_ban_until_ms(message: str) -> Optional[int]:
    match = re.search(r"banned until (\d+)", message, re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _sleep_until(timestamp_ms: int) -> None:
    now_ms = int(time.time() * 1000)
    if timestamp_ms <= now_ms:
        return
    wait_seconds = max(0.0, (timestamp_ms - now_ms) / 1000.0) + 1.0
    time.sleep(wait_seconds)


class _GlobalRateLimiter:
    def __init__(self, min_interval: float = 0.2) -> None:
        self._min_interval = max(0.0, float(min_interval))
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
            self._next_allowed = time.time() + self._min_interval


GLOBAL_RATE_LIMITER = _GlobalRateLimiter()


def _call_with_retries(action: Callable[[], Any], *, retries: int = 3) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            GLOBAL_RATE_LIMITER.wait()
            return action()
        except Exception as exc:  # pragma: no cover - defensive
            last_exc = exc
            if not _is_rate_limit_error(exc):
                raise
            ban_until = _extract_ban_until_ms(str(exc))
            if ban_until:
                _sleep_until(ban_until)
            else:
                time.sleep(_rate_limit_backoff(attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("request failed without exception")


def fetch_ohlcv(
    exchange: Any,
    symbol: str,
    timeframe: str,
    limit: int,
    *,
    fast_scan: bool = False,
) -> List[Dict[str, float]]:
    """Fetch OHLCV data while preserving full history for structural parity.

    Binance USDT-M returns at most 1500 candles per request.  TradingView keeps
    indicator state across the entire available history, so requesting only the
    latest ``limit`` bars leads to structural mismatches (missing legacy
    pullbacks/ChoCh/OB states).  To replicate the indicator faithfully we walk
    the history from the earliest candle and keep the trailing slice when the
    caller specifies ``limit``.  Passing ``limit<=0`` fetches the entire
    available history.
    """

    timeframe_seconds = _parse_timeframe_to_seconds(timeframe, None) or 60
    timeframe_ms = timeframe_seconds * 1000
    max_batch = 1500
    since = 0
    candles: List[Dict[str, float]] = []
    target = limit if limit > 0 else None

    if fast_scan:
        request_limit = target or max_batch
        raw = _call_with_retries(
            lambda: exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=request_limit),
            retries=3,
        )
        return_list: List[Dict[str, float]] = []
        for entry in raw:
            if not entry or len(entry) < 6:
                continue
            t = _coerce_float(entry[0], default=NA)
            o = _coerce_float(entry[1], default=NA)
            h = _coerce_float(entry[2], default=NA)
            l = _coerce_float(entry[3], default=NA)
            c = _coerce_float(entry[4], default=NA)
            v = _coerce_float(entry[5], default=0.0)
            if math.isnan(t) or math.isnan(o) or math.isnan(h) or math.isnan(l) or math.isnan(c):
                continue
            return_list.append({'time': int(t), 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v})
        return return_list
    while True:
        request_limit = max_batch
        if target is not None and target < max_batch and not candles:
            # first batch can be trimmed if the caller only needs a small window
            request_limit = target
        raw: List[List[float]]
        raw = _call_with_retries(
            lambda: exchange.fetch_ohlcv(
                symbol, timeframe=timeframe, limit=request_limit, since=since
            ),
            retries=3,
        )
        if not raw:
            break
        for entry in raw:
            if not entry or len(entry) < 6:
                continue
            t = _coerce_float(entry[0], default=NA)
            o = _coerce_float(entry[1], default=NA)
            h = _coerce_float(entry[2], default=NA)
            l = _coerce_float(entry[3], default=NA)
            c = _coerce_float(entry[4], default=NA)
            v = _coerce_float(entry[5], default=0.0)
            if math.isnan(t) or math.isnan(o) or math.isnan(h) or math.isnan(l) or math.isnan(c):
                continue
            candles.append({'time': int(t), 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v})
        if target is not None and len(candles) > target:
            candles = candles[-target:]
        last_open = raw[-1][0]
        next_since = last_open + timeframe_ms
        if len(raw) < request_limit:
            break
        if next_since <= since:
            next_since = since + timeframe_ms
        since = next_since
    return candles


METRIC_LABELS = [
    ("alerts", "عدد التنبيهات"),
    ("pullback_arrows", "إشارات Pullback"),
    ("choch_labels", "علامات CHoCH"),
    ("bos_labels", "علامات BOS"),
    ("idm_labels", "علامات IDM"),
    ("demand_zones", "مناطق الطلب"),
    ("supply_zones", "مناطق العرض"),
    ("idm_ob_new", "IDM OB تم إنشائها حديثاً"),
    ("idm_ob_touched", "IDM OB تم ملامستها"),
    ("ext_ob_new", "EXT OB تم إنشائها حديثاً"),
    ("ext_ob_touched", "EXT OB تم ملامستها"),
    ("bullish_fvg", "فجوات FVG صاعدة"),
    ("bearish_fvg", "فجوات FVG هابطة"),
    ("order_flow_boxes", "صناديق Order Flow"),
    ("liquidity_objects", "مستويات السيولة"),
    ("scob_colored_bars", "شموع SCOB"),
]


EVENT_DISPLAY_ORDER = [
    ("BOS", "BOS"),
    ("BOS_PLUS", "BOS+"),
    ("CHOCH", "CHOCH"),
    ("MSS_PLUS", "MSS+"),
    ("MSS", "MSS"),
    ("IDM", "IDM"),
    ("ORDER_BLOCK", "Order Block"),
    ("BREAKER_BLOCK", "Breaker Block"),
    ("MITIGATION_BLOCK", "Mitigation Block"),
    ("PROPULSION_BLOCK", "Propulsion Block"),
    ("DEMAND_ZONE", "Demand Zone"),
    ("SUPPLY_ZONE", "Supply Zone"),
    ("ORDER_FLOW_BREAK_MAJOR", "Order Flow Break (Major)"),
    ("ORDER_FLOW_BREAK_MINOR", "Order Flow Break (Minor)"),
    ("ORDER_FLOW_MAJOR", "Major OF"),
    ("ORDER_FLOW_MINOR", "Minor OF"),
    ("SCOB", "SCOB"),
    ("SCOB_BULLISH", "Bullish SCOB"),
    ("SCOB_BEARISH", "Bearish SCOB"),
    ("INSIDE_BAR", "Inside Bar"),
    ("INSIDE_BAR_CANDLE", "Inside Bar Candle"),
    ("IDM_OB", "IDM OB"),
    ("EXT_OB", "EXT OB"),
    ("HIST_IDM_OB", "Hist IDM OB"),
    ("HIST_EXT_OB", "Hist EXT OB"),
    ("GOLDEN_ZONE", "Golden zone"),
    ("GOLDEN_ZONE_TOUCH", "Golden zone (Touch)"),
    ("LIQUIDITY_TOUCH", "Liquidity Sweep"),
    ("X", "X"),
    ("RED_CIRCLE", "الدوائر الحمراء"),
    ("GREEN_CIRCLE", "الدوائر الخضراء"),
    ("FUTURE_BOS", "ليبل BOS المستقبلي"),
    ("FUTURE_CHOCH", "ليبل CHOCH المستقبلي"),
]


def print_symbol_summary(index: int, symbol: str, timeframe: str, candle_count: int, metrics: dict) -> None:
    header_color = ANSI_HEADER_COLORS[index % len(ANSI_HEADER_COLORS)]
    symbol_display = _format_symbol(symbol)
    header_lines = [
        f"{header_color}{ANSI_BOLD}════ تحليل {symbol_display}{header_color}{ANSI_BOLD} ({timeframe}) ════{ANSI_RESET}",
        f"{ANSI_DIM}عدد الشموع: {candle_count}{ANSI_RESET}",
    ]
    price_value = metrics.get("current_price")
    if isinstance(price_value, (int, float)):
        price_value = float(price_value)
        if not math.isnan(price_value):
            header_lines.append(f"{ANSI_DIM}السعر الحالي: {format_price(price_value)}{ANSI_RESET}")
    elif isinstance(price_value, str):
        header_lines.append(f"{ANSI_DIM}السعر الحالي: {price_value}{ANSI_RESET}")
    change_value = metrics.get("daily_change_percent")
    if isinstance(change_value, (int, float)):
        header_lines.append(f"{ANSI_DIM}تغير 24 ساعة: {change_value:+.2f}%{ANSI_RESET}")
    header = "\n".join(header_lines)
    print(header, flush=True)
    for key, label in METRIC_LABELS:
        value = metrics.get(key, 0)
        value_color = ANSI_VALUE_POS if value > 0 else ANSI_VALUE_ZERO
        print(f"  {ANSI_LABEL}{label:<26}{ANSI_RESET}: {value_color}{value}{ANSI_RESET}", flush=True)
    latest_events = metrics.get("latest_events") or {}
    print(f"{ANSI_BOLD}أحدث الإشارات مع الأسعار{ANSI_RESET}", flush=True)
    for key, label in EVENT_DISPLAY_ORDER:
        event = latest_events.get(key)
        if event:
            display_text = event.get("display")
            if display_text is None:
                price = event.get("price")
                if isinstance(price, tuple):
                    display_text = " → ".join(format_price(p) for p in price)
                else:
                    display_text = format_price(price if isinstance(price, (int, float)) else None)
            status_display = event.get("status_display")
            if status_display:
                display_text = f"{display_text} [{status_display}]"
            time_display = event.get("time_display") or format_timestamp(event.get("time"))
            if time_display and time_display != "—":
                display_text = f"{display_text} | {time_display}"
            direction_hint = _resolve_direction(
                event.get("direction"),
                event.get("direction_display"),
                event.get("status"),
                event.get("text"),
                display_text,
            )
            colored_display = _colorize_directional_text(
                display_text,
                direction=direction_hint,
                fallback=ANSI_VALUE_POS,
            )
        else:
            colored_display = _colorize_directional_text("—", direction=None, fallback=ANSI_VALUE_ZERO)
        print(f"  {ANSI_LABEL}{label:<26}{ANSI_RESET}: {colored_display}", flush=True)
    print(f"{ANSI_DIM}{'-'*48}{ANSI_RESET}", flush=True)


def print_trace_comparison(result: 'TraceComparisonResult') -> None:
    status = "مطابق" if result.matches else "اختلاف"
    print(
        f"Trace comparison: {status} (المرجع={result.reference_events}, الحالي={result.current_events})",
        flush=True,
    )
    if result.mismatches:
        preview = result.mismatches[:5]
        for mismatch in preview:
            print(
                "  - الحدث #{idx}: المرجع={ref} | الحالي={cur}".format(
                    idx=mismatch.get("index"),
                    ref=json.dumps(mismatch.get("reference", {}), ensure_ascii=False),
                    cur=json.dumps(mismatch.get("current", {}), ensure_ascii=False),
                ),
                flush=True,
            )
        extra = len(result.mismatches) - len(preview)
        if extra > 0:
            print(f"  - ... {extra} اختلافات إضافية", flush=True)


def _extract_daily_change_percent(ticker: dict) -> float | None:
    value = ticker.get("percentage") if isinstance(ticker, dict) else None
    if value is None:
        open_price = ticker.get("open") if isinstance(ticker, dict) else None
        last_price = ticker.get("last") if isinstance(ticker, dict) else None
        if open_price and last_price and open_price != 0:
            value = ((float(last_price) - float(open_price)) / float(open_price)) * 100.0
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _collect_recent_event_hits(series: object, latest_events: object, *, bars: int = 2) -> tuple[list[str], list[int]]:
    if bars <= 0:
        return [], []

    recent_times: list[int] = []
    if hasattr(series, "get_time"):
        for offset in range(bars):
            try:
                ts = series.get_time(offset)
            except Exception:
                ts = None
            if isinstance(ts, (int, float)) and ts > 0:
                recent_times.append(int(ts))

    if not recent_times or not isinstance(latest_events, dict):
        return [], recent_times

    hits: list[str] = []
    for key, payload in latest_events.items():
        timestamp = None
        if isinstance(payload, dict):
            timestamp = payload.get("time") or payload.get("ts") or payload.get("timestamp")
        if isinstance(timestamp, (int, float)) and int(timestamp) in recent_times:
            hits.append(str(key))
    return hits, recent_times



# -----------------------------------------------------------------------------
# طباعة/تنبيه الأحداث: اطبع أحدث الأحداث فقط + منع تكرار الإشعارات بين الدورات
# -----------------------------------------------------------------------------
EVENT_PRINT_ONLY = True  # اجعلها False إذا تريد أيضًا طباعة ملخص الجدول لكل رمز.
SILENT_WHEN_NO_EVENTS = True  # إذا True: لا يطبع "تخطي ..." ولا يطبع ملخصًا عندما لا توجد أحداث جديدة.

EVENT_DEDUP_ENABLED = True
EVENT_DEDUP_PERSIST = True
EVENT_DEDUP_CACHE_FILE = Path("event_seen_cache.json")

_EVENT_SENT_CACHE: Dict[str, Dict[str, str]] = {}

def _load_event_seen_cache() -> None:
    global _EVENT_SENT_CACHE
    if not EVENT_DEDUP_PERSIST:
        _EVENT_SENT_CACHE = {}
        return
    try:
        if EVENT_DEDUP_CACHE_FILE.exists():
            raw = json.loads(EVENT_DEDUP_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                cleaned: Dict[str, Dict[str, str]] = {}
                for sym, mapping in raw.items():
                    if isinstance(sym, str) and isinstance(mapping, dict):
                        cleaned[sym] = {str(k): str(v) for k, v in mapping.items() if v is not None}
                _EVENT_SENT_CACHE = cleaned
    except Exception:
        _EVENT_SENT_CACHE = {}

def _save_event_seen_cache() -> None:
    if not EVENT_DEDUP_PERSIST:
        return
    try:
        EVENT_DEDUP_CACHE_FILE.write_text(
            json.dumps(_EVENT_SENT_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass

def _event_dedup_token(key: str, payload: Dict[str, Any]) -> str:
    """Build a stable token to avoid re-sending the same event."""
    fp = payload.get("fingerprint")
    if fp:
        return str(fp)
    ts = payload.get("time") or payload.get("ts") or payload.get("timestamp")
    ts_int = int(ts) if isinstance(ts, (int, float)) else 0
    display = payload.get("display")
    price = payload.get("price")
    return f"{key}:{ts_int}:{display}:{price}"

def _format_event_line(key: str, payload: Dict[str, Any]) -> str:
    label = EVENT_PRINT_LABELS.get(key, key)
    display = payload.get("display")
    if display is None:
        price = payload.get("price")
        if isinstance(price, tuple):
            display = " → ".join(format_price(p) for p in price)
        else:
            display = format_price(price if isinstance(price, (int, float)) else None)
    status_display = payload.get("status_display")
    if status_display:
        display = f"{display} [{status_display}]"
    time_display = payload.get("time_display") or format_timestamp(payload.get("time"))
    if time_display and time_display != "—":
        display = f"{display} | {time_display}"
    return f"{label}: {display}"

def _emit_recent_events(symbol: str, timeframe: str, latest_events: Dict[str, Any], recent_hits: List[str]) -> List[str]:
    """Return formatted lines for events that happened in the most recent candles only."""
    if not recent_hits:
        return []
    order_map: Dict[str, int] = {name: idx for idx, (name, _) in enumerate(EVENT_DISPLAY_ORDER)}
    items: List[Tuple[int, int, str, Dict[str, Any]]] = []
    for key in recent_hits:
        payload = latest_events.get(key)
        if not isinstance(payload, dict):
            continue
        if EVENT_PRINT_TOGGLES and not EVENT_PRINT_TOGGLES.get(str(key), True):
            continue
        ts = payload.get("time") or payload.get("ts") or payload.get("timestamp")
        ts_int = int(ts) if isinstance(ts, (int, float)) else 0
        items.append((ts_int, order_map.get(str(key), 999), str(key), payload))
    items.sort(key=lambda it: (it[0], it[1], it[2]))

    if EVENT_DEDUP_ENABLED:
        sym_cache = _EVENT_SENT_CACHE.setdefault(symbol, {})
    else:
        sym_cache = {}

    lines_out: List[str] = []
    for _, _, key, payload in items:
        if EVENT_PRINT_KEYS and key not in EVENT_PRINT_KEYS:
            continue
        token = _event_dedup_token(key, payload)
        if EVENT_DEDUP_ENABLED and sym_cache.get(key) == token:
            continue
        lines_out.append(_format_event_line(key, payload))
        if EVENT_DEDUP_ENABLED:
            sym_cache[key] = token
    return lines_out

def _event_timestamp(payload: Dict[str, Any]) -> Optional[int]:
    ts = payload.get("time") or payload.get("ts") or payload.get("timestamp")
    if isinstance(ts, (int, float)):
        return int(ts)
    return None

def _latest_event_payload(latest_events: Dict[str, Any], keys: Sequence[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[int]]:
    best_key = None
    best_payload = None
    best_ts = None
    for key in keys:
        payload = latest_events.get(key)
        if not isinstance(payload, dict):
            continue
        ts = _event_timestamp(payload)
        if ts is None:
            continue
        if best_ts is None or ts > best_ts:
            best_key = key
            best_payload = payload
            best_ts = ts
    return best_key, best_payload, best_ts

def _price_in_range(price: float, payload: Dict[str, Any]) -> bool:
    if math.isnan(price):
        return False
    value = payload.get("price")
    if isinstance(value, tuple) and len(value) == 2:
        low = min(value[0], value[1])
        high = max(value[0], value[1])
        return low <= price <= high
    return False

def _signal_direction(payload: Dict[str, Any]) -> Optional[str]:
    direction = payload.get("direction")
    if direction in ("bullish", "bearish"):
        return direction
    return _resolve_direction(payload.get("direction_display"), payload.get("text"), payload.get("display"))

def scan_binance(
    timeframe: str,
    limit: int,
    symbols: list[str] | None,
    concurrency: int,
    tracer: 'ExecutionTracer' | None = None,
    *,
    min_daily_change: float = 0.0,
    inputs: 'IndicatorInputs' | None = None,
    recent_window_bars: int | None = None,
    max_symbols: int | None = None,
    symbol_selector: 'BinanceSymbolSelectorConfig' | None = None,
    fast_scan: bool = True,
) -> tuple['SmartMoneyAlgoProE5', list[dict]]:
    if ccxt is None:
        raise RuntimeError("ccxt is not available")
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    _ensure_markets_loaded(exchange)
    all_symbols = symbols or fetch_binance_usdtm_symbols(
        exchange,
        limit=max_symbols,
        selector=symbol_selector,
    )
    if max_symbols and max_symbols > 0:
        all_symbols = all_symbols[: int(max_symbols)]
    summaries: list[dict] = []
    primary_runtime = None
    window = recent_window_bars
    if window is None:
        console_inputs = getattr(inputs, "console", None) if inputs else None
        if console_inputs is not None and getattr(console_inputs, "max_age_bars", None) is not None:
            try:
                window = int(console_inputs.max_age_bars) + 1
            except Exception:
                window = 2
        else:
            window = 2
    window = max(1, int(window))

    tickers: dict = {}
    ticker_error = None
    try:
        if all_symbols:
            tickers = _call_with_retries(
                lambda: exchange.fetch_tickers(all_symbols),
                retries=3,
            )
        else:
            tickers = _call_with_retries(exchange.fetch_tickers, retries=3)
    except Exception as exc:
        try:
            tickers = _call_with_retries(exchange.fetch_tickers, retries=3)
        except Exception as fallback_exc:
            ticker_error = fallback_exc
            print(f"تعذر جلب بيانات التيكر بشكلٍ مجمّع: {fallback_exc}", flush=True)
        else:
            ticker_error = exc

    exchange_local = threading.local()

    def _get_exchange() -> object:
        local_exchange = getattr(exchange_local, "client", None)
        if local_exchange is None:
            local_exchange = ccxt.binanceusdm({"enableRateLimit": True})
            if not _ensure_markets_loaded(local_exchange):
                raise RuntimeError("تعذر تحميل الأسواق بسبب معدل الطلبات")
            exchange_local.client = local_exchange
        return local_exchange

    def scan_symbol(idx: int, symbol: str):
        try:
            ticker = tickers.get(symbol)
            if ticker is None and (min_daily_change > 0.0 or ticker_error is not None):
                try:
                    ticker = _get_exchange().fetch_ticker(symbol)
                except Exception as exc:
                    print(
                        f"تخطي {_format_symbol(symbol)} بسبب فشل fetch_ticker: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return idx, None, None
            daily_change = _extract_daily_change_percent(ticker)
            if min_daily_change > 0.0 and daily_change is not None and daily_change <= min_daily_change:
                print(
                    f"تخطي {_format_symbol(symbol)} (تغير 24 ساعة {daily_change:.2f}% ≤ الحد الأدنى {min_daily_change:.2f}%)",
                    flush=True,
                )
                if tracer and tracer.enabled:
                    tracer.log(
                        "scan",
                        "symbol_skipped_daily_change",
                        timestamp=None,
                        symbol=symbol,
                        change=daily_change,
                        threshold=min_daily_change,
                    )
                return idx, None, None
            candles = fetch_ohlcv(_get_exchange(), symbol, timeframe, limit, fast_scan=fast_scan)
            runtime = SmartMoneyAlgoProE5(inputs=inputs, base_timeframe=timeframe, tracer=tracer)
            runtime.process(candles)
            metrics = runtime.gather_console_metrics()
            latest_events = metrics.get("latest_events") or {}
            recent_hits, recent_times = _collect_recent_event_hits(runtime.series, latest_events, bars=window)
            if not recent_hits:
                if not SILENT_WHEN_NO_EVENTS:
                    print(
                        f"تخطي {_format_symbol(symbol)} لعدم وجود أحداث خلال آخر {window} شموع",
                        flush=True,
                    )
                if tracer and tracer.enabled:
                    tracer.log(
                        "scan",
                        "symbol_skipped_stale_events",
                        timestamp=runtime.series.get_time(0) or None,
                        symbol=symbol,
                        timeframe=timeframe,
                        reference_times=recent_times,
                        window=window,
                    )
                return idx, None, None

            liquidity_key, liquidity_payload, liquidity_time = _latest_event_payload(
                latest_events,
                ("LIQUIDITY_TOUCH",),
            )
            structure_key, structure_payload, structure_time = _latest_event_payload(
                latest_events,
                ("MSS", "MSS_PLUS", "BOS", "BOS_PLUS", "CHOCH"),
            )
            liquidity_structure_match = (
                liquidity_payload is not None
                and liquidity_time is not None
                and structure_time is not None
                and structure_time >= liquidity_time
            )

            signal_key, signal_payload, signal_time = _latest_event_payload(
                latest_events,
                ("CHOCH", "MSS", "MSS_PLUS"),
            )
            signal_direction = _signal_direction(signal_payload or {})
            close_price = runtime.series.get("close")
            retrace_match = False
            if signal_payload and signal_direction and not math.isnan(close_price):
                fvg_holder = (
                    getattr(runtime, "bullish_gap_holder", PineArray())
                    if signal_direction == "bullish"
                    else getattr(runtime, "bearish_gap_holder", PineArray())
                )
                for i in range(fvg_holder.size()):
                    box = fvg_holder.get(i)
                    if isinstance(box, Box) and box.bottom <= close_price <= box.top:
                        retrace_match = True
                        break
                if not retrace_match:
                    zone_box = getattr(runtime, "bxf", None)
                    if isinstance(zone_box, Box) and zone_box.bottom <= close_price <= zone_box.top:
                        retrace_match = True
                if not retrace_match:
                    for zone_key in ("IDM_OB", "EXT_OB"):
                        zone_payload = latest_events.get(zone_key)
                        if isinstance(zone_payload, dict) and _price_in_range(close_price, zone_payload):
                            retrace_match = True
                            break
                if not retrace_match:
                    fvg_payload = latest_events.get("FVG")
                    if isinstance(fvg_payload, dict) and _price_in_range(close_price, fvg_payload):
                        retrace_match = True

            if not (liquidity_structure_match or retrace_match):
                if tracer and tracer.enabled:
                    tracer.log(
                        "scan",
                        "symbol_skipped_conditions",
                        timestamp=runtime.series.get_time(0) or None,
                        symbol=symbol,
                        timeframe=timeframe,
                        liquidity_key=liquidity_key,
                        structure_key=structure_key,
                        signal_key=signal_key,
                        signal_direction=signal_direction,
                    )
                return idx, None, None

            # اطبع أحدث الأحداث فقط (آخر شموع ضمن النافذة) مثل سكربت FINAL_liqui
            try:
                trigger_lines = _emit_recent_events(symbol, timeframe, latest_events, recent_hits)
            except Exception:
                trigger_lines = []
            if trigger_lines:
                print(f"\n{_format_symbol(symbol)} ({timeframe})", flush=True)
                for _ln in trigger_lines:
                    print(_ln, flush=True)
                _save_event_seen_cache()

            metrics["daily_change_percent"] = daily_change
            summary = {
                "symbol": symbol,
                "timeframe": timeframe,
                "candles": len(candles),
                "alerts": metrics.get("alerts", len(getattr(runtime, "alerts", []))),
                "boxes": metrics.get("boxes", len(runtime.boxes)),
                "metrics": metrics,
            }
            if not EVENT_PRINT_ONLY:
                print_symbol_summary(idx, symbol, timeframe, len(candles), metrics)
            if tracer and tracer.enabled:
                tracer.log(
                    "scan",
                    "symbol_complete",
                    timestamp=runtime.series.get_time(0),
                    symbol=symbol,
                    timeframe=timeframe,
                    candles=len(candles),
                )
            return idx, runtime, summary
        except Exception as exc:
            print(f"فشل مسح {_format_symbol(symbol)}: {exc}", flush=True)
            return idx, None, None

    if concurrency > 1:
        print("تم فرض التوازي = 1 لتجنّب حظر REST من Binance.", flush=True)
    results = [scan_symbol(idx, symbol) for idx, symbol in enumerate(all_symbols)]

    results.sort(key=lambda item: item[0])
    for idx, runtime, summary in results:
        if runtime is None or summary is None:
            continue
        summaries.append(summary)
        if primary_runtime is None:
            primary_runtime = runtime
    if primary_runtime is None:
        primary_runtime = SmartMoneyAlgoProE5(inputs=inputs, tracer=tracer)
        primary_runtime.process([])
    return primary_runtime, summaries


def _parse_bool_token(token: str) -> bool:
    normalized = token.strip().lower()
    if normalized in {"1", "true", "yes", "on", "y", "enable", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "n", "disable", "disabled"}:
        return False
    raise ValueError(f"قيمة منطقية غير صالحة: {token!r}")


class _OptionalBoolAction(argparse.Action):
    """argparse action allowing ``--flag`` or ``--flag=false`` patterns."""

    def __init__(self, option_strings, dest, **kwargs):
        if "nargs" in kwargs:
            raise ValueError("_OptionalBoolAction لا يدعم تحديد nargs")
        kwargs.setdefault("default", False)
        super().__init__(option_strings, dest, nargs="?", **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            setattr(namespace, self.dest, True)
            return
        try:
            parsed = _parse_bool_token(str(values))
        except ValueError as exc:
            parser.error(str(exc))
        setattr(namespace, self.dest, parsed)


def print_trace_comparison(result: TraceComparisonResult) -> None:
    status = "مطابق" if result.matches else "اختلاف"
    print(f"Trace comparison: {status} (المرجع={result.reference_events}, الحالي={result.current_events})", flush=True)
    if result.mismatches:
        preview = result.mismatches[:5]
        for mismatch in preview:
            print(
                "  - الحدث #{idx}: المرجع={ref} | الحالي={cur}".format(
                    idx=mismatch.get("index"),
                    ref=json.dumps(mismatch.get("reference", {}), ensure_ascii=False),
                    cur=json.dumps(mismatch.get("current", {}), ensure_ascii=False),
                ),
                flush=True,
            )
        extra = len(result.mismatches) - len(preview)
        if extra > 0:
            print(f"  - ... {extra} اختلافات إضافية", flush=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Smart Money Algo Pro E5 Python port")
    parser.add_argument("--data", type=Path, help="JSON file with OHLCV candles", required=False)
    parser.add_argument("--analysis-timeframe", type=str, default="", help="Override base timeframe when using --data")
    parser.add_argument("--timeframe", type=str, default=SCANNER_TIMEFRAME, help="Timeframe used when scanning Binance")
    parser.add_argument(
        "--lookback",
        type=int,
        default=SCANNER_LOOKBACK,
        help="Number of candles to request per symbol when scanning (0 = full history)",
    )
    parser.add_argument("--bars", type=int, default=0, help="Limit number of candles to analyse from --data source")
    parser.add_argument("--symbols", type=str, default="")
    parser.add_argument("--concurrency", type=int, default=EDITOR_AUTORUN_DEFAULTS.concurrency)
    parser.add_argument(
        "--min-daily-change",
        type=float,
        default=SCANNER_MIN_DAILY_CHANGE,
        help="الحد الأدنى لتغير 24 ساعة (٪) لاختيار الرمز عند مسح Binance، 0 لتعطيل الفلتر",
    )
    parser.add_argument("--no-scan", action="store_true")
    parser.add_argument("--trace", action="store_true", help="Enable execution tracing")
    parser.add_argument("--trace-file", type=Path, help="Write execution trace to JSON file")
    parser.add_argument("--compare-trace", type=Path, help="قارن التتبع الحالي بملف JSON مرجعي")
    parser.add_argument(
        "--max-age-bars",
        type=int,
        default=EVENT_PRINT_MAX_AGE_BARS,
        help="Ignore console events older than this many completed bars (minimum 1)",
    )
    parser.add_argument(
        "--continuous",
        "--continuous-scan",
        dest="continuous_scan",
        action=_OptionalBoolAction,
        default=SCANNER_CONTINUOUS,
        help="تشغيل ماسح Binance في حلقة متواصلة بدون توقف (يدعم true/false)",
    )
    parser.add_argument(
        "--no-continuous",
        "--no-continuous-scan",
        dest="continuous_scan",
        action="store_false",
        help="تعطيل حلقة المسح المستمرة",
    )
    parser.add_argument(
        "--scan-interval",
        type=float,
        default=EDITOR_AUTORUN_DEFAULTS.scan_interval,
        help="عدد الثواني للانتظار قبل إعادة تشغيل المسح عند تفعيل --continuous-scan",
    )
    parser.add_argument(
        "--fast-scan",
        dest="fast_scan",
        action=_OptionalBoolAction,
        default=EDITOR_AUTORUN_DEFAULTS.fast_scan,
        help="تسريع المسح بجلب آخر الشموع فقط بدون تحميل التاريخ الكامل (أسرع بكثير)",
    )
    parser.add_argument(
        "--no-fast-scan",
        dest="fast_scan",
        action="store_false",
        help="تعطيل وضع المسح السريع (الحصول على التاريخ الكامل)",
    )
    args = parser.parse_args(argv)
    if args.min_daily_change < 0.0:
        parser.error("--min-daily-change يجب أن يكون رقمًا غير سالب")
    if args.max_age_bars <= 0:
        parser.error("--max-age-bars يجب أن يكون رقمًا موجبًا")
    if args.scan_interval < 0.0:
        parser.error("--scan-interval يجب أن يكون رقمًا غير سالب")
    if args.continuous_scan and args.scan_interval <= 0.0:
        args.scan_interval = max(EDITOR_AUTORUN_DEFAULTS.scan_interval, 2.0)

    tracer = ExecutionTracer(enabled=args.trace, outfile=args.trace_file)

    # تحميل كاش الأحداث المرسلة (لمنع تكرار الإشعارات بين الدورات)
    _load_event_seen_cache()


    def perform_comparison() -> None:
        if args.compare_trace:
            result = tracer.compare(args.compare_trace)
            print_trace_comparison(result)

    indicator_inputs = IndicatorInputs()
    indicator_inputs.console.max_age_bars = args.max_age_bars

    # Apply user feature toggles to enable/disable detection logic
    apply_feature_toggles(indicator_inputs, FEATURE_TOGGLES)

    if args.data:
        candles = json.loads(args.data.read_text())
        if args.bars > 0:
            candles = candles[-args.bars :]
        runtime = SmartMoneyAlgoProE5(
            inputs=indicator_inputs,
            base_timeframe=args.analysis_timeframe or None,
            tracer=tracer,
        )
        runtime.process(candles)
        perform_comparison()
        metrics = runtime.gather_console_metrics()
        metrics["daily_change_percent"] = None
        print_symbol_summary(0, "DATA", args.analysis_timeframe or args.timeframe, len(candles), metrics)
        tracer.emit()
        return 0

    if args.no_scan:
        runtime = SmartMoneyAlgoProE5(
            inputs=indicator_inputs,
            base_timeframe=args.analysis_timeframe or None,
            tracer=tracer,
        )
        runtime.process([])
        perform_comparison()
        metrics = runtime.gather_console_metrics()
        metrics["daily_change_percent"] = None
        print_symbol_summary(0, "EMPTY", args.analysis_timeframe or args.timeframe, 0, metrics)
        tracer.emit()
        return 0

    manual_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or None

    iteration = 0
    try:
        while True:
            iteration += 1
            tracer.clear()
            if args.continuous_scan and iteration > 1:
                print(f"\nإعادة تشغيل المسح (الدورة {iteration})", flush=True)
            scan_binance(
                args.timeframe,
                args.lookback,
                manual_symbols,
                args.concurrency,
                tracer,
                min_daily_change=args.min_daily_change,
                inputs=indicator_inputs,
                fast_scan=args.fast_scan,
            )
            perform_comparison()
            tracer.emit()
            if not args.continuous_scan:
                print(
                    "اكتمل المسح بعد دورة واحدة لأن خيار التشغيل المستمر غير مُفعّل."
                    " لتفعيل الحلقة استخدم --continuous-scan=true أو فعّل المتغير"
                    " AUTORUN_CONTINUOUS_SCAN في أعلى الملف.",
                    flush=True,
                )
                break
            if args.scan_interval > 0.0:
                print(
                    f"انتظار {args.scan_interval:.2f} ثانية قبل تشغيل المسح التالي",
                    flush=True,
                )
                time.sleep(args.scan_interval)
    except KeyboardInterrupt:
        print("تم إيقاف المسح من قبل المستخدم.", flush=True)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())