import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import threading
import warnings
import traceback
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# CONFIGURAZIONE (KRYON ULTIMATE PRO V 16.4.5 - STRATEGY REBALANCE + BLOCK AUDIT)
# ==============================================================================
PROFIT_MODE = True
PRIMARY_SYMBOL = "XAUUSD"
SECONDARY_SYMBOLS = ["USTECH", "GER40", "US500", "JPN225", "EUSTX50", "US30", "UK100", "US2000", "FRA40", "SMI20"]
CRYPTO_SYMBOLS = ["BTCUSD", "ETHUSD"]

symbols_list = [PRIMARY_SYMBOL, *SECONDARY_SYMBOLS]
symbols = symbols_list.copy()

TIMEFRAME_MAIN = mt5.TIMEFRAME_M5
MAX_UNIQUE_SYMBOLS = 13
MAX_CLUSTERS_PER_SYMBOL = 5
SYMBOL_CLUSTER_CAPS = {"USTECH": 5, "GER40": 5, "US500": 5, "XAUUSD": 5, "BTCUSD": 5}

MAX_NEW_TRADES_PER_CYCLE = 4
MAX_TRADES_PER_CYCLE = 8
MAX_PARALLEL_SYMBOL_SCANS = MAX_UNIQUE_SYMBOLS
MAX_COMPATIBLE_STRATEGIES_PER_SYMBOL = 5
RISK_PER_TRADE_PERCENT = 0.65
MAX_TOTAL_RISK = 0.11
CURRENT_OPTIMIZER_PROFILE = "BALANCED"
STRESS_TEST_ACTIVE = True
USE_FULL_SYMBOL_SCAN_PARALLELISM = True
USE_FULL_STRATEGY_EVAL_PARALLELISM = True
MAX_PARALLEL_STRATEGY_EVAL = 10
PROBABILITY_ENGINE_ENABLED = True
PROBABILITY_MIN_THRESHOLD = 20.0
PROBABILITY_HARD_THRESHOLD = 20.0
PROBABILITY_CONFIRM_CYCLES = 3
PROBABILITY_MIN_AGE_SEC = 75
PROBABILITY_RECHECK_SEC = 3.0
STRATEGY_EVENT_SAMPLE_SEC = 20.0

OPTIMIZER_PROFILES = {
    "SAFE": {"risk_per_trade_percent": 0.40, "max_total_risk": 0.07, "max_trades_per_cycle": 4},
    "BALANCED": {"risk_per_trade_percent": 0.65, "max_total_risk": 0.11, "max_trades_per_cycle": 8},
    "ATTACK": {"risk_per_trade_percent": 0.90, "max_total_risk": 0.15, "max_trades_per_cycle": 10},
}

ASSET_WEIGHTS = {
    "XAUUSD": 1.0,
    "USTECH": 0.8,
    "US500": 0.7,
    "GER40": 0.65,
    "JPN225": 0.58,
    "EUSTX50": 0.60,
    "US30": 0.69,
    "UK100": 0.61,
    "US2000": 0.63,
    "FRA40": 0.57,
    "SMI20": 0.56,
    "BTCUSD": 0.62,
    "ETHUSD": 0.54,
}

MAX_RETRIES = 3
BASE_DEVIATION = 10
MAX_DEVIATION = 50
RETRY_DELAY = 0.4
MAGIC_ID = 600000

MAX_LOG_LINES = 50
SCALPING_SESSION_START = 7
SCALPING_SESSION_END = 21
STRATEGY_EVAL_MIN_TRADES = 25
SYMBOL_ALIASES = {
    "XAUUSD": ["XAUUSD", "GOLD", "XAUUSD.", "XAUUSDm", "XAUUSDz"],
    "USTECH": ["USTECH", "US100", "NAS100", "USTEC", "NASDAQ", "NAS"],
    "US500": ["US500", "SPX500", "SP500", "USSPX500", "US500."],
    "GER40": ["GER40", "DE40", "DAX40", "DAX40.fs", "DAX"],
    "JPN225": ["JPN225", "JP225", "NIKKEI225", "NIKKEI", "JAP225", "JPN225.fs"],
    "EUSTX50": ["EUSTX50", "EU50", "EU50.fs", "STOXX50", "EUROSTOXX50", "FESX", "STX50"],
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100."],
    "US30": ["US30", "DJ30", "DJ30.fs", "US30.", "DOW30", "WALLSTREET30"],
    "US2000": ["US2000", "RTY", "RUSSELL2000", "US2000.", "US2000.fs"],
    "FRA40": ["FRA40", "CAC40", "FR40", "FRA40.", "CAC"],
    "SMI20": ["SMI20", "SWI20", "SMI", "SWISS20", "SMI20."],
    "VIX": ["VIX", "USVIX", "VIX.", "VOLX", "CBOE VIX"],
    "BTCUSD": ["BTCUSD", "XBTUSD", "BTCUSD.", "BTCUSDm", "BTC-USD"],
    "ETHUSD": ["ETHUSD", "ETHUSD.", "ETHUSDm", "ETH-USD"],
}

TAG_ALIASES = {
    "U1TR": "USTR",
}

# ==============================================================================
# MULTI TP ENGINE CONFIG
# ==============================================================================
USE_MULTI_TP = True
TP_POINTS = [150, 200, 300, 500, 700, 1000, 1500, 2000]
SPLITS = len(TP_POINTS)
SPREAD_BUFFER_POINTS = 50

MIN_ATR_MULTIPLIER = 1.5
MIN_TREND_STRENGTH = 0.6

MIN_LOT_PER_SPLIT = 0.01
MAX_SPLITS = 8

# ==============================================================================
# GLOBALS & MEMORY
# ==============================================================================
latest_log_message = ""
latest_live_analysis_msgs = []
last_trade_time = {}
last_strategy_trade_time = {}
last_signal = {s: "NONE" for s in symbols_list}
last_global_trade_time = 0
last_heartbeat_time = 0

bot_killed = False
crypto_enabled = False
scalping_enabled = False

mt5_lock = threading.Lock()
strategy_runtime_lock = threading.Lock()
liquidity_sniper_cycle_lock = threading.Lock()
history_state_lock = threading.RLock()

performance_memory = {"wins": 0, "losses": 0}
strategy_stats = {}
strategy_runtime = {}
legacy_stats = {"wins": 0, "losses": 0, "flat": 0, "gross_profit": 0.0, "gross_loss": 0.0, "net_profit": 0.0, "closed": 0}
asset_performance = {}
session_stats = {"profit": 0.0, "trades": 0, "wins": 0, "losses": 0, "flat": 0}
session_decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
session_started_at = datetime.now()
session_broker_started_at = None
session_balance_start = None
session_active = False
session_baseline_ready = False
session_runtime_id = session_started_at.strftime("%Y%m%d%H%M%S%f")
session_trade_ledger = []
portfolio_snapshot = {}
strategy_matrix_snapshot = []
session_ignored_position_ids = set()
equity_peak = 0.0
equity_mode = "NORMAL"
ai_mode = "STABLE"

SESSION_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kryon_session_state.json")
RUNTIME_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kryon_runtime_state.json")

processed_deals = set()
trade_memory = {}
position_strategy_map = {}
resolved_symbol_map = {}
liquidity_sniper_cycle_symbols = []
titanyx_week_state = {"week": None, "balance": None}
luke_reentry_state = {}
liquidity_sniper_day_state = {
    "day": None,
    "total_trades": 0,
    "total_pnl": 0.0,
    "consecutive_losses": 0,
    "per_symbol": {},
}

last_history_check = datetime.now()

m1_cache = {}
m1_cache_time = {}
m5_cache = {}
m5_cache_time = {}
m15_cache = {}
m15_cache_time = {}
m30_cache = {}
m30_cache_time = {}
h4_cache = {}
h4_cache_time = {}
d1_cache = {}
d1_cache_time = {}
h1_cache = {}
h1_cache_time = {}
broker_time_offset = timedelta(0)
broker_offset_updated_at = 0.0

radar_state = {
    s: {
        "sig": "NEUTRAL",
        "timing": "---",
        "conf": 0,
        "strat": "---",
        "status": "INIT",
        "live_conf": "N/A",
        "action": "---",
    }
    for s in symbols_list
}
decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
debug_state = {
    s: {
        "data": False,
        "signal": "NONE",
        "blocked_by": "INIT",
        "stage": "INIT",
        "final": "NONE",
    }
    for s in symbols_list
}
market_state = {"quality": 0.0, "pause_active": False, "pause_reason": "OK"}

RETCODE_RETRY = {
    mt5.TRADE_RETCODE_REQUOTE,
    mt5.TRADE_RETCODE_PRICE_OFF,
    mt5.TRADE_RETCODE_REJECT,
    mt5.TRADE_RETCODE_INVALID_PRICE,
    mt5.TRADE_RETCODE_PRICE_CHANGED,
    mt5.TRADE_RETCODE_CONNECTION,
}

# ================= STRATEGY REGISTRY =================
STRATEGIES = []


def register_strategy(
    name,
    func,
    enabled=True,
    toggle_arm=True,
    tag=None,
    symbol_family=None,
    execution_mode="AUTO",
    risk_multiplier=1.0,
    min_score=0.45,
    cooldown_sec=60,
    allowed_regimes=None,
    filter_func=None,
    market_filter_func=None,
    sl_atr_multiplier=1.2,
    tp_rr_multiplier=2.0,
    auto_disable=False,
    min_winrate=0.45,
    min_profit_factor=1.0,
    min_eval_trades=STRATEGY_EVAL_MIN_TRADES,
    be_trigger_progress=0.45,
    be_buffer_progress=0.05,
    trail_steps=None,
    session_label="24H",
    multi_tp_eur_min=0.50,
    multi_tp_eur_max=1.50,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.12,
    multi_tp_curve=None,
    multi_tp_tp2_gap_eur=0.35,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=MAX_SPLITS,
    exclusive_symbol=False,
):
    cfg = {
        "name": name,
        "func": func,
        "enabled": enabled,
        "toggle_arm": toggle_arm,
        "tag": tag or name[:8],
        "symbol_family": symbol_family,
        "execution_mode": execution_mode,
        "risk_multiplier": risk_multiplier,
        "min_score": min_score,
        "cooldown_sec": cooldown_sec,
        "allowed_regimes": set(allowed_regimes or []),
        "filter_func": filter_func,
        "market_filter_func": market_filter_func,
        "sl_atr_multiplier": sl_atr_multiplier,
        "tp_rr_multiplier": tp_rr_multiplier,
        "auto_disable": auto_disable,
        "min_winrate": min_winrate,
        "min_profit_factor": min_profit_factor,
        "min_eval_trades": min_eval_trades,
        "be_trigger_progress": be_trigger_progress,
        "be_buffer_progress": be_buffer_progress,
        "trail_steps": list(trail_steps or []),
        "session_label": session_label,
        "multi_tp_eur_min": multi_tp_eur_min,
        "multi_tp_eur_max": multi_tp_eur_max,
        "multi_tp_target_eur": multi_tp_target_eur,
        "multi_tp_first_atr": multi_tp_first_atr,
        "multi_tp_curve": list(multi_tp_curve or [1.0, 1.8, 2.8, 4.0, 5.6, 7.6, 10.0, 13.0]),
        "multi_tp_tp2_gap_eur": multi_tp_tp2_gap_eur,
        "multi_tp_step_gap_eur": multi_tp_step_gap_eur,
        "min_splits": max(1, int(min_splits)),
        "max_splits": max(1, int(max_splits)),
        "exclusive_symbol": bool(exclusive_symbol),
        "disabled_reason": "",
    }
    STRATEGIES.append(cfg)
    strategy_stats.setdefault(
        name,
        {
            "wins": 0,
            "losses": 0,
            "flat": 0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "net_profit": 0.0,
            "closed": 0,
        },
    )
    strategy_runtime.setdefault(
        name,
        {
            "last_signal": "---",
            "last_signal_conf": 0.0,
            "last_signal_symbol": "---",
            "last_signal_time": None,
            "last_result": "---",
            "last_result_profit": 0.0,
            "last_result_time": None,
            "last_event": "INIT",
            "live_block": "INIT",
            "pretrigger": "---",
            "last_tp_plan": "---",
            "probability": 0.0,
            "probability_reason": "---",
            "probability_mode": "OFF",
            "probability_threshold": PROBABILITY_MIN_THRESHOLD,
            "probability_symbol": "---",
            "probability_time": None,
            "session_attempts": 0,
            "session_signals": 0,
            "session_blocked": 0,
            "session_block_counts": {},
            "session_last_block": "---",
            "session_last_block_time": None,
            "session_last_block_key": None,
            "session_last_block_at": None,
            "session_last_signal_key": None,
            "session_last_signal_at": None,
        },
    )


def _unique_symbols(items):
    seen = set()
    ordered = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _ensure_symbol_state(symbol):
    if symbol not in last_signal:
        last_signal[symbol] = "NONE"
    if symbol not in radar_state:
        radar_state[symbol] = {
            "sig": "NEUTRAL",
            "timing": "---",
            "conf": 0,
            "strat": "---",
            "status": "INIT",
            "live_conf": "N/A",
            "action": "---",
        }
    if symbol not in debug_state:
        debug_state[symbol] = {
            "data": False,
            "signal": "NONE",
            "blocked_by": "INIT",
            "stage": "INIT",
            "final": "NONE",
        }


def _family_key_for_symbol(symbol):
    upper_symbol = str(symbol).upper()
    for family, aliases in SYMBOL_ALIASES.items():
        for alias in aliases:
            if alias.upper() in upper_symbol:
                return family
    return str(symbol)


def _symbol_state_keys(symbol):
    keys = [str(symbol)]
    family_key = _family_key_for_symbol(symbol)
    if family_key not in keys:
        keys.append(family_key)
    return keys


def symbol_in_family(symbol, family):
    upper_symbol = str(symbol).upper()
    for alias in SYMBOL_ALIASES.get(family, [family]):
        if alias.upper() in upper_symbol:
            return True
    return False


def get_max_clusters_for_symbol(symbol):
    for family, cap in SYMBOL_CLUSTER_CAPS.items():
        if symbol_in_family(symbol, family):
            return int(cap)
    return MAX_CLUSTERS_PER_SYMBOL


def is_crypto_family(family):
    return str(family).upper() in {str(symbol).upper() for symbol in CRYPTO_SYMBOLS}


def resolve_broker_symbol(symbol):
    cached_symbol = resolved_symbol_map.get(symbol)
    if cached_symbol and mt5.terminal_info():
        cached_info = safe_info(cached_symbol)
        if cached_info:
            return cached_symbol
        resolved_symbol_map.pop(symbol, None)
    elif cached_symbol:
        return cached_symbol

    with mt5_lock:
        if not mt5.terminal_info():
            resolved_symbol_map[symbol] = symbol
            return symbol
        all_symbols = mt5.symbols_get()

    if not all_symbols:
        resolved_symbol_map[symbol] = symbol
        return symbol

    candidates = SYMBOL_ALIASES.get(symbol, [symbol])
    available = [s.name for s in all_symbols]

    for candidate in candidates:
        for name in available:
            if name.upper() == candidate.upper():
                with mt5_lock:
                    mt5.symbol_select(name, True)
                resolved_symbol_map[symbol] = name
                return name

    for candidate in candidates:
        for name in available:
            if candidate.upper() in name.upper():
                with mt5_lock:
                    mt5.symbol_select(name, True)
                resolved_symbol_map[symbol] = name
                return name

    resolved_symbol_map[symbol] = symbol
    return symbol


def get_strategy_config(name):
    for strategy in STRATEGIES:
        if strategy["name"] == name:
            return strategy
    return None


def get_strategy_by_tag(tag):
    tag = TAG_ALIASES.get(tag, tag)
    for strategy in STRATEGIES:
        if strategy["tag"] == tag:
            return strategy
    return None


def extract_leg_index(comment):
    if not comment or "_TP" not in comment:
        return None
    try:
        return int(str(comment).rsplit("_TP", 1)[1])
    except (TypeError, ValueError):
        return None


def extract_strategy_tag(comment):
    if not comment:
        return None
    text = str(comment).strip()
    if not text:
        return None
    if "[" in text:
        text = text.split("[", 1)[0].strip()
    if "_TP" in text:
        text = text.rsplit("_TP", 1)[0]
    elif "_" in text:
        text = text.rsplit("_", 1)[0]
    candidate = TAG_ALIASES.get(text, text)
    if any(strategy.get("tag") == candidate for strategy in STRATEGIES):
        return candidate
    return None


def compute_profit_factor(stats):
    gross_loss = abs(stats.get("gross_loss", 0.0))
    if gross_loss == 0:
        return float("inf") if stats.get("gross_profit", 0.0) > 0 else 0.0
    return stats.get("gross_profit", 0.0) / gross_loss


def _blank_stats_row():
    return {
        "wins": 0,
        "losses": 0,
        "flat": 0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "net_profit": 0.0,
        "closed": 0,
    }


def _blank_strategy_stats():
    return {strategy["name"]: _blank_stats_row() for strategy in STRATEGIES}


def _new_session_runtime_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def _deal_ticket(deal):
    return getattr(deal, "ticket", None) or getattr(deal, "deal", None)


REALIZED_EXIT_DEAL_ENTRIES = {
    entry
    for entry in (
        getattr(mt5, "DEAL_ENTRY_OUT", None),
        getattr(mt5, "DEAL_ENTRY_OUT_BY", None),
        getattr(mt5, "DEAL_ENTRY_INOUT", None),
    )
    if entry is not None
}


def _is_realized_exit_deal(deal):
    entry = getattr(deal, "entry", None)
    if entry not in REALIZED_EXIT_DEAL_ENTRIES:
        return False
    if entry == getattr(mt5, "DEAL_ENTRY_INOUT", None):
        return abs(get_deal_realized_pnl(deal)) > 1e-9
    return True


def get_deal_realized_pnl(deal):
    total = 0.0
    for field in ("profit", "commission", "swap", "fee"):
        total += float(getattr(deal, field, 0.0) or 0.0)
    return total


def format_runtime_timestamp(ts):
    if not ts:
        return "---"
    if isinstance(ts, datetime):
        return ts.strftime("%H:%M:%S")
    return str(ts)


def _blank_strategy_runtime_session():
    return {
        "session_attempts": 0,
        "session_signals": 0,
        "session_entries": 0,
        "session_blocked": 0,
        "session_block_counts": {},
        "session_last_block": "---",
        "session_last_block_time": None,
        "session_last_block_key": None,
        "session_last_block_at": None,
        "session_last_signal_key": None,
        "session_last_signal_at": None,
    }


def _normalize_strategy_event_block(event_text):
    event_upper = str(event_text or "INIT").upper()
    if event_upper.startswith("NO SETUP "):
        return event_upper.replace("NO SETUP ", "")[:24]
    if "NO SETUP" in event_upper:
        return "NO SETUP"
    if "COOLDOWN" in event_upper:
        return "COOLDOWN"
    if "OUT SESSION" in event_upper:
        return "OUT SESSION"
    if "FAST" in event_upper:
        return "FAST"
    if "SPREAD" in event_upper:
        return "SPREAD"
    if "LOW SCORE" in event_upper:
        return "LOW SCORE"
    if "REGIME" in event_upper:
        return "REGIME"
    if "LIVE" in event_upper or "ENTRY" in event_upper:
        return "LIVE"
    if "FAIL" in event_upper:
        return "FAIL"
    if "FILTER" in event_upper:
        return event_upper.replace("FILTER ", "")[:18]
    return event_upper[:18] or "INIT"


def _is_strategy_block_event(event_text, live_block):
    event_upper = str(event_text or "").upper()
    live_upper = str(live_block or "INIT").upper()
    if event_upper.startswith("ENTRY"):
        return False
    if event_upper.startswith("RESULT"):
        return False
    if event_upper == "LIVE":
        return False
    if " EXIT" in event_upper or event_upper.endswith("EXIT"):
        return False
    if live_upper in {"LIVE", "WIN", "LOSS", "BE", "READY", "INIT"}:
        return False
    return True


def _sample_strategy_runtime_event(runtime, prefix, event_key, now, sample_sec=STRATEGY_EVENT_SAMPLE_SEC):
    key_field = f"{prefix}_key"
    at_field = f"{prefix}_at"
    last_key = runtime.get(key_field)
    last_at = runtime.get(at_field)
    if last_key == event_key and isinstance(last_at, datetime):
        if (now - last_at).total_seconds() < sample_sec:
            return False
    runtime[key_field] = event_key
    runtime[at_field] = now
    return True


def _get_runtime_main_block(runtime):
    counts = dict(runtime.get("session_block_counts") or {})
    if not counts:
        return "---"
    last_block = str(runtime.get("session_last_block") or "---")
    return max(counts.items(), key=lambda item: (int(item[1] or 0), item[0] == last_block))[0]


def _blank_portfolio_snapshot():
    if not session_active:
        started_label = "--:-- | MT5 --:--"
    else:
        started_label = f"{session_started_at.strftime('%H:%M')} | MT5 --:--"
    return {
        "session_id": session_runtime_id,
        "balance": 0.0,
        "equity": 0.0,
        "live_pnl": 0.0,
        "open_positions": 0,
        "session_profit": 0.0,
        "session_closed_profit": 0.0,
        "session_open_pnl": 0.0,
        "closed_trades": 0,
        "flat_trades": 0,
        "wins": 0,
        "losses": 0,
        "winrate": 0.0,
        "tracked_results": 0,
        "session_started_at": session_started_at,
        "session_started_at_label": started_label,
    }


def _build_trade_ledger_entry(deal, strategy_cfg, strategy_tag, realized_pnl, deal_time, leg_comment, exit_reason, mapped):
    return {
        "session_id": session_runtime_id,
        "deal_ticket": _deal_ticket(deal),
        "position_id": getattr(deal, "position_id", None) or getattr(deal, "position", None),
        "symbol": getattr(deal, "symbol", ""),
        "strategy_name": strategy_cfg["name"] if strategy_cfg and mapped else None,
        "strategy_tag": strategy_tag if strategy_cfg and mapped else None,
        "mapped": bool(strategy_cfg and mapped),
        "pnl": round(realized_pnl, 2),
        "result": "WIN" if realized_pnl > 0 else ("LOSS" if realized_pnl < 0 else "BE"),
        "closed_at": deal_time,
        "deal_comment": str(getattr(deal, "comment", "") or ""),
        "entry_comment": str(leg_comment or ""),
        "leg_index": extract_leg_index(leg_comment),
        "exit_reason": exit_reason,
    }


def _strategy_status_and_block(strategy, runtime):
    status = "ACTIVE" if strategy.get("enabled") else "OFF"
    is_crypto_branch = is_crypto_family(strategy.get("symbol_family"))
    if is_crypto_branch and not crypto_enabled:
        status = "STANDBY"
    elif strategy["name"].endswith("_SCALP") and not scalping_enabled:
        status = "STANDBY"
    if strategy.get("disabled_reason"):
        status = "LOCKED"

    live_block = runtime.get("live_block", "INIT")
    if status == "STANDBY" and is_crypto_branch and not crypto_enabled:
        live_block = "CRYPTO OFF"
    elif status == "STANDBY":
        live_block = "SCALP OFF"
    elif status == "LOCKED":
        live_block = "LOCKED"
    elif status == "OFF":
        live_block = "OFF"
    return status, live_block


def _collect_strategy_live_state(live_positions=None):
    if not session_active:
        return {
            "strategy_live": {},
            "unmapped_live": {"open_positions": 0, "live_pnl": 0.0, "symbols": []},
            "total_live_pnl": 0.0,
            "total_open_positions": 0,
        }
    positions = live_positions if live_positions is not None else safe_positions()
    strategy_live = {}
    unmapped_live = {"open_positions": 0, "live_pnl": 0.0, "symbols": set()}
    total_live_pnl = 0.0
    total_open_positions = 0

    for p in positions:
        if getattr(p, "magic", None) != MAGIC_ID:
            continue
        total_open_positions += 1
        pnl = float(getattr(p, "profit", 0.0) or 0.0)
        total_live_pnl += pnl
        strategy_tag = position_strategy_map.get(getattr(p, "ticket", None)) or extract_strategy_tag(getattr(p, "comment", ""))
        strategy_cfg = get_strategy_by_tag(strategy_tag) if strategy_tag else None
        if strategy_cfg and symbol_in_family(getattr(p, "symbol", ""), strategy_cfg.get("symbol_family")):
            strategy_state = strategy_live.setdefault(
                strategy_cfg["name"],
                {"open_positions": 0, "live_pnl": 0.0, "symbols": set()},
            )
        else:
            strategy_state = unmapped_live
        strategy_state["open_positions"] += 1
        strategy_state["live_pnl"] += pnl
        strategy_state["symbols"].add(getattr(p, "symbol", ""))

    return {
        "strategy_live": strategy_live,
        "unmapped_live": {
            "open_positions": unmapped_live["open_positions"],
            "live_pnl": round(unmapped_live["live_pnl"], 2),
            "symbols": sorted(sym for sym in unmapped_live["symbols"] if sym),
        },
        "total_live_pnl": round(total_live_pnl, 2),
        "total_open_positions": total_open_positions,
    }


def _rebuild_portfolio_snapshot(acc=None, live_pnl=None):
    global portfolio_snapshot
    live_state = _collect_strategy_live_state()
    if live_pnl is None:
        live_pnl = live_state["total_live_pnl"]
    broker_session_start = get_broker_session_start() if session_active else None
    session_closed_profit = float(session_stats.get("profit", 0.0) or 0.0)
    session_profit = session_closed_profit + float(live_pnl or 0.0)
    wins = int(session_stats.get("wins", 0) or 0)
    losses = int(session_stats.get("losses", 0) or 0)
    tracked_results = wins + losses
    portfolio_snapshot = {
        "session_id": session_runtime_id,
        "balance": round(float(getattr(acc, "balance", 0.0) or 0.0), 2) if acc else 0.0,
        "equity": round(float(getattr(acc, "equity", 0.0) or 0.0), 2) if acc else 0.0,
        "live_pnl": round(float(live_pnl or 0.0), 2),
        "open_positions": int(live_state.get("total_open_positions", 0) or 0),
        "session_profit": round(session_profit, 2),
        "session_closed_profit": round(session_closed_profit, 2),
        "session_open_pnl": round(float(live_pnl or 0.0), 2),
        "closed_trades": int(session_stats.get("trades", 0) or 0),
        "flat_trades": int(session_stats.get("flat", 0) or 0),
        "wins": wins,
        "losses": losses,
        "winrate": round(_get_session_winrate() * 100, 1),
        "tracked_results": tracked_results,
        "session_started_at": session_started_at,
        "session_started_at_label": (
            f"{session_started_at.strftime('%H:%M')} | MT5 {broker_session_start.strftime('%H:%M') if broker_session_start else '--:--'}"
            if session_active
            else "--:-- | MT5 --:--"
        ),
    }


def _rebuild_strategy_matrix_snapshot():
    global strategy_matrix_snapshot
    with strategy_runtime_lock:
        strategy_runtime_snapshot = {name: dict(runtime) for name, runtime in strategy_runtime.items()}
    live_state = _collect_strategy_live_state()
    strategy_live = live_state.get("strategy_live", {})

    latest_session_close = {}
    for entry in session_trade_ledger:
        strategy_name = entry.get("strategy_name")
        if not strategy_name:
            continue
        current = latest_session_close.get(strategy_name)
        if current is None or entry.get("closed_at") > current.get("closed_at"):
            latest_session_close[strategy_name] = entry

    dashboard = []
    for strategy in STRATEGIES:
        stats = dict(strategy_stats.get(strategy["name"], {}))
        runtime = strategy_runtime_snapshot.get(strategy["name"], {})
        wins = int(stats.get("wins", 0) or 0)
        losses = int(stats.get("losses", 0) or 0)
        total = wins + losses
        winrate = (wins / total) * 100 if total > 0 else 0.0
        profit_factor = compute_profit_factor(stats)
        profit_factor_display = "INF" if profit_factor == float("inf") else round(profit_factor, 2)
        status, live_block = _strategy_status_and_block(strategy, runtime)
        last_close = latest_session_close.get(strategy["name"], {})
        live_row = strategy_live.get(strategy["name"], {})
        live_pnl = round(float(live_row.get("live_pnl", 0.0) or 0.0), 2)
        open_positions = int(live_row.get("open_positions", 0) or 0)
        session_closed = int(stats.get("closed", 0) or 0)
        session_net_profit = round(float(stats.get("net_profit", 0.0) or 0.0), 2)
        session_attempts = int(runtime.get("session_attempts", 0) or 0)
        session_signals = int(runtime.get("session_signals", 0) or 0)
        session_entries = int(runtime.get("session_entries", 0) or 0)
        session_blocked = int(runtime.get("session_blocked", 0) or 0)
        session_main_block = _get_runtime_main_block(runtime)
        session_avg_profit = round(session_net_profit / session_closed, 2) if session_closed > 0 else 0.0
        used_today = open_positions > 0 or session_closed > 0
        blocked_today = (not used_today) and (
            session_attempts > 0
            or session_signals > 0
            or session_blocked > 0
            or str(runtime.get("pretrigger", "---") or "---") not in {"---", ""}
        )
        dashboard.append(
            {
                "session_id": session_runtime_id,
                "name": strategy["name"],
                "family": strategy.get("symbol_family", "---"),
                "tag": strategy["tag"],
                "status": status,
                "mode": strategy["execution_mode"],
                "session": strategy.get("session_label", "24H"),
                "risk_multiplier": strategy["risk_multiplier"],
                "sl_atr": strategy.get("sl_atr_multiplier", 1.2),
                "tp_rr": strategy.get("tp_rr_multiplier", 2.0),
                "tp_mode": "MULTI" if strategy["execution_mode"] == "MULTI_ONLY" else ("STD" if strategy["execution_mode"] == "STD_ONLY" else "AUTO"),
                "split_range": f"{strategy.get('min_splits', 1)}-{strategy.get('max_splits', MAX_SPLITS)}" if strategy["execution_mode"] == "MULTI_ONLY" else "1",
                "be_trigger": round(strategy.get("be_trigger_progress", 0.45) * 100),
                "trail": " | ".join([f"{int(t*100)}>{int(l*100)}" for t, l in strategy.get("trail_steps", [])]) if strategy.get("trail_steps") else "---",
                "winrate": round(winrate, 1),
                "profit_factor": profit_factor_display,
                "closed": session_closed,
                "net_profit": session_net_profit,
                "live_pnl": live_pnl,
                "open_positions": open_positions,
                "total_net_profit": round(session_net_profit + live_pnl, 2),
                "session_winrate": round(winrate, 1),
                "session_closed": session_closed,
                "session_net_profit": session_net_profit,
                "session_avg_profit": session_avg_profit,
                "session_attempts": session_attempts,
                "session_signals": session_signals,
                "session_entries": session_entries,
                "session_blocked": session_blocked,
                "session_main_block": session_main_block,
                "used_today": used_today,
                "blocked_today": blocked_today,
                "disabled_reason": strategy.get("disabled_reason", ""),
                "auto_disable": strategy.get("auto_disable", False),
                "last_signal": runtime.get("last_signal", "---"),
                "last_signal_conf": runtime.get("last_signal_conf", 0.0),
                "last_signal_symbol": runtime.get("last_signal_symbol", "---"),
                "last_signal_time": format_runtime_timestamp(runtime.get("last_signal_time")),
                "last_result": runtime.get("last_result", "---"),
                "last_result_profit": runtime.get("last_result_profit", 0.0),
                "last_result_time": format_runtime_timestamp(runtime.get("last_result_time")),
                "last_event": runtime.get("last_event", "INIT"),
                "live_block": live_block,
                "pretrigger": runtime.get("pretrigger", "---"),
                "tp_plan": runtime.get("last_tp_plan", "---"),
                "probability": round(float(runtime.get("probability", 0.0) or 0.0), 1),
                "probability_reason": runtime.get("probability_reason", "---"),
                "probability_mode": runtime.get("probability_mode", "OFF"),
                "probability_threshold": round(float(runtime.get("probability_threshold", PROBABILITY_MIN_THRESHOLD) or PROBABILITY_MIN_THRESHOLD), 1),
                "probability_symbol": runtime.get("probability_symbol", "---"),
                "probability_time": format_runtime_timestamp(runtime.get("probability_time")),
                "last_close_symbol": last_close.get("symbol", "---"),
                "last_close_time": format_runtime_timestamp(last_close.get("closed_at")),
                "last_close_reason": last_close.get("exit_reason", "---"),
            }
        )

    legacy_total = legacy_stats.get("wins", 0) + legacy_stats.get("losses", 0)
    if legacy_stats.get("closed", 0) > 0 or abs(legacy_stats.get("net_profit", 0.0)) > 0:
        legacy_pf = compute_profit_factor(legacy_stats)
        legacy_live = live_state.get("unmapped_live", {})
        dashboard.append(
            {
                "session_id": session_runtime_id,
                "name": "LEGACY",
                "family": "UNMAPPED",
                "tag": "LEGACY",
                "status": "LOCKED",
                "mode": "HISTORY",
                "session": "DAY",
                "risk_multiplier": 0.0,
                "sl_atr": 0.0,
                "tp_rr": 0.0,
                "tp_mode": "N/A",
                "split_range": "-",
                "be_trigger": 0,
                "trail": "---",
                "winrate": round((legacy_stats.get("wins", 0) / legacy_total) * 100, 1) if legacy_total > 0 else 0.0,
                "profit_factor": "INF" if legacy_pf == float("inf") else round(legacy_pf, 2),
                "closed": legacy_stats.get("closed", 0),
                "net_profit": round(legacy_stats.get("net_profit", 0.0), 2),
                "live_pnl": round(float(legacy_live.get("live_pnl", 0.0) or 0.0), 2),
                "open_positions": int(legacy_live.get("open_positions", 0) or 0),
                "total_net_profit": round(
                    float(legacy_stats.get("net_profit", 0.0) or 0.0) + float(legacy_live.get("live_pnl", 0.0) or 0.0),
                    2,
                ),
                "session_winrate": round((legacy_stats.get("wins", 0) / legacy_total) * 100, 1) if legacy_total > 0 else 0.0,
                "session_closed": legacy_stats.get("closed", 0),
                "session_net_profit": round(legacy_stats.get("net_profit", 0.0), 2),
                "session_entries": legacy_stats.get("closed", 0),
                "disabled_reason": "UNMAPPED DEALS",
                "auto_disable": False,
                "last_signal": "---",
                "last_signal_conf": 0.0,
                "last_signal_symbol": "---",
                "last_signal_time": "---",
                "last_result": "---",
                "last_result_profit": 0.0,
                "last_result_time": "---",
                "last_event": "UNMAPPED SESSION DEAL",
                "live_block": "UNMAPPED",
                "pretrigger": "---",
                "tp_plan": "---",
                "last_close_symbol": "---",
                "last_close_time": "---",
                "last_close_reason": "---",
            }
        )

    strategy_matrix_snapshot = dashboard


def _rebuild_visual_snapshots(acc=None, live_pnl=None):
    _rebuild_portfolio_snapshot(acc=acc, live_pnl=live_pnl)
    _rebuild_strategy_matrix_snapshot()



def record_strategy_signal(name, symbol, signal, confidence):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        now = datetime.now()
        runtime["last_signal"] = signal
        runtime["last_signal_conf"] = round(confidence, 1)
        runtime["last_signal_symbol"] = symbol
        runtime["last_signal_time"] = now
        runtime["last_event"] = f"SIGNAL {signal}"
        runtime["live_block"] = "READY"
        runtime["pretrigger"] = "TRIGGER READY"
        if session_active:
            signal_key = f"{symbol}|{signal}"
            if _sample_strategy_runtime_event(runtime, "session_last_signal", signal_key, now, max(8.0, STRATEGY_EVENT_SAMPLE_SEC * 0.5)):
                runtime["session_signals"] = int(runtime.get("session_signals", 0) or 0) + 1
                runtime["session_attempts"] = int(runtime.get("session_attempts", 0) or 0) + 1


def record_strategy_entry(name, symbol, signal):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        now = datetime.now()
        runtime["last_event"] = f"ENTRY {signal}"
        runtime["live_block"] = "LIVE"
        if session_active:
            entry_key = f"{symbol}|{signal}"
            if _sample_strategy_runtime_event(runtime, "session_last_entry", entry_key, now, 2.0):
                runtime["session_entries"] = int(runtime.get("session_entries", 0) or 0) + 1


def set_strategy_pretrigger(name, pretrigger_text_value="---"):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        runtime["pretrigger"] = pretrigger_text_value or "---"


def record_strategy_tp_plan(name, plan_text):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        runtime["last_tp_plan"] = plan_text or "---"


def record_strategy_probability(name, symbol, probability, reason, mode="OFF", threshold=PROBABILITY_MIN_THRESHOLD):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        runtime["probability"] = round(float(probability or 0.0), 1)
        runtime["probability_reason"] = reason or "---"
        runtime["probability_mode"] = mode or "OFF"
        runtime["probability_threshold"] = round(float(threshold or PROBABILITY_MIN_THRESHOLD), 1)
        runtime["probability_symbol"] = symbol or "---"
        runtime["probability_time"] = datetime.now()


def record_strategy_event(name, event_text):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        runtime["last_event"] = event_text
        now = datetime.now()
        live_block = _normalize_strategy_event_block(event_text)
        runtime["live_block"] = live_block
        if session_active and _is_strategy_block_event(event_text, live_block):
            runtime["session_last_block"] = live_block
            runtime["session_last_block_time"] = now
            if _sample_strategy_runtime_event(runtime, "session_last_block", live_block, now):
                runtime["session_blocked"] = int(runtime.get("session_blocked", 0) or 0) + 1
                runtime["session_attempts"] = int(runtime.get("session_attempts", 0) or 0) + 1
                block_counts = runtime.setdefault("session_block_counts", {})
                block_counts[live_block] = int(block_counts.get(live_block, 0) or 0) + 1


def record_strategy_result(name, profit, result_time=None):
    with strategy_runtime_lock:
        runtime = strategy_runtime.setdefault(name, {})
        ts = result_time if isinstance(result_time, datetime) else datetime.now()
        runtime["last_result"] = "WIN" if profit > 0 else ("LOSS" if profit < 0 else "BE")
        runtime["last_result_profit"] = round(profit, 2)
        runtime["last_result_time"] = ts
        runtime["last_event"] = f"RESULT {runtime['last_result']}"
        runtime["live_block"] = runtime["last_result"]
        runtime["pretrigger"] = "---"
        if profit < 0:
            runtime["last_loss_time"] = ts
            runtime["last_loss_profit"] = round(profit, 2)


def evaluate_strategy_health():
    for strategy in STRATEGIES:
        if not strategy.get("auto_disable"):
            continue
        stats = strategy_stats.get(strategy["name"], {})
        closed = stats.get("closed", 0)
        if closed < strategy.get("min_eval_trades", STRATEGY_EVAL_MIN_TRADES):
            continue

        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        total = wins + losses
        winrate = (wins / total) if total > 0 else 0.0
        profit_factor = compute_profit_factor(stats)

        if winrate < strategy.get("min_winrate", 0.45) or profit_factor < strategy.get("min_profit_factor", 1.0):
            if strategy["enabled"]:
                strategy["enabled"] = False
                strategy["disabled_reason"] = f"WR {round(winrate*100,1)}% | PF {round(profit_factor,2)}"
                set_log(f"🛑 STRATEGY DISABLED: {strategy['name']} ({strategy['disabled_reason']})")


def refresh_symbols():
    global symbols, symbols_list
    base_symbols = _unique_symbols([PRIMARY_SYMBOL, *SECONDARY_SYMBOLS])
    crypto_symbols = _unique_symbols(CRYPTO_SYMBOLS) if crypto_enabled else []
    resolved_base_symbols = [resolve_broker_symbol(symbol) for symbol in base_symbols]
    resolved_crypto_symbols = [resolve_broker_symbol(symbol) for symbol in crypto_symbols]
    active_symbols = _unique_symbols([*resolved_base_symbols, *resolved_crypto_symbols])
    if MAX_UNIQUE_SYMBOLS > 0:
        if resolved_crypto_symbols and len(active_symbols) > MAX_UNIQUE_SYMBOLS:
            crypto_keep = _unique_symbols(resolved_crypto_symbols)
            base_slots = max(0, MAX_UNIQUE_SYMBOLS - len(crypto_keep))
            base_keep = _unique_symbols(resolved_base_symbols)[:base_slots]
            active_symbols = _unique_symbols([*base_keep, *crypto_keep])
        else:
            active_symbols = active_symbols[:MAX_UNIQUE_SYMBOLS]
    symbols_list = active_symbols
    symbols = active_symbols
    for symbol in active_symbols:
        _ensure_symbol_state(symbol)
    return symbols


refresh_symbols()


def _default_session_start():
    return datetime.now()


def _load_session_state():
    return _default_session_start(), None, None


def _save_session_state():
    try:
        payload = {
            "session_started_at": session_started_at.isoformat(),
            "session_broker_started_at": session_broker_started_at.isoformat() if session_broker_started_at else None,
            "session_balance_start": session_balance_start,
        }
        with open(SESSION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


def _load_runtime_state():
    try:
        with open(RUNTIME_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        profile = str(data.get("optimizer_profile") or CURRENT_OPTIMIZER_PROFILE).upper()
        if profile not in OPTIMIZER_PROFILES:
            profile = "BALANCED"
        return {
            "optimizer_profile": profile,
            "crypto_enabled": bool(data.get("crypto_enabled", False)),
            "scalping_enabled": bool(data.get("scalping_enabled", False)),
        }
    except Exception:
        return {
            "optimizer_profile": CURRENT_OPTIMIZER_PROFILE,
            "crypto_enabled": False,
            "scalping_enabled": False,
        }


def _save_runtime_state():
    try:
        payload = {
            "optimizer_profile": CURRENT_OPTIMIZER_PROFILE,
            "crypto_enabled": bool(crypto_enabled),
            "scalping_enabled": bool(scalping_enabled),
        }
        with open(RUNTIME_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


session_started_at, session_broker_started_at, session_balance_start = _load_session_state()
_runtime_state = _load_runtime_state()
CURRENT_OPTIMIZER_PROFILE = _runtime_state.get("optimizer_profile", CURRENT_OPTIMIZER_PROFILE)
_boot_profile = OPTIMIZER_PROFILES.get(CURRENT_OPTIMIZER_PROFILE, OPTIMIZER_PROFILES["BALANCED"])
RISK_PER_TRADE_PERCENT = _boot_profile["risk_per_trade_percent"]
MAX_TOTAL_RISK = _boot_profile["max_total_risk"]
MAX_TRADES_PER_CYCLE = _boot_profile["max_trades_per_cycle"]
crypto_enabled = bool(_runtime_state.get("crypto_enabled", False))
scalping_enabled = bool(_runtime_state.get("scalping_enabled", False))

# ==============================================================================
# THREAD-SAFE DATA ACCESS & UTILS
# ==============================================================================
def safe_tick(symbol):
    with mt5_lock:
        return mt5.symbol_info_tick(symbol)


def safe_info(symbol):
    with mt5_lock:
        return mt5.symbol_info(symbol)


def safe_positions():
    with mt5_lock:
        return mt5.positions_get() or []


def points_to_price(symbol, points):
    info = safe_info(symbol)
    if not info:
        return 0
    return points * info.point


def normalize_trade_volume(info, volume):
    if not info:
        return volume
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    min_vol = float(getattr(info, "volume_min", step) or step)
    max_vol = float(getattr(info, "volume_max", volume) or volume)
    normalized = round(float(volume) / step) * step
    normalized = max(min_vol, min(normalized, max_vol))
    step_text = f"{step:.8f}".rstrip("0")
    digits = len(step_text.split(".")[1]) if "." in step_text else 0
    return round(normalized, digits)


def gold_points_to_price(info, points):
    if not info:
        return 0.0
    return float(points) * float(getattr(info, "point", 0.01) or 0.01) * 10.0


def price_to_gold_points(info, distance):
    if not info:
        return 0.0
    unit = gold_points_to_price(info, 1.0)
    if unit <= 0:
        return 0.0
    return float(distance) / unit


def build_fixed_weight_lot_plan(total_lot, info, weights):
    if not weights:
        return []
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    min_vol = float(getattr(info, "volume_min", step) or step)
    scaled_total = np.floor((float(total_lot) / step)) * step
    if scaled_total < min_vol:
        return []
    raw_weights = [max(0.0, float(w or 0.0)) for w in weights]
    weight_sum = sum(raw_weights)
    if weight_sum <= 0:
        return []
    normalized = [w / weight_sum for w in raw_weights]
    raw_lots = [scaled_total * w for w in normalized]
    lot_plan = [np.floor(raw / step) * step for raw in raw_lots]
    remainder_steps = int(round((scaled_total - sum(lot_plan)) / step))
    order = sorted(range(len(raw_lots)), key=lambda idx: ((raw_lots[idx] - lot_plan[idx]), normalized[idx]), reverse=True)
    for idx in order:
        if remainder_steps <= 0:
            break
        lot_plan[idx] += step
        remainder_steps -= 1
    final_lots = [round(lot, 8) for lot in lot_plan if lot + 1e-9 >= min_vol]
    return final_lots


def get_supported_close_fill_modes(info):
    filling_mode = int(getattr(info, "filling_mode", 0) or 0) if info else 0
    modes = []
    if filling_mode & 2:
        modes.append(mt5.ORDER_FILLING_IOC)
    if filling_mode & 1:
        modes.append(mt5.ORDER_FILLING_FOK)
    if not modes:
        modes = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK]
    ordered = []
    for mode in modes:
        if mode not in ordered:
            ordered.append(mode)
    return ordered


def profit_to_price_distance(info, volume, target_profit):
    if not info or volume <= 0 or info.trade_tick_size <= 0 or info.trade_tick_value <= 0:
        return 0.0
    ticks_needed = target_profit / max(info.trade_tick_value * volume, 1e-9)
    return ticks_needed * info.trade_tick_size


def price_distance_to_profit(info, volume, price_distance):
    if not info or volume <= 0 or info.trade_tick_size <= 0 or info.trade_tick_value <= 0:
        return 0.0
    ticks = price_distance / max(info.trade_tick_size, 1e-9)
    return ticks * info.trade_tick_value * volume


def clamp(value, low, high):
    return max(low, min(high, value))


def compute_trade_quality(strategy_cfg, score, confidence, df, spread_price):
    atr = df["atr"].iloc[-1]
    trend_strength = compute_trend_strength(df)
    min_score = strategy_cfg.get("min_score", 0.45)
    conf_floor = max(60.0, min_score * 100)
    conf_ceiling = max(conf_floor + 8.0, 82.0)
    conf_norm = clamp((confidence - conf_floor) / max(conf_ceiling - conf_floor, 1e-6), 0.0, 1.0)
    score_norm = clamp((score - min_score) / max(1.0 - min_score, 1e-6), 0.0, 1.0)
    trend_norm = clamp(trend_strength / 1.6, 0.0, 1.0)
    spread_penalty = min(0.22, spread_price / max(atr, 1e-6))
    quality = clamp((conf_norm * 0.42) + (score_norm * 0.36) + (trend_norm * 0.22) - (spread_penalty * 0.35), 0.0, 1.0)
    return {
        "quality": round(quality, 3),
        "conf_norm": round(conf_norm, 3),
        "score_norm": round(score_norm, 3),
        "trend_norm": round(trend_norm, 3),
        "spread_penalty": round(spread_penalty, 3),
    }


def get_cluster_profile(strategy_cfg, cluster_state):
    strategy_cfg = strategy_cfg or {}
    cluster_state = cluster_state or {}
    is_orb_cluster = str(strategy_cfg.get("name", "")).endswith("_ORB")
    is_gold_cluster = symbol_in_family(strategy_cfg.get("symbol_family", ""), "XAUUSD")
    try:
        quality = float(cluster_state.get("tp_quality", strategy_cfg.get("effective_tp_quality", 0.5)))
    except Exception:
        quality = 0.5
    quality = clamp(quality, 0.0, 1.0)

    base_be = strategy_cfg.get("be_trigger_progress", 0.45)
    base_buffer = strategy_cfg.get("be_buffer_progress", 0.05)
    be_trigger = cluster_state.get("be_trigger_progress")
    if be_trigger is None:
        be_trigger = clamp(base_be + (quality - 0.5) * 0.18, 0.06, 0.82)

    buffer_progress = cluster_state.get("be_buffer_progress")
    if buffer_progress is None:
        buffer_progress = clamp(base_buffer * (0.86 + quality * 0.28), 0.02, 0.25)

    lock_bias = cluster_state.get("cluster_lock_bias", strategy_cfg.get("cluster_lock_bias"))
    if lock_bias is None:
        lock_bias = (0.5 - quality) * (0.20 if is_orb_cluster else 0.14)
    if is_gold_cluster:
        lock_bias += 0.04

    protect_bias = cluster_state.get("protect_bias", strategy_cfg.get("protect_bias"))
    if protect_bias is None:
        protect_bias = (0.5 - quality) * (0.26 if is_orb_cluster else 0.18)
    if is_gold_cluster:
        protect_bias += 0.06

    early_cashout_ratio = cluster_state.get("early_cashout_ratio", strategy_cfg.get("early_cashout_ratio"))
    if early_cashout_ratio is None:
        early_cashout_ratio = clamp(0.10 + (0.5 - quality) * (0.16 if is_orb_cluster else 0.10), 0.04, 0.24)

    orb_cashout_ratio = cluster_state.get("orb_cashout_ratio", strategy_cfg.get("orb_cashout_ratio"))
    if orb_cashout_ratio is None:
        orb_cashout_ratio = clamp(0.18 + (0.5 - quality) * 0.20, 0.08, 0.32)

    hard_lock_ratio = cluster_state.get("hard_lock_ratio", strategy_cfg.get("hard_lock_ratio"))
    if hard_lock_ratio is None:
        hard_lock_ratio = clamp(0.14 + (0.5 - quality) * 0.18, 0.06, 0.30)

    cascade_ratio = cluster_state.get("cascade_ratio", strategy_cfg.get("cascade_ratio"))
    if cascade_ratio is None:
        cascade_ratio = clamp(0.10 + (0.5 - quality) * 0.16, 0.04, 0.26)

    tp1_guard_ratio = cluster_state.get("tp1_guard_ratio", strategy_cfg.get("tp1_guard_ratio"))
    if tp1_guard_ratio is None:
        tp1_guard_ratio = clamp(0.08 + (0.5 - quality) * (0.12 if is_orb_cluster else 0.08), 0.03, 0.18)
    if is_gold_cluster:
        tp1_guard_ratio = clamp(tp1_guard_ratio + 0.02, 0.04, 0.24)

    weak_floor = cluster_state.get("weak_profit_floor", strategy_cfg.get("weak_profit_floor"))
    if weak_floor is None:
        weak_floor = max(0.35, strategy_cfg.get("multi_tp_eur_min", 0.50) * clamp(1.00 + (0.5 - quality) * 0.40, 0.70, 1.20))

    tp3_bridge_ratio = cluster_state.get("tp3_bridge_ratio", strategy_cfg.get("tp3_bridge_ratio"))
    if tp3_bridge_ratio is None:
        tp3_bridge_ratio = clamp(0.46 + (quality - 0.5) * 0.22 + (0.06 if is_gold_cluster else 0.0), 0.34, 0.70)

    runner_peak_keep_ratio = cluster_state.get("runner_peak_keep_ratio", strategy_cfg.get("runner_peak_keep_ratio"))
    if runner_peak_keep_ratio is None:
        runner_peak_keep_ratio = clamp(0.60 - quality * (0.18 if is_orb_cluster else 0.22) + (0.10 if is_gold_cluster else 0.0), 0.36, 0.76)

    runner_realized_lock_ratio = cluster_state.get("runner_realized_lock_ratio", strategy_cfg.get("runner_realized_lock_ratio"))
    if runner_realized_lock_ratio is None:
        runner_realized_lock_ratio = clamp(0.70 + (0.5 - quality) * (0.18 if is_orb_cluster else 0.16) + (0.10 if is_gold_cluster else 0.0), 0.52, 0.96)

    return {
        "quality": quality,
        "be_trigger_progress": be_trigger,
        "be_buffer_progress": buffer_progress,
        "cluster_lock_bias": lock_bias,
        "protect_bias": protect_bias,
        "early_cashout_ratio": early_cashout_ratio,
        "orb_cashout_ratio": orb_cashout_ratio,
        "hard_lock_ratio": hard_lock_ratio,
        "cascade_ratio": cascade_ratio,
        "tp1_guard_ratio": tp1_guard_ratio,
        "weak_profit_floor": round(weak_floor, 2),
        "tp3_bridge_ratio": tp3_bridge_ratio,
        "runner_peak_keep_ratio": runner_peak_keep_ratio,
        "runner_realized_lock_ratio": runner_realized_lock_ratio,
    }


def build_dynamic_multi_tp_prices(symbol, signal, entry_price, info, lot_per_trade, atr, spread_price, strategy_cfg, splits):
    fixed_tp_points = list(strategy_cfg.get("fixed_tp_points") or [])
    point_value_price = float(strategy_cfg.get("point_value_price", info.point if info else 0.0) or 0.0)
    tp_point_buffer = float(strategy_cfg.get("tp_point_buffer", 0.0) or 0.0)
    if fixed_tp_points and point_value_price > 0:
        prices = []
        projected_profits = []
        prev_distance = 0.0
        step_floor = max(info.point * 6, spread_price * 0.35, atr * 0.03)
        for i in range(splits):
            base_points = float(fixed_tp_points[i] if i < len(fixed_tp_points) else fixed_tp_points[-1])
            distance = max((base_points + tp_point_buffer) * point_value_price, prev_distance + step_floor)
            tp_price = entry_price + distance if signal == "BUY" else entry_price - distance
            prices.append(round(tp_price, info.digits))
            projected_profits.append(round(price_distance_to_profit(info, lot_per_trade, distance), 2))
            prev_distance = distance
        return prices, projected_profits

    eur_min = strategy_cfg.get("multi_tp_eur_min", 0.50)
    eur_max = strategy_cfg.get("multi_tp_eur_max", 1.50)
    target_eur = min(max(strategy_cfg.get("effective_multi_tp_target_eur", strategy_cfg.get("multi_tp_target_eur", 1.00)), eur_min), eur_max)
    curve = list(strategy_cfg.get("effective_multi_tp_curve") or strategy_cfg.get("multi_tp_curve") or [1.0, 1.8, 2.8, 4.0, 5.6, 7.6, 10.0, 13.0])
    tp2_gap_eur = max(0.25, strategy_cfg.get("effective_multi_tp_tp2_gap_eur", strategy_cfg.get("multi_tp_tp2_gap_eur", 0.35)))
    step_gap_eur = max(0.10, strategy_cfg.get("effective_multi_tp_step_gap_eur", strategy_cfg.get("multi_tp_step_gap_eur", 0.18)))

    money_floor = profit_to_price_distance(info, lot_per_trade, eur_min)
    money_target = profit_to_price_distance(info, lot_per_trade, target_eur)
    money_cap = profit_to_price_distance(info, lot_per_trade, eur_max)
    tp2_gap_floor = profit_to_price_distance(info, lot_per_trade, tp2_gap_eur)
    step_gap_floor = profit_to_price_distance(info, lot_per_trade, step_gap_eur)
    volatility_floor = atr * strategy_cfg.get("effective_multi_tp_first_atr", strategy_cfg.get("multi_tp_first_atr", 0.12))
    market_floor = max((info.trade_stops_level + 5) * info.point, spread_price * 1.4, info.point * 8, volatility_floor)

    if market_floor <= money_cap:
        first_distance = min(max(market_floor, money_floor, money_target), money_cap)
    else:
        first_distance = max(market_floor, money_floor, money_target)

    step_floor = max(info.point * 6, spread_price * 0.35, atr * 0.03)
    prices = []
    projected_profits = []
    prev_distance = 0.0

    for i in range(splits):
        factor = curve[i] if i < len(curve) else curve[-1] + (i - len(curve) + 1) * 2.5
        distance = max(first_distance * factor, prev_distance + step_floor)
        if i == 1:
            distance = max(distance, first_distance + tp2_gap_floor)
        elif i >= 2:
            progressive_gap = step_gap_floor * (1.0 + min(1.0, i * 0.18))
            distance = max(distance, prev_distance + progressive_gap)
        tp_price = entry_price + distance if signal == "BUY" else entry_price - distance
        prices.append(round(tp_price, info.digits))
        projected_profits.append(round(price_distance_to_profit(info, lot_per_trade, distance), 2))
        prev_distance = distance

    return prices, projected_profits


def build_dynamic_multi_tp_plan(strategy_cfg, score, confidence, df, info, spread_price):
    price = df["close"].iloc[-1]
    atr = df["atr"].iloc[-1]
    quality_metrics = compute_trade_quality(strategy_cfg, score, confidence, df, spread_price)
    quality = quality_metrics["quality"]
    forced_splits = strategy_cfg.get("fixed_tp_splits")
    min_splits = max(4, int(strategy_cfg.get("min_splits", 4)))
    max_splits = max(min_splits, min(MAX_SPLITS, int(strategy_cfg.get("max_splits", MAX_SPLITS))))

    split_span = max_splits - min_splits
    if forced_splits is not None:
        splits = max(1, min(MAX_SPLITS, int(forced_splits)))
    elif split_span <= 0:
        splits = min_splits
    else:
        extra_budget = 0
        if quality >= 0.58:
            extra_budget = min(split_span, 1)
        if quality >= 0.68:
            extra_budget = min(split_span, 2)
        if quality >= 0.78:
            extra_budget = min(split_span, 3)
        if quality >= 0.86:
            extra_budget = split_span
        splits = min_splits + extra_budget

    eur_min = max(0.50, strategy_cfg.get("multi_tp_eur_min", 0.50))
    eur_max = max(eur_min, strategy_cfg.get("multi_tp_eur_max", 1.50))
    base_target = min(max(strategy_cfg.get("multi_tp_target_eur", eur_min), eur_min), eur_max)
    dynamic_target = eur_min + (eur_max - eur_min) * clamp(quality * 1.10, 0.0, 1.0)
    target_floor = min(eur_max, eur_min + max(0.12, (eur_max - eur_min) * 0.12))
    target_eur = min(eur_max, max(target_floor, (base_target * 0.16) + (dynamic_target * 0.84)))

    base_first_atr = strategy_cfg.get("multi_tp_first_atr", 0.12)
    first_atr = base_first_atr * (0.94 + quality * 0.38)
    base_curve = list(strategy_cfg.get("multi_tp_curve") or [1.0, 1.8, 2.8, 4.0, 5.6, 7.6, 10.0, 13.0])
    curve_scale = 0.96 + quality * 0.36
    effective_curve = [round(max(1.0, factor * curve_scale), 3) for factor in base_curve]
    base_tp2_gap = max(0.25, strategy_cfg.get("multi_tp_tp2_gap_eur", 0.35))
    base_step_gap = max(0.10, strategy_cfg.get("multi_tp_step_gap_eur", 0.18))

    plan_cfg = dict(strategy_cfg)
    plan_cfg["effective_multi_tp_target_eur"] = round(target_eur, 2)
    plan_cfg["effective_multi_tp_first_atr"] = round(first_atr, 4)
    plan_cfg["effective_multi_tp_curve"] = effective_curve
    plan_cfg["effective_multi_tp_tp2_gap_eur"] = round(base_tp2_gap * (1.02 + quality * 0.28), 2)
    plan_cfg["effective_multi_tp_step_gap_eur"] = round(base_step_gap * (0.98 + quality * 0.24), 2)
    plan_cfg["effective_tp_quality"] = round(quality, 3)
    plan_cfg["effective_be_trigger"] = round(clamp(strategy_cfg.get("be_trigger_progress", 0.45) + (quality - 0.5) * 0.18, 0.06, 0.82), 4)
    plan_cfg["effective_be_buffer_progress"] = round(clamp(strategy_cfg.get("be_buffer_progress", 0.05) * (0.86 + quality * 0.28), 0.02, 0.25), 4)
    if forced_splits is not None:
        plan_cfg["fixed_tp_splits"] = splits
    if strategy_cfg.get("fixed_tp_points"):
        plan_cfg["fixed_tp_points"] = list(strategy_cfg.get("fixed_tp_points") or [])
    if strategy_cfg.get("point_value_price"):
        plan_cfg["point_value_price"] = float(strategy_cfg.get("point_value_price"))
    if strategy_cfg.get("tp_point_buffer") is not None:
        plan_cfg["tp_point_buffer"] = float(strategy_cfg.get("tp_point_buffer"))
    plan_text = f"{splits}TP | C{int(round(confidence))} | S{int(round(score * 100))} | Q{int(round(quality * 100))} | TP1~{round(target_eur, 2)}€"
    return plan_cfg, splits, plan_text


def _multi_tp_weight_template(strategy_cfg, quality):
    name = str(strategy_cfg.get("name", "") or "")
    family = strategy_cfg.get("symbol_family", "")
    if name.endswith("_ORB"):
        weights = [1.20, 1.15, 1.08, 1.00, 0.82, 0.68, 0.56, 0.46]
        base_scale = 0.90
    elif name.endswith("_TOP"):
        weights = [1.16, 1.12, 1.08, 1.00, 0.88, 0.76, 0.66, 0.58]
        base_scale = 0.93
    elif "TREND" in name or "BREAK_RETEST" in name:
        weights = [1.14, 1.12, 1.08, 1.00, 0.90, 0.80, 0.70, 0.62]
        base_scale = 0.96
    else:
        weights = [1.16, 1.12, 1.08, 1.00, 0.88, 0.76, 0.66, 0.58]
        base_scale = 0.94

    if symbol_in_family(family, "BTCUSD") or symbol_in_family(family, "ETHUSD"):
        base_scale -= 0.03
    elif symbol_in_family(family, "XAUUSD"):
        base_scale -= 0.02

    if quality >= 0.78:
        weights = [round(w * (0.97 if idx < 2 else 1.04), 4) for idx, w in enumerate(weights)]
        base_scale += 0.02
    return weights, clamp(base_scale, 0.80, 1.00)


def build_dynamic_multi_tp_lot_plan(total_lot, info, strategy_cfg, splits, quality):
    if splits <= 0:
        return []
    fixed_weights = list(strategy_cfg.get("lot_weights_override") or [])
    if fixed_weights:
        weighted_plan = build_fixed_weight_lot_plan(total_lot, info, fixed_weights[:splits])
        if weighted_plan:
            return weighted_plan
    step = float(info.volume_step or 0.01)
    min_vol = float(info.volume_min or step)
    weights, base_scale = _multi_tp_weight_template(strategy_cfg, quality)
    split_penalty = max(0, splits - 4) * 0.04
    quality_bonus = max(0.0, quality - 0.70) * 0.12
    risk_scale = clamp(base_scale - split_penalty + quality_bonus, 0.76, 1.00)
    scaled_total = np.floor((total_lot * risk_scale) / step) * step
    if scaled_total < min_vol:
        return []

    use_weights = weights[:splits]
    weight_sum = sum(use_weights) or float(splits)
    normalized = [w / weight_sum for w in use_weights]
    raw_lots = [scaled_total * w for w in normalized]
    lot_plan = [np.floor(raw / step) * step for raw in raw_lots]

    remainder_steps = int(round((scaled_total - sum(lot_plan)) / step))
    order = sorted(range(len(raw_lots)), key=lambda idx: ((raw_lots[idx] - lot_plan[idx]), normalized[idx]), reverse=True)
    for idx in order:
        if remainder_steps <= 0:
            break
        lot_plan[idx] += step
        remainder_steps -= 1

    while lot_plan and lot_plan[-1] + 1e-9 < min_vol:
        lot_plan.pop()
    if not lot_plan:
        return []

    for idx in range(len(lot_plan) - 1, -1, -1):
        if lot_plan[idx] + 1e-9 >= min_vol:
            continue
        if idx == 0:
            return []
        lot_plan[idx - 1] += lot_plan[idx]
        lot_plan[idx] = 0.0

    final_lots = [round(lot, 8) for lot in lot_plan if lot + 1e-9 >= min_vol]
    while final_lots and sum(final_lots) - scaled_total > step * 0.5:
        final_lots[-1] = round(max(0.0, final_lots[-1] - step), 8)
        if final_lots[-1] + 1e-9 < min_vol:
            final_lots.pop()
    return final_lots


def get_tp_management_rules(cluster_state, cluster_profile):
    cluster_state = cluster_state or {}
    cluster_profile = cluster_profile or {}
    try:
        quality = float(cluster_profile.get("quality", cluster_state.get("tp_quality", 0.5)))
    except Exception:
        quality = 0.5
    try:
        confidence = float(cluster_state.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0
    high_conf = bool(cluster_state.get("tp_high_conf"))
    if "tp_high_conf" not in cluster_state:
        high_conf = quality >= 0.62 or confidence >= 80.0
    be_after_legs = int(cluster_state.get("tp_be_after_legs", 2 if high_conf else 1))
    lock_offset = int(cluster_state.get("tp_lock_offset", 2 if high_conf else 1))
    return {
        "high_conf": high_conf,
        "be_after_legs": max(1, be_after_legs),
        "lock_offset": max(1, lock_offset),
        "mode": "HIGH" if high_conf else "LOW",
    }


def get_cluster_tp_ladder_stop(pos_type, entry, buffer_points, info, cluster_state, positive_legs, tp_rules):
    tp_prices = list(cluster_state.get("tp_prices") or [])
    if not tp_prices or positive_legs < tp_rules["be_after_legs"]:
        return None, None

    buffer_price = buffer_points * info.point
    be_price = entry + buffer_price if pos_type == 0 else entry - buffer_price
    if positive_legs <= tp_rules["lock_offset"]:
        return be_price, f"TP{positive_legs}>BE"

    if tp_rules["high_conf"] and positive_legs == (tp_rules["be_after_legs"] + 1) and len(tp_prices) >= 2:
        tp1 = float(tp_prices[0])
        tp2 = float(tp_prices[1])
        bridge_ratio = clamp(float(cluster_state.get("tp3_bridge_ratio", 0.5) or 0.5), 0.25, 0.75)
        ladder_price = tp1 + ((tp2 - tp1) * bridge_ratio)
        if pos_type == 0:
            ladder_price = max(be_price, ladder_price)
        else:
            ladder_price = min(be_price, ladder_price)
        return ladder_price, f"TP{positive_legs}>TP1+"

    lock_leg = min(len(tp_prices), max(1, positive_legs - tp_rules["lock_offset"]))
    ladder_price = float(tp_prices[lock_leg - 1])
    if pos_type == 0:
        ladder_price = max(be_price, ladder_price)
    else:
        ladder_price = min(be_price, ladder_price)
    return ladder_price, f"TP{positive_legs}>TP{lock_leg}"


def hydrate_cluster_tp_state(cluster_key, cluster_positions):
    cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
    if cluster_state.get("tp_prices"):
        return cluster_state

    if not cluster_positions:
        return cluster_state

    tp_by_leg = {}
    fallback_prices = []
    pos_type = cluster_positions[0].type
    for p in cluster_positions:
        tp_value = float(getattr(p, "tp", 0.0) or 0.0)
        if tp_value <= 0:
            continue
        leg_idx = extract_leg_index(getattr(p, "comment", "") or "")
        if leg_idx is not None:
            tp_by_leg[leg_idx] = tp_value
        fallback_prices.append(tp_value)

    if tp_by_leg:
        ordered_tp_prices = [float(tp) for _, tp in sorted(tp_by_leg.items())]
    elif fallback_prices:
        ordered_tp_prices = sorted(fallback_prices, reverse=bool(pos_type == 1))
    else:
        return cluster_state

    cluster_state["tp_prices"] = ordered_tp_prices
    cluster_state["tp_splits"] = max(int(cluster_state.get("tp_splits", 0) or 0), len(ordered_tp_prices))
    return cluster_state


def compute_trend_strength(df):
    ema20 = df["close"].ewm(span=20).mean()
    ema50 = df["close"].ewm(span=50).mean()
    atr = df["atr"].iloc[-1]
    return abs(ema20.iloc[-1] - ema50.iloc[-1]) / max(atr, 1e-6)


def compute_rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def compute_zscore(series, lookback=50):
    rolling_mean = series.rolling(lookback).mean()
    rolling_std = series.rolling(lookback).std().replace(0, np.nan)
    zscore = (series - rolling_mean) / rolling_std
    return zscore.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def compute_adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    tr0 = (high - low).abs()
    tr1 = (high - close.shift(1)).abs()
    tr2 = (low - close.shift(1)).abs()
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean().replace(0, np.nan)

    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / di_sum
    adx = dx.ewm(alpha=1 / period, adjust=False).mean().fillna(0)
    return adx.fillna(0), plus_di.fillna(0), minus_di.fillna(0)


def compute_awesome_oscillator(df, fast=5, slow=34):
    median_price = (df["high"] + df["low"]) / 2.0
    ao = median_price.rolling(fast).mean() - median_price.rolling(slow).mean()
    return ao.fillna(0)


def compute_wma(series, period):
    weights = np.arange(1, period + 1, dtype=float)
    divisor = weights.sum()
    return series.rolling(period).apply(lambda values: float(np.dot(values, weights) / divisor), raw=True)


def compute_macd_hist(series, fast=15, slow=26):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    return ema_fast - ema_slow


def compute_macd(series, fast=12, slow=26, signal_period=9):
    macd_line = compute_macd_hist(series, fast, slow)
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line.fillna(0), signal_line.fillna(0), hist.fillna(0)


def compute_heiken_ashi_smoothed(df, pre_fast=2, pre_slow=6, post_open=3, post_close=2):
    smooth_open = df["open"].ewm(span=pre_fast, adjust=False).mean().ewm(span=pre_slow, adjust=False).mean()
    smooth_high = df["high"].ewm(span=pre_fast, adjust=False).mean().ewm(span=pre_slow, adjust=False).mean()
    smooth_low = df["low"].ewm(span=pre_fast, adjust=False).mean().ewm(span=pre_slow, adjust=False).mean()
    smooth_close = df["close"].ewm(span=pre_fast, adjust=False).mean().ewm(span=pre_slow, adjust=False).mean()

    ha_close_raw = (smooth_open + smooth_high + smooth_low + smooth_close) / 4.0
    ha_open_raw = ha_close_raw.copy()
    if len(ha_open_raw) > 0:
        ha_open_raw.iloc[0] = (smooth_open.iloc[0] + smooth_close.iloc[0]) / 2.0
    for idx in range(1, len(ha_open_raw)):
        ha_open_raw.iloc[idx] = (ha_open_raw.iloc[idx - 1] + ha_close_raw.iloc[idx - 1]) / 2.0

    ha_high_raw = pd.concat([smooth_high, ha_open_raw, ha_close_raw], axis=1).max(axis=1)
    ha_low_raw = pd.concat([smooth_low, ha_open_raw, ha_close_raw], axis=1).min(axis=1)

    ha_open = ha_open_raw.ewm(span=post_open, adjust=False).mean()
    ha_close = ha_close_raw.ewm(span=post_close, adjust=False).mean()
    ha_high = pd.concat([ha_high_raw, ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([ha_low_raw, ha_open, ha_close], axis=1).min(axis=1)
    return ha_open.fillna(0), ha_high.fillna(0), ha_low.fillna(0), ha_close.fillna(0)


def compute_tdi(series, rsi_period=13, mbl_period=34, green_period=2, red_period=7):
    rsi = compute_rsi(series, rsi_period)
    green = rsi.ewm(span=green_period, adjust=False).mean()
    red = green.ewm(span=red_period, adjust=False).mean()
    yellow = rsi.ewm(span=mbl_period, adjust=False).mean()
    return green.fillna(50), red.fillna(50), yellow.fillna(50), rsi.fillna(50)


def count_consecutive_state(series, end_offset=2):
    if len(series) < end_offset:
        return 0, None
    idx = len(series) - end_offset
    current = bool(series.iloc[idx])
    count = 0
    for pos in range(idx, -1, -1):
        if bool(series.iloc[pos]) != current:
            break
        count += 1
    return count, current


def _probability_direction_ok(side, left, right):
    if side == "BUY":
        return left >= right
    return left <= right


def _infer_probability_profile(strategy_cfg):
    name = str(strategy_cfg.get("name", "") or "")
    suite = str(strategy_cfg.get("suite", "") or "")
    if suite == "TITANYX" or name.endswith("_LUKE"):
        return "MEAN_REVERSION"
    if "LIQUIDITY_PULLBACK" in name or "REVERSAL_SWEEP" in name:
        return "STRUCTURE"
    if name.endswith("_ORB") or name.endswith("_BREAK_RETEST") or name.endswith("_KUMO_BREAKOUT"):
        return "BREAKOUT"
    if name.endswith("_VWAP_RECLAIM") or name.endswith("_VWAP_TREND"):
        return "VWAP"
    if name.endswith("_SCALP"):
        return "SCALP"
    if (
        name.endswith("_TREND")
        or name.endswith("_SANTO_GRAAL")
        or name.endswith("_DOUBLE_MACD")
        or name.endswith("_SIDUS")
        or name.endswith("_PURIA")
        or name.endswith("_HEIKEN_TDI")
    ):
        return "TREND"
    return "GENERIC"


def _infer_probability_mode(strategy_cfg):
    name = str(strategy_cfg.get("name", "") or "")
    suite = str(strategy_cfg.get("suite", "") or "")
    if (
        name.endswith("_ORB")
        or name.endswith("_BREAK_RETEST")
        or name.endswith("_TREND")
        or name.endswith("_VWAP_RECLAIM")
        or name.endswith("_VWAP_TREND")
        or name.endswith("_SCALP")
    ):
        return "LIVE"
    if (
        suite == "TITANYX"
        or name.endswith("_LUKE")
        or "LIQUIDITY_PULLBACK" in name
        or "REVERSAL_SWEEP" in name
        or name.endswith("_SANTO_GRAAL")
        or name.endswith("_DOUBLE_MACD")
        or name.endswith("_SIDUS")
        or name.endswith("_PURIA")
        or name.endswith("_HEIKEN_TDI")
        or name.endswith("_KUMO_BREAKOUT")
    ):
        return "SHADOW"
    return "OFF"


def _resolve_cluster_opened_at(cluster_state, live_positions):
    opened_at = cluster_state.get("opened_at")
    if isinstance(opened_at, datetime):
        return opened_at
    try:
        timestamps = []
        for p in live_positions:
            opened_epoch = getattr(p, "time", None) or getattr(p, "time_update", None)
            if opened_epoch:
                timestamps.append(datetime.fromtimestamp(int(opened_epoch)))
        if timestamps:
            return min(timestamps)
    except Exception:
        pass
    return None


def _build_cluster_probability_reason(contributions):
    positive = [label for label, delta in sorted(contributions.items(), key=lambda item: item[1], reverse=True) if delta > 0][:2]
    negative = [label for label, delta in sorted(contributions.items(), key=lambda item: item[1]) if delta < 0][:2]
    if positive and negative:
        return f"{'+'.join(positive)} | {'/'.join(negative)}"
    if positive:
        return "+".join(positive)
    if negative:
        return "/".join(negative)
    return "NEUTRAL"


def evaluate_cluster_probability(symbol, strategy_cfg, cluster_state, live_positions, cluster_meta=None):
    cluster_state = cluster_state or {}
    cluster_meta = cluster_meta or {}
    mode = _infer_probability_mode(strategy_cfg)
    profile = _infer_probability_profile(strategy_cfg)
    result = {
        "mode": mode,
        "profile": profile,
        "probability": 50.0,
        "threshold": PROBABILITY_MIN_THRESHOLD,
        "hard_threshold": PROBABILITY_HARD_THRESHOLD,
        "reason": "NO DATA",
        "should_exit": False,
        "below_count": 0,
    }
    if not PROBABILITY_ENGINE_ENABLED or mode == "OFF" or not live_positions:
        return result

    now = datetime.now()
    last_eval = cluster_state.get("probability_last_eval_at")
    if isinstance(last_eval, datetime) and (now - last_eval).total_seconds() < PROBABILITY_RECHECK_SEC:
        cached = cluster_state.get("probability_cached")
        if isinstance(cached, dict):
            return dict(cached)

    info = safe_info(symbol)
    tick = safe_tick(symbol)
    df_m5 = get_m5_cached(symbol)
    if not info or not tick or df_m5 is None or len(df_m5) < 30:
        result["reason"] = "NO LIVE DATA"
        cluster_state["probability_cached"] = dict(result)
        cluster_state["probability_last_eval_at"] = now
        return result

    pos_type = live_positions[0].type
    side = "BUY" if pos_type == 0 else "SELL"
    price = float(tick.bid if side == "BUY" else tick.ask)
    total_volume = max(sum(float(getattr(p, "volume", 0.0) or 0.0) for p in live_positions), 1e-9)
    avg_entry = sum(float(p.price_open) * float(getattr(p, "volume", 0.0) or 0.0) for p in live_positions) / total_volume

    tp_candidates = [float(getattr(p, "tp", 0.0) or 0.0) for p in live_positions if float(getattr(p, "tp", 0.0) or 0.0) > 0]
    sl_candidates = [float(getattr(p, "sl", 0.0) or 0.0) for p in live_positions if float(getattr(p, "sl", 0.0) or 0.0) > 0]
    if side == "BUY":
        tp_candidates = [tp for tp in tp_candidates if tp > avg_entry]
        nearest_tp = min(tp_candidates) if tp_candidates else avg_entry + float(df_m5["atr"].iloc[-1])
        sl_ref = max(sl_candidates) if sl_candidates else avg_entry - float(df_m5["atr"].iloc[-1])
    else:
        tp_candidates = [tp for tp in tp_candidates if tp < avg_entry]
        nearest_tp = max(tp_candidates) if tp_candidates else avg_entry - float(df_m5["atr"].iloc[-1])
        sl_ref = min(sl_candidates) if sl_candidates else avg_entry + float(df_m5["atr"].iloc[-1])

    atr_m5 = float(df_m5["atr"].iloc[-1] or 0.0)
    if atr_m5 <= 0:
        atr_m5 = max(abs(avg_entry - nearest_tp), info.point * 50)
    ema20_m5 = df_m5["close"].ewm(span=20, adjust=False).mean()
    ema50_m5 = df_m5["close"].ewm(span=50, adjust=False).mean()
    m5_close = float(df_m5["close"].iloc[-1])
    vwap_m5 = float(df_m5["vwap"].iloc[-1])
    ema20_slope = float(ema20_m5.iloc[-1] - ema20_m5.iloc[-4]) if len(ema20_m5) >= 4 else 0.0

    df_m1 = get_m1_cached(symbol)
    df_m15 = get_m15_cached(symbol)
    df_h1 = get_h1_cached(symbol)
    m1_momentum = 0.0
    m1_against_shock = False
    if df_m1 is not None and len(df_m1) >= 6:
        m1_momentum = float(df_m1["close"].iloc[-1] - df_m1["close"].iloc[-4])
        last_range = float(df_m1["high"].iloc[-1] - df_m1["low"].iloc[-1])
        atr_m1 = float(df_m1["atr"].iloc[-1] or atr_m5)
        last_bar_dir = float(df_m1["close"].iloc[-1] - df_m1["open"].iloc[-1])
        m1_against_shock = last_range > (atr_m1 * 1.8) and not _probability_direction_ok(side, last_bar_dir, 0.0)
    else:
        atr_m1 = atr_m5

    m15_align = None
    if df_m15 is not None and len(df_m15) >= 25:
        ema20_m15 = df_m15["close"].ewm(span=20, adjust=False).mean()
        m15_align = _probability_direction_ok(side, float(df_m15["close"].iloc[-1]), float(ema20_m15.iloc[-1]))

    h1_align = None
    if df_h1 is not None and len(df_h1) >= 25:
        ema20_h1 = df_h1["close"].ewm(span=20, adjust=False).mean()
        h1_align = _probability_direction_ok(side, float(df_h1["close"].iloc[-1]), float(ema20_h1.iloc[-1]))

    risk_dist = max(abs(avg_entry - sl_ref), atr_m5 * 0.35, info.point * 25)
    target_dist = max(abs(nearest_tp - avg_entry), risk_dist)
    signed_move = (price - avg_entry) if side == "BUY" else (avg_entry - price)
    tp_progress = signed_move / max(target_dist, 1e-9)
    sl_buffer = ((price - sl_ref) if side == "BUY" else (sl_ref - price)) / max(risk_dist, 1e-9)

    opened_at = _resolve_cluster_opened_at(cluster_state, live_positions)
    age_sec = max(0.0, (now - opened_at).total_seconds()) if opened_at else 0.0
    positive_legs = int(cluster_state.get("positive_legs", 0) or 0)
    realized_profit = float(cluster_state.get("realized_profit", 0.0) or 0.0)
    open_profit = float(cluster_meta.get("open_profit", 0.0) or 0.0)

    base_score = float(cluster_state.get("score", strategy_cfg.get("min_score", 0.45)) or strategy_cfg.get("min_score", 0.45))
    confidence = float(cluster_state.get("confidence", base_score * 100.0) or (base_score * 100.0))
    probability = 50.0
    contributions = {}

    def add(label, value):
        nonlocal probability
        probability += value
        contributions[label] = round(contributions.get(label, 0.0) + value, 2)

    add("SETUP", (base_score - 0.5) * 28.0)
    add("CONF", (confidence - 50.0) * 0.16)

    price_vs_ema20 = _probability_direction_ok(side, price, float(ema20_m5.iloc[-1]))
    ema_stack_ok = _probability_direction_ok(side, float(ema20_m5.iloc[-1]), float(ema50_m5.iloc[-1]))
    vwap_ok = _probability_direction_ok(side, price, vwap_m5)
    slope_ok = _probability_direction_ok(side, ema20_slope, 0.0)
    m1_ok = _probability_direction_ok(side, m1_momentum, 0.0)

    if profile in {"BREAKOUT", "SCALP", "VWAP", "TREND", "GENERIC"}:
        add("EMA20", 7.0 if price_vs_ema20 else -9.0)
        add("STACK", 7.0 if ema_stack_ok else -9.0)
        add("VWAP", 6.0 if vwap_ok else -7.0)
        add("MOMO", 6.0 if m1_ok else -7.0)
        add("SLOPE", 5.0 if slope_ok else -6.0)
    elif profile == "MEAN_REVERSION":
        zscore = compute_zscore(df_m5["close"], 50)
        z_now = float(zscore.iloc[-1])
        z_prev = float(zscore.iloc[-3]) if len(zscore) >= 3 else z_now
        add("REVERT", 7.0 if abs(z_now) < abs(z_prev) else -7.0)
        add("VWAP", 5.0 if abs(price - vwap_m5) < abs(avg_entry - vwap_m5) else -6.0)
        add("MOMO", 5.0 if m1_ok else -5.0)
    elif profile == "STRUCTURE":
        add("EMA20", 5.0 if price_vs_ema20 else -7.0)
        add("H1", 4.0 if h1_align is True else (-4.0 if h1_align is False else 0.0))
        add("MOMO", 5.0 if m1_ok else -6.0)

    if m15_align is True:
        add("M15", 4.0)
    elif m15_align is False:
        add("M15", -4.0)
    if h1_align is True:
        add("H1", 4.0)
    elif h1_align is False:
        add("H1", -5.0)

    if tp_progress >= 0.55:
        add("PROGRESS", 8.0)
    elif tp_progress >= 0.25:
        add("PROGRESS", 4.0)
    elif tp_progress <= -0.25:
        add("PROGRESS", -6.0)

    if sl_buffer <= 0.20:
        add("SLRISK", -20.0)
    elif sl_buffer <= 0.35:
        add("SLRISK", -12.0)
    elif sl_buffer >= 0.75:
        add("SLRISK", 4.0)

    if positive_legs >= 1:
        add("TPLOCK", 5.0)
    if positive_legs >= 2 or realized_profit > 0:
        add("TPLOCK", 7.0)
    if open_profit > 0.0 and positive_legs == 0:
        add("OPENPNL", 2.0)

    if age_sec >= 900 and tp_progress < 0.08:
        add("DECAY", -7.0)
    elif age_sec >= 300 and tp_progress < -0.08:
        add("DECAY", -5.0)
    elif age_sec <= 180 and tp_progress >= 0.12:
        add("DECAY", 2.0)

    if m1_against_shock:
        add("SHOCK", -7.0)
    if abs(price - avg_entry) > atr_m5 * 2.6 and tp_progress < 0:
        add("EXTREME", -6.0)

    probability = clamp(probability, 0.0, 100.0)
    previous_below_count = int(cluster_state.get("probability_below_count", 0) or 0)
    below_count = previous_below_count + 1 if probability <= PROBABILITY_MIN_THRESHOLD else 0
    should_exit = False
    if mode == "LIVE" and age_sec >= PROBABILITY_MIN_AGE_SEC:
        should_exit = probability <= PROBABILITY_HARD_THRESHOLD or below_count >= PROBABILITY_CONFIRM_CYCLES

    result.update(
        {
            "probability": round(probability, 1),
            "reason": _build_cluster_probability_reason(contributions),
            "should_exit": bool(should_exit),
            "below_count": below_count,
            "age_sec": round(age_sec, 1),
            "tp_progress": round(tp_progress, 3),
            "sl_buffer": round(sl_buffer, 3),
            "open_profit": round(open_profit, 2),
        }
    )
    cluster_state["probability_below_count"] = below_count
    cluster_state["probability_last_eval_at"] = now
    cluster_state["probability_cached"] = dict(result)
    return result


def compute_kumo_cloud(df, conv_period=8, base_period=29, span_b_period=34, displacement=29):
    high = df["high"]
    low = df["low"]
    tenkan = (high.rolling(conv_period).max() + low.rolling(conv_period).min()) / 2.0
    kijun = (high.rolling(base_period).max() + low.rolling(base_period).min()) / 2.0
    span_a = ((tenkan + kijun) / 2.0).shift(displacement)
    span_b = ((high.rolling(span_b_period).max() + low.rolling(span_b_period).min()) / 2.0).shift(displacement)
    kumo_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
    kumo_bottom = pd.concat([span_a, span_b], axis=1).min(axis=1)
    return span_a.fillna(0), span_b.fillna(0), kumo_top.fillna(0), kumo_bottom.fillna(0)


def atr_gap_pct(gap, atr):
    return round(max(0.0, gap) / max(atr, 1e-6) * 100)


def pretrigger_text(label, gap, atr):
    return f"{label} {atr_gap_pct(gap, atr)}% ATR"


def should_use_multi_tp(df, symbol):
    atr = df["atr"].iloc[-1]
    price = df["close"].iloc[-1]
    atr_ratio = atr / price
    trend_strength = compute_trend_strength(df)

    if atr_ratio < 0.0008:
        return False
    if trend_strength < (MIN_TREND_STRENGTH * 0.85):
        return False
    return True


# ==============================================================================
# LOGGING UTILS (Event Stream per la UI)
# ==============================================================================
def set_log(msg):
    global latest_log_message
    latest_log_message = msg


def get_latest_log():
    global latest_log_message
    m = latest_log_message
    latest_log_message = ""
    return m


def set_live_log(symbol, action_text):
    global latest_live_analysis_msgs
    msg = f"{datetime.now().strftime('%H:%M:%S')} | {str(symbol).ljust(6)} | {action_text}"
    latest_live_analysis_msgs.append(msg)
    if len(latest_live_analysis_msgs) > MAX_LOG_LINES:
        latest_live_analysis_msgs.pop(0)


def get_latest_live_logs():
    global latest_live_analysis_msgs
    m = list(latest_live_analysis_msgs)
    latest_live_analysis_msgs.clear()
    return m


def push_debug(symbol, stage, reason):
    for key in _symbol_state_keys(symbol):
        _ensure_symbol_state(key)
        debug_state[key]["stage"] = stage
        debug_state[key]["blocked_by"] = reason


def update_radar_state(symbol, sig, conf, strat, status):
    for key in _symbol_state_keys(symbol):
        if key not in radar_state:
            radar_state[key] = {
                "sig": "NEUTRAL",
                "timing": "---",
                "conf": 0,
                "strat": "---",
                "status": "INIT",
                "live_conf": "N/A",
                "action": "---",
            }
        radar_state[key].update({"sig": sig, "conf": conf, "strat": strat, "status": status})


def get_debug_state():
    refresh_symbols()
    return {symbol: state.copy() for symbol, state in debug_state.items() if symbol in symbols}


def get_drawdown():
    global equity_peak
    acc = mt5.account_info()
    if not acc:
        return 0.0
    baseline = max(acc.balance, acc.equity, equity_peak)
    if baseline <= 0:
        return 0.0
    equity_peak = max(equity_peak, acc.equity, acc.balance)
    return max(0.0, (equity_peak - acc.equity) / equity_peak)


def _get_winrate():
    total = performance_memory["wins"] + performance_memory["losses"]
    if total <= 0:
        return 0.0
    return performance_memory["wins"] / total


def _get_session_winrate():
    total = session_stats["wins"] + session_stats["losses"]
    if total <= 0:
        return 0.0
    return session_stats["wins"] / total


def get_live_bot_pnl():
    return round(sum(p.profit for p in safe_positions() if p.magic == MAGIC_ID), 2)


def ensure_session_balance_anchor():
    global session_balance_start
    if session_balance_start is None:
        acc = mt5.account_info()
        if acc:
            session_balance_start = acc.balance
            _save_session_state()


def _seed_session_baseline():
    global session_ignored_position_ids, last_history_check, session_baseline_ready
    with history_state_lock:
        processed_deals.clear()
        session_ignored_position_ids = set()
        if not mt5.terminal_info():
            try:
                mt5.initialize()
            except Exception:
                pass
        if not mt5.terminal_info():
            session_baseline_ready = False
            last_history_check = datetime.now()
            return False

        now = datetime.now()
        broker_now = now + get_live_broker_offset(force=True)
        seed_start = now - timedelta(days=5)
        with mt5_lock:
            seed_deals = mt5.history_deals_get(seed_start, broker_now) or []
        for d in seed_deals:
            if getattr(d, "magic", None) != MAGIC_ID or not _is_realized_exit_deal(d):
                continue
            deal_ticket = _deal_ticket(d)
            if deal_ticket is not None:
                processed_deals.add(deal_ticket)

        for p in safe_positions():
            if getattr(p, "magic", None) != MAGIC_ID:
                continue
            position_id = getattr(p, "ticket", None)
            if position_id is not None:
                session_ignored_position_ids.add(position_id)
            identifier = getattr(p, "identifier", None)
            if identifier is not None:
                session_ignored_position_ids.add(identifier)
        last_history_check = datetime.now()
        session_baseline_ready = True
        return True


def _refresh_runtime_modes():
    global equity_mode, ai_mode
    drawdown = get_drawdown()
    if drawdown < 0.02:
        equity_mode = "AGGRESSIVE"
    elif drawdown < 0.05:
        equity_mode = "NORMAL"
    elif drawdown < 0.08:
        equity_mode = "DEFENSIVE"
    else:
        equity_mode = "RECOVERY"

    winrate = _get_winrate()
    closed = performance_memory["wins"] + performance_memory["losses"]
    if drawdown >= 0.05:
        ai_mode = "DEFENSIVE"
    elif closed >= 5 and winrate >= 0.60:
        ai_mode = "AGGRESSIVE"
    else:
        ai_mode = "STABLE"


def get_decision_data():
    if not session_active:
        acc = mt5.account_info()
        with history_state_lock:
            _rebuild_visual_snapshots(acc=acc, live_pnl=0.0)
            snapshot = dict(portfolio_snapshot or _blank_portfolio_snapshot())
            signals = session_decision_stats["signals"]
            filtered = session_decision_stats["filtered"]
            executed = session_decision_stats["executed"]
        return {
            "ai_mode": ai_mode,
            "equity_mode": equity_mode,
            "optimizer_profile": CURRENT_OPTIMIZER_PROFILE,
            "drawdown": round(get_drawdown() * 100, 2),
            "winrate": 0.0,
            "total_winrate": 0.0,
            "signals": signals,
            "filtered": filtered,
            "executed": executed,
            "equity": float(snapshot.get("equity", 0.0) or 0.0),
            "balance": float(snapshot.get("balance", 0.0) or 0.0),
            "session_profit": 0.0,
            "session_closed_profit": 0.0,
            "session_open_pnl": 0.0,
            "closed_trades": 0,
            "flat_trades": 0,
            "session_started_at_label": snapshot.get("session_started_at_label", "--:-- | MT5 --:--"),
            "tracked_results": 0,
            "session_id": snapshot.get("session_id", session_runtime_id),
        }
    if mt5.terminal_info():
        try:
            with history_state_lock:
                history_stale = (datetime.now() - last_history_check).total_seconds() > 0.75
            if history_stale:
                learn_from_history()
        except Exception:
            pass
    _refresh_runtime_modes()
    acc = mt5.account_info()
    ensure_session_balance_anchor()
    session_open_pnl = get_live_bot_pnl()
    with history_state_lock:
        _rebuild_visual_snapshots(acc=acc, live_pnl=session_open_pnl)
        snapshot = dict(portfolio_snapshot or _blank_portfolio_snapshot())
        total_wr = round(_get_winrate() * 100, 1)
        signals = session_decision_stats["signals"]
        filtered = session_decision_stats["filtered"]
        executed = session_decision_stats["executed"]
    return {
        "ai_mode": ai_mode,
        "equity_mode": equity_mode,
        "optimizer_profile": CURRENT_OPTIMIZER_PROFILE,
        "drawdown": round(get_drawdown() * 100, 2),
        "winrate": float(snapshot.get("winrate", 0.0) or 0.0),
        "total_winrate": total_wr,
        "signals": signals,
        "filtered": filtered,
        "executed": executed,
        "equity": float(snapshot.get("equity", 0.0) or 0.0),
        "balance": float(snapshot.get("balance", 0.0) or 0.0),
        "session_profit": float(snapshot.get("session_profit", 0.0) or 0.0),
        "session_closed_profit": float(snapshot.get("session_closed_profit", 0.0) or 0.0),
        "session_open_pnl": float(snapshot.get("session_open_pnl", 0.0) or 0.0),
        "closed_trades": int(snapshot.get("closed_trades", 0) or 0),
        "flat_trades": int(snapshot.get("flat_trades", 0) or 0),
        "session_started_at_label": snapshot.get("session_started_at_label", "--:-- | MT5 --:--"),
        "tracked_results": int(snapshot.get("tracked_results", 0) or 0),
        "session_id": snapshot.get("session_id", session_runtime_id),
    }


def reset_session_tracking():
    global session_stats, session_decision_stats, decision_stats, session_started_at, session_broker_started_at, session_balance_start
    global last_history_check, performance_memory, legacy_stats, asset_performance
    global session_runtime_id, session_trade_ledger, portfolio_snapshot, strategy_matrix_snapshot, session_ignored_position_ids, session_active, session_baseline_ready
    with history_state_lock:
        session_active = False
        session_baseline_ready = False
        session_started_at = datetime.now()
        session_broker_started_at = session_started_at + get_live_broker_offset(force=True)
        session_runtime_id = _new_session_runtime_id()
        session_stats = {"profit": 0.0, "trades": 0, "wins": 0, "losses": 0, "flat": 0}
        session_decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
        decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
        performance_memory = {"wins": 0, "losses": 0}
        legacy_stats = {"wins": 0, "losses": 0, "flat": 0, "gross_profit": 0.0, "gross_loss": 0.0, "net_profit": 0.0, "closed": 0}
        asset_performance = {}
        session_trade_ledger = []
        portfolio_snapshot = _blank_portfolio_snapshot()
        strategy_matrix_snapshot = []
        session_ignored_position_ids = set()
        for strategy_name in list(strategy_stats.keys()):
            strategy_stats[strategy_name] = _blank_stats_row()
        for runtime in strategy_runtime.values():
            runtime["last_signal"] = "---"
            runtime["last_signal_conf"] = 0.0
            runtime["last_signal_symbol"] = "---"
            runtime["last_signal_time"] = None
            runtime["last_result"] = "---"
            runtime["last_result_profit"] = 0.0
            runtime["last_result_time"] = None
            runtime["last_event"] = "INIT"
            runtime["live_block"] = "INIT"
            runtime["last_tp_plan"] = "---"
            runtime["pretrigger"] = "---"
            runtime["probability"] = 0.0
            runtime["probability_reason"] = "---"
            runtime["probability_mode"] = "OFF"
            runtime["probability_threshold"] = PROBABILITY_MIN_THRESHOLD
            runtime["probability_symbol"] = "---"
            runtime["probability_time"] = None
            runtime.update(_blank_strategy_runtime_session())
        processed_deals.clear()
        trade_memory.clear()
        position_strategy_map.clear()
        last_history_check = session_started_at
        acc = mt5.account_info()
        session_balance_start = acc.balance if acc else None
        _save_session_state()
        _rebuild_visual_snapshots(acc=acc, live_pnl=0.0)


def start_session_tracking():
    global session_active
    reset_session_tracking()
    with history_state_lock:
        session_active = True
        _rebuild_visual_snapshots(acc=mt5.account_info(), live_pnl=0.0)
    _seed_session_baseline()


def set_optimizer_profile(profile_name):
    global CURRENT_OPTIMIZER_PROFILE, RISK_PER_TRADE_PERCENT, MAX_TOTAL_RISK, MAX_TRADES_PER_CYCLE
    profile_key = str(profile_name).upper()
    profile = OPTIMIZER_PROFILES.get(profile_key)
    if not profile:
        return False
    CURRENT_OPTIMIZER_PROFILE = profile_key
    RISK_PER_TRADE_PERCENT = profile["risk_per_trade_percent"]
    MAX_TOTAL_RISK = profile["max_total_risk"]
    MAX_TRADES_PER_CYCLE = profile["max_trades_per_cycle"]
    _save_runtime_state()
    set_log(f"⚙️ OPTIMIZER {profile_key} | risk {RISK_PER_TRADE_PERCENT}% | max risk {round(MAX_TOTAL_RISK*100,1)}% | cycle {MAX_TRADES_PER_CYCLE}")
    return True


def get_optimizer_profile():
    return {
        "name": CURRENT_OPTIMIZER_PROFILE,
        "risk_per_trade_percent": RISK_PER_TRADE_PERCENT,
        "max_total_risk": MAX_TOTAL_RISK,
        "max_trades_per_cycle": MAX_TRADES_PER_CYCLE,
    }


def get_strategy_dashboard():
    if not session_active:
        with history_state_lock:
            _rebuild_strategy_matrix_snapshot()
            return [dict(row) for row in strategy_matrix_snapshot]
    if mt5.terminal_info():
        try:
            with history_state_lock:
                history_stale = (datetime.now() - last_history_check).total_seconds() > 0.75
            if history_stale:
                learn_from_history()
        except Exception:
            pass
    with history_state_lock:
        _rebuild_strategy_matrix_snapshot()
        return [dict(row) for row in strategy_matrix_snapshot]


def toggle_crypto(state):
    global crypto_enabled
    crypto_enabled = bool(state)
    armed = []
    for strategy in STRATEGIES:
        if not is_crypto_family(strategy.get("symbol_family")):
            continue
        if not strategy.get("toggle_arm", True):
            continue
        strategy["enabled"] = crypto_enabled
        if crypto_enabled:
            strategy["disabled_reason"] = ""
            armed.append(strategy["name"])
    refresh_symbols()
    if crypto_enabled and CRYPTO_SYMBOLS:
        armed_text = ", ".join(armed) if armed else "NO ACTIVE CRYPTO BRANCHES"
        set_log(f"🌐 CRYPTO ENABLED: {armed_text}")
    elif crypto_enabled:
        set_log("🌐 CRYPTO ENABLED: nessun simbolo crypto configurato")
    else:
        set_log("🌐 CRYPTO DISABLED")
    _save_runtime_state()


def toggle_scalping(state):
    global scalping_enabled
    scalping_enabled = bool(state)
    armed = []
    for strategy in STRATEGIES:
        if not str(strategy.get("name", "")).endswith("_SCALP"):
            continue
        if not strategy.get("toggle_arm", True):
            continue
        strategy["enabled"] = scalping_enabled
        if scalping_enabled:
            strategy["disabled_reason"] = ""
            armed.append(strategy["name"])
    if scalping_enabled:
        armed_text = ", ".join(armed) if armed else "NO ACTIVE SCALP BRANCHES"
        set_log(f"⚡ SCALPING ENABLED: {armed_text}")
    else:
        set_log("⚡ SCALPING DISABLED")
    _save_runtime_state()


# ==============================================================================
# MANUAL OVERRIDES
# ==============================================================================
def close_only_profit():
    closed = 0
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        if p.profit > 0:
            tick = safe_tick(p.symbol)
            if tick:
                with mt5_lock:
                    mt5.order_send(
                        {
                            "action": mt5.TRADE_ACTION_DEAL,
                            "symbol": p.symbol,
                            "volume": p.volume,
                            "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY,
                            "position": p.ticket,
                            "price": tick.bid if p.type == 0 else tick.ask,
                            "magic": MAGIC_ID,
                        }
                    )
                closed += 1
    if closed > 0:
        set_log(f"💰 CLOSED PROFIT ONLY: {closed} trades")


def force_break_even_plus(buffer_ratio=0.2):
    adjusted = 0
    positions = safe_positions()
    sg_exit_logged = set()
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        if p.profit <= 0:
            continue
        info = safe_info(p.symbol)
        tick = safe_tick(p.symbol)
        if not info or not tick:
            continue
        if p.sl == 0:
            continue

        current_price = tick.bid if p.type == 0 else tick.ask
        risk = abs(p.price_open - p.sl)
        if risk <= 0:
            continue

        buffer = risk * buffer_ratio
        if p.type == 0:
            if current_price <= p.price_open:
                continue
            new_sl = min(p.price_open + buffer, current_price - info.point * 10)
            if new_sl > p.sl:
                with mt5_lock:
                    mt5.order_send(
                        {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "symbol": p.symbol,
                            "position": p.ticket,
                            "sl": round(new_sl, info.digits),
                            "tp": p.tp,
                        }
                    )
                adjusted += 1
        else:
            if current_price >= p.price_open:
                continue
            new_sl = max(p.price_open - buffer, current_price + info.point * 10)
            if p.sl == 0 or new_sl < p.sl:
                with mt5_lock:
                    mt5.order_send(
                        {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "symbol": p.symbol,
                            "position": p.ticket,
                            "sl": round(new_sl, info.digits),
                            "tp": p.tp,
                        }
                    )
                adjusted += 1

    set_log(f"🛡️ BE+ APPLIED: {adjusted} profitable trades protected")


def close_all_now():
    set_log("⚠️ CHIUSURA TOTALE...")
    positions = safe_positions()
    for p in positions:
        if p.magic == MAGIC_ID:
            close_position(p)


def update_position_sltp(p, info, new_sl):
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": p.symbol,
        "position": p.ticket,
        "sl": round(new_sl, info.digits),
        "tp": p.tp,
    }
    with mt5_lock:
        res = mt5.order_send(request)
    retcode = getattr(res, "retcode", None) if res is not None else None
    ok_codes = {
        mt5.TRADE_RETCODE_DONE,
        getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", -1),
        getattr(mt5, "TRADE_RETCODE_PLACED", -1),
    }
    return retcode in ok_codes, retcode


def update_position_sltp_levels(p, info, new_sl=None, new_tp=None):
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": p.symbol,
        "position": p.ticket,
        "sl": round(float(new_sl if new_sl is not None else p.sl), info.digits) if (new_sl is not None or p.sl) else 0.0,
        "tp": round(float(new_tp if new_tp is not None else p.tp), info.digits) if (new_tp is not None or p.tp) else 0.0,
    }
    with mt5_lock:
        res = mt5.order_send(request)
    retcode = getattr(res, "retcode", None) if res is not None else None
    ok_codes = {
        mt5.TRADE_RETCODE_DONE,
        getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", -1),
        getattr(mt5, "TRADE_RETCODE_PLACED", -1),
    }
    return retcode in ok_codes, retcode


def get_strategy_cluster_positions(symbol, strategy_tag):
    positions = []
    for p in safe_positions():
        if p.magic != MAGIC_ID or p.symbol != symbol:
            continue
        p_tag = position_strategy_map.get(p.ticket) or extract_strategy_tag(getattr(p, "comment", ""))
        if p_tag == strategy_tag:
            positions.append(p)
    return positions


def close_position(p):
    deviation = BASE_DEVIATION
    info = safe_info(p.symbol)
    fill_modes = get_supported_close_fill_modes(info)
    last_retcode = None
    last_mode = None
    for _ in range(MAX_RETRIES):
        tick = safe_tick(p.symbol)
        if not tick:
            time.sleep(RETRY_DELAY)
            deviation = min(MAX_DEVIATION, deviation + 10)
            continue
        for fill_mode in fill_modes:
            last_mode = fill_mode
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": p.symbol,
                "volume": normalize_trade_volume(info, p.volume),
                "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY,
                "position": p.ticket,
                "price": round((tick.bid if p.type == 0 else tick.ask), info.digits) if info else (tick.bid if p.type == 0 else tick.ask),
                "magic": MAGIC_ID,
                "deviation": deviation,
                "type_filling": fill_mode,
            }
            with mt5_lock:
                res = mt5.order_send(request)
            if res is not None:
                last_retcode = getattr(res, "retcode", None)
                if res.retcode in {mt5.TRADE_RETCODE_DONE, getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", -1), getattr(mt5, "TRADE_RETCODE_PLACED", -1)}:
                    return True
        time.sleep(RETRY_DELAY)
        deviation = min(MAX_DEVIATION, deviation + 10)
    set_log(f"❌ CLOSE FAIL: {p.symbol} rc={last_retcode} fill={last_mode}")
    return False


def close_strategy_cluster(symbol, strategy_tag):
    closed = 0
    remaining = []
    for _ in range(3):
        positions = get_strategy_cluster_positions(symbol, strategy_tag)
        if not positions:
            return closed
        pass_closed = 0
        for p in positions:
            if close_position(p):
                closed += 1
                pass_closed += 1
        remaining = get_strategy_cluster_positions(symbol, strategy_tag)
        if not remaining or pass_closed == 0:
            break
        time.sleep(RETRY_DELAY)
    if remaining:
        set_log(f"⚠️ CLUSTER PARTIAL CLOSE: {symbol} {strategy_tag} remain={len(remaining)}")
    return closed


# ==============================================================================
# CORE DATA & REGIME
# ==============================================================================
def get_data(symbol, timeframe, n_bars=300):
    broker_symbol = resolve_broker_symbol(symbol)
    with mt5_lock:
        if broker_symbol:
            mt5.symbol_select(broker_symbol, True)
        rates = mt5.copy_rates_from_pos(broker_symbol or symbol, timeframe, 0, n_bars)
        if (rates is None or len(rates) < 50) and broker_symbol and broker_symbol != symbol:
            rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
    if rates is None or len(rates) < 50:
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df["vol_price"] = df["close"] * df["tick_volume"]
    df["date"] = df["time"].dt.date
    df["cum_vol"] = df.groupby("date")["tick_volume"].cumsum()
    df["cum_vol_price"] = df.groupby("date")["vol_price"].cumsum()
    if df["cum_vol"].iloc[-1] == 0:
        return None
    df["vwap"] = df["cum_vol_price"] / df["cum_vol"]
    df.drop(columns=["vol_price", "date"], inplace=True)
    df["tr0"] = abs(df["high"] - df["low"])
    df["tr1"] = abs(df["high"] - df["close"].shift(1))
    df["tr2"] = abs(df["low"] - df["close"].shift(1))
    df["tr"] = df[["tr0", "tr1", "tr2"]].max(axis=1)
    df["atr"] = df["tr"].rolling(14).mean()
    return df.dropna()


def get_m1_cached(symbol):
    global m1_cache, m1_cache_time
    if symbol not in m1_cache or time.time() - m1_cache_time.get(symbol, 0) > 2:
        m1_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M1, 120)
        m1_cache_time[symbol] = time.time()
    return m1_cache[symbol]


def get_m5_cached(symbol):
    global m5_cache, m5_cache_time
    if symbol not in m5_cache or time.time() - m5_cache_time.get(symbol, 0) > 12:
        m5_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M5, 220)
        m5_cache_time[symbol] = time.time()
    return m5_cache[symbol]


def get_m15_cached(symbol):
    global m15_cache, m15_cache_time
    if symbol not in m15_cache or time.time() - m15_cache_time.get(symbol, 0) > 60:
        m15_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M15, 100)
        m15_cache_time[symbol] = time.time()
    return m15_cache[symbol]


def get_m30_cached(symbol):
    global m30_cache, m30_cache_time
    if symbol not in m30_cache or time.time() - m30_cache_time.get(symbol, 0) > 90:
        m30_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M30, 220)
        m30_cache_time[symbol] = time.time()
    return m30_cache[symbol]


def get_h4_cached(symbol):
    global h4_cache, h4_cache_time
    if symbol not in h4_cache or time.time() - h4_cache_time.get(symbol, 0) > 120:
        h4_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_H4, 180)
        h4_cache_time[symbol] = time.time()
    return h4_cache[symbol]


def get_d1_cached(symbol):
    global d1_cache, d1_cache_time
    if symbol not in d1_cache or time.time() - d1_cache_time.get(symbol, 0) > 600:
        d1_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_D1, 180)
        d1_cache_time[symbol] = time.time()
    return d1_cache[symbol]


def get_h1_cached(symbol):
    global h1_cache, h1_cache_time
    if symbol not in h1_cache or time.time() - h1_cache_time.get(symbol, 0) > 120:
        h1_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_H1, 220)
        h1_cache_time[symbol] = time.time()
    return h1_cache[symbol]


def _prime_symbol_scan_context(symbol, eligible_strategies):
    names = {str(cfg.get("name", "") or "") for cfg in (eligible_strategies or [])}
    filter_names = {getattr(cfg.get("filter_func"), "__name__", "") for cfg in (eligible_strategies or [])}

    # M5 is the common base for all branches.
    get_m5_cached(symbol)

    need_m1 = (
        any(name.endswith("_SCALP") or name.endswith("_ORB") or name == "XAU_LUKE" for name in names)
        or "scalping_filter" in filter_names
        or "luke_filter" in filter_names
        or any("LIQUIDITY_PULLBACK" in name or "REVERSAL_SWEEP" in name for name in names)
    )
    need_m15 = (
        any(name.endswith("_ORB") or name.endswith("_KUMO_BREAKOUT") for name in names)
        or any(name.endswith("_TITANYX") for name in names)
    )
    need_m30 = any(name.endswith("_SIDUS") or name.endswith("_PURIA") for name in names)
    need_h1 = (
        any(
            name.endswith("_SIDUS")
            or name.endswith("_PURIA")
            or name.endswith("_KUMO_BREAKOUT")
            or name.endswith("_HEIKEN_TDI")
            or name.endswith("_TITANYX")
            or "LIQUIDITY_PULLBACK" in name
            or "REVERSAL_SWEEP" in name
            for name in names
        )
    )
    need_h4 = (
        any(
            name.endswith("_SANTO_GRAAL")
            or name.endswith("_SIDUS")
            or name.endswith("_KUMO_BREAKOUT")
            or name.endswith("_DOUBLE_MACD")
            or name.endswith("_HEIKEN_TDI")
            or "LIQUIDITY_PULLBACK" in name
            or "REVERSAL_SWEEP" in name
            for name in names
        )
    )
    need_d1 = any(
        name.endswith("_SANTO_GRAAL")
        or name.endswith("_DOUBLE_MACD")
        or name.endswith("_HEIKEN_TDI")
        or name.endswith("_PURIA")
        for name in names
    )

    if need_m1:
        get_m1_cached(symbol)
    if need_m15:
        get_m15_cached(symbol)
    if need_m30:
        get_m30_cached(symbol)
    if need_h1:
        get_h1_cached(symbol)
    if need_h4:
        get_h4_cached(symbol)
    if need_d1:
        get_d1_cached(symbol)

    return True


def _run_strategy_eval(strategy, df, symbol):
    try:
        return {"strategy": strategy, "result": strategy["func"](df, symbol), "error": None}
    except Exception as exc:
        return {"strategy": strategy, "result": None, "error": exc}


def get_broker_offset_from_df(df):
    if df is None or len(df) == 0:
        return timedelta(0)
    broker_now = df["time"].iloc[-1].to_pydatetime().replace(second=0, microsecond=0)
    local_now = datetime.now().replace(second=0, microsecond=0)
    delta_minutes = int(round((broker_now - local_now).total_seconds() / 60.0))
    return timedelta(minutes=delta_minutes)


def broker_window_from_local_df(df, start_hour, start_minute, end_hour, end_minute):
    local_now = datetime.now().replace(second=0, microsecond=0)
    offset = get_broker_offset_from_df(df)
    start_local = local_now.replace(hour=start_hour, minute=start_minute)
    end_local = local_now.replace(hour=end_hour, minute=end_minute)
    if end_local <= start_local:
        end_local += timedelta(days=1)
    return start_local + offset, end_local + offset


def get_live_broker_offset(force=False):
    global broker_time_offset, broker_offset_updated_at
    if not force and broker_offset_updated_at and (time.time() - broker_offset_updated_at) < 60:
        return broker_time_offset

    for base_symbol in ["XAUUSD", "BTCUSD", "ETHUSD", "USTECH", "US500", "GER40", "JPN225"]:
        broker_symbol = resolve_broker_symbol(base_symbol)
        with mt5_lock:
            rates = mt5.copy_rates_from_pos(broker_symbol or base_symbol, mt5.TIMEFRAME_M1, 0, 3)
        if rates is None or len(rates) == 0:
            continue
        broker_now = datetime.utcfromtimestamp(int(rates[-1]["time"])).replace(second=0, microsecond=0)
        local_now = datetime.now().replace(second=0, microsecond=0)
        delta_minutes = int(round((broker_now - local_now).total_seconds() / 60.0))
        broker_time_offset = timedelta(minutes=delta_minutes)
        broker_offset_updated_at = time.time()
        return broker_time_offset
    return broker_time_offset


def get_broker_session_start(force=False):
    global session_broker_started_at
    if force:
        session_broker_started_at = session_started_at + get_live_broker_offset(force=force)
        _save_session_state()
        return session_broker_started_at
    if session_broker_started_at is None:
        if not mt5.terminal_info():
            return session_started_at
        session_broker_started_at = session_started_at + get_live_broker_offset()
        _save_session_state()
        return session_broker_started_at
    if mt5.terminal_info():
        live_offset = get_live_broker_offset()
        stored_offset = session_broker_started_at - session_started_at
        if abs((stored_offset - live_offset).total_seconds()) > 1800:
            session_broker_started_at = session_started_at + live_offset
            _save_session_state()
    return session_broker_started_at


def get_broker_now():
    return datetime.now() + get_live_broker_offset()


def get_broker_hour():
    return get_broker_now().hour


def detect_market_regime(df):
    if df is None or len(df) < 50:
        return "UNKNOWN"
    atr = df["atr"].iloc[-1]
    price = df["close"]
    ema20 = price.ewm(span=20).mean()
    ema50 = price.ewm(span=50).mean()
    trend_strength = abs(ema20.iloc[-1] - ema50.iloc[-1]) / max(atr, 1e-6)
    if trend_strength > 0.8:
        return "TREND"
    return "RANGE"


# ==============================================================================
# FILTERS & RISK
# ==============================================================================
def final_filter(symbol, df, score):
    atr = df["atr"].iloc[-1]
    price = df["close"].iloc[-1]
    momentum = abs(df["close"].iloc[-1] - df["close"].iloc[-5])
    vol = atr / price
    attack = is_attack_runtime()
    is_crypto = any(symbol_in_family(symbol, family) for family in ["BTCUSD", "ETHUSD"])
    broker_hour = get_broker_hour()
    late_gold = symbol_in_family(symbol, "XAUUSD") and (broker_hour >= 20 or broker_hour <= 3)
    min_score = 0.34 if attack else 0.38
    min_momentum = atr * (0.04 if attack else 0.07)
    min_vol = 0.00022 if attack else 0.00035
    max_spread = atr * (0.18 if attack else 0.14)
    if is_crypto:
        min_score = 0.30 if attack else 0.34
        min_momentum = atr * (0.03 if attack else 0.05)
        min_vol = 0.00012 if attack else 0.00020
        max_spread = atr * (0.28 if attack else 0.22)
    elif late_gold:
        min_score = 0.33 if attack else 0.36
        min_momentum = atr * (0.04 if attack else 0.06)
        min_vol = 0.00018 if attack else 0.00028
        max_spread = atr * (0.24 if attack else 0.18)
    if STRESS_TEST_ACTIVE:
        min_score = max(0.28, min_score - 0.03)
        min_momentum *= 0.88
        min_vol *= 0.82
        max_spread *= 1.18
    if score < min_score:
        return False, f"LOW SCORE ({round(score,2)})"
    if momentum < min_momentum:
        return False, "WEAK MOMENTUM"
    if vol < min_vol:
        return False, "ULTRA LOW VOL"
    tick = safe_tick(symbol)
    if tick and (tick.ask - tick.bid) > max_spread:
        return False, "HIGH SPREAD"
    return True, "OK"


def swing_filter(symbol, signal, score, market_df, execution_df, strategy_cfg):
    working_df = execution_df if execution_df is not None else market_df
    if working_df is None or len(working_df) < 40:
        return False, "NO TF DATA"

    tick = safe_tick(symbol)
    if not tick:
        return False, "NO TICK"

    atr = float(working_df["atr"].iloc[-2] or 0.0)
    if atr <= 0:
        return False, "NO ATR"

    close_price = float(working_df["close"].iloc[-2])
    momentum = abs(float(working_df["close"].iloc[-2] - working_df["close"].iloc[-6]))
    vol = atr / max(close_price, 1e-6)
    attack = is_attack_runtime()
    is_crypto = is_crypto_family(symbol)

    min_score = 0.36 if attack else 0.40
    min_momentum = atr * (0.025 if is_crypto else 0.032)
    min_vol = 0.00010 if is_crypto else 0.00016
    max_spread = atr * (0.28 if is_crypto else 0.18)
    if STRESS_TEST_ACTIVE:
        min_score = max(0.30, min_score - 0.04)
        min_momentum *= 0.84
        min_vol *= 0.78
        max_spread *= 1.16

    if score < min_score:
        return False, f"LOW SCORE ({round(score,2)})"
    if momentum < min_momentum:
        return False, "WEAK SWING"
    if vol < min_vol:
        return False, "LOW SWING VOL"
    if (tick.ask - tick.bid) > max_spread:
        return False, "HIGH SPREAD"
    return True, "OK"


def allow_trading(symbol):
    hour = get_broker_hour()
    if symbol_in_family(symbol, "XAUUSD"):
        return 0 <= hour <= 23
    if symbol_in_family(symbol, "BTCUSD") or symbol_in_family(symbol, "ETHUSD"):
        return 0 <= hour <= 23
    if symbol_in_family(symbol, "JPN225"):
        return 1 <= hour <= 13
    if symbol_in_family(symbol, "US2000"):
        return 8 <= hour <= 23
    if symbol_in_family(symbol, "USTECH"):
        return 6 <= hour <= 23
    if symbol_in_family(symbol, "US500") or symbol_in_family(symbol, "US30"):
        return 8 <= hour <= 23
    if symbol_in_family(symbol, "GER40") or symbol_in_family(symbol, "UK100") or symbol_in_family(symbol, "EUSTX50") or symbol_in_family(symbol, "FRA40") or symbol_in_family(symbol, "SMI20"):
        return 7 <= hour <= 20
    return False


def fast_market_filter(symbol):
    t1 = safe_tick(symbol)
    time.sleep(0.02)
    t2 = safe_tick(symbol)
    if not t1 or not t2:
        return False
    move = abs(t2.bid - t1.bid)
    return move <= t1.bid * 0.006


def fast_market_filter_scalping(symbol):
    t1 = safe_tick(symbol)
    time.sleep(0.01)
    t2 = safe_tick(symbol)
    if not t1 or not t2:
        return False
    move = abs(t2.bid - t1.bid)
    return move <= t1.bid * 0.009


def compute_dynamic_risk():
    return RISK_PER_TRADE_PERCENT / 100.0


def is_attack_runtime():
    return (
        str(CURRENT_OPTIMIZER_PROFILE).upper() == "ATTACK"
        or str(ai_mode).upper() == "AGGRESSIVE"
        or str(equity_mode).upper() == "AGGRESSIVE"
    )


def get_total_open_risk():
    acc = mt5.account_info()
    if not acc:
        return 0
    p = safe_positions()
    risk_money = sum(
        [abs(pos.price_open - pos.sl) * pos.volume for pos in p if pos.magic == MAGIC_ID and pos.sl != 0]
    )
    return risk_money / acc.equity


def default_strategy_filter(symbol, signal, score, market_df, execution_df, strategy_cfg):
    return final_filter(symbol, market_df, score)


def titanyx_filter(symbol, signal, score, market_df, execution_df, strategy_cfg):
    working_df = execution_df if execution_df is not None else market_df
    if working_df is None or len(working_df) < 60:
        return False, "NO M5 DATA"

    tick = safe_tick(symbol)
    if not tick:
        return False, "NO TICK"

    atr = float(working_df["atr"].iloc[-2] or 0.0)
    if atr <= 0:
        return False, "NO ATR"

    spread = abs(float(tick.ask - tick.bid))
    max_spread = atr * strategy_cfg.get("titanyx_spread_atr", 0.20)
    min_score = strategy_cfg.get("min_score", 0.50) - 0.05
    if score < min_score:
        return False, f"LOW SCORE ({round(score, 2)})"
    if spread > max_spread:
        return False, "HIGH SPREAD"
    return True, "OK"


def scalping_filter(symbol, signal, score, market_df, execution_df, strategy_cfg):
    if execution_df is None or len(execution_df) < 30:
        return False, "NO M1 DATA"

    tick = safe_tick(symbol)
    if not tick:
        return False, "NO TICK"

    atr_m1 = execution_df["atr"].iloc[-1]
    spread = tick.ask - tick.bid
    candle_body = abs(execution_df["close"].iloc[-1] - execution_df["open"].iloc[-1])
    momentum = abs(execution_df["close"].iloc[-1] - execution_df["close"].iloc[-4])

    attack = is_attack_runtime()
    broker_hour = get_broker_hour()
    late_gold = symbol_in_family(symbol, "XAUUSD") and (broker_hour >= 20 or broker_hour <= 3)
    min_score = strategy_cfg.get("min_score", 0.55) - 0.04
    spread_limit = atr_m1 * 0.22
    body_floor = atr_m1 * 0.09
    momentum_floor = atr_m1 * 0.16
    if late_gold:
        min_score = max(0.31 if attack else 0.35, min_score - 0.05)
        spread_limit = atr_m1 * (0.32 if attack else 0.28)
        body_floor = atr_m1 * (0.055 if attack else 0.072)
        momentum_floor = atr_m1 * (0.10 if attack else 0.12)
    if STRESS_TEST_ACTIVE:
        min_score = max(0.32, min_score - 0.03)
        spread_limit *= 1.12
        body_floor *= 0.88
        momentum_floor *= 0.84
    if score < min_score:
        return False, f"LOW SCORE ({round(score,2)})"
    if spread > spread_limit:
        return False, "SCALP SPREAD"
    if candle_body < body_floor:
        return False, "WEAK CANDLE"
    if momentum < momentum_floor:
        return False, "WEAK SCALP MOM"
    return True, "OK"


def luke_filter(symbol, signal, score, market_df, execution_df, strategy_cfg):
    if execution_df is None or len(execution_df) < 40:
        return False, "NO M1 DATA"
    tick = safe_tick(symbol)
    if not tick:
        return False, "NO TICK"
    return True, "OK"


def get_correlation_group(symbol):
    if any(symbol_in_family(symbol, family) for family in ["USTECH", "US500", "US30", "GER40", "UK100"]):
        return "INDEX"
    if any(symbol_in_family(symbol, family) for family in ["BTCUSD", "ETHUSD"]):
        return "CRYPTO"
    if symbol in ["EURUSD", "GBPUSD"]:
        return "FX_USD"
    return symbol


def correlated_directional_exposure(target_symbol, target_signal):
    risk_money = 0
    acc = mt5.account_info()
    if not acc:
        return 0
    target_group = get_correlation_group(target_symbol)
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID or get_correlation_group(p.symbol) != target_group:
            continue
        if target_signal == "BUY" and p.type != 0:
            continue
        if target_signal == "SELL" and p.type != 1:
            continue
        info = safe_info(p.symbol)
        if info and info.trade_tick_size > 0:
            loss_ticks = abs(p.price_open - p.sl) / info.trade_tick_size
            risk_money += loss_ticks * info.trade_tick_value * p.volume
    return risk_money / acc.equity


# ==============================================================================
# STRATEGIA
# ==============================================================================
def get_multi_tf_signal(symbol):
    df_m1 = get_m1_cached(symbol)
    df_m5 = get_data(symbol, mt5.TIMEFRAME_M5, 50)
    df_m15 = get_m15_cached(symbol)
    if any(d is None for d in [df_m1, df_m5, df_m15]):
        return "NEUTRAL", 0

    def trend(df):
        return 1 if df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1] else -1

    t1 = trend(df_m1)
    t5 = trend(df_m5)
    t15 = trend(df_m15)
    alignment = t1 + t5 + t15

    if alignment >= 2:
        return "BUY", 3
    elif alignment <= -2:
        return "SELL", 3
    return "NEUTRAL", 0


def gold_profit_entry(df):
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    atr = df["atr"].iloc[-1]

    conf = 60

    if ema20 > ema50 and price > vwap:
        if abs(price - vwap) < atr * 1.45:
            return "BUY", conf, "GLD_FLOW"

    if ema20 < ema50 and price < vwap:
        if abs(price - vwap) < atr * 1.45:
            return "SELL", conf, "GLD_FLOW"

    return "NEUTRAL", 0, "---"


def blocked_signal(name, reason, pretrigger="---"):
    return {"name": name, "blocked": reason, "pretrigger": pretrigger}


LUKE_PROFILES = {
    "XAUUSD": {
        "display": "XAUUSD",
        "session_tz": "Europe/Rome",
        "session_windows": [("09:30", "22:00")],
        "news_windows": [],
        "max_risk_pct": 0.75,
        "legs": 6,
        "spread_buffer_points": 0.6,
        "min_stop_points": 8.0,
        "max_stop_points": 12.0,
        "no_chase_points": 4.8,
        "tp_points": [4, 8, 12, 15, 19, 26],
        "runner_enabled": False,
        "runner_points": 35.0,
        "runner_weight": 0.5,
        "reentry_tp_points": [12, 15, 19],
        "reentry_legs": 3,
        "normal_be_after_tp": 2,
        "reentry_be_after_tp": 3,
        "max_reentries_per_zone": 1,
        "max_minutes_from_first_entry": 30,
        "max_consecutive_losses": 2,
        "continuation_zone_low": 0.38,
        "continuation_zone_high": 0.61,
        "exhaustion_atr": 2.0,
        "volatility_ratio_max": 2.60,
        "range_ratio_max": 1.80,
        "ema_divergence_atr": 2.10,
    }
}


def _luke_runtime(symbol):
    return luke_reentry_state.setdefault(symbol, {"eligible": False, "used": False, "loss_streak": 0})


def _luke_reset_reentry(symbol):
    state = _luke_runtime(symbol)
    state.update(
        {
            "eligible": False,
            "used": False,
            "direction": None,
            "zone_low": None,
            "zone_high": None,
            "sl_price": None,
            "expires_at": None,
            "tp_points": None,
            "tp_be_after_legs": None,
            "entry_time": None,
            "source_setup": None,
        }
    )


def _luke_point_value(info):
    return gold_points_to_price(info, 1.0)


def _luke_tp_payload(profile, info, tp_points, reentry=False):
    tp_targets = list(tp_points)
    lot_weights = [1.0] * len(tp_targets)
    if not reentry and profile.get("runner_enabled"):
        tp_targets.append(float(profile.get("runner_points", 35.0)))
        lot_weights.append(float(profile.get("runner_weight", 0.5)))
    return {
        "force_splits": len(tp_targets),
        "tp_point_targets": tp_targets,
        "tp_point_buffer": float(profile["spread_buffer_points"]),
        "point_value_price": _luke_point_value(info),
        "lot_weights_override": lot_weights,
        "tp_be_after_legs_override": int(profile["reentry_be_after_tp"] if reentry else profile["normal_be_after_tp"]),
        "tp_lock_offset_override": 1,
        "risk_percent_override": float(profile["max_risk_pct"]),
    }


def _luke_session_slice(df_m5, now_date):
    if df_m5 is None or df_m5.empty:
        return df_m5
    session_df = df_m5[df_m5["time"].dt.date == now_date]
    return session_df if not session_df.empty else df_m5.iloc[-60:].copy()


def _luke_reentry_signal(symbol, branch_name, profile, info, tick, df_m1, atr_m1):
    state = _luke_runtime(symbol)
    if not state.get("eligible") or state.get("used"):
        return None
    expires_at = state.get("expires_at")
    if not isinstance(expires_at, datetime) or datetime.now() > expires_at:
        _luke_reset_reentry(symbol)
        return blocked_signal(branch_name, "REENTRY EXPIRED")

    direction = state.get("direction")
    zone_low = float(state.get("zone_low") or 0.0)
    zone_high = float(state.get("zone_high") or 0.0)
    sl_price = state.get("sl_price")
    if not direction or sl_price is None or zone_high <= zone_low:
        _luke_reset_reentry(symbol)
        return blocked_signal(branch_name, "REENTRY INVALID")

    entry_price = float(tick.ask if direction == "BUY" else tick.bid)
    price = float(df_m1["close"].iloc[-1])
    if price < zone_low or price > zone_high:
        return blocked_signal(branch_name, "REENTRY WAIT", f"ZONE {zone_low:.2f}-{zone_high:.2f}")

    ema20_m1 = df_m1["close"].ewm(span=20).mean()
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    body = abs(float(last["close"] - last["open"]))
    strong_body = body >= atr_m1 * 0.08
    if direction == "BUY":
        confirm_ok = (
            float(last["close"]) > float(last["open"])
            and float(last["close"]) > float(prev["high"])
            and float(last["close"]) >= float(ema20_m1.iloc[-1])
        )
    else:
        confirm_ok = (
            float(last["close"]) < float(last["open"])
            and float(last["close"]) < float(prev["low"])
            and float(last["close"]) <= float(ema20_m1.iloc[-1])
        )
    if not strong_body or not confirm_ok:
        return blocked_signal(branch_name, "REENTRY NO CONFIRM")

    stop_points = price_to_gold_points(info, abs(entry_price - float(sl_price)))
    if stop_points < profile["min_stop_points"] or stop_points > profile["max_stop_points"]:
        return blocked_signal(branch_name, "REENTRY SL RANGE", f"SL {stop_points:.1f}pt")

    payload = _luke_tp_payload(profile, info, state.get("tp_points") or profile["reentry_tp_points"], reentry=True)
    details = {
        "instrument": profile["display"],
        "setup": "RE-ENTRY",
        "reentry": True,
        "direction": "LONG" if direction == "BUY" else "SHORT",
        "zone_low": round(zone_low, 2),
        "zone_high": round(zone_high, 2),
        "entry_price": round(entry_price, 2),
        "stop_loss": round(float(sl_price), 2),
        "take_profit": f"TP {state.get('tp_points') or profile['reentry_tp_points']}",
        "summary": f"{profile['display']} {direction} | LUKE re-entry | zone {zone_low:.2f}-{zone_high:.2f} | SL {float(sl_price):.2f}",
    }
    return {
        "signal": direction,
        "confidence": 78,
        "name": branch_name,
        "execution_df": df_m1,
        "sl_price": round(float(sl_price), 5),
        "signal_key": f"LUKE|REENTRY|{direction}|{int(expires_at.timestamp())}",
        **payload,
        "signal_details": details,
    }


def strategy_xau_luke(df, symbol):
    branch_name = "XAU_LUKE"
    profile = LUKE_PROFILES["XAUUSD"]
    if not scalping_enabled:
        return blocked_signal(branch_name, "SCALP OFF")
    if not _time_in_exchange_windows(profile["session_tz"], profile["session_windows"]):
        return blocked_signal(branch_name, "OUT SESSION")
    if _news_window_blocked(profile["session_tz"], profile["news_windows"]):
        return blocked_signal(branch_name, "NEWS BLOCK")

    df_m5 = get_m5_cached(symbol)
    df_m1 = get_m1_cached(symbol)
    if df_m5 is None or len(df_m5) < 80:
        return blocked_signal(branch_name, "NO M5 DATA")
    if df_m1 is None or len(df_m1) < 90:
        return blocked_signal(branch_name, "NO M1 DATA")

    tick = safe_tick(symbol)
    info = safe_info(symbol)
    if not tick or not info:
        return blocked_signal(branch_name, "NO TICK")

    state = _luke_runtime(symbol)
    if int(state.get("loss_streak", 0) or 0) >= int(profile["max_consecutive_losses"]):
        return blocked_signal(branch_name, "LOCKOUT LOSS")

    point_value = _luke_point_value(info)
    spread_points = abs(float(tick.ask - tick.bid)) / max(point_value, 1e-9)
    if spread_points > 1.60:
        return blocked_signal(branch_name, "SPREAD HIGH", f"SP {spread_points:.1f}pt")

    atr_m1 = float(df_m1["atr"].iloc[-2] or 0.0)
    atr_m5 = float(df_m5["atr"].iloc[-2] or 0.0)
    atr_m1_avg = float(df_m1["atr"].iloc[-60:-2].mean() or 0.0)
    if atr_m1 <= 0 or atr_m5 <= 0 or atr_m1_avg <= 0:
        return blocked_signal(branch_name, "NO ATR")
    if atr_m1 > atr_m1_avg * profile["volatility_ratio_max"]:
        return blocked_signal(branch_name, "VOLATILITY BLOCK", pretrigger_text("ATR", atr_m1 - (atr_m1_avg * profile["volatility_ratio_max"]), atr_m1_avg))

    last_1m = df_m1.iloc[-1]
    current_range = float(last_1m["high"] - last_1m["low"])
    if current_range > atr_m1 * profile["range_ratio_max"]:
        return blocked_signal(branch_name, "RANGE ERRATICO", pretrigger_text("RNG", current_range - (atr_m1 * profile["range_ratio_max"]), atr_m1))

    ema20_m5 = df_m5["close"].ewm(span=20).mean()
    ema50_m5 = df_m5["close"].ewm(span=50).mean()
    ema_divergence = abs(float(ema20_m5.iloc[-2] - ema50_m5.iloc[-2]))
    if ema_divergence > atr_m5 * profile["ema_divergence_atr"]:
        return blocked_signal(branch_name, "TREND TOO STRONG", pretrigger_text("EMA", ema_divergence - (atr_m5 * profile["ema_divergence_atr"]), atr_m5))

    now_broker = get_broker_now()
    session_df = _luke_session_slice(df_m5, now_broker.date())
    if len(session_df) >= 12:
        first_hour = session_df.iloc[:12]
        first_hour_high = float(first_hour["high"].max())
        first_hour_low = float(first_hour["low"].min())
        last_close = float(df_m5["close"].iloc[-2])
        if last_close > first_hour_high + atr_m5 * 0.65 or last_close < first_hour_low - atr_m5 * 0.65:
            return blocked_signal(branch_name, "SESSION BREAKOUT ACTIVE")

    reentry = _luke_reentry_signal(symbol, branch_name, profile, info, tick, df_m1, atr_m1)
    if reentry and not reentry.get("blocked"):
        return reentry

    ema20_slope = float(ema20_m5.iloc[-2] - ema20_m5.iloc[-5])
    vwap_m5 = float(df_m5["vwap"].iloc[-2])
    price_m5 = float(df_m5["close"].iloc[-2])
    last_m1 = df_m1.iloc[-1]
    prev_m1 = df_m1.iloc[-2]
    recent_m1 = df_m1.iloc[-8:-1]
    bullish_confirm = (
        float(last_m1["close"]) > float(last_m1["open"])
        and float(last_m1["close"]) > float(prev_m1["high"])
        and float(last_m1["close"]) > float(df_m1["vwap"].iloc[-1])
    )
    bearish_confirm = (
        float(last_m1["close"]) < float(last_m1["open"])
        and float(last_m1["close"]) < float(prev_m1["low"])
        and float(last_m1["close"]) < float(df_m1["vwap"].iloc[-1])
    )

    recent_m5 = df_m5.iloc[-18:-2].copy()
    if len(recent_m5) < 12:
        return blocked_signal(branch_name, "NO STRUCTURE")

    long_low_idx = int(recent_m5["low"].idxmin())
    long_impulse = recent_m5.loc[long_low_idx:]
    short_high_idx = int(recent_m5["high"].idxmax())
    short_impulse = recent_m5.loc[short_high_idx:]
    current_ask = float(tick.ask)
    current_bid = float(tick.bid)

    if (
        price_m5 > vwap_m5
        and float(ema20_m5.iloc[-2]) > float(ema50_m5.iloc[-2])
        and ema20_slope > 0
        and float(recent_m5["high"].iloc[-2]) > float(recent_m5["high"].iloc[-6])
        and float(recent_m5["low"].iloc[-2]) > float(recent_m5["low"].iloc[-6])
        and len(long_impulse) >= 5
    ):
        impulse_low = float(long_impulse["low"].iloc[0])
        impulse_high = float(long_impulse["high"].max())
        impulse_range = impulse_high - impulse_low
        if impulse_range >= atr_m5 * 1.2:
            zone_low = impulse_high - impulse_range * profile["continuation_zone_high"]
            zone_high = impulse_high - impulse_range * profile["continuation_zone_low"]
            if current_ask > zone_high + gold_points_to_price(info, profile["no_chase_points"]):
                return blocked_signal(branch_name, "NO CHASE", "LONG moved past TP1 window")
            pullback_low = float(recent_m1["low"].min())
            sl_price = min(pullback_low, impulse_low) - gold_points_to_price(info, profile["spread_buffer_points"])
            stop_points = price_to_gold_points(info, abs(current_ask - sl_price))
            if zone_low <= current_ask <= zone_high and bullish_confirm and profile["min_stop_points"] <= stop_points <= profile["max_stop_points"]:
                payload = _luke_tp_payload(profile, info, profile["tp_points"], reentry=False)
                details = {
                    "instrument": profile["display"],
                    "setup": "CONTINUATION RETEST",
                    "reentry": False,
                    "direction": "LONG",
                    "zone_low": round(zone_low, 2),
                    "zone_high": round(zone_high, 2),
                    "entry_price": round(current_ask, 2),
                    "stop_loss": round(sl_price, 2),
                    "take_profit": f"TP {profile['tp_points']}",
                    "summary": f"{profile['display']} LONG | LUKE continuation | zone {zone_low:.2f}-{zone_high:.2f} | SL {sl_price:.2f}",
                }
                return {
                    "signal": "BUY",
                    "confidence": 80,
                    "name": branch_name,
                    "execution_df": df_m1,
                    "sl_price": round(sl_price, 5),
                    "signal_key": f"LUKE|CONT|BUY|{df_m1.iloc[-2]['time'].isoformat()}",
                    **payload,
                    "signal_details": details,
                }

    if (
        price_m5 < vwap_m5
        and float(ema20_m5.iloc[-2]) < float(ema50_m5.iloc[-2])
        and ema20_slope < 0
        and float(recent_m5["high"].iloc[-2]) < float(recent_m5["high"].iloc[-6])
        and float(recent_m5["low"].iloc[-2]) < float(recent_m5["low"].iloc[-6])
        and len(short_impulse) >= 5
    ):
        impulse_high = float(short_impulse["high"].iloc[0])
        impulse_low = float(short_impulse["low"].min())
        impulse_range = impulse_high - impulse_low
        if impulse_range >= atr_m5 * 1.2:
            zone_low = impulse_low + impulse_range * profile["continuation_zone_low"]
            zone_high = impulse_low + impulse_range * profile["continuation_zone_high"]
            if current_bid < zone_low - gold_points_to_price(info, profile["no_chase_points"]):
                return blocked_signal(branch_name, "NO CHASE", "SHORT moved past TP1 window")
            pullback_high = float(recent_m1["high"].max())
            sl_price = max(pullback_high, impulse_high) + gold_points_to_price(info, profile["spread_buffer_points"])
            stop_points = price_to_gold_points(info, abs(current_bid - sl_price))
            if zone_low <= current_bid <= zone_high and bearish_confirm and profile["min_stop_points"] <= stop_points <= profile["max_stop_points"]:
                payload = _luke_tp_payload(profile, info, profile["tp_points"], reentry=False)
                details = {
                    "instrument": profile["display"],
                    "setup": "CONTINUATION RETEST",
                    "reentry": False,
                    "direction": "SHORT",
                    "zone_low": round(zone_low, 2),
                    "zone_high": round(zone_high, 2),
                    "entry_price": round(current_bid, 2),
                    "stop_loss": round(sl_price, 2),
                    "take_profit": f"TP {profile['tp_points']}",
                    "summary": f"{profile['display']} SHORT | LUKE continuation | zone {zone_low:.2f}-{zone_high:.2f} | SL {sl_price:.2f}",
                }
                return {
                    "signal": "SELL",
                    "confidence": 80,
                    "name": branch_name,
                    "execution_df": df_m1,
                    "sl_price": round(sl_price, 5),
                    "signal_key": f"LUKE|CONT|SELL|{df_m1.iloc[-2]['time'].isoformat()}",
                    **payload,
                    "signal_details": details,
                }

    session_high = float(session_df["high"].max()) if len(session_df) else float(df_m5["high"].iloc[-24:-2].max())
    session_low = float(session_df["low"].min()) if len(session_df) else float(df_m5["low"].iloc[-24:-2].min())
    vwap_m1 = float(df_m1["vwap"].iloc[-1])
    rejection_body = abs(float(last_m1["close"] - last_m1["open"]))
    rejection_ok = rejection_body >= atr_m1 * 0.16
    two_bar_high = float(df_m1["high"].iloc[-3:-1].max())
    two_bar_low = float(df_m1["low"].iloc[-3:-1].min())

    if (
        current_bid >= session_high - gold_points_to_price(info, 0.8)
        and (current_bid - vwap_m1) >= atr_m1 * profile["exhaustion_atr"]
        and float(last_m1["high"] - max(last_m1["open"], last_m1["close"])) >= rejection_body * 0.6
        and float(last_m1["close"]) < two_bar_high
        and rejection_ok
    ):
        sl_price = float(last_m1["high"]) + gold_points_to_price(info, profile["spread_buffer_points"])
        stop_points = price_to_gold_points(info, abs(current_bid - sl_price))
        if profile["min_stop_points"] <= stop_points <= profile["max_stop_points"]:
            payload = _luke_tp_payload(profile, info, profile["tp_points"], reentry=False)
            details = {
                "instrument": profile["display"],
                "setup": "EXHAUSTION FADE",
                "reentry": False,
                "direction": "SHORT",
                "zone_low": round(two_bar_low, 2),
                "zone_high": round(two_bar_high, 2),
                "entry_price": round(current_bid, 2),
                "stop_loss": round(sl_price, 2),
                "take_profit": f"TP {profile['tp_points']}",
                "summary": f"{profile['display']} SHORT | LUKE fade | session high sweep | SL {sl_price:.2f}",
            }
            return {
                "signal": "SELL",
                "confidence": 77,
                "name": branch_name,
                "execution_df": df_m1,
                "sl_price": round(sl_price, 5),
                "signal_key": f"LUKE|FADE|SELL|{df_m1.iloc[-2]['time'].isoformat()}",
                **payload,
                "signal_details": details,
            }

    if (
        current_ask <= session_low + gold_points_to_price(info, 0.8)
        and (vwap_m1 - current_ask) >= atr_m1 * profile["exhaustion_atr"]
        and float(min(last_m1["open"], last_m1["close"]) - last_m1["low"]) >= rejection_body * 0.6
        and float(last_m1["close"]) > two_bar_low
        and rejection_ok
    ):
        sl_price = float(last_m1["low"]) - gold_points_to_price(info, profile["spread_buffer_points"])
        stop_points = price_to_gold_points(info, abs(current_ask - sl_price))
        if profile["min_stop_points"] <= stop_points <= profile["max_stop_points"]:
            payload = _luke_tp_payload(profile, info, profile["tp_points"], reentry=False)
            details = {
                "instrument": profile["display"],
                "setup": "EXHAUSTION FADE",
                "reentry": False,
                "direction": "LONG",
                "zone_low": round(two_bar_low, 2),
                "zone_high": round(two_bar_high, 2),
                "entry_price": round(current_ask, 2),
                "stop_loss": round(sl_price, 2),
                "take_profit": f"TP {profile['tp_points']}",
                "summary": f"{profile['display']} LONG | LUKE fade | session low flush | SL {sl_price:.2f}",
            }
            return {
                "signal": "BUY",
                "confidence": 77,
                "name": branch_name,
                "execution_df": df_m1,
                "sl_price": round(sl_price, 5),
                "signal_key": f"LUKE|FADE|BUY|{df_m1.iloc[-2]['time'].isoformat()}",
                **payload,
                "signal_details": details,
            }

    if reentry and reentry.get("blocked"):
        return reentry
    if spread_points > 0.9:
        return blocked_signal(branch_name, "SPREAD HIGH", f"SP {spread_points:.1f}pt")
    return blocked_signal(branch_name, "LUKE WAIT", "NO CLEAN ZONE")


def get_luke_exit_reason(symbol, strategy_cfg, signal):
    profile = LUKE_PROFILES.get("XAUUSD")
    if not profile:
        return None
    now = get_exchange_now(profile["session_tz"])
    if now.weekday() >= 5:
        return "weekend flat"
    if not _time_in_exchange_windows(profile["session_tz"], profile["session_windows"]):
        return "session flat"
    return None


LIQUIDITY_SNIPER_PROFILES = {
    "USTECH": {
        "display": "US100",
        "priority_rank": 1,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "US/Eastern",
        "session_windows": [("09:30", "15:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 4.8,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "GER40": {
        "display": "DAX",
        "priority_rank": 2,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "Europe/Berlin",
        "session_windows": [("09:00", "16:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 4.0,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "US500": {
        "display": "US500",
        "priority_rank": 3,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "US/Eastern",
        "session_windows": [("09:30", "15:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 4.4,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "JPN225": {
        "display": "JP225",
        "priority_rank": 4,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "Asia/Tokyo",
        "session_windows": [("09:00", "14:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 3.8,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "EUSTX50": {
        "display": "EU50",
        "priority_rank": 5,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "Europe/Berlin",
        "session_windows": [("09:00", "16:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 3.8,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "US30": {
        "display": "US30",
        "priority_rank": 6,
        "preferred": "REVERSAL",
        "secondary": "CONTINUATION",
        "session_tz": "US/Eastern",
        "session_windows": [("09:30", "15:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 4.6,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "UK100": {
        "display": "UK100",
        "priority_rank": 7,
        "preferred": "REVERSAL",
        "secondary": "CONTINUATION",
        "session_tz": "Europe/London",
        "session_windows": [("08:00", "16:00")],
        "news_windows": [],
        "max_daily_drawdown_eur": 3.6,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "US2000": {
        "display": "US2000",
        "priority_rank": 8,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "US/Eastern",
        "session_windows": [("09:30", "15:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 4.0,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "FRA40": {
        "display": "FRA40",
        "priority_rank": 9,
        "preferred": "CONTINUATION",
        "secondary": "REVERSAL",
        "session_tz": "Europe/Paris",
        "session_windows": [("09:00", "16:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 3.5,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
    "SMI20": {
        "display": "SMI20",
        "priority_rank": 10,
        "preferred": "REVERSAL",
        "secondary": "CONTINUATION",
        "session_tz": "Europe/Zurich",
        "session_windows": [("09:00", "16:30")],
        "news_windows": [],
        "max_daily_drawdown_eur": 3.4,
        "max_symbol_trades": 2,
        "max_total_trades": 4,
    },
}

LIQUIDITY_SNIPER_STRATEGY_NAMES = set()


def _blank_liquidity_sniper_day_state(ref_day=None):
    return {
        "day": ref_day or datetime.now().date(),
        "total_trades": 0,
        "total_pnl": 0.0,
        "consecutive_losses": 0,
        "per_symbol": {},
    }


def get_exchange_now(tz_name):
    return datetime.now(ZoneInfo(tz_name))


def broker_time_to_exchange(ts, tz_name):
    if ts is None:
        return None
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    local_naive = ts - get_live_broker_offset()
    local_aware = local_naive.replace(tzinfo=ZoneInfo("Europe/Rome"))
    return local_aware.astimezone(ZoneInfo(tz_name))


def _time_hhmm_to_minutes(value):
    hour, minute = str(value).split(":")
    return int(hour) * 60 + int(minute)


def _time_in_exchange_windows(tz_name, windows):
    now = get_exchange_now(tz_name)
    now_minutes = now.hour * 60 + now.minute
    for start_text, end_text in windows:
        start_minutes = _time_hhmm_to_minutes(start_text)
        end_minutes = _time_hhmm_to_minutes(end_text)
        if start_minutes <= end_minutes:
            if start_minutes <= now_minutes <= end_minutes:
                return True
        else:
            if now_minutes >= start_minutes or now_minutes <= end_minutes:
                return True
    return False


def _news_window_blocked(tz_name, news_windows):
    if not news_windows:
        return False
    now = get_exchange_now(tz_name)
    now_minutes = now.hour * 60 + now.minute
    for center_text in news_windows:
        center_minutes = _time_hhmm_to_minutes(center_text)
        if abs(now_minutes - center_minutes) <= 10:
            return True
    return False


def _sniper_range_position(price, low, high):
    span = max(high - low, 1e-9)
    return clamp((price - low) / span, 0.0, 1.0)


def _compute_htf_structure(df_tf):
    if df_tf is None or len(df_tf) < 30:
        return "MIXED"
    highs = df_tf["high"]
    lows = df_tf["low"]
    closes = df_tf["close"]
    recent_high = float(highs.iloc[-6:-1].max())
    prev_high = float(highs.iloc[-12:-6].max())
    recent_low = float(lows.iloc[-6:-1].min())
    prev_low = float(lows.iloc[-12:-6].min())
    close_now = float(closes.iloc[-2])
    close_prev = float(closes.iloc[-6])
    if recent_high > prev_high and recent_low > prev_low and close_now >= close_prev:
        return "UP"
    if recent_high < prev_high and recent_low < prev_low and close_now <= close_prev:
        return "DOWN"
    return "MIXED"


def _get_supply_demand_zone(df_h1, bias):
    if df_h1 is None or len(df_h1) < 40:
        return None
    lookback = df_h1.iloc[-16:-1]
    if bias == "BUY":
        candidates = lookback[lookback["close"] < lookback["open"]]
        if candidates.empty:
            idx = int(lookback["low"].idxmin())
            row = df_h1.loc[idx]
        else:
            row = candidates.iloc[-1]
        return {"low": float(row["low"]), "high": float(max(row["open"], row["close"]))}
    candidates = lookback[lookback["close"] > lookback["open"]]
    if candidates.empty:
        idx = int(lookback["high"].idxmax())
        row = df_h1.loc[idx]
    else:
        row = candidates.iloc[-1]
    return {"low": float(min(row["open"], row["close"])), "high": float(row["high"])}


def _price_in_zone(price, zone, atr, pad_mult=0.30):
    if not zone:
        return False
    return (zone["low"] - atr * pad_mult) <= price <= (zone["high"] + atr * pad_mult)


def _build_liquidity_sniper_day_state(seed_deals, position_map):
    state = _blank_liquidity_sniper_day_state(get_broker_session_start().date())
    relevant = [
        d for d in sorted(seed_deals, key=lambda item: (getattr(item, "time", 0), _deal_ticket(item) or 0))
        if d.magic == MAGIC_ID and _is_realized_exit_deal(d)
    ]
    for d in relevant:
        position_id = getattr(d, "position_id", None) or getattr(d, "position", None)
        strategy_tag = position_map.get(position_id)
        if not strategy_tag:
            continue
        strategy_cfg = get_strategy_by_tag(strategy_tag)
        if not strategy_cfg or strategy_cfg["name"] not in LIQUIDITY_SNIPER_STRATEGY_NAMES:
            continue
        pnl = get_deal_realized_pnl(d)
        symbol_state = state["per_symbol"].setdefault(d.symbol, {"trades": 0, "pnl": 0.0, "consecutive_losses": 0})
        state["total_trades"] += 1
        state["total_pnl"] += pnl
        symbol_state["trades"] += 1
        symbol_state["pnl"] += pnl
        if pnl < 0:
            state["consecutive_losses"] += 1
            symbol_state["consecutive_losses"] += 1
        else:
            state["consecutive_losses"] = 0
            symbol_state["consecutive_losses"] = 0
    return state


def _liquidity_sniper_limits_ok(symbol, profile):
    state = liquidity_sniper_day_state or _blank_liquidity_sniper_day_state()
    if state.get("total_trades", 0) >= profile["max_total_trades"]:
        return False, "DAILY TOTAL LIMIT"
    if state.get("consecutive_losses", 0) >= 2:
        return False, "2 LOSSES STOP"
    symbol_state = state.get("per_symbol", {}).get(symbol, {})
    if symbol_state.get("trades", 0) >= profile["max_symbol_trades"]:
        return False, "SYMBOL DAILY LIMIT"
    if symbol_state.get("pnl", 0.0) <= -abs(profile["max_daily_drawdown_eur"]):
        return False, "SYMBOL DD STOP"
    return True, "OK"


def _allow_liquidity_sniper_priority(symbol, confluences):
    global liquidity_sniper_cycle_symbols
    with liquidity_sniper_cycle_lock:
        if symbol in liquidity_sniper_cycle_symbols:
            return True
        if confluences >= (6 if STRESS_TEST_ACTIVE else 7):
            return True
        max_symbols = 8 if STRESS_TEST_ACTIVE else 5
        if len(liquidity_sniper_cycle_symbols) < max_symbols:
            liquidity_sniper_cycle_symbols.append(symbol)
            return True
        return False


def _price_midrange_block(price, range_low, range_high):
    range_pos = _sniper_range_position(price, range_low, range_high)
    return 0.42 <= range_pos <= 0.58, range_pos


def _find_liquidity_sweep(df_ltf, side):
    if df_ltf is None or len(df_ltf) < 20:
        return None
    sweep_bar = df_ltf.iloc[-3]
    displacement_bar = df_ltf.iloc[-2]
    prior = df_ltf.iloc[-12:-3]
    atr = float(df_ltf["atr"].iloc[-2] or 0.0)
    if atr <= 0:
        return None
    if side == "BUY":
        liquidity_level = float(prior["low"].min())
        sweep = float(sweep_bar["low"]) < liquidity_level and float(sweep_bar["close"]) > liquidity_level
        structure_level = float(prior["high"].max())
        body = abs(float(displacement_bar["close"] - displacement_bar["open"]))
        displacement = (
            float(displacement_bar["close"]) > structure_level
            and body >= atr * (0.72 if STRESS_TEST_ACTIVE else 0.80)
            and float(displacement_bar["close"]) > float(displacement_bar["open"])
        )
        if not sweep or not displacement:
            return None
        return {
            "liquidity_level": liquidity_level,
            "sweep_extreme": float(sweep_bar["low"]),
            "displacement_close": float(displacement_bar["close"]),
            "displacement_open": float(displacement_bar["open"]),
            "direction": "BUY",
            "body": body,
            "atr": atr,
            "bos_level": structure_level,
        }
    liquidity_level = float(prior["high"].max())
    sweep = float(sweep_bar["high"]) > liquidity_level and float(sweep_bar["close"]) < liquidity_level
    structure_level = float(prior["low"].min())
    body = abs(float(displacement_bar["close"] - displacement_bar["open"]))
    displacement = (
        float(displacement_bar["close"]) < structure_level
        and body >= atr * (0.72 if STRESS_TEST_ACTIVE else 0.80)
        and float(displacement_bar["close"]) < float(displacement_bar["open"])
    )
    if not sweep or not displacement:
        return None
    return {
        "liquidity_level": liquidity_level,
        "sweep_extreme": float(sweep_bar["high"]),
        "displacement_close": float(displacement_bar["close"]),
        "displacement_open": float(displacement_bar["open"]),
        "direction": "SELL",
        "body": body,
        "atr": atr,
        "bos_level": structure_level,
    }


def _retrace_ok(price, sweep_info, side):
    if not sweep_info:
        return False, None
    impulse_low = sweep_info["sweep_extreme"] if side == "BUY" else sweep_info["displacement_close"]
    impulse_high = sweep_info["displacement_close"] if side == "BUY" else sweep_info["sweep_extreme"]
    total_move = max(abs(impulse_high - impulse_low), 1e-9)
    if side == "BUY":
        retrace_ratio = (sweep_info["displacement_close"] - price) / total_move
        zone_ok = (0.52 if STRESS_TEST_ACTIVE else 0.62) <= retrace_ratio <= 0.79
    else:
        retrace_ratio = (price - sweep_info["displacement_close"]) / total_move
        zone_ok = (0.45 if STRESS_TEST_ACTIVE else 0.50) <= retrace_ratio <= 0.79
    return zone_ok, retrace_ratio


def _continuation_tp_levels(df_h1, entry_price, side):
    swing_high = float(df_h1["high"].iloc[-10:-2].max())
    swing_low = float(df_h1["low"].iloc[-10:-2].min())
    external_high = float(df_h1["high"].iloc[-24:-2].max())
    external_low = float(df_h1["low"].iloc[-24:-2].min())
    if side == "BUY":
        tp1 = max(entry_price, swing_high)
        tp2 = max(tp1, external_high)
    else:
        tp1 = min(entry_price, swing_low)
        tp2 = min(tp1, external_low)
    return tp1, tp2


def _reversal_tp_levels(df_h4, entry_price, side):
    range_high = float(df_h4["high"].iloc[-24:-2].max())
    range_low = float(df_h4["low"].iloc[-24:-2].min())
    mid = (range_high + range_low) / 2.0
    if side == "BUY":
        return max(entry_price, mid), range_high
    return min(entry_price, mid), range_low


def _build_continuation_liquidity_signal(df, symbol, branch_name, family):
    profile = LIQUIDITY_SNIPER_PROFILES[family]
    if not _time_in_exchange_windows(profile["session_tz"], profile["session_windows"]):
        return blocked_signal(branch_name, "OUT SESSION")
    if _news_window_blocked(profile["session_tz"], profile.get("news_windows", [])):
        return blocked_signal(branch_name, "NEWS BLOCK")

    limits_ok, limit_reason = _liquidity_sniper_limits_ok(symbol, profile)
    if not limits_ok:
        return blocked_signal(branch_name, limit_reason)

    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    df_m1 = get_m1_cached(symbol)
    if df_h1 is None or df_h4 is None or df_m1 is None or len(df) < 40:
        return blocked_signal(branch_name, "NO DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    price_bid = float(tick.bid)
    price_ask = float(tick.ask)
    bias_h4 = _compute_htf_structure(df_h4)
    bias_h1 = _compute_htf_structure(df_h1)
    if bias_h4 == "UP" and bias_h1 == "UP":
        signal = "BUY"
        entry_price = price_ask
    elif bias_h4 == "DOWN" and bias_h1 == "DOWN":
        signal = "SELL"
        entry_price = price_bid
    else:
        return blocked_signal(branch_name, "HTF BIAS MIXED")

    htf_high = float(df_h4["high"].iloc[-20:-2].max())
    htf_low = float(df_h4["low"].iloc[-20:-2].min())
    mid_block, range_pos = _price_midrange_block((price_bid + price_ask) / 2.0, htf_low, htf_high)
    if mid_block:
        return blocked_signal(branch_name, "MID HTF RANGE", f"RNG {range_pos:.2f}")

    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    zone = _get_supply_demand_zone(df_h1, signal)
    in_zone = _price_in_zone((price_bid + price_ask) / 2.0, zone, atr_h1, 0.48 if STRESS_TEST_ACTIVE else 0.35)
    sweep_info = _find_liquidity_sweep(df, signal)
    if not sweep_info:
        return blocked_signal(branch_name, "NO REAL SWEEP")
    retrace_ok, retrace_ratio = _retrace_ok(entry_price, sweep_info, signal)
    if not retrace_ok:
        return blocked_signal(branch_name, "NO RETRACE", f"RET {retrace_ratio:.2f}" if retrace_ratio is not None else "---")

    premium_discount_ok = range_pos <= 0.55 if signal == "BUY" else range_pos >= 0.45
    tp1, tp2 = _continuation_tp_levels(df_h1, entry_price, signal)
    buffer_pct = entry_price * 0.001
    buffer_atr = sweep_info["atr"] * 0.15
    sl_buffer = max(buffer_pct, buffer_atr)
    sl_price = sweep_info["sweep_extreme"] - sl_buffer if signal == "BUY" else sweep_info["sweep_extreme"] + sl_buffer
    rr = abs(tp2 - entry_price) / max(abs(entry_price - sl_price), 1e-9)
    displacement_ok = bool(sweep_info["body"] >= sweep_info["atr"] * (0.72 if STRESS_TEST_ACTIVE else 0.80))
    bos_ok = True

    confluences = 0
    checks = [
        bias_h4 == bias_h1 and bias_h4 in {"UP", "DOWN"},
        in_zone,
        True,
        bos_ok,
        displacement_ok,
        retrace_ok,
        premium_discount_ok,
        rr >= (1.8 if STRESS_TEST_ACTIVE else 2.0),
    ]
    confluences = sum(1 for x in checks if x)
    if confluences < (5 if STRESS_TEST_ACTIVE else 6):
        return blocked_signal(branch_name, "LOW CONFLUENCE", f"{confluences}/8")
    if not _allow_liquidity_sniper_priority(symbol, confluences):
        return blocked_signal(branch_name, "LOWER PRIORITY", f"{confluences}/8")

    confidence = int(clamp(54 + confluences * 5 + max(0.0, min(rr, 3.5) - 2.0) * 8, 0, 100))
    invalidation = sweep_info["sweep_extreme"]
    details = {
        "instrument": profile["display"],
        "strategy": "Continuation Liquidity Pullback",
        "direction": "LONG" if signal == "BUY" else "SHORT",
        "entry_zone": f"{entry_price:.2f} | retrace {retrace_ratio:.2f}",
        "stop_loss": round(sl_price, 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "rr": round(rr, 2),
        "confluences": confluences,
        "invalidation_level": round(invalidation, 5),
        "confidence_score": confidence,
        "entry_price": entry_price,
        "take_profit": f"TP1 {tp1:.2f} | TP2 {tp2:.2f}",
        "summary": (
            f"{profile['display']} | Continuation Liquidity Pullback | {'LONG' if signal == 'BUY' else 'SHORT'} | "
            f"zone {entry_price:.2f} | SL {sl_price:.2f} | TP1 {tp1:.2f} | TP2 {tp2:.2f} | "
            f"RR {rr:.2f} | {confluences}/8 | invalid {invalidation:.2f} | conf {confidence}"
        ),
    }
    return {
        "signal": signal,
        "confidence": confidence,
        "name": branch_name,
        "execution_df": df,
        "sl_price": round(sl_price, 5),
        "tp_price": round(tp2, 5),
        "signal_key": f"{family}|CLP|{df.iloc[-2]['time'].isoformat()}|{signal}",
        "signal_details": details,
    }


def _build_reversal_external_sweep_signal(df, symbol, branch_name, family):
    profile = LIQUIDITY_SNIPER_PROFILES[family]
    if not _time_in_exchange_windows(profile["session_tz"], profile["session_windows"]):
        return blocked_signal(branch_name, "OUT SESSION")
    if _news_window_blocked(profile["session_tz"], profile.get("news_windows", [])):
        return blocked_signal(branch_name, "NEWS BLOCK")

    limits_ok, limit_reason = _liquidity_sniper_limits_ok(symbol, profile)
    if not limits_ok:
        return blocked_signal(branch_name, limit_reason)

    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_h1 is None or df_h4 is None or len(df) < 40:
        return blocked_signal(branch_name, "NO DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    atr_h4 = float(df_h4["atr"].iloc[-2] or 0.0)
    atr_m5 = float(df["atr"].iloc[-2] or 0.0)
    if atr_h4 <= 0 or atr_m5 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    price_mid = (float(tick.bid) + float(tick.ask)) / 2.0
    h4_high = float(df_h4["high"].iloc[-24:-2].max())
    h4_low = float(df_h4["low"].iloc[-24:-2].min())
    range_pos = _sniper_range_position(price_mid, h4_low, h4_high)
    long_context = range_pos <= (0.44 if STRESS_TEST_ACTIVE else 0.38)
    short_context = range_pos >= (0.56 if STRESS_TEST_ACTIVE else 0.62)
    if not long_context and not short_context:
        return blocked_signal(branch_name, "NO PREMIUM DISCOUNT", f"RNG {range_pos:.2f}")

    long_sweep = _find_liquidity_sweep(df, "BUY")
    short_sweep = _find_liquidity_sweep(df, "SELL")
    signal = None
    sweep_info = None
    if long_context and long_sweep and long_sweep["sweep_extreme"] <= h4_low + atr_h4 * 0.35:
        signal = "BUY"
        sweep_info = long_sweep
    elif short_context and short_sweep and short_sweep["sweep_extreme"] >= h4_high - atr_h4 * 0.35:
        signal = "SELL"
        sweep_info = short_sweep
    else:
        return blocked_signal(branch_name, "NO EXTERNAL SWEEP")

    entry_price = float(tick.ask) if signal == "BUY" else float(tick.bid)
    retrace_ok, retrace_ratio = _retrace_ok(entry_price, sweep_info, signal)
    if not retrace_ok:
        return blocked_signal(branch_name, "NO CLEAN RETRACE", f"RET {retrace_ratio:.2f}" if retrace_ratio is not None else "---")

    zone = _get_supply_demand_zone(df_h1, signal)
    in_zone = _price_in_zone(price_mid, zone, float(df_h1["atr"].iloc[-2] or 0.0), 0.58 if STRESS_TEST_ACTIVE else 0.45)
    tp1, tp2 = _reversal_tp_levels(df_h4, entry_price, signal)
    sl_price = sweep_info["sweep_extreme"] - atr_m5 * 0.18 if signal == "BUY" else sweep_info["sweep_extreme"] + atr_m5 * 0.18
    rr = abs(tp2 - entry_price) / max(abs(entry_price - sl_price), 1e-9)
    displacement_ok = sweep_info["body"] >= sweep_info["atr"] * (0.76 if STRESS_TEST_ACTIVE else 0.85)
    bos_ok = True
    confluences = sum(
        1
        for x in [
            True,
            in_zone,
            True,
            bos_ok,
            displacement_ok,
            retrace_ok,
            True,
            rr >= (1.9 if STRESS_TEST_ACTIVE else 2.2),
        ]
        if x
    )
    if confluences < (5 if STRESS_TEST_ACTIVE else 6):
        return blocked_signal(branch_name, "LOW CONFLUENCE", f"{confluences}/8")
    if not _allow_liquidity_sniper_priority(symbol, confluences):
        return blocked_signal(branch_name, "LOWER PRIORITY", f"{confluences}/8")

    confidence = int(clamp(56 + confluences * 5 + max(0.0, min(rr, 3.8) - 2.2) * 8, 0, 100))
    invalidation = sweep_info["sweep_extreme"]
    details = {
        "instrument": profile["display"],
        "strategy": "Reversal after External Liquidity Sweep",
        "direction": "LONG" if signal == "BUY" else "SHORT",
        "entry_zone": f"{entry_price:.2f} | retrace {retrace_ratio:.2f}",
        "stop_loss": round(sl_price, 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "rr": round(rr, 2),
        "confluences": confluences,
        "invalidation_level": round(invalidation, 5),
        "confidence_score": confidence,
        "entry_price": entry_price,
        "take_profit": f"TP1 {tp1:.2f} | TP2 {tp2:.2f}",
        "summary": (
            f"{profile['display']} | Reversal after External Sweep | {'LONG' if signal == 'BUY' else 'SHORT'} | "
            f"zone {entry_price:.2f} | SL {sl_price:.2f} | TP1 {tp1:.2f} | TP2 {tp2:.2f} | "
            f"RR {rr:.2f} | {confluences}/8 | invalid {invalidation:.2f} | conf {confidence}"
        ),
    }
    return {
        "signal": signal,
        "confidence": confidence,
        "name": branch_name,
        "execution_df": df,
        "sl_price": round(sl_price, 5),
        "tp_price": round(tp2, 5),
        "signal_key": f"{family}|ELS|{df.iloc[-2]['time'].isoformat()}|{signal}",
        "signal_details": details,
    }


def get_liquidity_sniper_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = LIQUIDITY_SNIPER_PROFILES.get(family)
    if not profile:
        return None
    if not _time_in_exchange_windows(profile["session_tz"], profile["session_windows"]) and strategy_cfg["name"].endswith("_REVERSAL_SWEEP"):
        return None

    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_h1 is None or df_h4 is None or len(df_h1) < 20 or len(df_h4) < 20:
        return None
    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    close_h1 = float(df_h1["close"].iloc[-2])
    h4_high = float(df_h4["high"].iloc[-18:-2].max())
    h4_low = float(df_h4["low"].iloc[-18:-2].min())
    range_pos = _sniper_range_position(close_h1, h4_low, h4_high)
    if signal == "BUY" and range_pos >= 0.78:
        return "HTF external liquidity reached"
    if signal == "SELL" and range_pos <= 0.22:
        return "HTF external liquidity reached"
    recent_range = abs(float(df_h1["high"].iloc[-2] - df_h1["low"].iloc[-2]))
    if recent_range < atr_h1 * 0.45:
        return "displacement faded on H1"
    return None


def strategy_continuation_liquidity_pullback(df, symbol, branch_name, family):
    return _build_continuation_liquidity_signal(df, symbol, branch_name, family)


def strategy_reversal_after_external_sweep(df, symbol, branch_name, family):
    return _build_reversal_external_sweep_signal(df, symbol, branch_name, family)


def _make_liquidity_runner(kind, branch_name, family):
    if kind == "CONTINUATION":
        return lambda df, symbol, _branch=branch_name, _family=family: strategy_continuation_liquidity_pullback(df, symbol, _branch, _family)
    return lambda df, symbol, _branch=branch_name, _family=family: strategy_reversal_after_external_sweep(df, symbol, _branch, _family)


def strategy_trend_vwap(df, symbol):
    trend_strength = compute_trend_strength(df)
    regime = detect_market_regime(df)
    attack = is_attack_runtime()
    hour = get_broker_hour()
    is_xau = symbol_in_family(symbol, "XAUUSD")
    overnight_xau = is_xau and (hour >= 21 or hour <= 2)
    friction_xau = is_xau and (5 <= hour <= 7 or 12 <= hour <= 13)

    min_trend = 0.10 if attack else 0.28
    vwap_limit = 1.85 if attack else 1.22
    confirm_pad = 0.18 if attack else 0.10
    ema_hold_pad = 0.24 if attack else 0.15
    body_floor = 0.025 if attack else 0.05
    wick_mult = 1.50 if attack else 1.22
    wick_atr = 0.22 if attack else 0.16

    if overnight_xau:
        min_trend = max(min_trend, 0.12 if attack else 0.20)
        if trend_strength >= (0.85 if attack else 0.65):
            vwap_limit = max(vwap_limit, 3.60 if attack else 2.80)
        confirm_pad = 0.22 if attack else 0.12
        ema_hold_pad = 0.28 if attack else 0.18
        body_floor = 0.022 if attack else 0.042
        wick_mult = 1.58 if attack else 1.30
        wick_atr = 0.24 if attack else 0.17
    elif friction_xau:
        min_trend = max(min_trend, 0.16 if attack else 0.32)
        vwap_limit = min(vwap_limit, 1.60 if attack else 1.10)
        confirm_pad = max(confirm_pad, 0.22 if attack else 0.14)
        ema_hold_pad = max(ema_hold_pad, 0.28 if attack else 0.18)
        body_floor = max(body_floor, 0.035 if attack else 0.06)
        wick_mult = min(wick_mult, 1.38 if attack else 1.14)
        wick_atr = min(wick_atr, 0.20 if attack else 0.14)
    if regime == "CHAOS" or trend_strength < min_trend:
        return blocked_signal("XAU_TREND", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    atr = df["atr"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    candle_key = last["time"].isoformat() if "time" in df.columns else str(df.index[-1])
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    distance = abs(price - vwap)
    clean_long_candle = wick_up <= max(body * wick_mult, atr * wick_atr)
    clean_short_candle = wick_down <= max(body * wick_mult, atr * wick_atr)
    strong_body = body >= atr * body_floor

    sig, conf, strat = gold_profit_entry(df)

    if sig == "NEUTRAL":
        if (
            ema20 > ema50
            and price > vwap
            and distance <= atr * vwap_limit
            and price >= prev["high"] - atr * confirm_pad
            and last["low"] >= ema20 - atr * ema_hold_pad
            and strong_body
            and clean_long_candle
        ):
            return {"signal": "BUY", "confidence": 72, "name": "XAU_TREND", "signal_key": f"XTRD|BUY|{candle_key}"}
        if (
            ema20 < ema50
            and price < vwap
            and distance <= atr * vwap_limit
            and price <= prev["low"] + atr * confirm_pad
            and last["high"] <= ema20 + atr * ema_hold_pad
            and strong_body
            and clean_short_candle
            and trend_strength >= (0.28 if attack else 0.42)
        ):
            return {"signal": "SELL", "confidence": 72, "name": "XAU_TREND", "signal_key": f"XTRD|SELL|{candle_key}"}
        if ema20 > ema50 and price > vwap and distance >= atr * vwap_limit:
            return blocked_signal("XAU_TREND", "VWAP TOO FAR", pretrigger_text("RETRACE", distance - atr * vwap_limit, atr))
        if ema20 < ema50 and price < vwap and distance >= atr * vwap_limit:
            return blocked_signal("XAU_TREND", "VWAP TOO FAR", pretrigger_text("RETRACE", distance - atr * vwap_limit, atr))
        if ema20 > ema50 and price <= vwap:
            return blocked_signal("XAU_TREND", "BELOW VWAP", pretrigger_text("VWAP", vwap - price, atr))
        if ema20 < ema50 and price >= vwap:
            return blocked_signal("XAU_TREND", "ABOVE VWAP", pretrigger_text("VWAP", price - vwap, atr))
        if not strong_body:
            return blocked_signal("XAU_TREND", "WEAK BODY", pretrigger_text("BODY", atr * body_floor - body, atr))
        return blocked_signal("XAU_TREND", "EMA NOT READY", pretrigger_text("EMA", abs(ema20 - ema50), atr))

    mtf_sig = get_multi_tf_signal(symbol)[0]
    if is_xau and sig == "SELL" and mtf_sig != "SELL":
        return blocked_signal("XAU_TREND", "MTF NOT ALIGNED", "SELL NEEDS M1/M5/M15")
    if mtf_sig not in {sig, "NEUTRAL"}:
        return blocked_signal("XAU_TREND", "MTF NOT ALIGNED", "ALIGN M1/M5/M15")
    if mtf_sig == "NEUTRAL":
        conf = max(68, int(conf * 0.92))

    if sig == "BUY":
        if distance > atr * vwap_limit:
            return blocked_signal("XAU_TREND", "VWAP TOO FAR", pretrigger_text("RETRACE", distance - atr * vwap_limit, atr))
        if last["low"] < ema20 - atr * ema_hold_pad:
            return blocked_signal("XAU_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", (ema20 - atr * ema_hold_pad) - last["low"], atr))
        if not strong_body:
            return blocked_signal("XAU_TREND", "WEAK BODY", pretrigger_text("BODY", atr * body_floor - body, atr))
        if not clean_long_candle:
            return blocked_signal("XAU_TREND", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * wick_mult, atr * wick_atr), atr))
        if price < prev["high"] - atr * confirm_pad:
            return blocked_signal("XAU_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", (prev["high"] - atr * confirm_pad) - price, atr))
        conf = max(conf, 72)
    elif sig == "SELL":
        if distance > atr * vwap_limit:
            return blocked_signal("XAU_TREND", "VWAP TOO FAR", pretrigger_text("RETRACE", distance - atr * vwap_limit, atr))
        if last["high"] > ema20 + atr * ema_hold_pad:
            return blocked_signal("XAU_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", last["high"] - (ema20 + atr * ema_hold_pad), atr))
        if not strong_body:
            return blocked_signal("XAU_TREND", "WEAK BODY", pretrigger_text("BODY", atr * body_floor - body, atr))
        if not clean_short_candle:
            return blocked_signal("XAU_TREND", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * wick_mult, atr * wick_atr), atr))
        if price > prev["low"] + atr * confirm_pad:
            return blocked_signal("XAU_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", price - (prev["low"] + atr * confirm_pad), atr))
        conf = max(conf, 72)

    return {"signal": sig, "confidence": conf, "name": "XAU_TREND", "signal_key": f"XTRD|{sig}|{candle_key}"}


def strategy_liquidity_sweep(df, symbol):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    atr = df["atr"].iloc[-1]

    if len(df) < 25:
        return blocked_signal("LSR", "NO DATA")

    recent_high = high.rolling(20).max().iloc[-2]
    recent_low = low.rolling(20).min().iloc[-2]

    last_high = high.iloc[-1]
    last_low = low.iloc[-1]
    last_close = close.iloc[-1]

    regime = detect_market_regime(df)
    if regime == "TREND":
        return blocked_signal("LSR", "TREND BLOCKED")

    if last_low < recent_low and last_close > recent_low + atr * 0.15:
        if abs(last_close - last_low) > atr * 0.6:
            return {"signal": "BUY", "confidence": 70, "name": "LSR"}

    if last_high > recent_high and last_close < recent_high - atr * 0.15:
        if abs(last_high - last_close) > atr * 0.6:
            return {"signal": "SELL", "confidence": 70, "name": "LSR"}

    if last_low < recent_low and last_close <= recent_low + atr * 0.15:
        return blocked_signal("LSR", "NO RECLAIM")
    if last_high > recent_high and last_close >= recent_high - atr * 0.15:
        return blocked_signal("LSR", "NO REJECT")
    return blocked_signal("LSR", "NO SWEEP")


def strategy_gold_scalping(df, symbol):
    if not scalping_enabled:
        return blocked_signal("XAU_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < SCALPING_SESSION_START or hour > SCALPING_SESSION_END:
        return blocked_signal("XAU_SCALP", "OUT SESSION")

    df_m1 = get_m1_cached(symbol)
    if df_m1 is None or len(df_m1) < 60 or len(df) < 60:
        return blocked_signal("XAU_SCALP", "NO M1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("XAU_SCALP", "NO TICK")

    spread = tick.ask - tick.bid
    atr_m1 = df_m1["atr"].iloc[-1]
    if spread > atr_m1 * 0.14:
        return blocked_signal("XAU_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    open_m1 = df_m1["open"]

    ema9_m1 = close_m1.ewm(span=9).mean()
    ema20_m1 = close_m1.ewm(span=20).mean()
    ema50_m1 = close_m1.ewm(span=50).mean()
    ema20_m5 = df["close"].ewm(span=20).mean()
    ema50_m5 = df["close"].ewm(span=50).mean()
    rsi_m1 = compute_rsi(close_m1, 14)

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    trend_up = ema20_m5.iloc[-1] > ema50_m5.iloc[-1] and ema9_m1.iloc[-1] > ema20_m1.iloc[-1] > ema50_m1.iloc[-1]
    trend_down = ema20_m5.iloc[-1] < ema50_m5.iloc[-1] and ema9_m1.iloc[-1] < ema20_m1.iloc[-1] < ema50_m1.iloc[-1]

    long_pullback = min(prev["low"], last["low"]) <= (ema20_m1.iloc[-1] + atr_m1 * 0.10)
    long_reclaim = last["close"] > ema9_m1.iloc[-1]
    long_break = last["close"] >= (high_m1.iloc[-2] - atr_m1 * 0.10)
    long_body = (last["close"] - last["open"]) > atr_m1 * 0.08
    if trend_up and long_pullback and long_reclaim and long_body and long_break and 48 <= rsi_m1.iloc[-1] <= 74:
        return {"signal": "BUY", "confidence": 74, "name": "XAU_SCALP", "execution_df": df_m1}

    short_pullback = max(prev["high"], last["high"]) >= (ema20_m1.iloc[-1] - atr_m1 * 0.10)
    short_reject = last["close"] < ema9_m1.iloc[-1]
    short_break = last["close"] <= (low_m1.iloc[-2] + atr_m1 * 0.10)
    short_body = (last["open"] - last["close"]) > atr_m1 * 0.08
    if trend_down and short_pullback and short_reject and short_body and short_break and 26 <= rsi_m1.iloc[-1] <= 52:
        return {"signal": "SELL", "confidence": 74, "name": "XAU_SCALP", "execution_df": df_m1}

    if trend_up:
        if not long_pullback:
            return blocked_signal("XAU_SCALP", "NO PULLBACK")
        if not long_reclaim:
            return blocked_signal("XAU_SCALP", "EMA9 NOT RECLAIMED")
        if not long_body:
            return blocked_signal("XAU_SCALP", "WEAK CANDLE")
        if not long_break:
            return blocked_signal("XAU_SCALP", "NO BREAKOUT")
        return blocked_signal("XAU_SCALP", "RSI BLOCKED")
    if trend_down:
        if not short_pullback:
            return blocked_signal("XAU_SCALP", "NO PULLBACK")
        if not short_reject:
            return blocked_signal("XAU_SCALP", "EMA9 NOT LOST")
        if not short_body:
            return blocked_signal("XAU_SCALP", "WEAK CANDLE")
        if not short_break:
            return blocked_signal("XAU_SCALP", "NO BREAKDOWN")
        return blocked_signal("XAU_SCALP", "RSI BLOCKED")
    return blocked_signal("XAU_SCALP", "TREND NOT READY", pretrigger_text("EMA", abs(ema20_m5.iloc[-1] - ema50_m5.iloc[-1]), df["atr"].iloc[-1]))


def strategy_xau_m1_scalp(df, symbol):
    if not scalping_enabled:
        return blocked_signal("XAU_M1_SCALP", "SCALP OFF")

    now = get_broker_now()
    if not (now.hour >= 11 or now.hour <= 4):
        return blocked_signal("XAU_M1_SCALP", "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 220)
    df_m15 = get_m15_cached(symbol)
    if df_m1 is None or len(df_m1) < 90:
        return blocked_signal("XAU_M1_SCALP", "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 50:
        return blocked_signal("XAU_M1_SCALP", "NO M15 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("XAU_M1_SCALP", "NO TICK")

    atr_m1 = df_m1["atr"].iloc[-1]
    atr_m5 = df["atr"].iloc[-1]
    spread = tick.ask - tick.bid
    attack = is_attack_runtime()
    if spread > atr_m1 * (0.42 if attack else 0.34):
        return blocked_signal("XAU_M1_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    ema8 = close_m1.ewm(span=8).mean()
    ema21 = close_m1.ewm(span=21).mean()
    ema50 = close_m1.ewm(span=50).mean()
    ema20_m5 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50_m5 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    rsi = compute_rsi(close_m1, 14)
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    candle_key = last["time"].isoformat() if "time" in df_m1.columns else str(df_m1.index[-1])
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    recent_high = high_m1.tail(6).max()
    recent_low = low_m1.tail(6).min()
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    trend_strength = compute_trend_strength(df)

    min_trend = 0.09 if attack else 0.12
    vwap_bias_pad = atr_m5 * (0.05 if attack else 0.04)
    long_bias = ema20_m5 > ema50_m5 and ema20_m15 > ema50_m15 and price > vwap - vwap_bias_pad and trend_strength >= min_trend
    short_bias = ema20_m5 < ema50_m5 and ema20_m15 < ema50_m15 and price < vwap + vwap_bias_pad and trend_strength >= min_trend

    long_stack = ema8.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1]
    short_stack = ema8.iloc[-1] < ema21.iloc[-1] < ema50.iloc[-1]
    long_pullback = min(prev["low"], last["low"]) <= (ema21.iloc[-1] + atr_m1 * (0.26 if attack else 0.18))
    short_pullback = max(prev["high"], last["high"]) >= (ema21.iloc[-1] - atr_m1 * (0.26 if attack else 0.18))
    long_reclaim = last["close"] > ema8.iloc[-1] and last["close"] > last["open"]
    short_reject = last["close"] < ema8.iloc[-1] and last["close"] < last["open"]
    long_break = last["close"] >= (recent_high - atr_m1 * (0.18 if attack else 0.08))
    short_break = last["close"] <= (recent_low + atr_m1 * (0.18 if attack else 0.08))
    strong_body = body >= atr_m1 * (0.05 if attack else 0.06)
    clean_long = wick_up <= max(body * (1.24 if attack else 1.08), atr_m1 * (0.20 if attack else 0.14))
    clean_short = wick_down <= max(body * (1.24 if attack else 1.08), atr_m1 * (0.20 if attack else 0.14))

    if long_bias and long_stack and long_pullback and long_reclaim and long_break and strong_body and clean_long and 46 <= rsi.iloc[-1] <= 80:
        conf = 78
        if price >= vwap + atr_m5 * 0.08 and trend_strength > 0.70:
            conf = 82
        return {"signal": "BUY", "confidence": conf, "name": "XAU_M1_SCALP", "execution_df": df_m1, "signal_key": f"XM1S|BUY|{candle_key}"}

    if short_bias and short_stack and short_pullback and short_reject and short_break and strong_body and clean_short and 20 <= rsi.iloc[-1] <= 54:
        conf = 78
        if price <= vwap - atr_m5 * 0.08 and trend_strength > 0.70:
            conf = 82
        return {"signal": "SELL", "confidence": conf, "name": "XAU_M1_SCALP", "execution_df": df_m1, "signal_key": f"XM1S|SELL|{candle_key}"}

    if not long_bias and not short_bias:
        return blocked_signal("XAU_M1_SCALP", "TREND NOT READY", f"STR {round(trend_strength, 2)}")
    if long_bias:
        if not long_stack:
            return blocked_signal("XAU_M1_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema21.iloc[-1] - ema8.iloc[-1], ema50.iloc[-1] - ema21.iloc[-1]), atr_m1))
        if not long_pullback:
            return blocked_signal("XAU_M1_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev['low'], last['low']) - (ema21.iloc[-1] + atr_m1 * (0.26 if attack else 0.18)), atr_m1))
        if not long_reclaim:
            return blocked_signal("XAU_M1_SCALP", "EMA8 NOT RECLAIMED", pretrigger_text("EMA8", ema8.iloc[-1] - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal("XAU_M1_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
        if not clean_long:
            return blocked_signal("XAU_M1_SCALP", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.24 if attack else 1.08), atr_m1 * (0.20 if attack else 0.14)), atr_m1))
        if not long_break:
            return blocked_signal("XAU_M1_SCALP", "NO BREAKOUT", pretrigger_text("BRK", (recent_high - atr_m1 * (0.16 if attack else 0.04)) - last["close"], atr_m1))
        return blocked_signal("XAU_M1_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")

    if not short_stack:
        return blocked_signal("XAU_M1_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema8.iloc[-1] - ema21.iloc[-1], ema21.iloc[-1] - ema50.iloc[-1]), atr_m1))
    if not short_pullback:
        return blocked_signal("XAU_M1_SCALP", "NO PULLBACK", pretrigger_text("PB", (ema21.iloc[-1] - atr_m1 * (0.26 if attack else 0.18)) - max(prev['high'], last['high']), atr_m1))
    if not short_reject:
        return blocked_signal("XAU_M1_SCALP", "EMA8 NOT LOST", pretrigger_text("EMA8", last["close"] - ema8.iloc[-1], atr_m1))
    if not strong_body:
        return blocked_signal("XAU_M1_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
    if not clean_short:
        return blocked_signal("XAU_M1_SCALP", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.24 if attack else 1.08), atr_m1 * (0.20 if attack else 0.14)), atr_m1))
    if not short_break:
        return blocked_signal("XAU_M1_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - (recent_low + atr_m1 * (0.16 if attack else 0.04)), atr_m1))
    return blocked_signal("XAU_M1_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")


def strategy_btc_trend(df, symbol):
    df_m15 = get_m15_cached(symbol)
    df_m1 = get_m1_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("BTC_TREND", "NO M15 DATA")
    if df_m1 is None or len(df_m1) < 80:
        return blocked_signal("BTC_TREND", "NO M1 DATA")

    attack = is_attack_runtime()
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    ema9_m1 = df_m1["close"].ewm(span=9).mean().iloc[-1]
    ema20_m1 = df_m1["close"].ewm(span=20).mean().iloc[-1]
    ema50_m1 = df_m1["close"].ewm(span=50).mean().iloc[-1]

    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr <= 0 or atr_m1 <= 0:
        return blocked_signal("BTC_TREND", "NO ATR")

    trend_strength = compute_trend_strength(df)
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)
    rsi = compute_rsi(df["close"], 14).iloc[-1]

    last = df.iloc[-1]
    m1_last = df_m1.iloc[-1]
    m1_prev = df_m1.iloc[-2]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    vwap_dist = abs(price - vwap)
    recent_high = df["high"].rolling(14).max().iloc[-2]
    recent_low = df["low"].rolling(14).min().iloc[-2]

    trend_min = 0.18 if attack else 0.30
    m15_min = 0.08 if attack else 0.16
    if trend_strength >= (1.00 if attack else 0.86):
        m15_min = max(0.08 if attack else 0.14, m15_min - 0.04)
    vwap_limit = 4.05 if attack else 3.45
    if trend_strength >= (0.90 if attack else 1.05):
        vwap_limit += 0.75
    if m15_strength >= (0.55 if attack else 0.42):
        vwap_limit += 0.55
    long_trigger = recent_high - atr * (0.10 if attack else 0.04)
    short_trigger = recent_low + atr * (0.10 if attack else 0.04)
    long_break_gap = max(0.0, long_trigger - price)
    short_break_gap = max(0.0, price - short_trigger)
    long_pullback = min(m1_prev["low"], m1_last["low"]) <= (ema20_m1 + atr_m1 * (0.44 if attack else 0.34))
    short_pullback = max(m1_prev["high"], m1_last["high"]) >= (ema20_m1 - atr_m1 * (0.44 if attack else 0.34))
    reclaim_slack = atr_m1 * (0.028 if attack else 0.014)
    long_reclaim = m1_last["close"] >= ema9_m1 - reclaim_slack and ema9_m1 > ema20_m1 > ema50_m1
    short_reclaim = m1_last["close"] <= ema9_m1 + reclaim_slack and ema9_m1 < ema20_m1 < ema50_m1
    strong_body = body >= atr * (0.032 if attack else 0.045)
    clean_long_candle = wick_up <= max(body * (1.34 if attack else 1.10), atr * 0.18)
    clean_short_candle = wick_down <= max(body * (1.34 if attack else 1.10), atr * 0.18)
    long_break_failed = m1_last["high"] >= long_trigger and m1_last["close"] < long_trigger
    short_break_failed = m1_last["low"] <= short_trigger and m1_last["close"] > short_trigger

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and vwap_dist <= atr * vwap_limit
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and long_pullback
        and long_reclaim
        and strong_body
        and clean_long_candle
        and price >= long_trigger
        and 48 <= rsi <= 80
    ):
        confidence = 78 if trend_strength < 1.10 else 82
        return {"signal": "BUY", "confidence": confidence, "name": "BTC_TREND"}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and price < vwap
        and vwap_dist <= atr * vwap_limit
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and short_pullback
        and short_reclaim
        and strong_body
        and clean_short_candle
        and price <= short_trigger
        and 20 <= rsi <= 52
    ):
        confidence = 78 if trend_strength < 1.10 else 82
        return {"signal": "SELL", "confidence": confidence, "name": "BTC_TREND"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("BTC_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("BTC_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if trend_strength < trend_min:
        return blocked_signal("BTC_TREND", "TREND WEAK", f"STR {round(trend_strength, 2)}")
    if m15_strength < m15_min:
        return blocked_signal("BTC_TREND", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("BTC_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("BTC_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if vwap_dist > atr * vwap_limit:
        return blocked_signal("BTC_TREND", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * vwap_limit, atr))
    if not strong_body:
        return blocked_signal("BTC_TREND", "WEAK BODY", pretrigger_text("BODY", atr * 0.10 - body, atr))
    if ema20 > ema50 and not long_pullback:
        return blocked_signal("BTC_TREND", "NO PULLBACK", pretrigger_text("PB", min(m1_prev["low"], m1_last["low"]) - (ema20_m1 + atr_m1 * 0.14), atr_m1))
    if ema20 < ema50 and not short_pullback:
        return blocked_signal("BTC_TREND", "NO PULLBACK", pretrigger_text("PB", (ema20_m1 - atr_m1 * 0.14) - max(m1_prev["high"], m1_last["high"]), atr_m1))
    if ema20 > ema50 and not long_reclaim:
        return blocked_signal("BTC_TREND", "RECLAIM MISS", pretrigger_text("EMA9", ema9_m1 - m1_last["close"], atr_m1))
    if ema20 < ema50 and not short_reclaim:
        return blocked_signal("BTC_TREND", "RECLAIM MISS", pretrigger_text("EMA9", m1_last["close"] - ema9_m1, atr_m1))
    if ema20 > ema50 and long_break_failed:
        return blocked_signal("BTC_TREND", "BREAKOUT MISSED", pretrigger_text("BRK", max(0.0, long_trigger - m1_last["close"]), atr))
    if ema20 < ema50 and short_break_failed:
        return blocked_signal("BTC_TREND", "BREAKDOWN MISSED", pretrigger_text("BRK", max(0.0, m1_last["close"] - short_trigger), atr))
    if ema20 > ema50 and price < long_trigger:
        reason = "BREAKOUT READY" if long_break_gap <= atr * (0.22 if attack else 0.14) else "NO BREAKOUT"
        return blocked_signal("BTC_TREND", reason, pretrigger_text("BRK", long_break_gap, atr))
    if ema20 < ema50 and price > short_trigger:
        reason = "BREAKDOWN READY" if short_break_gap <= atr * (0.22 if attack else 0.14) else "NO BREAKDOWN"
        return blocked_signal("BTC_TREND", reason, pretrigger_text("BRK", short_break_gap, atr))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("BTC_TREND", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 0.85, atr * 0.12), atr))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("BTC_TREND", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 0.85, atr * 0.12), atr))
    return blocked_signal("BTC_TREND", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_eth_pulse(df, symbol):
    df_m15 = get_m15_cached(symbol)
    df_m1 = get_m1_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("ETH_PULSE", "NO M15 DATA")
    if df_m1 is None or len(df_m1) < 90:
        return blocked_signal("ETH_PULSE", "NO M1 DATA")

    attack = is_attack_runtime()
    close_m1 = df_m1["close"]
    ema9_m1 = close_m1.ewm(span=9).mean()
    ema21_m1 = close_m1.ewm(span=21).mean()
    ema55_m1 = close_m1.ewm(span=55).mean()
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]

    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr <= 0 or atr_m1 <= 0:
        return blocked_signal("ETH_PULSE", "NO ATR")

    trend_strength = compute_trend_strength(df)
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)
    rsi_m1 = compute_rsi(close_m1, 14)

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    vwap_dist = abs(price - vwap)
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    long_pullback_band = ema21_m1.iloc[-1] + atr_m1 * (0.24 if attack else 0.20)
    short_pullback_band = ema21_m1.iloc[-1] - atr_m1 * (0.24 if attack else 0.20)
    long_break_trigger = df_m1["high"].iloc[-2] - atr_m1 * (0.12 if attack else 0.05)
    short_break_trigger = df_m1["low"].iloc[-2] + atr_m1 * (0.12 if attack else 0.05)
    trend_min = 0.18 if attack else 0.34
    m15_min = 0.12 if attack else 0.20

    long_setup = (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and vwap_dist <= atr * (3.05 if attack else 2.60)
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and ema9_m1.iloc[-1] > ema21_m1.iloc[-1] > ema55_m1.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema9_m1.iloc[-1]
        and (last["close"] - last["open"]) > atr_m1 * (0.06 if attack else 0.06)
        and wick_up <= max(body * 1.22, atr_m1 * 0.16)
        and last["close"] >= long_break_trigger
        and 47 <= rsi_m1.iloc[-1] <= 78
    )
    if long_setup:
        confidence = 76 if trend_strength < 0.95 else 80
        return {"signal": "BUY", "confidence": confidence, "name": "ETH_PULSE", "execution_df": df_m1}

    short_setup = (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and price < vwap
        and vwap_dist <= atr * (3.05 if attack else 2.60)
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and ema9_m1.iloc[-1] < ema21_m1.iloc[-1] < ema55_m1.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema9_m1.iloc[-1]
        and (last["open"] - last["close"]) > atr_m1 * (0.06 if attack else 0.06)
        and wick_down <= max(body * 1.22, atr_m1 * 0.16)
        and last["close"] <= short_break_trigger
        and 22 <= rsi_m1.iloc[-1] <= 53
    )
    if short_setup:
        confidence = 76 if trend_strength < 0.95 else 80
        return {"signal": "SELL", "confidence": confidence, "name": "ETH_PULSE", "execution_df": df_m1}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("ETH_PULSE", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("ETH_PULSE", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if trend_strength < trend_min:
        return blocked_signal("ETH_PULSE", "TREND WEAK", f"STR {round(trend_strength, 2)}")
    if m15_strength < m15_min:
        return blocked_signal("ETH_PULSE", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("ETH_PULSE", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("ETH_PULSE", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if vwap_dist > atr * (3.05 if attack else 2.60):
        return blocked_signal("ETH_PULSE", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * (3.05 if attack else 2.60), atr))
    if ema20 > ema50:
        if not (ema9_m1.iloc[-1] > ema21_m1.iloc[-1] > ema55_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA STACK MISS", pretrigger_text("STACK", max(ema21_m1.iloc[-1] - ema9_m1.iloc[-1], ema55_m1.iloc[-1] - ema21_m1.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("ETH_PULSE", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema9_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA9 NOT HELD", pretrigger_text("EMA9", ema9_m1.iloc[-1] - last["close"], atr_m1))
        if not ((last["close"] - last["open"]) > atr_m1 * (0.06 if attack else 0.06)):
            return blocked_signal("ETH_PULSE", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.06 if attack else 0.06) - (last["close"] - last["open"]), atr_m1))
        if not (last["close"] >= long_break_trigger):
            return blocked_signal("ETH_PULSE", "NO BREAKOUT", pretrigger_text("BRK", long_break_trigger - last["close"], atr_m1))
        if not (wick_up <= max(body * 1.22, atr_m1 * 0.16)):
            return blocked_signal("ETH_PULSE", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 1.22, atr_m1 * 0.16), atr_m1))
        return blocked_signal("ETH_PULSE", "RSI BLOCKED", f"RSI {round(rsi_m1.iloc[-1], 1)}")
    if ema20 < ema50:
        if not (ema9_m1.iloc[-1] < ema21_m1.iloc[-1] < ema55_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA STACK MISS", pretrigger_text("STACK", max(ema9_m1.iloc[-1] - ema21_m1.iloc[-1], ema21_m1.iloc[-1] - ema55_m1.iloc[-1]), atr_m1))
        if not (max(prev["high"], last["high"]) >= short_pullback_band):
            return blocked_signal("ETH_PULSE", "NO PULLBACK", pretrigger_text("PB", short_pullback_band - max(prev["high"], last["high"]), atr_m1))
        if not (last["close"] < ema9_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA9 NOT LOST", pretrigger_text("EMA9", last["close"] - ema9_m1.iloc[-1], atr_m1))
        if not ((last["open"] - last["close"]) > atr_m1 * (0.06 if attack else 0.06)):
            return blocked_signal("ETH_PULSE", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.06 if attack else 0.06) - (last["open"] - last["close"]), atr_m1))
        if not (last["close"] <= short_break_trigger):
            return blocked_signal("ETH_PULSE", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - short_break_trigger, atr_m1))
        if not (wick_down <= max(body * 1.22, atr_m1 * 0.16)):
            return blocked_signal("ETH_PULSE", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 1.22, atr_m1 * 0.16), atr_m1))
        return blocked_signal("ETH_PULSE", "RSI BLOCKED", f"RSI {round(rsi_m1.iloc[-1], 1)}")
    return blocked_signal("ETH_PULSE", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_xau_orb(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("XAU_ORB", "NO M15 DATA")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 780)
    if df_m1 is None or len(df_m1) < 160:
        return blocked_signal("XAU_ORB", "NO M1 DATA")

    range_start, range_end = broker_window_from_local_df(df_m1, 14, 0, 14, 30)
    session_start, session_end = broker_window_from_local_df(df_m1, 14, 30, 23, 0)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("XAU_ORB", "BUILDING RANGE", "ORB 14:00-14:30")
    if broker_now > session_end:
        return blocked_signal("XAU_ORB", "OUT SESSION", "ORB 14:30-23:00")

    orb_window = df_m1[(df_m1["time"] >= range_start) & (df_m1["time"] < range_end)]
    if len(orb_window) < 12:
        return blocked_signal("XAU_ORB", "RANGE INCOMPLETE", f"{len(orb_window)} bars")

    post_orb = df_m1[(df_m1["time"] >= session_start) & (df_m1["time"] <= session_end)]
    if len(post_orb) < 5:
        return blocked_signal("XAU_ORB", "WAIT POST OPEN", "POST 14:30")

    orb_high = orb_window["high"].max()
    orb_low = orb_window["low"].min()
    orb_range = orb_high - orb_low
    last = post_orb.iloc[-1]
    prev = post_orb.iloc[-2]
    atr_m1 = post_orb["atr"].iloc[-1]
    atr_m5 = df["atr"].iloc[-1]
    if atr_m1 <= 0 or atr_m5 <= 0:
        return blocked_signal("XAU_ORB", "NO ATR")

    min_range = 0.18 if attack else 0.28
    max_range = 5.20 if attack else 4.20
    if orb_range < atr_m5 * min_range:
        return blocked_signal("XAU_ORB", "RANGE TOO TIGHT", pretrigger_text("ORB", atr_m5 * min_range - orb_range, atr_m5))
    if orb_range > atr_m5 * max_range:
        return blocked_signal("XAU_ORB", "RANGE TOO WIDE", pretrigger_text("ORB", orb_range - atr_m5 * max_range, atr_m5))

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    trend_strength = compute_trend_strength(df)
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    recent_high = post_orb["high"].tail(8).max()
    recent_low = post_orb["low"].tail(8).min()

    trend_min = 0.14 if attack else 0.26
    long_trigger = orb_high + atr_m5 * (0.01 if attack else 0.03)
    short_trigger = orb_low - atr_m5 * (0.01 if attack else 0.03)
    long_hold = min(prev["low"], last["low"]) >= orb_high - atr_m5 * (0.28 if attack else 0.18)
    short_hold = max(prev["high"], last["high"]) <= orb_low + atr_m5 * (0.28 if attack else 0.18)
    strong_body = body >= atr_m5 * (0.05 if attack else 0.06)
    clean_long_candle = wick_up <= max(body * (1.55 if attack else 1.20), atr_m5 * (0.30 if attack else 0.18))
    clean_short_candle = wick_down <= max(body * (1.55 if attack else 1.20), atr_m5 * (0.30 if attack else 0.18))
    long_close_confirm = last["close"] >= max(long_trigger, prev["high"] + atr_m5 * (0.01 if attack else 0.02))
    short_close_confirm = last["close"] <= min(short_trigger, prev["low"] - atr_m5 * (0.01 if attack else 0.02))

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and trend_strength >= trend_min
        and price > vwap
        and price >= long_trigger
        and long_close_confirm
        and long_hold
        and strong_body
        and clean_long_candle
        and recent_high >= orb_high
    ):
        confidence = 78
        if price >= orb_high + atr_m5 * 0.12 and trend_strength > 0.55:
            confidence = 82
        return {"signal": "BUY", "confidence": confidence, "name": "XAU_ORB", "execution_df": post_orb}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and trend_strength >= trend_min
        and price < vwap
        and price <= short_trigger
        and short_close_confirm
        and short_hold
        and strong_body
        and clean_short_candle
        and recent_low <= orb_low
    ):
        confidence = 78
        if price <= orb_low - atr_m5 * 0.12 and trend_strength > 0.55:
            confidence = 82
        return {"signal": "SELL", "confidence": confidence, "name": "XAU_ORB", "execution_df": post_orb}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("XAU_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr_m5))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("XAU_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr_m5))
    if trend_strength < trend_min:
        return blocked_signal("XAU_ORB", "TREND WEAK", f"STR {round(trend_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("XAU_ORB", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr_m5))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("XAU_ORB", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr_m5))
    if ema20 > ema50 and price < long_trigger:
        return blocked_signal("XAU_ORB", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr_m5))
    if ema20 < ema50 and price > short_trigger:
        return blocked_signal("XAU_ORB", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr_m5))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("XAU_ORB", "NO HOLD ABOVE ORB", pretrigger_text("HOLD", (orb_high - atr_m5 * 0.20) - min(prev["low"], last["low"]), atr_m5))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("XAU_ORB", "NO HOLD BELOW ORB", pretrigger_text("HOLD", max(prev["high"], last["high"]) - (orb_low + atr_m5 * 0.20), atr_m5))
    if not strong_body:
        return blocked_signal("XAU_ORB", "WEAK OPEN DRIVE", pretrigger_text("BODY", atr_m5 * 0.05 - body, atr_m5))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("XAU_ORB", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 1.25, atr_m5 * 0.20), atr_m5))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("XAU_ORB", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 1.25, atr_m5 * 0.20), atr_m5))
    return blocked_signal("XAU_ORB", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr_m5))


def strategy_ustech_trend(df, symbol):
    trend_strength = compute_trend_strength(df)
    attack = is_attack_runtime()
    if detect_market_regime(df) == "CHAOS" or trend_strength < (0.14 if attack else 1.08):
        return blocked_signal("USTECH_TREND", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("USTECH_TREND", "NO M15 DATA")

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    breakout_high = df["high"].rolling(12).max().iloc[-2]
    breakout_low = df["low"].rolling(12).min().iloc[-2]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    vwap_dist = abs(price - vwap)
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)

    long_trigger = breakout_high + atr * (0.02 if attack else 0.05)
    short_trigger = breakout_low - atr * (0.02 if attack else 0.05)
    long_hold = last["low"] >= (ema20 - atr * (0.24 if attack else 0.12))
    short_hold = last["high"] <= (ema20 + atr * (0.24 if attack else 0.12))
    strong_body = body >= atr * (0.05 if attack else 0.08)
    clean_long_candle = wick_up <= max(body * (1.26 if attack else 1.06), atr * (0.22 if attack else 0.16))
    clean_short_candle = wick_down <= max(body * (1.26 if attack else 1.06), atr * (0.22 if attack else 0.16))
    long_close_confirm = last["close"] >= long_trigger and last["close"] > prev["high"] - atr * (0.12 if attack else 0.06)
    short_close_confirm = last["close"] <= short_trigger and last["close"] < prev["low"] + atr * (0.12 if attack else 0.06)

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and vwap_dist <= atr * (2.35 if attack else 1.90)
        and price > vwap
        and long_close_confirm
        and long_hold
        and strong_body
        and clean_long_candle
        and m15_strength >= (0.28 if attack else 0.44)
    ):
        return {"signal": "BUY", "confidence": 80, "name": "USTECH_TREND"}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and vwap_dist <= atr * (2.35 if attack else 1.90)
        and price < vwap
        and short_close_confirm
        and short_hold
        and strong_body
        and clean_short_candle
        and m15_strength >= (0.28 if attack else 0.44)
    ):
        return {"signal": "SELL", "confidence": 80, "name": "USTECH_TREND"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("USTECH_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("USTECH_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if m15_strength < (0.28 if attack else 0.44):
        return blocked_signal("USTECH_TREND", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("USTECH_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("USTECH_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if vwap_dist > atr * (2.35 if attack else 1.90):
        return blocked_signal("USTECH_TREND", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * (2.35 if attack else 1.90), atr))
    if not strong_body:
        return blocked_signal("USTECH_TREND", "WEAK BODY", pretrigger_text("BODY", atr * 0.10 - body, atr))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("USTECH_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", (ema20 - atr * 0.12) - last["low"], atr))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("USTECH_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", last["high"] - (ema20 + atr * 0.12), atr))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("USTECH_TREND", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 0.98, atr * 0.12), atr))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("USTECH_TREND", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 0.98, atr * 0.12), atr))
    if ema20 > ema50:
        if price <= long_trigger:
            return blocked_signal("USTECH_TREND", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr))
        return blocked_signal("USTECH_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(long_trigger - last["close"], prev["high"] - last["close"]), atr))
    if ema20 < ema50:
        if price >= short_trigger:
            return blocked_signal("USTECH_TREND", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr))
        return blocked_signal("USTECH_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(last["close"] - short_trigger, last["close"] - prev["low"]), atr))
    if price <= vwap:
        return blocked_signal("USTECH_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    return blocked_signal("USTECH_TREND", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_ustech_orb(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("USTECH_ORB", "NO M15 DATA")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 600)
    if df_m1 is None or len(df_m1) < 120:
        return blocked_signal("USTECH_ORB", "NO M1 DATA")

    range_start, range_end = broker_window_from_local_df(df_m1, 15, 30, 15, 45)
    session_start, session_end = broker_window_from_local_df(df_m1, 15, 45, 23, 0)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("USTECH_ORB", "BUILDING RANGE", "ORB 15:30-15:45")
    if broker_now > session_end:
        return blocked_signal("USTECH_ORB", "OUT SESSION", "ORB 15:45-23:00")

    orb_window = df_m1[(df_m1["time"] >= range_start) & (df_m1["time"] < range_end)]
    if len(orb_window) < 8:
        return blocked_signal("USTECH_ORB", "RANGE INCOMPLETE", f"{len(orb_window)} bars")

    after_orb = df_m1[(df_m1["time"] >= session_start) & (df_m1["time"] <= session_end)]
    if len(after_orb) < 3:
        return blocked_signal("USTECH_ORB", "WAIT POST OPEN", "POST 15:45")

    orb_high = orb_window["high"].max()
    orb_low = orb_window["low"].min()
    orb_size = orb_high - orb_low
    atr_m5 = df["atr"].iloc[-1]
    if atr_m5 <= 0:
        return blocked_signal("USTECH_ORB", "NO ATR")

    min_range = 0.40 if attack else 0.55
    max_range = 3.20 if attack else 2.40
    if orb_size < atr_m5 * min_range:
        return blocked_signal("USTECH_ORB", "RANGE TOO TIGHT", pretrigger_text("ORB", atr_m5 * min_range - orb_size, atr_m5))
    if orb_size > atr_m5 * max_range:
        return blocked_signal("USTECH_ORB", "RANGE TOO WIDE", pretrigger_text("ORB", orb_size - atr_m5 * max_range, atr_m5))

    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    trend_strength = compute_trend_strength(df)
    last_m1 = after_orb.iloc[-1]
    prev_m1 = after_orb.iloc[-2]
    recent_high = after_orb["high"].tail(6).max()
    recent_low = after_orb["low"].tail(6).min()

    trend_min = 0.22 if attack else 1.0
    long_break = price >= (orb_high + atr_m5 * (0.02 if attack else 0.05))
    short_break = price <= (orb_low - atr_m5 * (0.02 if attack else 0.05))
    long_hold = min(prev_m1["low"], last_m1["low"]) >= (orb_high - atr_m5 * (0.18 if attack else 0.12))
    short_hold = max(prev_m1["high"], last_m1["high"]) <= (orb_low + atr_m5 * (0.18 if attack else 0.12))
    long_body = (last_m1["close"] - last_m1["open"]) >= atr_m5 * (0.07 if attack else 0.10)
    short_body = (last_m1["open"] - last_m1["close"]) >= atr_m5 * (0.07 if attack else 0.10)

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and trend_strength > trend_min
        and long_break
        and long_hold
        and long_body
        and recent_high >= orb_high
    ):
        confidence = 74
        if price >= orb_high + atr_m5 * 0.18 and trend_strength > 1.20:
            confidence = 79
        return {"signal": "BUY", "confidence": confidence, "name": "USTECH_ORB"}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and price < vwap
        and trend_strength > trend_min
        and short_break
        and short_hold
        and short_body
        and recent_low <= orb_low
    ):
        confidence = 74
        if price <= orb_low - atr_m5 * 0.18 and trend_strength > 1.20:
            confidence = 79
        return {"signal": "SELL", "confidence": confidence, "name": "USTECH_ORB"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("USTECH_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr_m5))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("USTECH_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr_m5))
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("USTECH_ORB", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr_m5))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("USTECH_ORB", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr_m5))
    if ema20 > ema50 and not long_break:
        return blocked_signal("USTECH_ORB", "NO ORB BREAK", pretrigger_text("ORB", (orb_high + atr_m5 * 0.05) - price, atr_m5))
    if ema20 < ema50 and not short_break:
        return blocked_signal("USTECH_ORB", "NO ORB BREAK", pretrigger_text("ORB", price - (orb_low - atr_m5 * 0.05), atr_m5))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("USTECH_ORB", "NO HOLD ABOVE ORB", pretrigger_text("HOLD", (orb_high - atr_m5 * 0.12) - min(prev_m1["low"], last_m1["low"]), atr_m5))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("USTECH_ORB", "NO HOLD BELOW ORB", pretrigger_text("HOLD", max(prev_m1["high"], last_m1["high"]) - (orb_low + atr_m5 * 0.12), atr_m5))
    if not long_body and not short_body:
        return blocked_signal("USTECH_ORB", "WEAK OPEN DRIVE", pretrigger_text("BODY", atr_m5 * 0.10 - abs(last_m1["close"] - last_m1["open"]), atr_m5))
    return blocked_signal("USTECH_ORB", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr_m5))


def strategy_ustech_break_retest(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60 or len(df) < 80:
        return blocked_signal("USTECH_BREAK_RETEST", "NO DATA")

    atr = df["atr"].iloc[-1]
    if atr <= 0:
        return blocked_signal("USTECH_BREAK_RETEST", "NO ATR")

    trend_strength = compute_trend_strength(df)
    if detect_market_regime(df) == "CHAOS" or trend_strength < (0.14 if attack else 0.58):
        return blocked_signal("USTECH_BREAK_RETEST", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    ema50 = df["close"].ewm(span=50).mean()
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    breakout_ref = df.iloc[-24:-3] if len(df) >= 30 else df.iloc[:-3]
    if len(breakout_ref) < 8:
        return blocked_signal("USTECH_BREAK_RETEST", "NO STRUCTURE")

    resistance = breakout_ref["high"].max()
    support = breakout_ref["low"].min()
    body = abs(last["close"] - last["open"])
    breakout_body = abs(prev["close"] - prev["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    clean_long = wick_up <= max(body * (1.26 if attack else 1.04), atr * (0.20 if attack else 0.16))
    clean_short = wick_down <= max(body * (1.26 if attack else 1.04), atr * (0.20 if attack else 0.16))
    strong_break = breakout_body >= atr * (0.08 if attack else 0.11)
    strong_confirm = body >= atr * (0.05 if attack else 0.07)

    long_break = prev["close"] >= resistance + atr * (0.01 if attack else 0.03) and prev["close"] > prev["open"] and strong_break
    short_break = prev["close"] <= support - atr * (0.01 if attack else 0.03) and prev["close"] < prev["open"] and strong_break
    long_retest = last["low"] <= resistance + atr * (0.22 if attack else 0.16) and last["close"] >= resistance - atr * (0.05 if attack else 0.03)
    short_retest = last["high"] >= support - atr * (0.22 if attack else 0.16) and last["close"] <= support + atr * (0.05 if attack else 0.03)
    long_confirm = last["close"] > last["open"] and last["close"] >= max(resistance, prev["high"] - atr * (0.12 if attack else 0.08))
    short_confirm = last["close"] < last["open"] and last["close"] <= min(support, prev["low"] + atr * (0.12 if attack else 0.08))
    m15_bias_up = df_m15["close"].iloc[-1] > ema50_m15
    m15_bias_down = df_m15["close"].iloc[-1] < ema50_m15
    vwap_limit = 2.35 if attack else 1.90

    if (
        price > ema50.iloc[-1]
        and price > vwap
        and m15_bias_up
        and abs(price - vwap) <= atr * vwap_limit
        and long_break
        and long_retest
        and long_confirm
        and strong_confirm
        and clean_long
    ):
        conf = 78
        if trend_strength > 1.05 and price >= resistance + atr * 0.16:
            conf = 82
        return {"signal": "BUY", "confidence": conf, "name": "USTECH_BREAK_RETEST"}

    if (
        price < ema50.iloc[-1]
        and price < vwap
        and m15_bias_down
        and abs(price - vwap) <= atr * vwap_limit
        and short_break
        and short_retest
        and short_confirm
        and strong_confirm
        and clean_short
    ):
        conf = 78
        if trend_strength > 1.05 and price <= support - atr * 0.16:
            conf = 82
        return {"signal": "SELL", "confidence": conf, "name": "USTECH_BREAK_RETEST"}

    if price > ema50.iloc[-1] and not m15_bias_up:
        return blocked_signal("USTECH_BREAK_RETEST", "M15 NOT ALIGNED", pretrigger_text("EMA50", ema50_m15 - df_m15["close"].iloc[-1], atr))
    if price < ema50.iloc[-1] and not m15_bias_down:
        return blocked_signal("USTECH_BREAK_RETEST", "M15 NOT ALIGNED", pretrigger_text("EMA50", df_m15["close"].iloc[-1] - ema50_m15, atr))
    if abs(price - vwap) > atr * vwap_limit:
        return blocked_signal("USTECH_BREAK_RETEST", "VWAP TOO FAR", pretrigger_text("VWAP", abs(price - vwap) - atr * vwap_limit, atr))
    if price > ema50.iloc[-1]:
        if not long_break:
            return blocked_signal("USTECH_BREAK_RETEST", "NO BREAKOUT", pretrigger_text("BRK", resistance - prev["close"], atr))
        if not long_retest:
            return blocked_signal("USTECH_BREAK_RETEST", "NO RETEST", pretrigger_text("RET", last["low"] - (resistance + atr * (0.18 if attack else 0.12)), atr))
        if not long_confirm:
            return blocked_signal("USTECH_BREAK_RETEST", "NO BULL CONFIRM", pretrigger_text("CONF", max(resistance - last["close"], prev["high"] - last["close"]), atr))
        if not strong_confirm:
            return blocked_signal("USTECH_BREAK_RETEST", "WEAK BODY", pretrigger_text("BODY", atr * (0.06 if attack else 0.09) - body, atr))
        if not clean_long:
            return blocked_signal("USTECH_BREAK_RETEST", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.20 if attack else 0.95), atr * (0.18 if attack else 0.12)), atr))
    if price < ema50.iloc[-1]:
        if not short_break:
            return blocked_signal("USTECH_BREAK_RETEST", "NO BREAKDOWN", pretrigger_text("BRK", prev["close"] - support, atr))
        if not short_retest:
            return blocked_signal("USTECH_BREAK_RETEST", "NO RETEST", pretrigger_text("RET", (support - atr * (0.18 if attack else 0.12)) - last["high"], atr))
        if not short_confirm:
            return blocked_signal("USTECH_BREAK_RETEST", "NO BEAR CONFIRM", pretrigger_text("CONF", max(last["close"] - support, last["close"] - prev["low"]), atr))
        if not strong_confirm:
            return blocked_signal("USTECH_BREAK_RETEST", "WEAK BODY", pretrigger_text("BODY", atr * (0.06 if attack else 0.09) - body, atr))
        if not clean_short:
            return blocked_signal("USTECH_BREAK_RETEST", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.20 if attack else 0.95), atr * (0.18 if attack else 0.12)), atr))
    return blocked_signal("USTECH_BREAK_RETEST", "TREND MIXED", pretrigger_text("EMA", abs(price - ema50.iloc[-1]), atr))


def strategy_ustech_pullback_scalp(df, symbol):
    if not scalping_enabled:
        return blocked_signal("USTECH_PULLBACK_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 8 or hour > 23:
        return blocked_signal("USTECH_PULLBACK_SCALP", "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 260)
    df_m15 = get_m15_cached(symbol)
    if df_m1 is None or len(df_m1) < 220:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO M15 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO TICK")

    attack = is_attack_runtime()
    close_m1 = df_m1["close"]
    ema50 = close_m1.ewm(span=50).mean()
    ema200 = close_m1.ewm(span=200).mean()
    rsi = compute_rsi(close_m1, 14)
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO ATR")

    spread = tick.ask - tick.bid
    if spread > atr_m1 * (0.30 if attack else 0.20):
        return blocked_signal("USTECH_PULLBACK_SCALP", "SPREAD HIGH")

    price_m5 = df["close"].iloc[-1]
    ema20_m5 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50_m5 = df["close"].ewm(span=50).mean().iloc[-1]
    vwap_m5 = df["vwap"].iloc[-1]
    trend_strength = compute_trend_strength(df)
    m15_close = df_m15["close"].iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    ema200_m15 = df_m15["close"].ewm(span=200).mean().iloc[-1]
    atr_m15 = df_m15["atr"].iloc[-1]
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    recent_high = df_m1["high"].tail(8).max()
    recent_low = df_m1["low"].tail(8).min()
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    strong_body = body >= atr_m1 * (0.08 if attack else 0.09)
    clean_long = wick_up <= max(body * (1.10 if attack else 0.88), atr_m1 * (0.18 if attack else 0.12))
    clean_short = wick_down <= max(body * (1.10 if attack else 0.88), atr_m1 * (0.18 if attack else 0.12))

    m15_strength = abs(ema50_m15 - ema200_m15) / max(atr_m15, 1e-6)
    long_trend = (
        ema20_m5 > ema50_m5
        and
        ema50.iloc[-1] > ema200.iloc[-1]
        and ema50_m15 > ema200_m15
        and price_m5 > vwap_m5
        and trend_strength >= (0.22 if attack else 0.40)
        and m15_close >= ema50_m15 + atr_m15 * (0.02 if attack else 0.05)
        and m15_strength >= (0.22 if attack else 0.36)
    )
    short_trend = (
        ema20_m5 < ema50_m5
        and
        ema50.iloc[-1] < ema200.iloc[-1]
        and ema50_m15 < ema200_m15
        and price_m5 < vwap_m5
        and trend_strength >= (0.22 if attack else 0.40)
        and m15_close <= ema50_m15 - atr_m15 * (0.02 if attack else 0.05)
        and m15_strength >= (0.22 if attack else 0.36)
    )
    long_pullback = (
        min(prev["low"], last["low"]) <= ema50.iloc[-1] + atr_m1 * (0.10 if attack else 0.06)
        and min(prev["low"], last["low"]) >= ema50.iloc[-1] - atr_m1 * (0.22 if attack else 0.16)
        and min(prev["low"], last["low"]) > ema200.iloc[-1]
    )
    short_pullback = (
        max(prev["high"], last["high"]) >= ema50.iloc[-1] - atr_m1 * (0.10 if attack else 0.06)
        and max(prev["high"], last["high"]) <= ema50.iloc[-1] + atr_m1 * (0.22 if attack else 0.16)
        and max(prev["high"], last["high"]) < ema200.iloc[-1]
    )
    long_confirm = (
        last["close"] > last["open"]
        and last["close"] > ema50.iloc[-1] + atr_m1 * (0.04 if attack else 0.06)
        and last["close"] >= recent_high - atr_m1 * (0.06 if attack else 0.02)
        and last["close"] > prev["high"] - atr_m1 * (0.02 if attack else 0.00)
    )
    short_confirm = (
        last["close"] < last["open"]
        and last["close"] < ema50.iloc[-1] - atr_m1 * (0.04 if attack else 0.06)
        and last["close"] <= recent_low + atr_m1 * (0.06 if attack else 0.02)
        and last["close"] < prev["low"] + atr_m1 * (0.02 if attack else 0.00)
    )

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi.iloc[-1] >= 54:
        conf = 76
        if rsi.iloc[-1] > 60 and last["close"] >= ema50.iloc[-1] + atr_m1 * 0.10:
            conf = 81
        return {"signal": "BUY", "confidence": conf, "name": "USTECH_PULLBACK_SCALP", "execution_df": df_m1}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi.iloc[-1] <= 46:
        conf = 76
        if rsi.iloc[-1] < 40 and last["close"] <= ema50.iloc[-1] - atr_m1 * 0.10:
            conf = 81
        return {"signal": "SELL", "confidence": conf, "name": "USTECH_PULLBACK_SCALP", "execution_df": df_m1}

    if not long_trend and not short_trend:
        return blocked_signal("USTECH_PULLBACK_SCALP", "M15 EMA MIXED", pretrigger_text("M15", abs(ema50_m15 - ema200_m15), atr_m15))
    if long_trend:
        if not long_pullback:
            return blocked_signal("USTECH_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - (ema50.iloc[-1] + atr_m1 * (0.10 if attack else 0.06)), atr_m1))
        if not long_confirm:
            return blocked_signal("USTECH_PULLBACK_SCALP", "NO BULL CONFIRM", pretrigger_text("CONF", (recent_high - atr_m1 * (0.06 if attack else 0.02)) - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal("USTECH_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.08 if attack else 0.09) - body, atr_m1))
        if not clean_long:
            return blocked_signal("USTECH_PULLBACK_SCALP", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.10 if attack else 0.88), atr_m1 * (0.18 if attack else 0.12)), atr_m1))
        return blocked_signal("USTECH_PULLBACK_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if not short_pullback:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", (ema50.iloc[-1] - atr_m1 * (0.10 if attack else 0.06)) - max(prev["high"], last["high"]), atr_m1))
    if not short_confirm:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO BEAR CONFIRM", pretrigger_text("CONF", last["close"] - (recent_low + atr_m1 * (0.06 if attack else 0.02)), atr_m1))
    if not strong_body:
        return blocked_signal("USTECH_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.08 if attack else 0.09) - body, atr_m1))
    if not clean_short:
        return blocked_signal("USTECH_PULLBACK_SCALP", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.10 if attack else 0.88), atr_m1 * (0.18 if attack else 0.12)), atr_m1))
    return blocked_signal("USTECH_PULLBACK_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")


def strategy_ger40_break_retest(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60 or len(df) < 80:
        return blocked_signal("GER40_BREAK_RETEST", "NO DATA")

    hour = get_broker_hour()
    if hour < 8 or hour > 18:
        return blocked_signal("GER40_BREAK_RETEST", "OUT SESSION")

    atr = df["atr"].iloc[-1]
    if atr <= 0:
        return blocked_signal("GER40_BREAK_RETEST", "NO ATR")

    trend_strength = compute_trend_strength(df)
    if detect_market_regime(df) == "CHAOS" or trend_strength < (0.16 if attack else 0.72):
        return blocked_signal("GER40_BREAK_RETEST", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    ema50 = df["close"].ewm(span=50).mean()
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    breakout_ref = df.iloc[-22:-3] if len(df) >= 28 else df.iloc[:-3]
    if len(breakout_ref) < 8:
        return blocked_signal("GER40_BREAK_RETEST", "NO STRUCTURE")

    resistance = breakout_ref["high"].max()
    support = breakout_ref["low"].min()
    body = abs(last["close"] - last["open"])
    breakout_body = abs(prev["close"] - prev["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    clean_long = wick_up <= max(body * (1.18 if attack else 0.92), atr * (0.20 if attack else 0.13))
    clean_short = wick_down <= max(body * (1.18 if attack else 0.92), atr * (0.20 if attack else 0.13))
    strong_break = breakout_body >= atr * (0.09 if attack else 0.12)
    strong_confirm = body >= atr * (0.05 if attack else 0.08)

    long_break = prev["close"] >= resistance + atr * (0.015 if attack else 0.03) and prev["close"] > prev["open"] and strong_break
    short_break = prev["close"] <= support - atr * (0.015 if attack else 0.03) and prev["close"] < prev["open"] and strong_break
    long_retest = last["low"] <= resistance + atr * (0.18 if attack else 0.12) and last["close"] >= resistance - atr * (0.04 if attack else 0.03)
    short_retest = last["high"] >= support - atr * (0.18 if attack else 0.12) and last["close"] <= support + atr * (0.04 if attack else 0.03)
    long_confirm = last["close"] > last["open"] and last["close"] >= max(resistance, prev["high"] - atr * (0.12 if attack else 0.04))
    short_confirm = last["close"] < last["open"] and last["close"] <= min(support, prev["low"] + atr * (0.12 if attack else 0.04))
    m15_bias_up = df_m15["close"].iloc[-1] > ema50_m15
    m15_bias_down = df_m15["close"].iloc[-1] < ema50_m15
    vwap_limit = 1.95 if attack else 1.45

    if (
        price > ema50.iloc[-1]
        and price > vwap
        and m15_bias_up
        and abs(price - vwap) <= atr * vwap_limit
        and long_break
        and long_retest
        and long_confirm
        and strong_confirm
        and clean_long
    ):
        conf = 77
        if trend_strength > 0.95 and price >= resistance + atr * 0.14:
            conf = 81
        return {"signal": "BUY", "confidence": conf, "name": "GER40_BREAK_RETEST"}

    if (
        price < ema50.iloc[-1]
        and price < vwap
        and m15_bias_down
        and abs(price - vwap) <= atr * vwap_limit
        and short_break
        and short_retest
        and short_confirm
        and strong_confirm
        and clean_short
    ):
        conf = 77
        if trend_strength > 0.95 and price <= support - atr * 0.14:
            conf = 81
        return {"signal": "SELL", "confidence": conf, "name": "GER40_BREAK_RETEST"}

    if price > ema50.iloc[-1] and not m15_bias_up:
        return blocked_signal("GER40_BREAK_RETEST", "M15 NOT ALIGNED", pretrigger_text("EMA50", ema50_m15 - df_m15["close"].iloc[-1], atr))
    if price < ema50.iloc[-1] and not m15_bias_down:
        return blocked_signal("GER40_BREAK_RETEST", "M15 NOT ALIGNED", pretrigger_text("EMA50", df_m15["close"].iloc[-1] - ema50_m15, atr))
    if abs(price - vwap) > atr * vwap_limit:
        return blocked_signal("GER40_BREAK_RETEST", "VWAP TOO FAR", pretrigger_text("VWAP", abs(price - vwap) - atr * vwap_limit, atr))
    if price > ema50.iloc[-1]:
        if not long_break:
            return blocked_signal("GER40_BREAK_RETEST", "NO BREAKOUT", pretrigger_text("BRK", resistance - prev["close"], atr))
        if not long_retest:
            return blocked_signal("GER40_BREAK_RETEST", "NO RETEST", pretrigger_text("RET", last["low"] - (resistance + atr * (0.16 if attack else 0.10)), atr))
        if not long_confirm:
            return blocked_signal("GER40_BREAK_RETEST", "NO BULL CONFIRM", pretrigger_text("CONF", max(resistance - last["close"], prev["high"] - last["close"]), atr))
        if not strong_confirm:
            return blocked_signal("GER40_BREAK_RETEST", "WEAK BODY", pretrigger_text("BODY", atr * (0.05 if attack else 0.08) - body, atr))
        if not clean_long:
            return blocked_signal("GER40_BREAK_RETEST", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.18 if attack else 0.92), atr * (0.20 if attack else 0.13)), atr))
    if price < ema50.iloc[-1]:
        if not short_break:
            return blocked_signal("GER40_BREAK_RETEST", "NO BREAKDOWN", pretrigger_text("BRK", prev["close"] - support, atr))
        if not short_retest:
            return blocked_signal("GER40_BREAK_RETEST", "NO RETEST", pretrigger_text("RET", (support - atr * (0.16 if attack else 0.10)) - last["high"], atr))
        if not short_confirm:
            return blocked_signal("GER40_BREAK_RETEST", "NO BEAR CONFIRM", pretrigger_text("CONF", max(last["close"] - support, last["close"] - prev["low"]), atr))
        if not strong_confirm:
            return blocked_signal("GER40_BREAK_RETEST", "WEAK BODY", pretrigger_text("BODY", atr * (0.05 if attack else 0.08) - body, atr))
        if not clean_short:
            return blocked_signal("GER40_BREAK_RETEST", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.18 if attack else 0.92), atr * (0.20 if attack else 0.13)), atr))
    return blocked_signal("GER40_BREAK_RETEST", "TREND MIXED", pretrigger_text("EMA", abs(price - ema50.iloc[-1]), atr))


def strategy_ger40_pullback_scalp(df, symbol):
    if not scalping_enabled:
        return blocked_signal("GER40_PULLBACK_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 8 or hour > 17:
        return blocked_signal("GER40_PULLBACK_SCALP", "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 260)
    df_m15 = get_m15_cached(symbol)
    df_h1 = get_h1_cached(symbol)
    if df_m1 is None or len(df_m1) < 220:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO M15 DATA")
    if df_h1 is None or len(df_h1) < 80:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO H1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO TICK")

    attack = is_attack_runtime()
    close_m1 = df_m1["close"]
    ema50 = close_m1.ewm(span=50).mean()
    ema200 = close_m1.ewm(span=200).mean()
    rsi = compute_rsi(close_m1, 14)
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO ATR")

    spread = tick.ask - tick.bid
    if spread > atr_m1 * (0.36 if attack else 0.26):
        return blocked_signal("GER40_PULLBACK_SCALP", "SPREAD HIGH")

    m15_close = df_m15["close"].iloc[-1]
    close_m15 = df_m15["close"]
    ema20_m15_series = close_m15.ewm(span=20).mean()
    ema50_m15_series = close_m15.ewm(span=50).mean()
    ema20_m15 = ema20_m15_series.iloc[-1]
    ema20_m15_prev = ema20_m15_series.iloc[-4]
    ema50_m15 = ema50_m15_series.iloc[-1]
    m15_slope = ema20_m15 - ema20_m15_prev
    h1_close = df_h1["close"]
    ema20_h1_series = h1_close.ewm(span=20).mean()
    ema50_h1_series = h1_close.ewm(span=50).mean()
    ema20_h1 = ema20_h1_series.iloc[-1]
    ema50_h1 = ema50_h1_series.iloc[-1]
    h1_slope = ema20_h1 - ema20_h1_series.iloc[-4]
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    recent_high = df_m1["high"].tail(6).max()
    recent_low = df_m1["low"].tail(6).min()
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    strong_body = body >= atr_m1 * (0.05 if attack else 0.06)
    clean_long = wick_up <= max(body * (1.28 if attack else 1.08), atr_m1 * (0.24 if attack else 0.16))
    clean_short = wick_down <= max(body * (1.28 if attack else 1.08), atr_m1 * (0.24 if attack else 0.16))

    long_trend = (
        ema50.iloc[-1] > ema200.iloc[-1]
        and ema20_m15 > ema50_m15
        and ema20_h1 >= ema50_h1
        and m15_close >= ema20_m15 - atr_m1 * 0.03
        and m15_slope >= atr_m1 * 0.02
        and h1_slope >= -atr_m1 * 0.02
    )
    short_trend = (
        ema50.iloc[-1] < ema200.iloc[-1]
        and ema20_m15 < ema50_m15
        and ema20_h1 < ema50_h1
        and m15_close <= ema50_m15 - atr_m1 * 0.10
        and m15_slope <= -atr_m1 * 0.05
        and h1_slope <= -atr_m1 * 0.02
        and last["close"] < ema200.iloc[-1] - atr_m1 * 0.05
    )
    long_pullback = min(prev["low"], last["low"]) <= ema50.iloc[-1] + atr_m1 * (0.20 if attack else 0.14) and min(prev["low"], last["low"]) >= ema200.iloc[-1] - atr_m1 * 0.10
    short_pullback = max(prev["high"], last["high"]) >= ema50.iloc[-1] - atr_m1 * (0.16 if attack else 0.10) and max(prev["high"], last["high"]) <= ema200.iloc[-1] + atr_m1 * 0.04
    long_confirm = last["close"] > last["open"] and last["close"] > ema50.iloc[-1] and last["close"] >= recent_high - atr_m1 * (0.16 if attack else 0.07)
    short_confirm = last["close"] < last["open"] and last["close"] < ema50.iloc[-1] and last["close"] <= recent_low - atr_m1 * (0.04 if attack else 0.03)

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi.iloc[-1] > 50:
        conf = 75
        if rsi.iloc[-1] > 57 and last["close"] >= ema50.iloc[-1] + atr_m1 * 0.07:
            conf = 80
        return {"signal": "BUY", "confidence": conf, "name": "GER40_PULLBACK_SCALP", "execution_df": df_m1}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi.iloc[-1] < 43:
        conf = 75
        if rsi.iloc[-1] < 40 and last["close"] <= ema50.iloc[-1] - atr_m1 * 0.10:
            conf = 80
        return {"signal": "SELL", "confidence": conf, "name": "GER40_PULLBACK_SCALP", "execution_df": df_m1}

    if not long_trend and not short_trend:
        trend_gap = max(abs(ema20_m15 - ema50_m15), abs(ema50.iloc[-1] - ema200.iloc[-1]))
        return blocked_signal("GER40_PULLBACK_SCALP", "EMA TREND MIXED", pretrigger_text("EMA", trend_gap, atr_m1))
    if long_trend:
        if not long_pullback:
            return blocked_signal("GER40_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - (ema50.iloc[-1] + atr_m1 * (0.20 if attack else 0.14)), atr_m1))
        if not long_confirm:
            return blocked_signal("GER40_PULLBACK_SCALP", "NO BULL CONFIRM", pretrigger_text("CONF", (recent_high - atr_m1 * (0.16 if attack else 0.07)) - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal("GER40_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
        if not clean_long:
            return blocked_signal("GER40_PULLBACK_SCALP", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.28 if attack else 1.08), atr_m1 * (0.24 if attack else 0.16)), atr_m1))
        return blocked_signal("GER40_PULLBACK_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if not short_pullback:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", (ema50.iloc[-1] - atr_m1 * (0.20 if attack else 0.14)) - max(prev["high"], last["high"]), atr_m1))
    if not short_confirm:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO BEAR CONFIRM", pretrigger_text("CONF", last["close"] - (recent_low + atr_m1 * (0.16 if attack else 0.07)), atr_m1))
    if not strong_body:
        return blocked_signal("GER40_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
    if not clean_short:
        return blocked_signal("GER40_PULLBACK_SCALP", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.28 if attack else 1.08), atr_m1 * (0.24 if attack else 0.16)), atr_m1))
    return blocked_signal("GER40_PULLBACK_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")


INDEX_TOP_PROFILES = {
    "XAUUSD": {
        "session": (13, 3),
        "spread_mult": (0.34, 0.26),
        "trend_min": (0.14, 0.21),
        "m15_min": (0.10, 0.16),
        "m15_bias_pad": (0.00, 0.01),
        "pullback_top": (0.20, 0.14),
        "pullback_bottom": (0.24, 0.17),
        "ema200_pad": 0.08,
        "confirm_pad": (0.14, 0.07),
        "reclaim_pad": (0.01, 0.00),
        "body_floor": (0.045, 0.055),
        "wick_ratio": (1.18, 1.00),
        "wick_atr": (0.18, 0.14),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (57, 43),
        "boost_pad": 0.08,
        "recent_bars": 6,
        "base_conf": 77,
        "boost_conf": 82,
    },
    "GER40": {
        "session": (8, 18),
        "spread_mult": (0.36, 0.26),
        "trend_min": (0.16, 0.24),
        "m15_min": (0.14, 0.20),
        "m15_bias_pad": (0.00, 0.02),
        "pullback_top": (0.18, 0.12),
        "pullback_bottom": (0.22, 0.15),
        "ema200_pad": 0.08,
        "confirm_pad": (0.14, 0.06),
        "reclaim_pad": (0.02, 0.00),
        "body_floor": (0.05, 0.06),
        "wick_ratio": (1.26, 1.08),
        "wick_atr": (0.22, 0.16),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (57, 43),
        "boost_pad": 0.06,
        "recent_bars": 6,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "UK100": {
        "session": (8, 18),
        "spread_mult": (0.38, 0.28),
        "trend_min": (0.16, 0.24),
        "m15_min": (0.14, 0.20),
        "m15_bias_pad": (0.00, 0.02),
        "pullback_top": (0.18, 0.12),
        "pullback_bottom": (0.22, 0.15),
        "ema200_pad": 0.08,
        "confirm_pad": (0.14, 0.06),
        "reclaim_pad": (0.02, 0.00),
        "body_floor": (0.05, 0.06),
        "wick_ratio": (1.24, 1.06),
        "wick_atr": (0.22, 0.16),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (57, 43),
        "boost_pad": 0.06,
        "recent_bars": 6,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "USTECH": {
        "session": (13, 23),
        "spread_mult": (0.30, 0.20),
        "trend_min": (0.20, 0.34),
        "m15_min": (0.18, 0.28),
        "m15_bias_pad": (0.02, 0.04),
        "pullback_top": (0.12, 0.08),
        "pullback_bottom": (0.20, 0.14),
        "ema200_pad": 0.04,
        "confirm_pad": (0.08, 0.03),
        "reclaim_pad": (0.04, 0.05),
        "body_floor": (0.07, 0.08),
        "wick_ratio": (1.12, 0.92),
        "wick_atr": (0.18, 0.12),
        "rsi_long": (54, 80),
        "rsi_short": (20, 46),
        "boost_rsi": (60, 40),
        "boost_pad": 0.10,
        "recent_bars": 8,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "US500": {
        "session": (13, 23),
        "spread_mult": (0.32, 0.22),
        "trend_min": (0.18, 0.30),
        "m15_min": (0.16, 0.24),
        "m15_bias_pad": (0.01, 0.03),
        "pullback_top": (0.16, 0.10),
        "pullback_bottom": (0.22, 0.15),
        "ema200_pad": 0.06,
        "confirm_pad": (0.12, 0.05),
        "reclaim_pad": (0.03, 0.03),
        "body_floor": (0.06, 0.07),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.20, 0.14),
        "rsi_long": (52, 79),
        "rsi_short": (21, 48),
        "boost_rsi": (58, 42),
        "boost_pad": 0.08,
        "recent_bars": 7,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "US30": {
        "session": (13, 23),
        "spread_mult": (0.36, 0.24),
        "trend_min": (0.18, 0.30),
        "m15_min": (0.16, 0.24),
        "m15_bias_pad": (0.01, 0.03),
        "pullback_top": (0.18, 0.12),
        "pullback_bottom": (0.24, 0.17),
        "ema200_pad": 0.07,
        "confirm_pad": (0.12, 0.05),
        "reclaim_pad": (0.03, 0.03),
        "body_floor": (0.06, 0.07),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.20, 0.14),
        "rsi_long": (52, 79),
        "rsi_short": (21, 48),
        "boost_rsi": (58, 42),
        "boost_pad": 0.08,
        "recent_bars": 7,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "JPN225": {
        "session": (2, 10),
        "spread_mult": (0.38, 0.28),
        "trend_min": (0.16, 0.26),
        "m15_min": (0.14, 0.22),
        "m15_bias_pad": (0.00, 0.02),
        "pullback_top": (0.18, 0.12),
        "pullback_bottom": (0.24, 0.18),
        "ema200_pad": 0.08,
        "confirm_pad": (0.12, 0.05),
        "reclaim_pad": (0.02, 0.02),
        "body_floor": (0.06, 0.07),
        "wick_ratio": (1.22, 1.02),
        "wick_atr": (0.22, 0.16),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (56, 44),
        "boost_pad": 0.07,
        "recent_bars": 6,
        "base_conf": 75,
        "boost_conf": 80,
    },
    "BTCUSD": {
        "session": (0, 23),
        "spread_mult": (0.40, 0.32),
        "trend_min": (0.16, 0.24),
        "m15_min": (0.12, 0.18),
        "m15_bias_pad": (0.00, 0.02),
        "pullback_top": (0.20, 0.14),
        "pullback_bottom": (0.26, 0.20),
        "ema200_pad": 0.06,
        "confirm_pad": (0.12, 0.05),
        "reclaim_pad": (0.02, 0.02),
        "body_floor": (0.04, 0.05),
        "wick_ratio": (1.18, 1.00),
        "wick_atr": (0.18, 0.14),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (58, 42),
        "boost_pad": 0.08,
        "recent_bars": 7,
        "base_conf": 76,
        "boost_conf": 81,
    },
    "ETHUSD": {
        "session": (0, 23),
        "spread_mult": (0.86, 0.70),
        "trend_min": (0.16, 0.24),
        "m15_min": (0.12, 0.18),
        "m15_bias_pad": (0.00, 0.02),
        "pullback_top": (0.20, 0.14),
        "pullback_bottom": (0.26, 0.20),
        "ema200_pad": 0.06,
        "confirm_pad": (0.12, 0.05),
        "reclaim_pad": (0.02, 0.02),
        "body_floor": (0.045, 0.055),
        "wick_ratio": (1.20, 1.02),
        "wick_atr": (0.18, 0.14),
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (58, 42),
        "boost_pad": 0.08,
        "recent_bars": 7,
        "base_conf": 76,
        "boost_conf": 81,
    },
}


def _session_open(hour, session_start, session_end):
    if session_start <= session_end:
        return session_start <= hour <= session_end
    return hour >= session_start or hour <= session_end


def strategy_index_top(df, symbol, branch_name, family):
    if is_crypto_family(family):
        if not crypto_enabled:
            return blocked_signal(branch_name, "CRYPTO OFF")
    elif not scalping_enabled:
        return blocked_signal(branch_name, "SCALP OFF")

    profile = INDEX_TOP_PROFILES[family]
    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 260)
    df_m15 = get_m15_cached(symbol)
    if df_m1 is None or len(df_m1) < 220:
        return blocked_signal(branch_name, "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal(branch_name, "NO M15 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    attack = is_attack_runtime()
    close_m1 = df_m1["close"]
    ema50_m1 = close_m1.ewm(span=50).mean()
    ema200_m1 = close_m1.ewm(span=200).mean()
    rsi = compute_rsi(close_m1, 14)
    atr_m1 = df_m1["atr"].iloc[-1]
    atr_m15 = df_m15["atr"].iloc[-1]
    if atr_m1 <= 0 or atr_m15 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    spread = tick.ask - tick.bid
    spread_mult = profile["spread_mult"][0] if attack else profile["spread_mult"][1]
    spread_limit = atr_m1 * spread_mult
    if is_crypto_family(family):
        atr_floor = float(df_m1["atr"].tail(90).median() or atr_m1)
        spread_limit = max(spread_limit, atr_floor * spread_mult * 1.10)
    if spread > spread_limit:
        return blocked_signal(branch_name, "SPREAD HIGH")

    price_m5 = df["close"].iloc[-1]
    vwap_m5 = df["vwap"].iloc[-1]
    trend_strength = compute_trend_strength(df)

    m15_close = df_m15["close"].iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    ema200_m15 = df_m15["close"].ewm(span=200).mean().iloc[-1]
    m15_strength = abs(ema50_m15 - ema200_m15) / max(atr_m15, 1e-6)

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    low_ref = min(prev["low"], last["low"])
    high_ref = max(prev["high"], last["high"])
    recent_high = df_m1["high"].tail(profile["recent_bars"]).max()
    recent_low = df_m1["low"].tail(profile["recent_bars"]).min()
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]

    trend_min = profile["trend_min"][0] if attack else profile["trend_min"][1]
    m15_min = profile["m15_min"][0] if attack else profile["m15_min"][1]
    m15_bias_pad = profile["m15_bias_pad"][0] if attack else profile["m15_bias_pad"][1]
    pullback_top = profile["pullback_top"][0] if attack else profile["pullback_top"][1]
    pullback_bottom = profile["pullback_bottom"][0] if attack else profile["pullback_bottom"][1]
    confirm_pad = profile["confirm_pad"][0] if attack else profile["confirm_pad"][1]
    reclaim_pad = profile["reclaim_pad"][0] if attack else profile["reclaim_pad"][1]
    body_floor = profile["body_floor"][0] if attack else profile["body_floor"][1]
    wick_ratio = profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]
    wick_atr = profile["wick_atr"][0] if attack else profile["wick_atr"][1]
    vwap_pad = 0.0
    default_vwap_limits = {
        "GER40": (1.95, 1.45),
        "UK100": (2.00, 1.50),
        "USTECH": (2.20, 1.60),
        "US500": (2.10, 1.55),
        "US30": (2.25, 1.65),
        "JPN225": (2.10, 1.55),
        "XAUUSD": (2.45, 1.85),
        "BTCUSD": (2.90, 2.20),
        "ETHUSD": (3.35, 2.55),
    }
    vwap_limit = (profile.get("vwap_limit") or default_vwap_limits.get(family, (1.95, 1.45)))[0 if attack else 1]

    evening_flex = family in {"USTECH", "US500", "US30"} and 19 <= hour <= 23
    if evening_flex:
        trend_min = max(0.14, trend_min - 0.04)
        m15_min = max(0.12, m15_min - 0.03)
        pullback_top += 0.02
        pullback_bottom += 0.02
        confirm_pad += 0.01
        vwap_pad = atr_m1 * (0.05 if family == "USTECH" else 0.04)
    if is_crypto_family(family):
        trend_min = max(0.12, trend_min - 0.02)
        m15_min = max(0.10, m15_min - 0.02)
        pullback_top += 0.02
        pullback_bottom += 0.02
        confirm_pad += 0.01
        vwap_pad = max(vwap_pad, atr_m1 * 0.06)
    elif family == "XAUUSD":
        trend_min = max(0.12, trend_min - 0.01)
        m15_min = max(0.09, m15_min - 0.01)
        pullback_top += 0.01
        pullback_bottom += 0.01
        confirm_pad += 0.01
        vwap_pad = max(vwap_pad, atr_m1 * 0.04)

    strong_body = body >= atr_m1 * body_floor
    clean_long = wick_up <= max(body * wick_ratio, atr_m1 * wick_atr)
    clean_short = wick_down <= max(body * wick_ratio, atr_m1 * wick_atr)
    vwap_dist = abs(price_m5 - vwap_m5)

    long_trend = (
        ema50_m1.iloc[-1] > ema200_m1.iloc[-1]
        and ema50_m15 > ema200_m15
        and price_m5 > vwap_m5 - vwap_pad
        and vwap_dist <= atr_m1 * vwap_limit
        and trend_strength >= trend_min
        and m15_close >= ema50_m15 + atr_m15 * m15_bias_pad
        and m15_strength >= m15_min
    )
    short_trend = (
        ema50_m1.iloc[-1] < ema200_m1.iloc[-1]
        and ema50_m15 < ema200_m15
        and price_m5 < vwap_m5 + vwap_pad
        and vwap_dist <= atr_m1 * vwap_limit
        and trend_strength >= trend_min
        and m15_close <= ema50_m15 - atr_m15 * m15_bias_pad
        and m15_strength >= m15_min
    )
    long_pullback = (
        low_ref <= ema50_m1.iloc[-1] + atr_m1 * pullback_top
        and low_ref >= max(ema50_m1.iloc[-1] - atr_m1 * pullback_bottom, ema200_m1.iloc[-1] - atr_m1 * profile["ema200_pad"])
    )
    short_pullback = (
        high_ref >= ema50_m1.iloc[-1] - atr_m1 * pullback_top
        and high_ref <= min(ema50_m1.iloc[-1] + atr_m1 * pullback_bottom, ema200_m1.iloc[-1] + atr_m1 * profile["ema200_pad"])
    )
    long_confirm = (
        last["close"] > last["open"]
        and last["close"] > ema50_m1.iloc[-1] + atr_m1 * reclaim_pad
        and last["close"] >= recent_high - atr_m1 * confirm_pad
        and last["close"] > prev["high"] - atr_m1 * 0.02
    )
    short_confirm = (
        last["close"] < last["open"]
        and last["close"] < ema50_m1.iloc[-1] - atr_m1 * reclaim_pad
        and last["close"] <= recent_low + atr_m1 * confirm_pad
        and last["close"] < prev["low"] + atr_m1 * 0.02
    )

    rsi_long_min, rsi_long_max = profile["rsi_long"]
    rsi_short_min, rsi_short_max = profile["rsi_short"]
    boost_rsi_long, boost_rsi_short = profile["boost_rsi"]

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi_long_min <= rsi.iloc[-1] <= rsi_long_max:
        conf = profile["base_conf"]
        if rsi.iloc[-1] >= boost_rsi_long and last["close"] >= ema50_m1.iloc[-1] + atr_m1 * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "BUY", "confidence": conf, "name": branch_name, "execution_df": df_m1}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi_short_min <= rsi.iloc[-1] <= rsi_short_max:
        conf = profile["base_conf"]
        if rsi.iloc[-1] <= boost_rsi_short and last["close"] <= ema50_m1.iloc[-1] - atr_m1 * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "SELL", "confidence": conf, "name": branch_name, "execution_df": df_m1}

    if not long_trend and not short_trend:
        if m15_strength < m15_min:
            return blocked_signal(branch_name, "M15 WEAK", f"STR {round(m15_strength, 2)}")
        if trend_strength < trend_min:
            return blocked_signal(branch_name, "TREND WEAK", f"STR {round(trend_strength, 2)}")
        if price_m5 > vwap_m5 and vwap_dist > atr_m1 * vwap_limit:
            return blocked_signal(branch_name, "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr_m1 * vwap_limit, atr_m1))
        if price_m5 < vwap_m5 and vwap_dist > atr_m1 * vwap_limit:
            return blocked_signal(branch_name, "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr_m1 * vwap_limit, atr_m1))
        if price_m5 >= ema50_m1.iloc[-1] and price_m5 <= vwap_m5:
            return blocked_signal(branch_name, "VWAP LOST", pretrigger_text("VWAP", vwap_m5 - price_m5, atr_m1))
        if price_m5 <= ema50_m1.iloc[-1] and price_m5 >= vwap_m5:
            return blocked_signal(branch_name, "VWAP LOST", pretrigger_text("VWAP", price_m5 - vwap_m5, atr_m1))
        return blocked_signal(branch_name, "M15 EMA MIXED", pretrigger_text("M15", abs(ema50_m15 - ema200_m15), atr_m15))
    if long_trend:
        if not long_pullback:
            return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("PB", low_ref - (ema50_m1.iloc[-1] + atr_m1 * pullback_top), atr_m1))
        if not long_confirm:
            return blocked_signal(branch_name, "NO BULL CONFIRM", pretrigger_text("CONF", (recent_high - atr_m1 * confirm_pad) - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal(branch_name, "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * body_floor - body, atr_m1))
        if not clean_long:
            return blocked_signal(branch_name, "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * wick_ratio, atr_m1 * wick_atr), atr_m1))
        return blocked_signal(branch_name, "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if not short_pullback:
        return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("PB", (ema50_m1.iloc[-1] - atr_m1 * pullback_top) - high_ref, atr_m1))
    if not short_confirm:
        return blocked_signal(branch_name, "NO BEAR CONFIRM", pretrigger_text("CONF", last["close"] - (recent_low + atr_m1 * confirm_pad), atr_m1))
    if not strong_body:
        return blocked_signal(branch_name, "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * body_floor - body, atr_m1))
    if not clean_short:
        return blocked_signal(branch_name, "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * wick_ratio, atr_m1 * wick_atr), atr_m1))
    return blocked_signal(branch_name, "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")


def strategy_ger40_top(df, symbol):
    return strategy_index_top(df, symbol, "GER40_TOP", "GER40")


def strategy_uk100_top(df, symbol):
    return strategy_index_top(df, symbol, "UK100_TOP", "UK100")


def strategy_ustech_top(df, symbol):
    return strategy_index_top(df, symbol, "USTECH_TOP", "USTECH")


def strategy_us500_top(df, symbol):
    return strategy_index_top(df, symbol, "US500_TOP", "US500")


def strategy_us30_top(df, symbol):
    return strategy_index_top(df, symbol, "US30_TOP", "US30")


def strategy_jpn225_top(df, symbol):
    return strategy_index_top(df, symbol, "JPN225_TOP", "JPN225")


def strategy_xau_top(df, symbol):
    return strategy_index_top(df, symbol, "XAU_TOP", "XAUUSD")


def strategy_btc_top(df, symbol):
    return strategy_index_top(df, symbol, "BTC_TOP", "BTCUSD")


def strategy_eth_top(df, symbol):
    return strategy_index_top(df, symbol, "ETH_TOP", "ETHUSD")


BREAK_RETEST_TEST_PROFILES = {
    "XAUUSD": {
        "session": (11, 4),
        "spread_mult": (0.46, 0.34),
        "trend_min": (0.10, 0.20),
        "vwap_limit": (2.50, 1.85),
        "lookback": 24,
        "break_pad": (0.02, 0.05),
        "retest_top": (0.22, 0.16),
        "retest_hold": (0.06, 0.04),
        "confirm_pad": (0.12, 0.06),
        "break_body": (0.08, 0.11),
        "body_floor": (0.05, 0.07),
        "wick_ratio": (1.22, 1.02),
        "wick_atr": (0.20, 0.14),
        "base_conf": 76,
        "boost_conf": 82,
        "boost_trend": 0.90,
        "boost_pad": 0.14,
    },
    "BTCUSD": {
        "session": (0, 23),
        "spread_mult": (0.44, 0.34),
        "trend_min": (0.12, 0.20),
        "vwap_limit": (3.00, 2.25),
        "lookback": 26,
        "break_pad": (0.00, 0.02),
        "retest_top": (0.26, 0.20),
        "retest_hold": (0.06, 0.04),
        "confirm_pad": (0.14, 0.08),
        "break_body": (0.07, 0.10),
        "body_floor": (0.05, 0.07),
        "wick_ratio": (1.18, 1.00),
        "wick_atr": (0.18, 0.12),
        "base_conf": 75,
        "boost_conf": 81,
        "boost_trend": 0.78,
        "boost_pad": 0.16,
    },
    "ETHUSD": {
        "session": (0, 23),
        "spread_mult": (0.95, 0.76),
        "trend_min": (0.14, 0.24),
        "vwap_limit": (3.10, 2.30),
        "lookback": 26,
        "break_pad": (0.015, 0.04),
        "retest_top": (0.22, 0.16),
        "retest_hold": (0.06, 0.04),
        "confirm_pad": (0.10, 0.05),
        "break_body": (0.08, 0.11),
        "body_floor": (0.055, 0.08),
        "wick_ratio": (1.20, 1.02),
        "wick_atr": (0.18, 0.12),
        "base_conf": 75,
        "boost_conf": 81,
        "boost_trend": 0.78,
        "boost_pad": 0.16,
    },
    "JPN225": {
        "session": (2, 10),
        "spread_mult": (0.40, 0.30),
        "trend_min": (0.14, 0.24),
        "vwap_limit": (2.10, 1.60),
        "lookback": 22,
        "break_pad": (0.02, 0.04),
        "retest_top": (0.18, 0.12),
        "retest_hold": (0.04, 0.03),
        "confirm_pad": (0.10, 0.05),
        "break_body": (0.08, 0.11),
        "body_floor": (0.05, 0.07),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.18, 0.12),
        "base_conf": 76,
        "boost_conf": 81,
        "boost_trend": 0.86,
        "boost_pad": 0.12,
    },
    "US30": {
        "session": (13, 23),
        "spread_mult": (0.38, 0.28),
        "trend_min": (0.16, 0.26),
        "vwap_limit": (2.25, 1.70),
        "lookback": 24,
        "break_pad": (0.015, 0.03),
        "retest_top": (0.20, 0.14),
        "retest_hold": (0.05, 0.03),
        "confirm_pad": (0.11, 0.06),
        "break_body": (0.08, 0.11),
        "body_floor": (0.05, 0.07),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.18, 0.12),
        "base_conf": 76,
        "boost_conf": 81,
        "boost_trend": 0.92,
        "boost_pad": 0.12,
    },
}

RECLAIM_TEST_PROFILES = {
    "XAUUSD": {
        "session": (11, 4),
        "spread_mult": (0.40, 0.30),
        "trend_min": (0.08, 0.12),
        "m15_min": (0.06, 0.10),
        "vwap_limit": (2.60, 2.00),
        "pullback_top": (0.14, 0.10),
        "pullback_bottom": (0.22, 0.16),
        "reclaim_pad": (0.02, 0.00),
        "confirm_pad": (0.16, 0.12),
        "body_floor": (0.04, 0.055),
        "wick_ratio": (1.24, 1.04),
        "wick_atr": (0.20, 0.14),
        "recent_bars": 8,
        "rsi_long": (48, 78),
        "rsi_short": (22, 52),
        "boost_rsi": (58, 42),
        "base_conf": 75,
        "boost_conf": 81,
        "boost_pad": 0.10,
    },
    "BTCUSD": {
        "session": (0, 23),
        "spread_mult": (0.42, 0.32),
        "trend_min": (0.12, 0.18),
        "m15_min": (0.10, 0.16),
        "vwap_limit": (2.90, 2.20),
        "pullback_top": (0.14, 0.10),
        "pullback_bottom": (0.22, 0.16),
        "reclaim_pad": (0.015, 0.00),
        "confirm_pad": (0.14, 0.07),
        "body_floor": (0.04, 0.055),
        "wick_ratio": (1.18, 1.00),
        "wick_atr": (0.18, 0.12),
        "recent_bars": 8,
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (58, 42),
        "base_conf": 75,
        "boost_conf": 81,
        "boost_pad": 0.08,
    },
    "ETHUSD": {
        "session": (0, 23),
        "spread_mult": (0.92, 0.74),
        "trend_min": (0.12, 0.18),
        "m15_min": (0.10, 0.16),
        "vwap_limit": (3.30, 2.55),
        "pullback_top": (0.14, 0.10),
        "pullback_bottom": (0.22, 0.16),
        "reclaim_pad": (0.02, 0.01),
        "confirm_pad": (0.12, 0.06),
        "body_floor": (0.04, 0.055),
        "wick_ratio": (1.20, 1.02),
        "wick_atr": (0.18, 0.12),
        "recent_bars": 8,
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (58, 42),
        "base_conf": 75,
        "boost_conf": 81,
        "boost_pad": 0.08,
    },
    "JPN225": {
        "session": (2, 10),
        "spread_mult": (0.38, 0.28),
        "trend_min": (0.14, 0.22),
        "m15_min": (0.12, 0.18),
        "vwap_limit": (2.10, 1.55),
        "pullback_top": (0.14, 0.10),
        "pullback_bottom": (0.22, 0.16),
        "reclaim_pad": (0.02, 0.01),
        "confirm_pad": (0.12, 0.06),
        "body_floor": (0.05, 0.06),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.18, 0.12),
        "recent_bars": 7,
        "rsi_long": (50, 77),
        "rsi_short": (23, 50),
        "boost_rsi": (57, 43),
        "base_conf": 76,
        "boost_conf": 81,
        "boost_pad": 0.08,
    },
    "US30": {
        "session": (13, 23),
        "spread_mult": (0.36, 0.26),
        "trend_min": (0.16, 0.24),
        "m15_min": (0.14, 0.20),
        "vwap_limit": (2.20, 1.60),
        "pullback_top": (0.14, 0.10),
        "pullback_bottom": (0.22, 0.16),
        "reclaim_pad": (0.015, 0.00),
        "confirm_pad": (0.13, 0.07),
        "body_floor": (0.05, 0.06),
        "wick_ratio": (1.18, 0.98),
        "wick_atr": (0.18, 0.12),
        "recent_bars": 7,
        "rsi_long": (50, 78),
        "rsi_short": (22, 50),
        "boost_rsi": (58, 42),
        "base_conf": 76,
        "boost_conf": 81,
        "boost_pad": 0.08,
    },
}


def strategy_break_retest_profiled(df, symbol, branch_name, family):
    profile = BREAK_RETEST_TEST_PROFILES[family]
    if is_crypto_family(family):
        if not crypto_enabled:
            return blocked_signal(branch_name, "CRYPTO OFF")
    elif not scalping_enabled:
        return blocked_signal(branch_name, "SCALP OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60 or len(df) < 80:
        return blocked_signal(branch_name, "NO DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    atr = df["atr"].iloc[-1]
    if atr <= 0:
        return blocked_signal(branch_name, "NO ATR")

    spread = tick.ask - tick.bid
    spread_limit = atr * (profile["spread_mult"][0] if attack else profile["spread_mult"][1])
    if spread > spread_limit:
        return blocked_signal(branch_name, "SPREAD HIGH")

    trend_strength = compute_trend_strength(df)
    trend_min = profile["trend_min"][0] if attack else profile["trend_min"][1]
    if detect_market_regime(df) == "CHAOS" or trend_strength < trend_min:
        return blocked_signal(branch_name, "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    ema50 = df["close"].ewm(span=50).mean()
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    vwap_limit = profile["vwap_limit"][0] if attack else profile["vwap_limit"][1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    candle_key = last["time"].isoformat() if "time" in df.columns else str(df.index[-1])
    breakout_ref = df.iloc[-profile["lookback"]:-3] if len(df) >= (profile["lookback"] + 4) else df.iloc[:-3]
    if len(breakout_ref) < 8:
        return blocked_signal(branch_name, "NO STRUCTURE")

    resistance = breakout_ref["high"].max()
    support = breakout_ref["low"].min()
    body = abs(last["close"] - last["open"])
    breakout_body = abs(prev["close"] - prev["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    clean_long = wick_up <= max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr * (profile["wick_atr"][0] if attack else profile["wick_atr"][1]))
    clean_short = wick_down <= max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr * (profile["wick_atr"][0] if attack else profile["wick_atr"][1]))
    strong_break = breakout_body >= atr * (profile["break_body"][0] if attack else profile["break_body"][1])
    strong_confirm = body >= atr * (profile["body_floor"][0] if attack else profile["body_floor"][1])

    break_pad = profile["break_pad"][0] if attack else profile["break_pad"][1]
    retest_top = profile["retest_top"][0] if attack else profile["retest_top"][1]
    retest_hold = profile["retest_hold"][0] if attack else profile["retest_hold"][1]
    confirm_pad = profile["confirm_pad"][0] if attack else profile["confirm_pad"][1]

    long_break = prev["close"] >= resistance + atr * break_pad and prev["close"] > prev["open"] and strong_break
    short_break = prev["close"] <= support - atr * break_pad and prev["close"] < prev["open"] and strong_break
    long_retest = last["low"] <= resistance + atr * retest_top and last["close"] >= resistance - atr * retest_hold
    short_retest = last["high"] >= support - atr * retest_top and last["close"] <= support + atr * retest_hold
    long_confirm = last["close"] > last["open"] and last["close"] >= max(resistance, prev["high"] - atr * confirm_pad)
    short_confirm = last["close"] < last["open"] and last["close"] <= min(support, prev["low"] + atr * confirm_pad)
    m15_bias_up = df_m15["close"].iloc[-1] > ema50_m15
    m15_bias_down = df_m15["close"].iloc[-1] < ema50_m15

    if (
        price > ema50.iloc[-1]
        and price > vwap
        and m15_bias_up
        and abs(price - vwap) <= atr * vwap_limit
        and long_break
        and long_retest
        and long_confirm
        and strong_confirm
        and clean_long
    ):
        conf = profile["base_conf"]
        if trend_strength > profile["boost_trend"] and price >= resistance + atr * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "BUY", "confidence": conf, "name": branch_name, "signal_key": f"{branch_name}|BUY|{candle_key}"}

    if (
        price < ema50.iloc[-1]
        and price < vwap
        and m15_bias_down
        and abs(price - vwap) <= atr * vwap_limit
        and short_break
        and short_retest
        and short_confirm
        and strong_confirm
        and clean_short
    ):
        conf = profile["base_conf"]
        if trend_strength > profile["boost_trend"] and price <= support - atr * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "SELL", "confidence": conf, "name": branch_name, "signal_key": f"{branch_name}|SELL|{candle_key}"}

    if price > ema50.iloc[-1] and not m15_bias_up:
        return blocked_signal(branch_name, "M15 NOT ALIGNED", pretrigger_text("EMA50", ema50_m15 - df_m15["close"].iloc[-1], atr))
    if price < ema50.iloc[-1] and not m15_bias_down:
        return blocked_signal(branch_name, "M15 NOT ALIGNED", pretrigger_text("EMA50", df_m15["close"].iloc[-1] - ema50_m15, atr))
    if abs(price - vwap) > atr * vwap_limit:
        return blocked_signal(branch_name, "VWAP TOO FAR", pretrigger_text("VWAP", abs(price - vwap) - atr * vwap_limit, atr))
    if price > ema50.iloc[-1]:
        if not long_break:
            return blocked_signal(branch_name, "NO BREAKOUT", pretrigger_text("BRK", resistance - prev["close"], atr))
        if not long_retest:
            return blocked_signal(branch_name, "NO RETEST", pretrigger_text("RET", last["low"] - (resistance + atr * retest_top), atr))
        if not long_confirm:
            return blocked_signal(branch_name, "NO BULL CONFIRM", pretrigger_text("CONF", max(resistance - last["close"], prev["high"] - last["close"]), atr))
        if not strong_confirm:
            return blocked_signal(branch_name, "WEAK BODY", pretrigger_text("BODY", atr * (profile["body_floor"][0] if attack else profile["body_floor"][1]) - body, atr))
        if not clean_long:
            return blocked_signal(branch_name, "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr * (profile["wick_atr"][0] if attack else profile["wick_atr"][1])), atr))
    if price < ema50.iloc[-1]:
        if not short_break:
            return blocked_signal(branch_name, "NO BREAKDOWN", pretrigger_text("BRK", prev["close"] - support, atr))
        if not short_retest:
            return blocked_signal(branch_name, "NO RETEST", pretrigger_text("RET", (support - atr * retest_top) - last["high"], atr))
        if not short_confirm:
            return blocked_signal(branch_name, "NO BEAR CONFIRM", pretrigger_text("CONF", max(last["close"] - support, last["close"] - prev["low"]), atr))
        if not strong_confirm:
            return blocked_signal(branch_name, "WEAK BODY", pretrigger_text("BODY", atr * (profile["body_floor"][0] if attack else profile["body_floor"][1]) - body, atr))
        if not clean_short:
            return blocked_signal(branch_name, "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr * (profile["wick_atr"][0] if attack else profile["wick_atr"][1])), atr))
    return blocked_signal(branch_name, "TREND MIXED", pretrigger_text("EMA", abs(price - ema50.iloc[-1]), atr))


def strategy_reclaim_profiled(df, symbol, branch_name, family):
    profile = RECLAIM_TEST_PROFILES[family]
    if is_crypto_family(family):
        if not crypto_enabled:
            return blocked_signal(branch_name, "CRYPTO OFF")
    elif not scalping_enabled:
        return blocked_signal(branch_name, "SCALP OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 220)
    df_m15 = get_m15_cached(symbol)
    if df_m1 is None or len(df_m1) < 180:
        return blocked_signal(branch_name, "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal(branch_name, "NO M15 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    attack = is_attack_runtime()
    close_m1 = df_m1["close"]
    ema9 = close_m1.ewm(span=9).mean()
    ema21 = close_m1.ewm(span=21).mean()
    ema55 = close_m1.ewm(span=55).mean()
    rsi = compute_rsi(close_m1, 14)
    atr_m1 = df_m1["atr"].iloc[-1]
    atr_m15 = df_m15["atr"].iloc[-1]
    if atr_m1 <= 0 or atr_m15 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    spread = tick.ask - tick.bid
    spread_limit = atr_m1 * (profile["spread_mult"][0] if attack else profile["spread_mult"][1])
    if spread > spread_limit:
        return blocked_signal(branch_name, "SPREAD HIGH")

    price_m5 = df["close"].iloc[-1]
    ema20_m5 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50_m5 = df["close"].ewm(span=50).mean().iloc[-1]
    vwap_m5 = df["vwap"].iloc[-1]
    vwap_dist = abs(price_m5 - vwap_m5)
    trend_strength = compute_trend_strength(df)

    m15_close = df_m15["close"].iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    m15_strength = abs(ema20_m15 - ema50_m15) / max(atr_m15, 1e-6)

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    candle_key = last["time"].isoformat() if "time" in df_m1.columns else str(df_m1.index[-1])
    recent_high = df_m1["high"].tail(profile["recent_bars"]).max()
    recent_low = df_m1["low"].tail(profile["recent_bars"]).min()
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    strong_body = body >= atr_m1 * (profile["body_floor"][0] if attack else profile["body_floor"][1])
    clean_long = wick_up <= max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr_m1 * (profile["wick_atr"][0] if attack else profile["wick_atr"][1]))
    clean_short = wick_down <= max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr_m1 * (profile["wick_atr"][0] if attack else profile["wick_atr"][1]))

    trend_min = profile["trend_min"][0] if attack else profile["trend_min"][1]
    m15_min = profile["m15_min"][0] if attack else profile["m15_min"][1]
    vwap_limit = profile["vwap_limit"][0] if attack else profile["vwap_limit"][1]
    pullback_top = profile["pullback_top"][0] if attack else profile["pullback_top"][1]
    pullback_bottom = profile["pullback_bottom"][0] if attack else profile["pullback_bottom"][1]
    reclaim_pad = profile["reclaim_pad"][0] if attack else profile["reclaim_pad"][1]
    confirm_pad = profile["confirm_pad"][0] if attack else profile["confirm_pad"][1]

    long_trend = (
        ema20_m5 > ema50_m5
        and ema9.iloc[-1] > ema21.iloc[-1] > ema55.iloc[-1]
        and ema20_m15 > ema50_m15
        and price_m5 > vwap_m5
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and vwap_dist <= atr_m1 * vwap_limit
    )
    short_trend = (
        ema20_m5 < ema50_m5
        and ema9.iloc[-1] < ema21.iloc[-1] < ema55.iloc[-1]
        and ema20_m15 < ema50_m15
        and price_m5 < vwap_m5
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and vwap_dist <= atr_m1 * vwap_limit
    )

    low_ref = min(prev["low"], last["low"])
    high_ref = max(prev["high"], last["high"])
    long_pullback = low_ref <= ema21.iloc[-1] + atr_m1 * pullback_top and low_ref >= ema21.iloc[-1] - atr_m1 * pullback_bottom
    short_pullback = high_ref >= ema21.iloc[-1] - atr_m1 * pullback_top and high_ref <= ema21.iloc[-1] + atr_m1 * pullback_bottom
    long_confirm = (
        last["close"] > last["open"]
        and last["close"] > ema9.iloc[-1] + atr_m1 * reclaim_pad
        and last["close"] >= recent_high - atr_m1 * confirm_pad
    )
    short_confirm = (
        last["close"] < last["open"]
        and last["close"] < ema9.iloc[-1] - atr_m1 * reclaim_pad
        and last["close"] <= recent_low + atr_m1 * confirm_pad
    )

    rsi_long_min, rsi_long_max = profile["rsi_long"]
    rsi_short_min, rsi_short_max = profile["rsi_short"]
    boost_rsi_long, boost_rsi_short = profile["boost_rsi"]

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi_long_min <= rsi.iloc[-1] <= rsi_long_max:
        conf = profile["base_conf"]
        if rsi.iloc[-1] >= boost_rsi_long and last["close"] >= ema9.iloc[-1] + atr_m1 * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "BUY", "confidence": conf, "name": branch_name, "execution_df": df_m1, "signal_key": f"{branch_name}|BUY|{candle_key}"}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi_short_min <= rsi.iloc[-1] <= rsi_short_max:
        conf = profile["base_conf"]
        if rsi.iloc[-1] <= boost_rsi_short and last["close"] <= ema9.iloc[-1] - atr_m1 * profile["boost_pad"]:
            conf = profile["boost_conf"]
        return {"signal": "SELL", "confidence": conf, "name": branch_name, "execution_df": df_m1, "signal_key": f"{branch_name}|SELL|{candle_key}"}

    if not long_trend and not short_trend:
        if m15_strength < m15_min:
            return blocked_signal(branch_name, "M15 WEAK", f"STR {round(m15_strength, 2)}")
        if trend_strength < trend_min:
            return blocked_signal(branch_name, "TREND WEAK", f"STR {round(trend_strength, 2)}")
        if vwap_dist > atr_m1 * vwap_limit:
            return blocked_signal(branch_name, "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr_m1 * vwap_limit, atr_m1))
        return blocked_signal(branch_name, "EMA STACK MISS", pretrigger_text("STACK", abs(ema21.iloc[-1] - ema55.iloc[-1]), atr_m1))
    if long_trend:
        if not long_pullback:
            return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("PB", low_ref - (ema21.iloc[-1] + atr_m1 * pullback_top), atr_m1))
        if not long_confirm:
            return blocked_signal(branch_name, "NO BULL CONFIRM", pretrigger_text("CONF", (recent_high - atr_m1 * confirm_pad) - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal(branch_name, "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (profile["body_floor"][0] if attack else profile["body_floor"][1]) - body, atr_m1))
        if not clean_long:
            return blocked_signal(branch_name, "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr_m1 * (profile["wick_atr"][0] if attack else profile["wick_atr"][1])), atr_m1))
        return blocked_signal(branch_name, "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if not short_pullback:
        return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("PB", (ema21.iloc[-1] - atr_m1 * pullback_top) - high_ref, atr_m1))
    if not short_confirm:
        return blocked_signal(branch_name, "NO BEAR CONFIRM", pretrigger_text("CONF", last["close"] - (recent_low + atr_m1 * confirm_pad), atr_m1))
    if not strong_body:
        return blocked_signal(branch_name, "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (profile["body_floor"][0] if attack else profile["body_floor"][1]) - body, atr_m1))
    if not clean_short:
        return blocked_signal(branch_name, "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (profile["wick_ratio"][0] if attack else profile["wick_ratio"][1]), atr_m1 * (profile["wick_atr"][0] if attack else profile["wick_atr"][1])), atr_m1))
    return blocked_signal(branch_name, "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")


def strategy_xau_break_retest(df, symbol):
    return strategy_break_retest_profiled(df, symbol, "XAU_BREAK_RETEST", "XAUUSD")


def strategy_xau_vwap_reclaim(df, symbol):
    return strategy_reclaim_profiled(df, symbol, "XAU_VWAP_RECLAIM", "XAUUSD")


def strategy_btc_break_retest(df, symbol):
    return strategy_break_retest_profiled(df, symbol, "BTC_BREAK_RETEST", "BTCUSD")


def strategy_btc_vwap_reclaim(df, symbol):
    return strategy_reclaim_profiled(df, symbol, "BTC_VWAP_RECLAIM", "BTCUSD")


def strategy_eth_break_retest(df, symbol):
    return strategy_break_retest_profiled(df, symbol, "ETH_BREAK_RETEST", "ETHUSD")


def strategy_eth_vwap_reclaim(df, symbol):
    return strategy_reclaim_profiled(df, symbol, "ETH_VWAP_RECLAIM", "ETHUSD")


def strategy_jpn225_vwap_trend(df, symbol):
    return strategy_reclaim_profiled(df, symbol, "JPN225_VWAP_TREND", "JPN225")


def strategy_jpn225_break_retest(df, symbol):
    return strategy_break_retest_profiled(df, symbol, "JPN225_BREAK_RETEST", "JPN225")


def strategy_us30_vwap_trend(df, symbol):
    return strategy_reclaim_profiled(df, symbol, "US30_VWAP_TREND", "US30")


def strategy_us30_break_retest(df, symbol):
    return strategy_break_retest_profiled(df, symbol, "US30_BREAK_RETEST", "US30")


SANTO_GRAAL_PROFILES = {
    "USTECH": {
        "display": "US100",
        "session": (13, 23),
        "adx_min": 27.0,
        "d1_adx_min": 22.0,
        "adx_high_exit": 42.0,
        "ema_pad": 0.04,
        "pullback_depth": 0.42,
        "confirm_pad": 0.10,
        "sl_buffer": 0.10,
        "base_conf": 77,
        "boost_conf": 84,
        "boost_adx": 38.0,
        "adx_drift_tol": 0.8,
        "d1_adx_drift_tol": 0.7,
        "permissive": True,
    },
    "US500": {
        "display": "US500",
        "session": (13, 23),
        "adx_min": 28.5,
        "d1_adx_min": 23.5,
        "adx_high_exit": 44.0,
        "ema_pad": 0.035,
        "pullback_depth": 0.36,
        "confirm_pad": 0.08,
        "sl_buffer": 0.09,
        "base_conf": 75,
        "boost_conf": 82,
        "boost_adx": 39.0,
        "adx_drift_tol": 0.8,
        "d1_adx_drift_tol": 0.7,
        "permissive": False,
    },
    "GER40": {
        "display": "DAX",
        "session": (8, 18),
        "adx_min": 31.0,
        "d1_adx_min": 26.0,
        "adx_high_exit": 44.0,
        "ema_pad": 0.04,
        "pullback_depth": 0.28,
        "confirm_pad": 0.06,
        "sl_buffer": 0.09,
        "base_conf": 75,
        "boost_conf": 82,
        "boost_adx": 39.0,
        "adx_drift_tol": 0.5,
        "d1_adx_drift_tol": 0.4,
        "permissive": False,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "adx_min": 26.5,
        "d1_adx_min": 20.5,
        "adx_high_exit": 40.0,
        "ema_pad": 0.05,
        "pullback_depth": 0.46,
        "confirm_pad": 0.12,
        "sl_buffer": 0.12,
        "base_conf": 78,
        "boost_conf": 85,
        "boost_adx": 36.0,
        "adx_drift_tol": 1.0,
        "d1_adx_drift_tol": 0.8,
        "permissive": True,
    },
}


def format_santo_graal_signal_details(details):
    if details.get("summary"):
        return details["summary"]
    return (
        f"{details['instrument']} {details['direction']} | {details['reason']} | "
        f"EMA20 {details['ema20_h4']:.2f} | ADX14 {details['adx14_h4']:.1f} | "
        f"entry {details['entry_price']:.2f} | SL {details['stop_loss']:.2f} | "
        f"{details['take_profit']} | D1 {details['trend_d1']} | H4 {details['trend_h4']}"
    )


def get_santo_graal_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = SANTO_GRAAL_PROFILES.get(family)
    if not profile:
        return None

    df_h4 = get_h4_cached(symbol)
    df_d1 = get_d1_cached(symbol)
    if df_h4 is None or df_d1 is None or len(df_h4) < 40 or len(df_d1) < 40:
        return None

    ema20_h4 = df_h4["close"].ewm(span=20).mean()
    ema20_d1 = df_d1["close"].ewm(span=20).mean()
    adx_h4, _, _ = compute_adx(df_h4, 14)
    adx_d1, _, _ = compute_adx(df_d1, 14)

    last_h4 = df_h4.iloc[-2]
    prev_h4 = df_h4.iloc[-3]
    last_d1 = df_d1.iloc[-2]

    adx_h4_now = float(adx_h4.iloc[-2])
    adx_h4_prev = float(adx_h4.iloc[-3])
    adx_d1_now = float(adx_d1.iloc[-2])
    adx_d1_prev = float(adx_d1.iloc[-3])

    if signal == "BUY":
        if adx_h4_prev >= profile["adx_high_exit"] and adx_h4_now < adx_h4_prev and last_h4["close"] < ema20_h4.iloc[-2]:
            return "ADX rollover + H4 under EMA20"
        if last_h4["close"] < ema20_h4.iloc[-2] and prev_h4["close"] < ema20_h4.iloc[-3]:
            return "H4 close below EMA20"
        if adx_d1_prev >= profile["adx_high_exit"] and adx_d1_now < adx_d1_prev and last_d1["close"] < ema20_d1.iloc[-2]:
            return "D1 ADX rollover"
    else:
        if adx_h4_prev >= profile["adx_high_exit"] and adx_h4_now < adx_h4_prev and last_h4["close"] > ema20_h4.iloc[-2]:
            return "ADX rollover + H4 over EMA20"
        if last_h4["close"] > ema20_h4.iloc[-2] and prev_h4["close"] > ema20_h4.iloc[-3]:
            return "H4 close above EMA20"
        if adx_d1_prev >= profile["adx_high_exit"] and adx_d1_now < adx_d1_prev and last_d1["close"] > ema20_d1.iloc[-2]:
            return "D1 ADX rollover"
    return None


def strategy_santo_graal(df, symbol, branch_name, family):
    profile = SANTO_GRAAL_PROFILES[family]
    if is_crypto_family(family) and not crypto_enabled:
        return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_h4 = get_h4_cached(symbol)
    df_d1 = get_d1_cached(symbol)
    if df_h4 is None or len(df_h4) < 60:
        return blocked_signal(branch_name, "NO H4 DATA")
    if df_d1 is None or len(df_d1) < 60:
        return blocked_signal(branch_name, "NO D1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    ema20_h4 = df_h4["close"].ewm(span=20).mean()
    ema20_d1 = df_d1["close"].ewm(span=20).mean()
    adx_h4, plus_di_h4, minus_di_h4 = compute_adx(df_h4, 14)
    adx_d1, plus_di_d1, minus_di_d1 = compute_adx(df_d1, 14)
    atr_h4 = float(df_h4["atr"].iloc[-2])
    if atr_h4 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    current_h4 = df_h4.iloc[-1]
    confirm = df_h4.iloc[-2]
    pullback = df_h4.iloc[-3]
    anchor = df_h4.iloc[-4]

    adx_h4_now = float(adx_h4.iloc[-2])
    adx_h4_prev = float(adx_h4.iloc[-3])
    adx_d1_now = float(adx_d1.iloc[-2])
    adx_d1_prev = float(adx_d1.iloc[-3])

    if adx_h4_now < 15 or adx_d1_now < 15:
        return blocked_signal(branch_name, "ADX TOO LOW", f"H4 {adx_h4_now:.1f} | D1 {adx_d1_now:.1f}")
    if adx_h4_now < adx_h4_prev - profile.get("adx_drift_tol", 0.0):
        return blocked_signal(branch_name, "ADX FLAT", f"H4 {adx_h4_prev:.1f}->{adx_h4_now:.1f}")
    if adx_h4_now < profile["adx_min"]:
        return blocked_signal(branch_name, "ADX BELOW 30", f"H4 {adx_h4_now:.1f}")

    d1_up = (
        df_d1["close"].iloc[-2] > ema20_d1.iloc[-2]
        and plus_di_d1.iloc[-2] >= minus_di_d1.iloc[-2]
        and adx_d1_now >= profile["d1_adx_min"]
        and adx_d1_now >= adx_d1_prev - profile.get("d1_adx_drift_tol", 0.0)
    )
    d1_down = (
        df_d1["close"].iloc[-2] < ema20_d1.iloc[-2]
        and minus_di_d1.iloc[-2] >= plus_di_d1.iloc[-2]
        and adx_d1_now >= profile["d1_adx_min"]
        and adx_d1_now >= adx_d1_prev - profile.get("d1_adx_drift_tol", 0.0)
    )

    h4_up_context = anchor["close"] > ema20_h4.iloc[-4] and plus_di_h4.iloc[-2] >= minus_di_h4.iloc[-2]
    h4_down_context = anchor["close"] < ema20_h4.iloc[-4] and minus_di_h4.iloc[-2] >= plus_di_h4.iloc[-2]

    long_pullback = (
        d1_up
        and h4_up_context
        and min(pullback["low"], confirm["low"]) < ema20_h4.iloc[-3]
        and pullback["high"] > ema20_h4.iloc[-3]
        and pullback["low"] >= ema20_h4.iloc[-3] - atr_h4 * profile["pullback_depth"]
    )
    short_pullback = (
        d1_down
        and h4_down_context
        and max(pullback["high"], confirm["high"]) > ema20_h4.iloc[-3]
        and pullback["low"] < ema20_h4.iloc[-3]
        and pullback["high"] <= ema20_h4.iloc[-3] + atr_h4 * profile["pullback_depth"]
    )

    long_confirm = (
        confirm["close"] > confirm["open"]
        and confirm["close"] > ema20_h4.iloc[-2] + atr_h4 * profile["ema_pad"]
        and confirm["close"] >= pullback["high"] - atr_h4 * profile["confirm_pad"]
        and current_h4["time"] > confirm["time"]
    )
    short_confirm = (
        confirm["close"] < confirm["open"]
        and confirm["close"] < ema20_h4.iloc[-2] - atr_h4 * profile["ema_pad"]
        and confirm["close"] <= pullback["low"] + atr_h4 * profile["confirm_pad"]
        and current_h4["time"] > confirm["time"]
    )

    if not d1_up and not d1_down:
        return blocked_signal(branch_name, "D1 TREND MIXED", f"ADX {adx_d1_now:.1f}")

    entry_price = tick.ask if d1_up else tick.bid
    signal_key = f"{family}|{confirm['time'].isoformat()}|{'BUY' if d1_up else 'SELL'}"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key == signal_key:
        return blocked_signal(branch_name, "DUPLICATE LEG", confirm["time"].strftime("%Y-%m-%d %H:%M"))

    if long_pullback and long_confirm:
        sl_price = min(pullback["low"], confirm["low"]) - atr_h4 * profile["sl_buffer"]
        conf = profile["boost_conf"] if adx_h4_now >= profile["boost_adx"] else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "H4 pullback through EMA20 with bullish reclaim in D1 uptrend",
            "ema20_h4": float(ema20_h4.iloc[-2]),
            "adx14_h4": adx_h4_now,
            "entry_price": float(entry_price),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while ADX strong",
            "trend_d1": f"UP | EMA20 {ema20_d1.iloc[-2]:.2f} | ADX {adx_d1_now:.1f}",
            "trend_h4": f"UP | EMA20 {ema20_h4.iloc[-2]:.2f} | ADX {adx_h4_now:.1f}",
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_h4,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key,
            "signal_details": details,
        }

    if short_pullback and short_confirm:
        sl_price = max(pullback["high"], confirm["high"]) + atr_h4 * profile["sl_buffer"]
        conf = profile["boost_conf"] if adx_h4_now >= profile["boost_adx"] else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "H4 pullback through EMA20 with bearish reclaim in D1 downtrend",
            "ema20_h4": float(ema20_h4.iloc[-2]),
            "adx14_h4": adx_h4_now,
            "entry_price": float(entry_price),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while ADX strong",
            "trend_d1": f"DOWN | EMA20 {ema20_d1.iloc[-2]:.2f} | ADX {adx_d1_now:.1f}",
            "trend_h4": f"DOWN | EMA20 {ema20_h4.iloc[-2]:.2f} | ADX {adx_h4_now:.1f}",
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_h4,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key,
            "signal_details": details,
        }

    if d1_up and h4_up_context:
        if not long_pullback:
            return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("EMA20", min(pullback["low"], confirm["low"]) - ema20_h4.iloc[-3], atr_h4))
        return blocked_signal(branch_name, "NO BULL CONFIRM", pretrigger_text("CONF", max(pullback["high"] - confirm["close"], 0.0), atr_h4))
    if d1_down and h4_down_context:
        if not short_pullback:
            return blocked_signal(branch_name, "NO PULLBACK", pretrigger_text("EMA20", ema20_h4.iloc[-3] - max(pullback["high"], confirm["high"]), atr_h4))
        return blocked_signal(branch_name, "NO BEAR CONFIRM", pretrigger_text("CONF", max(confirm["close"] - pullback["low"], 0.0), atr_h4))
    return blocked_signal(branch_name, "H4 TREND MIXED", f"H4 ADX {adx_h4_now:.1f}")


def strategy_ustech_santo_graal(df, symbol):
    return strategy_santo_graal(df, symbol, "USTECH_SANTO_GRAAL", "USTECH")


def strategy_us500_santo_graal(df, symbol):
    return strategy_santo_graal(df, symbol, "US500_SANTO_GRAAL", "US500")


def strategy_ger40_santo_graal(df, symbol):
    return strategy_santo_graal(df, symbol, "GER40_SANTO_GRAAL", "GER40")


def strategy_btc_santo_graal(df, symbol):
    return strategy_santo_graal(df, symbol, "BTC_SANTO_GRAAL", "BTCUSD")


KUMO_BREAKOUT_PROFILES = {
    "USTECH": {
        "display": "US100",
        "session": (13, 23),
        "breakout_pad": 0.06,
        "stop_buffer": 0.10,
        "max_extension": 1.10,
        "ao_delta": 0.00,
        "hold_bars": 1,
        "require_h4_bias": False,
        "base_conf": 76,
        "boost_conf": 82,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "breakout_pad": 0.08,
        "stop_buffer": 0.16,
        "max_extension": 1.80,
        "ao_delta": 0.00,
        "hold_bars": 1,
        "require_h4_bias": False,
        "base_conf": 77,
        "boost_conf": 84,
    },
    "ETHUSD": {
        "display": "ETH",
        "session": (0, 23),
        "breakout_pad": 0.08,
        "stop_buffer": 0.14,
        "max_extension": 1.55,
        "ao_delta": 0.00,
        "hold_bars": 2,
        "require_h4_bias": False,
        "base_conf": 76,
        "boost_conf": 83,
    },
}


def get_kumo_breakout_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = KUMO_BREAKOUT_PROFILES.get(family)
    if not profile:
        return None

    df_h1 = get_h1_cached(symbol)
    if df_h1 is None or len(df_h1) < 80:
        return None

    ao_h1 = compute_awesome_oscillator(df_h1)
    _, _, kumo_top_h1, kumo_bottom_h1 = compute_kumo_cloud(df_h1, 8, 29, 34, 29)
    last = df_h1.iloc[-2]
    prev = df_h1.iloc[-3]
    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    if atr_h1 <= 0:
        return None

    ao_now = float(ao_h1.iloc[-2])
    ao_prev = float(ao_h1.iloc[-3])
    kumo_top = float(kumo_top_h1.iloc[-2])
    kumo_bottom = float(kumo_bottom_h1.iloc[-2])

    if signal == "BUY":
        if ao_prev >= 0 and ao_now < 0:
            return "AO crossed below zero on H1"
        if last["close"] < kumo_top - atr_h1 * 0.04 and prev["close"] < kumo_top_h1.iloc[-3] - atr_h1 * 0.04:
            return "price re-entered Kumo on H1"
    else:
        if ao_prev <= 0 and ao_now > 0:
            return "AO crossed above zero on H1"
        if last["close"] > kumo_bottom + atr_h1 * 0.04 and prev["close"] > kumo_bottom_h1.iloc[-3] + atr_h1 * 0.04:
            return "price re-entered Kumo on H1"
    return None


def strategy_kumo_breakout(df, symbol, branch_name, family):
    profile = KUMO_BREAKOUT_PROFILES[family]
    if is_crypto_family(family):
        if not crypto_enabled:
            return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_m15 = get_data(symbol, mt5.TIMEFRAME_M15, 260)
    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_m15 is None or len(df_m15) < 120:
        return blocked_signal(branch_name, "NO M15 DATA")
    if df_h1 is None or len(df_h1) < 80:
        return blocked_signal(branch_name, "NO H1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    atr_m15 = float(df_m15["atr"].iloc[-2] or 0.0)
    if atr_m15 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    ao = compute_awesome_oscillator(df_m15)
    _, _, kumo_top, kumo_bottom = compute_kumo_cloud(df_m15, 8, 29, 34, 29)

    breakout = df_m15.iloc[-2]
    prev = df_m15.iloc[-3]
    current = df_m15.iloc[-1]
    current_time = current["time"]

    top_now = float(kumo_top.iloc[-2])
    bottom_now = float(kumo_bottom.iloc[-2])
    top_prev = float(kumo_top.iloc[-3])
    bottom_prev = float(kumo_bottom.iloc[-3])

    close_now = float(breakout["close"])
    close_prev = float(prev["close"])
    body = abs(float(breakout["close"] - breakout["open"]))
    extension = abs(close_now - top_now) if close_now > top_now else abs(bottom_now - close_now)
    ao_now = float(ao.iloc[-2])
    ao_prev = float(ao.iloc[-3])
    ao_prev2 = float(ao.iloc[-4])

    inside_now = bottom_now <= close_now <= top_now
    if inside_now:
        return blocked_signal(branch_name, "INSIDE KUMO")

    h4_bias = None
    if df_h4 is not None and len(df_h4) >= 50:
        ema20_h4 = df_h4["close"].ewm(span=20).mean().iloc[-2]
        h4_close = df_h4["close"].iloc[-2]
        if h4_close > ema20_h4:
            h4_bias = "UP"
        elif h4_close < ema20_h4:
            h4_bias = "DOWN"
        else:
            h4_bias = "FLAT"
    else:
        h4_bias = "N/A"

    breakout_up = close_prev <= top_prev + atr_m15 * 0.02 and close_now > top_now + atr_m15 * profile["breakout_pad"]
    breakout_down = close_prev >= bottom_prev - atr_m15 * 0.02 and close_now < bottom_now - atr_m15 * profile["breakout_pad"]
    ao_up = ao_now > 0 and ao_now > ao_prev + atr_m15 * profile["ao_delta"] and ao_prev <= ao_prev2
    ao_down = ao_now < 0 and ao_now < ao_prev - atr_m15 * profile["ao_delta"] and ao_prev >= ao_prev2

    if family == "ETHUSD":
        hold_ok_up = float(df_m15["close"].iloc[-3]) > float(kumo_top.iloc[-3])
        hold_ok_down = float(df_m15["close"].iloc[-3]) < float(kumo_bottom.iloc[-3])
    else:
        hold_ok_up = True
        hold_ok_down = True

    if extension > atr_m15 * profile["max_extension"]:
        return blocked_signal(branch_name, "OVEREXTENDED BREAKOUT", pretrigger_text("EXT", extension - atr_m15 * profile["max_extension"], atr_m15))

    signal_key_up = f"{family}|KUMO|{breakout['time'].isoformat()}|BUY"
    signal_key_down = f"{family}|KUMO|{breakout['time'].isoformat()}|SELL"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key in {signal_key_up, signal_key_down}:
        return blocked_signal(branch_name, "DUPLICATE BREAKOUT", breakout["time"].strftime("%Y-%m-%d %H:%M"))

    if breakout_up and ao_up and hold_ok_up:
        if h4_bias == "DOWN":
            return blocked_signal(branch_name, "H4 OPPOSITE", "H4 below EMA20")
        sl_price = min(float(breakout["low"]), top_now) - atr_m15 * profile["stop_buffer"]
        conf = profile["boost_conf"] if ao_now > abs(ao_prev) and h4_bias == "UP" else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "bullish Kumo breakout confirmed by AO upturn",
            "kumo_side": "upper Kumo",
            "ao_state": f"AO up | {ao_prev:.2f}->{ao_now:.2f}",
            "entry_price": float(tick.ask),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while breakout holds",
            "position_state": "breakout valid",
            "summary": (
                f"{profile['display']} LONG | bullish Kumo breakout | upper Kumo | "
                f"AO {ao_prev:.2f}->{ao_now:.2f} | entry {tick.ask:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | state breakout valid"
            ),
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m15,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_up,
            "signal_details": details,
        }

    if breakout_down and ao_down and hold_ok_down:
        if h4_bias == "UP":
            return blocked_signal(branch_name, "H4 OPPOSITE", "H4 above EMA20")
        sl_price = max(float(breakout["high"]), bottom_now) + atr_m15 * profile["stop_buffer"]
        conf = profile["boost_conf"] if abs(ao_now) > ao_prev and h4_bias == "DOWN" else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "bearish Kumo breakout confirmed by AO downturn",
            "kumo_side": "lower Kumo",
            "ao_state": f"AO down | {ao_prev:.2f}->{ao_now:.2f}",
            "entry_price": float(tick.bid),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while breakout holds",
            "position_state": "breakout valid",
            "summary": (
                f"{profile['display']} SHORT | bearish Kumo breakout | lower Kumo | "
                f"AO {ao_prev:.2f}->{ao_now:.2f} | entry {tick.bid:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | state breakout valid"
            ),
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m15,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_down,
            "signal_details": details,
        }

    if close_now > top_now:
        if not breakout_up:
            return blocked_signal(branch_name, "WEAK BREAKOUT", pretrigger_text("KUMO", max(0.0, (top_now + atr_m15 * profile["breakout_pad"]) - close_now), atr_m15))
        if not ao_up:
            return blocked_signal(branch_name, "AO NOT CONFIRMED", f"AO {ao_prev:.2f}->{ao_now:.2f}")
        if not hold_ok_up:
            return blocked_signal(branch_name, "BREAKOUT ANNULLED", "ETH hold outside Kumo failed")
    elif close_now < bottom_now:
        if not breakout_down:
            return blocked_signal(branch_name, "WEAK BREAKOUT", pretrigger_text("KUMO", max(0.0, close_now - (bottom_now - atr_m15 * profile["breakout_pad"])), atr_m15))
        if not ao_down:
            return blocked_signal(branch_name, "AO NOT CONFIRMED", f"AO {ao_prev:.2f}->{ao_now:.2f}")
        if not hold_ok_down:
            return blocked_signal(branch_name, "BREAKOUT ANNULLED", "ETH hold outside Kumo failed")
    return blocked_signal(branch_name, "BREAKOUT ANNULLED", current_time.strftime("%H:%M"))


def strategy_ustech_kumo_breakout(df, symbol):
    return strategy_kumo_breakout(df, symbol, "USTECH_KUMO_BREAKOUT", "USTECH")


def strategy_btc_kumo_breakout(df, symbol):
    return strategy_kumo_breakout(df, symbol, "BTC_KUMO_BREAKOUT", "BTCUSD")


def strategy_eth_kumo_breakout(df, symbol):
    return strategy_kumo_breakout(df, symbol, "ETH_KUMO_BREAKOUT", "ETHUSD")


SIDUS_PROFILES = {
    "GER40": {
        "display": "DAX",
        "session": (8, 18),
        "cross_pad": 0.04,
        "close_pad": 0.03,
        "compression": 0.24,
        "flat_slope": 0.09,
        "max_extension": 1.15,
        "body_floor": 0.08,
        "wick_ratio": 1.18,
        "sl_buffer": 0.10,
        "h1_bias_pad": 0.02,
        "base_conf": 74,
        "boost_conf": 82,
        "whipsaw_limit": 3,
    },
    "USTECH": {
        "display": "US100",
        "session": (13, 23),
        "cross_pad": 0.03,
        "close_pad": 0.025,
        "compression": 0.20,
        "flat_slope": 0.075,
        "max_extension": 1.35,
        "body_floor": 0.06,
        "wick_ratio": 1.35,
        "sl_buffer": 0.10,
        "h1_bias_pad": 0.02,
        "base_conf": 76,
        "boost_conf": 84,
        "whipsaw_limit": 3,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "cross_pad": 0.06,
        "close_pad": 0.05,
        "compression": 0.26,
        "flat_slope": 0.09,
        "max_extension": 1.65,
        "body_floor": 0.06,
        "wick_ratio": 1.45,
        "sl_buffer": 0.14,
        "h1_bias_pad": 0.03,
        "base_conf": 77,
        "boost_conf": 85,
        "whipsaw_limit": 3,
    },
}


def _sidus_channel_snapshot(ema18, ema28, wma5, wma8, idx):
    red_low = min(float(ema18.iloc[idx]), float(ema28.iloc[idx]))
    red_high = max(float(ema18.iloc[idx]), float(ema28.iloc[idx]))
    blue_low = min(float(wma5.iloc[idx]), float(wma8.iloc[idx]))
    blue_high = max(float(wma5.iloc[idx]), float(wma8.iloc[idx]))
    return red_low, red_high, blue_low, blue_high


def _recent_ma_cross_direction(fast, slow, lookback=4, end_offset=2):
    end = len(fast) - end_offset
    start = max(1, end - lookback + 1)
    up = False
    down = False
    for pos in range(start, end + 1):
        prev_fast = float(fast.iloc[pos - 1])
        prev_slow = float(slow.iloc[pos - 1])
        now_fast = float(fast.iloc[pos])
        now_slow = float(slow.iloc[pos])
        if prev_fast <= prev_slow and now_fast > now_slow:
            up = True
        if prev_fast >= prev_slow and now_fast < now_slow:
            down = True
    return up, down


def _count_sign_flips(series, lookback=6, end_offset=2):
    end = len(series) - end_offset
    start = max(0, end - lookback + 1)
    values = np.sign(series.iloc[start : end + 1].fillna(0).to_numpy(dtype=float))
    flips = 0
    for left, right in zip(values[:-1], values[1:]):
        if left == 0 or right == 0:
            continue
        if left != right:
            flips += 1
    return flips


def _sidus_higher_bias(df_tf, atr_pad=0.02):
    if df_tf is None or len(df_tf) < 60:
        return "N/A"
    close = df_tf["close"]
    atr = float(df_tf["atr"].iloc[-2] or 0.0)
    ema18 = close.ewm(span=18).mean()
    ema28 = close.ewm(span=28).mean()
    wma5 = compute_wma(close, 5)
    wma8 = compute_wma(close, 8)
    red_low, red_high, blue_low, blue_high = _sidus_channel_snapshot(ema18, ema28, wma5, wma8, -2)
    pad = atr * atr_pad
    last_close = float(close.iloc[-2])
    if blue_low > red_high + pad and last_close > red_high + pad:
        return "UP"
    if blue_high < red_low - pad and last_close < red_low - pad:
        return "DOWN"
    return "MIXED"


def get_sidus_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = SIDUS_PROFILES.get(family)
    if not profile:
        return None

    df_h1 = get_h1_cached(symbol)
    if df_h1 is None or len(df_h1) < 80:
        return None

    close = df_h1["close"]
    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    if atr_h1 <= 0:
        return None

    ema18 = close.ewm(span=18).mean()
    ema28 = close.ewm(span=28).mean()
    wma5 = compute_wma(close, 5)
    wma8 = compute_wma(close, 8)
    red_low, red_high, blue_low, blue_high = _sidus_channel_snapshot(ema18, ema28, wma5, wma8, -2)
    pad = atr_h1 * profile["h1_bias_pad"]

    cross_up, cross_down = _recent_ma_cross_direction(wma5, wma8, lookback=2, end_offset=2)

    if signal == "BUY":
        if cross_down:
            return "WMA 5/8 bearish cross on H1"
        if blue_high < red_low - pad:
            return "blue channel fell below red channel on H1"
    else:
        if cross_up:
            return "WMA 5/8 bullish cross on H1"
        if blue_low > red_high + pad:
            return "blue channel rose above red channel on H1"
    return None


def strategy_sidus(df, symbol, branch_name, family):
    profile = SIDUS_PROFILES[family]
    if is_crypto_family(family) and not crypto_enabled:
        return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_m30 = get_m30_cached(symbol)
    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_m30 is None or len(df_m30) < 90:
        return blocked_signal(branch_name, "NO M30 DATA")
    if df_h1 is None or len(df_h1) < 80:
        return blocked_signal(branch_name, "NO H1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    close = df_m30["close"]
    atr_m30 = float(df_m30["atr"].iloc[-2] or 0.0)
    if atr_m30 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    ema18 = close.ewm(span=18).mean()
    ema28 = close.ewm(span=28).mean()
    wma5 = compute_wma(close, 5)
    wma8 = compute_wma(close, 8)
    confirm = df_m30.iloc[-2]
    current = df_m30.iloc[-1]

    red_low_now, red_high_now, blue_low_now, blue_high_now = _sidus_channel_snapshot(ema18, ema28, wma5, wma8, -2)
    red_low_prev, red_high_prev, blue_low_prev, blue_high_prev = _sidus_channel_snapshot(ema18, ema28, wma5, wma8, -3)
    red_low_prev2, red_high_prev2, blue_low_prev2, blue_high_prev2 = _sidus_channel_snapshot(ema18, ema28, wma5, wma8, -4)

    body = abs(float(confirm["close"] - confirm["open"]))
    wick_up = float(confirm["high"] - max(confirm["open"], confirm["close"]))
    wick_down = float(min(confirm["open"], confirm["close"]) - confirm["low"])
    extension_up = max(0.0, float(confirm["close"]) - red_high_now)
    extension_down = max(0.0, red_low_now - float(confirm["close"]))
    channel_span = max(red_high_now, blue_high_now) - min(red_low_now, blue_low_now)
    ema_slope = max(abs(float(ema18.iloc[-2] - ema18.iloc[-6])), abs(float(ema28.iloc[-2] - ema28.iloc[-6]))) / max(atr_m30, 1e-6)
    whipsaw_flips = _count_sign_flips(wma5 - wma8, lookback=7, end_offset=2)

    if channel_span < atr_m30 * profile["compression"]:
        return blocked_signal(branch_name, "MA COMPRESSED", pretrigger_text("MA", atr_m30 * profile["compression"] - channel_span, atr_m30))
    if ema_slope < profile["flat_slope"]:
        return blocked_signal(branch_name, "EMA FLAT", f"SLOPE {round(ema_slope, 2)}")
    if whipsaw_flips > profile["whipsaw_limit"]:
        return blocked_signal(branch_name, "WMA WHIPSAW", f"FLIPS {whipsaw_flips}")

    cross_hint_up, cross_hint_down = _recent_ma_cross_direction(wma5, wma8, lookback=4, end_offset=2)
    full_below_recent = (
        blue_high_prev < red_low_prev - atr_m30 * profile["cross_pad"]
        or blue_high_prev2 < red_low_prev2 - atr_m30 * profile["cross_pad"]
    )
    full_above_recent = (
        blue_low_prev > red_high_prev + atr_m30 * profile["cross_pad"]
        or blue_low_prev2 > red_high_prev2 + atr_m30 * profile["cross_pad"]
    )

    long_cross = full_below_recent and blue_low_now > red_high_now + atr_m30 * profile["cross_pad"]
    short_cross = full_above_recent and blue_high_now < red_low_now - atr_m30 * profile["cross_pad"]

    long_confirm = (
        float(confirm["close"]) > red_high_now + atr_m30 * profile["close_pad"]
        and body >= atr_m30 * profile["body_floor"]
        and wick_up <= max(body * profile["wick_ratio"], atr_m30 * 0.28)
        and extension_up <= atr_m30 * profile["max_extension"]
        and current["time"] > confirm["time"]
    )
    short_confirm = (
        float(confirm["close"]) < red_low_now - atr_m30 * profile["close_pad"]
        and body >= atr_m30 * profile["body_floor"]
        and wick_down <= max(body * profile["wick_ratio"], atr_m30 * 0.28)
        and extension_down <= atr_m30 * profile["max_extension"]
        and current["time"] > confirm["time"]
    )

    h1_bias = _sidus_higher_bias(df_h1, profile["h1_bias_pad"])
    h4_bias = _sidus_higher_bias(df_h4, profile["h1_bias_pad"])
    if long_cross and h1_bias == "DOWN":
        return blocked_signal(branch_name, "H1 OPPOSITE", "H1 blue below red")
    if short_cross and h1_bias == "UP":
        return blocked_signal(branch_name, "H1 OPPOSITE", "H1 blue above red")
    if long_cross and h4_bias == "DOWN":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 bias down")
    if short_cross and h4_bias == "UP":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 bias up")

    prelim_text = "YES" if (cross_hint_up or cross_hint_down) else "NO"
    long_signal_key = f"{family}|SIDUS|{confirm['time'].isoformat()}|BUY"
    short_signal_key = f"{family}|SIDUS|{confirm['time'].isoformat()}|SELL"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key in {long_signal_key, short_signal_key}:
        return blocked_signal(branch_name, "DUPLICATE LEG", confirm["time"].strftime("%Y-%m-%d %H:%M"))

    if long_cross and long_confirm:
        swing_low = float(df_m30["low"].iloc[-7:-1].min())
        sl_price = min(swing_low, red_low_now) - atr_m30 * profile["sl_buffer"]
        conf = profile["boost_conf"] if cross_hint_up and h1_bias == "UP" else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "Sidus bullish full blue-channel cross above red channel",
            "channel_state": f"WMA5/WMA8 above EMA18/EMA28 | blue {blue_low_now:.2f}-{blue_high_now:.2f} vs red {red_low_now:.2f}-{red_high_now:.2f}",
            "preliminary_cross": "WMA5 crossed WMA8 up before channel cross" if cross_hint_up else "no early WMA 5/8 cross",
            "entry_price": float(tick.ask),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing until opposite Sidus cross",
            "trend_m30": f"UP | cross clean | slope {ema_slope:.2f}",
            "trend_h1": f"{h1_bias} | H4 {h4_bias}",
            "summary": (
                f"{profile['display']} LONG | Sidus bullish cross | WMA5/WMA8 above EMA18/EMA28 | "
                f"pre-cross {prelim_text} | entry {tick.ask:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | M30 UP | H1 {h1_bias}"
            ),
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m30,
            "sl_price": round(sl_price, 5),
            "signal_key": long_signal_key,
            "signal_details": details,
        }

    if short_cross and short_confirm:
        swing_high = float(df_m30["high"].iloc[-7:-1].max())
        sl_price = max(swing_high, red_high_now) + atr_m30 * profile["sl_buffer"]
        conf = profile["boost_conf"] if cross_hint_down and h1_bias == "DOWN" else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "Sidus bearish full blue-channel cross below red channel",
            "channel_state": f"WMA5/WMA8 below EMA18/EMA28 | blue {blue_low_now:.2f}-{blue_high_now:.2f} vs red {red_low_now:.2f}-{red_high_now:.2f}",
            "preliminary_cross": "WMA5 crossed WMA8 down before channel cross" if cross_hint_down else "no early WMA 5/8 cross",
            "entry_price": float(tick.bid),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing until opposite Sidus cross",
            "trend_m30": f"DOWN | cross clean | slope {ema_slope:.2f}",
            "trend_h1": f"{h1_bias} | H4 {h4_bias}",
            "summary": (
                f"{profile['display']} SHORT | Sidus bearish cross | WMA5/WMA8 below EMA18/EMA28 | "
                f"pre-cross {prelim_text} | entry {tick.bid:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | M30 DOWN | H1 {h1_bias}"
            ),
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m30,
            "sl_price": round(sl_price, 5),
            "signal_key": short_signal_key,
            "signal_details": details,
        }

    if blue_low_now <= red_high_now and blue_high_now >= red_low_now:
        return blocked_signal(branch_name, "PARTIAL CROSS", pretrigger_text("SIDUS", abs(blue_low_now - red_high_now), atr_m30))
    if full_below_recent and not long_cross:
        return blocked_signal(branch_name, "WAIT BULL CROSS", pretrigger_text("SIDUS", (red_high_now + atr_m30 * profile["cross_pad"]) - blue_low_now, atr_m30))
    if full_above_recent and not short_cross:
        return blocked_signal(branch_name, "WAIT BEAR CROSS", pretrigger_text("SIDUS", blue_high_now - (red_low_now - atr_m30 * profile["cross_pad"]), atr_m30))
    return blocked_signal(branch_name, "SIDUS NOT READY", confirm["time"].strftime("%H:%M"))


def strategy_ustech_sidus(df, symbol):
    return strategy_sidus(df, symbol, "USTECH_SIDUS", "USTECH")


def strategy_btc_sidus(df, symbol):
    return strategy_sidus(df, symbol, "BTC_SIDUS", "BTCUSD")


def strategy_ger40_sidus(df, symbol):
    return strategy_sidus(df, symbol, "GER40_SIDUS", "GER40")


PURIA_PROFILES = {
    "GER40": {
        "display": "DAX",
        "session": (8, 18),
        "cross_pad": 0.04,
        "close_pad": 0.03,
        "compression": 0.18,
        "flat_slope": 0.07,
        "body_floor": 0.08,
        "wick_ratio": 1.16,
        "macd_floor": 0.040,
        "macd_expand": 0.016,
        "sl_buffer": 0.10,
        "whipsaw_limit": 3,
        "base_conf": 74,
        "boost_conf": 82,
    },
    "USTECH": {
        "display": "US100",
        "session": (13, 23),
        "cross_pad": 0.03,
        "close_pad": 0.025,
        "compression": 0.16,
        "flat_slope": 0.06,
        "body_floor": 0.06,
        "wick_ratio": 1.28,
        "macd_floor": 0.035,
        "macd_expand": 0.014,
        "sl_buffer": 0.10,
        "whipsaw_limit": 3,
        "base_conf": 76,
        "boost_conf": 84,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "cross_pad": 0.06,
        "close_pad": 0.05,
        "compression": 0.22,
        "flat_slope": 0.08,
        "body_floor": 0.06,
        "wick_ratio": 1.40,
        "macd_floor": 0.055,
        "macd_expand": 0.020,
        "sl_buffer": 0.15,
        "whipsaw_limit": 3,
        "base_conf": 77,
        "boost_conf": 85,
    },
    "ETHUSD": {
        "display": "ETH",
        "session": (0, 23),
        "cross_pad": 0.06,
        "close_pad": 0.05,
        "compression": 0.20,
        "flat_slope": 0.08,
        "body_floor": 0.07,
        "wick_ratio": 1.32,
        "macd_floor": 0.055,
        "macd_expand": 0.020,
        "sl_buffer": 0.14,
        "whipsaw_limit": 3,
        "base_conf": 76,
        "boost_conf": 84,
    },
}


def _puria_bias(df_tf, macd_floor_mult=0.05):
    if df_tf is None or len(df_tf) < 100:
        return "N/A"
    close = df_tf["close"]
    low = df_tf["low"]
    atr = float(df_tf["atr"].iloc[-2] or 0.0)
    if atr <= 0:
        return "N/A"
    ema5 = close.ewm(span=5, adjust=False).mean()
    ma75 = compute_wma(low, 75)
    ma85 = compute_wma(low, 85)
    macd_hist = compute_macd_hist(close, 15, 26)
    slow_low = min(float(ma75.iloc[-2]), float(ma85.iloc[-2]))
    slow_high = max(float(ma75.iloc[-2]), float(ma85.iloc[-2]))
    ema_now = float(ema5.iloc[-2])
    macd_now = float(macd_hist.iloc[-2])
    if ema_now > slow_high + atr * 0.02 and macd_now > atr * macd_floor_mult:
        return "UP"
    if ema_now < slow_low - atr * 0.02 and macd_now < -atr * macd_floor_mult:
        return "DOWN"
    return "MIXED"


def get_puria_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = PURIA_PROFILES.get(family)
    if not profile:
        return None

    df_h1 = get_h1_cached(symbol)
    if df_h1 is None or len(df_h1) < 100:
        return None

    close = df_h1["close"]
    low = df_h1["low"]
    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    if atr_h1 <= 0:
        return None

    ema5 = close.ewm(span=5, adjust=False).mean()
    ma75 = compute_wma(low, 75)
    ma85 = compute_wma(low, 85)
    macd_hist = compute_macd_hist(close, 15, 26)

    ema_now = float(ema5.iloc[-2])
    ema_prev = float(ema5.iloc[-3])
    ma75_now = float(ma75.iloc[-2])
    ma75_prev = float(ma75.iloc[-3])
    ma85_now = float(ma85.iloc[-2])
    ma85_prev = float(ma85.iloc[-3])
    macd_now = float(macd_hist.iloc[-2])
    macd_prev = float(macd_hist.iloc[-3])
    macd_floor = atr_h1 * profile["macd_floor"]

    if signal == "BUY":
        if macd_now < macd_prev and macd_prev > macd_floor:
            return "MACD momentum rolled over on H1"
        if ema_prev >= max(ma75_prev, ma85_prev) and ema_now < min(ma75_now, ma85_now):
            return "EMA5 crossed back below MA75/MA85 on H1"
    else:
        if macd_now > macd_prev and macd_prev < -macd_floor:
            return "MACD momentum rolled over on H1"
        if ema_prev <= min(ma75_prev, ma85_prev) and ema_now > max(ma75_now, ma85_now):
            return "EMA5 crossed back above MA75/MA85 on H1"
    return None


def strategy_puria(df, symbol, branch_name, family):
    profile = PURIA_PROFILES[family]
    if is_crypto_family(family) and not crypto_enabled:
        return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_m30 = get_m30_cached(symbol)
    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_m30 is None or len(df_m30) < 110:
        return blocked_signal(branch_name, "NO M30 DATA")
    if df_h1 is None or len(df_h1) < 100:
        return blocked_signal(branch_name, "NO H1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    close = df_m30["close"]
    low = df_m30["low"]
    atr_m30 = float(df_m30["atr"].iloc[-2] or 0.0)
    if atr_m30 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    ema5 = close.ewm(span=5, adjust=False).mean()
    ma75 = compute_wma(low, 75)
    ma85 = compute_wma(low, 85)
    macd_hist = compute_macd_hist(close, 15, 26)
    confirm = df_m30.iloc[-2]
    current = df_m30.iloc[-1]

    ema_now = float(ema5.iloc[-2])
    ema_prev = float(ema5.iloc[-3])
    ma75_now = float(ma75.iloc[-2])
    ma75_prev = float(ma75.iloc[-3])
    ma85_now = float(ma85.iloc[-2])
    ma85_prev = float(ma85.iloc[-3])
    macd_now = float(macd_hist.iloc[-2])
    macd_prev = float(macd_hist.iloc[-3])
    macd_prev2 = float(macd_hist.iloc[-4])

    slow_low_now = min(ma75_now, ma85_now)
    slow_high_now = max(ma75_now, ma85_now)
    slow_low_prev = min(ma75_prev, ma85_prev)
    slow_high_prev = max(ma75_prev, ma85_prev)
    slow_span = abs(ma75_now - ma85_now)
    slow_slope = max(abs(ma75_now - float(ma75.iloc[-6])), abs(ma85_now - float(ma85.iloc[-6]))) / max(atr_m30, 1e-6)
    body = abs(float(confirm["close"] - confirm["open"]))
    wick_up = float(confirm["high"] - max(confirm["open"], confirm["close"]))
    wick_down = float(min(confirm["open"], confirm["close"]) - confirm["low"])
    macd_floor = atr_m30 * profile["macd_floor"]
    macd_expand = atr_m30 * profile["macd_expand"]

    if slow_span < atr_m30 * profile["compression"] and slow_slope < profile["flat_slope"]:
        return blocked_signal(branch_name, "MA FLAT", pretrigger_text("MA", atr_m30 * profile["compression"] - slow_span, atr_m30))
    if abs(macd_now) < macd_floor and abs(macd_now - macd_prev) < macd_expand:
        return blocked_signal(branch_name, "MACD FLAT", f"MACD {macd_now:.3f}")

    flips_75 = _count_sign_flips(ema5 - ma75, lookback=7, end_offset=2)
    flips_85 = _count_sign_flips(ema5 - ma85, lookback=7, end_offset=2)
    if max(flips_75, flips_85) > profile["whipsaw_limit"]:
        return blocked_signal(branch_name, "EMA5 WHIPSAW", f"75:{flips_75} 85:{flips_85}")

    long_cross = ema_prev <= slow_low_prev - atr_m30 * profile["cross_pad"] and ema_now > slow_high_now + atr_m30 * profile["cross_pad"]
    short_cross = ema_prev >= slow_high_prev + atr_m30 * profile["cross_pad"] and ema_now < slow_low_now - atr_m30 * profile["cross_pad"]

    above_75 = ema_now > ma75_now + atr_m30 * profile["cross_pad"]
    above_85 = ema_now > ma85_now + atr_m30 * profile["cross_pad"]
    below_75 = ema_now < ma75_now - atr_m30 * profile["cross_pad"]
    below_85 = ema_now < ma85_now - atr_m30 * profile["cross_pad"]
    partial_cross = (above_75 ^ above_85) or (below_75 ^ below_85)
    if partial_cross:
        return blocked_signal(branch_name, "SINGLE MA CROSS", "EMA5 crossed only one slow MA")

    long_macd_ok = (macd_now > macd_floor or macd_prev > macd_floor) and macd_now >= macd_prev - macd_expand
    short_macd_ok = (macd_now < -macd_floor or macd_prev < -macd_floor) and macd_now <= macd_prev + macd_expand

    long_confirm = (
        float(confirm["close"]) > slow_high_now + atr_m30 * profile["close_pad"]
        and body >= atr_m30 * profile["body_floor"]
        and wick_up <= max(body * profile["wick_ratio"], atr_m30 * 0.28)
        and current["time"] > confirm["time"]
    )
    short_confirm = (
        float(confirm["close"]) < slow_low_now - atr_m30 * profile["close_pad"]
        and body >= atr_m30 * profile["body_floor"]
        and wick_down <= max(body * profile["wick_ratio"], atr_m30 * 0.28)
        and current["time"] > confirm["time"]
    )

    h1_bias = _puria_bias(df_h1, profile["macd_floor"])
    h4_bias = _puria_bias(df_h4, profile["macd_floor"])
    if long_cross and h1_bias == "DOWN":
        return blocked_signal(branch_name, "H1 OPPOSITE", "H1 Puria bias down")
    if short_cross and h1_bias == "UP":
        return blocked_signal(branch_name, "H1 OPPOSITE", "H1 Puria bias up")
    if long_cross and h4_bias == "DOWN":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 Puria bias down")
    if short_cross and h4_bias == "UP":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 Puria bias up")

    signal_key_buy = f"{family}|PURIA|{confirm['time'].isoformat()}|BUY"
    signal_key_sell = f"{family}|PURIA|{confirm['time'].isoformat()}|SELL"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key in {signal_key_buy, signal_key_sell}:
        return blocked_signal(branch_name, "DUPLICATE LEG", confirm["time"].strftime("%Y-%m-%d %H:%M"))

    if family == "ETHUSD":
        long_confirm = long_confirm and macd_now > macd_floor and float(confirm["close"]) > slow_high_now + atr_m30 * (profile["close_pad"] + 0.01)
        short_confirm = short_confirm and macd_now < -macd_floor and float(confirm["close"]) < slow_low_now - atr_m30 * (profile["close_pad"] + 0.01)
    if family == "BTCUSD":
        long_macd_ok = long_macd_ok and macd_now > macd_floor and (macd_now - macd_prev) >= -macd_expand * 0.4
        short_macd_ok = short_macd_ok and macd_now < -macd_floor and (macd_now - macd_prev) <= macd_expand * 0.4

    if long_cross and long_macd_ok and long_confirm:
        swing_low = float(df_m30["low"].iloc[-7:-1].min())
        sl_price = min(swing_low, slow_low_now) - atr_m30 * profile["sl_buffer"]
        conf = profile["boost_conf"] if h1_bias == "UP" and h4_bias != "DOWN" and macd_now > max(macd_prev, macd_floor) else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "Puria bullish EMA5 cross above both LWMA75/85 with MACD above zero",
            "ema_state": f"EMA5 {ema_now:.2f} > MA75 {ma75_now:.2f} / MA85 {ma85_now:.2f}",
            "macd_state": f"MACD {macd_prev:.3f}->{macd_now:.3f} above zero",
            "entry_price": float(tick.ask),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing until Puria reversal",
            "trend_m30": f"UP | slope {slow_slope:.2f}",
            "trend_h1": f"{h1_bias} | H4 {h4_bias}",
            "summary": (
                f"{profile['display']} LONG | Puria bullish cross | EMA5 above MA75/MA85 | "
                f"MACD {macd_prev:.3f}->{macd_now:.3f} > 0 | entry {tick.ask:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | M30 UP | H1 {h1_bias}"
            ),
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m30,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_buy,
            "signal_details": details,
        }

    if short_cross and short_macd_ok and short_confirm:
        swing_high = float(df_m30["high"].iloc[-7:-1].max())
        sl_price = max(swing_high, slow_high_now) + atr_m30 * profile["sl_buffer"]
        conf = profile["boost_conf"] if h1_bias == "DOWN" and h4_bias != "UP" and macd_now < min(macd_prev, -macd_floor) else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "Puria bearish EMA5 cross below both LWMA75/85 with MACD below zero",
            "ema_state": f"EMA5 {ema_now:.2f} < MA75 {ma75_now:.2f} / MA85 {ma85_now:.2f}",
            "macd_state": f"MACD {macd_prev:.3f}->{macd_now:.3f} below zero",
            "entry_price": float(tick.bid),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing until Puria reversal",
            "trend_m30": f"DOWN | slope {slow_slope:.2f}",
            "trend_h1": f"{h1_bias} | H4 {h4_bias}",
            "summary": (
                f"{profile['display']} SHORT | Puria bearish cross | EMA5 below MA75/MA85 | "
                f"MACD {macd_prev:.3f}->{macd_now:.3f} < 0 | entry {tick.bid:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | M30 DOWN | H1 {h1_bias}"
            ),
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m30,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_sell,
            "signal_details": details,
        }

    if long_cross and not long_macd_ok:
        return blocked_signal(branch_name, "MACD NOT CONFIRMED", f"MACD {macd_prev:.3f}->{macd_now:.3f}")
    if short_cross and not short_macd_ok:
        return blocked_signal(branch_name, "MACD NOT CONFIRMED", f"MACD {macd_prev:.3f}->{macd_now:.3f}")
    if long_cross and not long_confirm:
        return blocked_signal(branch_name, "NO BULL CONFIRM", pretrigger_text("CONF", (slow_high_now + atr_m30 * profile["close_pad"]) - float(confirm['close']), atr_m30))
    if short_cross and not short_confirm:
        return blocked_signal(branch_name, "NO BEAR CONFIRM", pretrigger_text("CONF", float(confirm['close']) - (slow_low_now - atr_m30 * profile["close_pad"]), atr_m30))
    return blocked_signal(branch_name, "PURIA NOT READY", confirm["time"].strftime("%H:%M"))


def strategy_ger40_puria(df, symbol):
    return strategy_puria(df, symbol, "GER40_PURIA", "GER40")


def strategy_ustech_puria(df, symbol):
    return strategy_puria(df, symbol, "USTECH_PURIA", "USTECH")


def strategy_btc_puria(df, symbol):
    return strategy_puria(df, symbol, "BTC_PURIA", "BTCUSD")


def strategy_eth_puria(df, symbol):
    return strategy_puria(df, symbol, "ETH_PURIA", "ETHUSD")


DOUBLE_MACD_PROFILES = {
    "US500": {
        "display": "US500",
        "session": (0, 23),
        "sma_sep": 0.24,
        "senior_floor": 0.055,
        "senior_hist_floor": 0.014,
        "junior_floor": 0.035,
        "junior_near_zero": 0.24,
        "junior_expand": 0.014,
        "whipsaw_limit": 3,
        "swing_lookback": 8,
        "sl_buffer": 0.10,
        "base_conf": 75,
        "boost_conf": 83,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "sma_sep": 0.30,
        "senior_floor": 0.080,
        "senior_hist_floor": 0.020,
        "junior_floor": 0.055,
        "junior_near_zero": 0.28,
        "junior_expand": 0.018,
        "whipsaw_limit": 3,
        "swing_lookback": 10,
        "sl_buffer": 0.16,
        "base_conf": 77,
        "boost_conf": 85,
    },
    "ETHUSD": {
        "display": "ETH",
        "session": (0, 23),
        "sma_sep": 0.32,
        "senior_floor": 0.085,
        "senior_hist_floor": 0.022,
        "junior_floor": 0.060,
        "junior_near_zero": 0.26,
        "junior_expand": 0.018,
        "whipsaw_limit": 3,
        "swing_lookback": 10,
        "sl_buffer": 0.15,
        "base_conf": 76,
        "boost_conf": 84,
    },
}


def _double_macd_h4_state(df_h4):
    if df_h4 is None or len(df_h4) < 100:
        return "N/A"
    close = df_h4["close"]
    sma60 = close.rolling(60).mean()
    senior_line, senior_signal, _ = compute_macd(close, 30, 60, 30)
    last_close = float(close.iloc[-2])
    last_sma = float(sma60.iloc[-2] or 0.0)
    senior_now = float(senior_line.iloc[-2])
    senior_sig = float(senior_signal.iloc[-2])
    if last_close > last_sma and senior_now > 0 and senior_now >= senior_sig:
        return "UP"
    if last_close < last_sma and senior_now < 0 and senior_now <= senior_sig:
        return "DOWN"
    return "MIXED"


def get_double_macd_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = DOUBLE_MACD_PROFILES.get(family)
    if not profile:
        return None

    df_d1 = get_d1_cached(symbol)
    if df_d1 is None or len(df_d1) < 110:
        return None

    close = df_d1["close"]
    high = df_d1["high"]
    low = df_d1["low"]
    atr_d1 = float(df_d1["atr"].iloc[-2] or 0.0)
    if atr_d1 <= 0:
        return None

    sma60 = close.rolling(60).mean()
    senior_line, senior_signal, senior_hist = compute_macd(close, 30, 60, 30)
    junior_line, junior_signal, junior_hist = compute_macd(close, 6, 12, 5)

    price_now = float(close.iloc[-2])
    sma_now = float(sma60.iloc[-2])
    senior_now = float(senior_line.iloc[-2])
    senior_prev = float(senior_line.iloc[-3])
    senior_sig = float(senior_signal.iloc[-2])
    senior_hist_now = float(senior_hist.iloc[-2])
    senior_hist_prev = float(senior_hist.iloc[-3])
    junior_now = float(junior_line.iloc[-2])
    junior_prev = float(junior_line.iloc[-3])
    junior_sig = float(junior_signal.iloc[-2])
    junior_hist_now = float(junior_hist.iloc[-2])
    junior_hist_prev = float(junior_hist.iloc[-3])

    if signal == "BUY":
        if price_now < sma_now - atr_d1 * 0.02:
            return "D1 close below SMA60"
        if senior_now < senior_sig and senior_now < senior_prev and senior_hist_now < senior_hist_prev and junior_now < junior_sig and junior_hist_now < junior_hist_prev:
            return "senior MACD rolled over and junior confirmed bearish"
    else:
        if price_now > sma_now + atr_d1 * 0.02:
            return "D1 close above SMA60"
        if senior_now > senior_sig and senior_now > senior_prev and senior_hist_now > senior_hist_prev and junior_now > junior_sig and junior_hist_now > junior_hist_prev:
            return "senior MACD rolled over and junior confirmed bullish"
    return None


def strategy_double_macd(df, symbol, branch_name, family):
    profile = DOUBLE_MACD_PROFILES[family]
    if is_crypto_family(family) and not crypto_enabled:
        return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_d1 = get_d1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_d1 is None or len(df_d1) < 120:
        return blocked_signal(branch_name, "NO D1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    close = df_d1["close"]
    high = df_d1["high"]
    low = df_d1["low"]
    atr_d1 = float(df_d1["atr"].iloc[-2] or 0.0)
    if atr_d1 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    sma60 = close.rolling(60).mean()
    senior_line, senior_signal, senior_hist = compute_macd(close, 30, 60, 30)
    junior_line, junior_signal, junior_hist = compute_macd(close, 6, 12, 5)

    confirm = df_d1.iloc[-2]
    current = df_d1.iloc[-1]
    price_now = float(confirm["close"])
    sma_now = float(sma60.iloc[-2])
    senior_now = float(senior_line.iloc[-2])
    senior_prev = float(senior_line.iloc[-3])
    senior_sig = float(senior_signal.iloc[-2])
    senior_hist_now = float(senior_hist.iloc[-2])
    senior_hist_prev = float(senior_hist.iloc[-3])
    junior_now = float(junior_line.iloc[-2])
    junior_prev = float(junior_line.iloc[-3])
    junior_sig = float(junior_signal.iloc[-2])
    junior_hist_now = float(junior_hist.iloc[-2])
    junior_hist_prev = float(junior_hist.iloc[-3])

    distance_from_sma = abs(price_now - sma_now)
    if distance_from_sma < atr_d1 * profile["sma_sep"]:
        return blocked_signal(branch_name, "TOO CLOSE SMA60", pretrigger_text("SMA60", atr_d1 * profile["sma_sep"] - distance_from_sma, atr_d1))

    if abs(senior_now) < atr_d1 * profile["senior_floor"] and abs(senior_hist_now) < atr_d1 * profile["senior_hist_floor"]:
        return blocked_signal(branch_name, "SENIOR FLAT", f"S {senior_now:.3f}")

    junior_flips = _count_sign_flips(junior_line, lookback=7, end_offset=2)
    if junior_flips > profile["whipsaw_limit"]:
        return blocked_signal(branch_name, "JUNIOR WHIPSAW", f"FLIPS {junior_flips}")

    long_context = (
        price_now > sma_now + atr_d1 * profile["sma_sep"]
        and senior_now > atr_d1 * profile["senior_floor"]
        and senior_now >= senior_sig - atr_d1 * profile["senior_hist_floor"]
        and senior_hist_now >= senior_hist_prev - atr_d1 * profile["senior_hist_floor"]
    )
    short_context = (
        price_now < sma_now - atr_d1 * profile["sma_sep"]
        and senior_now < -atr_d1 * profile["senior_floor"]
        and senior_now <= senior_sig + atr_d1 * profile["senior_hist_floor"]
        and senior_hist_now <= senior_hist_prev + atr_d1 * profile["senior_hist_floor"]
    )

    junior_long = (
        (junior_now > atr_d1 * profile["junior_floor"] and junior_hist_now >= junior_hist_prev - atr_d1 * profile["junior_expand"])
        or (
            junior_prev <= 0
            and junior_now > junior_prev
            and junior_now > junior_sig
            and abs(junior_now) <= atr_d1 * profile["junior_near_zero"]
        )
    )
    junior_short = (
        (junior_now < -atr_d1 * profile["junior_floor"] and junior_hist_now <= junior_hist_prev + atr_d1 * profile["junior_expand"])
        or (
            junior_prev >= 0
            and junior_now < junior_prev
            and junior_now < junior_sig
            and abs(junior_now) <= atr_d1 * profile["junior_near_zero"]
        )
    )

    signal_key_buy = f"{family}|DMACD|{confirm['time'].isoformat()}|BUY"
    signal_key_sell = f"{family}|DMACD|{confirm['time'].isoformat()}|SELL"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key in {signal_key_buy, signal_key_sell}:
        return blocked_signal(branch_name, "DUPLICATE LEG", confirm["time"].strftime("%Y-%m-%d"))

    h4_state = _double_macd_h4_state(df_h4)

    if long_context and junior_long:
        swing_low = float(low.iloc[-profile["swing_lookback"]:-1].min())
        sl_price = min(swing_low, sma_now) - atr_d1 * profile["sl_buffer"]
        conf = profile["boost_conf"] if junior_now > max(junior_prev, atr_d1 * profile["junior_floor"]) and senior_now > senior_prev else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "price above SMA60 with senior MACD above zero and junior timing aligned",
            "price_vs_sma60": f"close {price_now:.2f} > SMA60 {sma_now:.2f}",
            "senior_state": f"MACD {senior_now:.3f} | signal {senior_sig:.3f} | hist {senior_hist_now:.3f}",
            "junior_state": f"MACD {junior_prev:.3f}->{junior_now:.3f} | hist {junior_hist_now:.3f}",
            "entry_price": float(tick.ask),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while senior MACD stays aligned",
            "trade_state": f"setup valid | H4 {h4_state}",
            "summary": (
                f"{profile['display']} LONG | Doppio MACD bullish | close above SMA60 | "
                f"senior {senior_now:.3f} | junior {junior_prev:.3f}->{junior_now:.3f} | "
                f"entry {tick.ask:.2f} | SL {sl_price:.2f} | 1R partial + trailing | setup valid"
            ),
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_d1,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_buy,
            "signal_details": details,
        }

    if short_context and junior_short:
        swing_high = float(high.iloc[-profile["swing_lookback"]:-1].max())
        sl_price = max(swing_high, sma_now) + atr_d1 * profile["sl_buffer"]
        conf = profile["boost_conf"] if junior_now < min(junior_prev, -atr_d1 * profile["junior_floor"]) and senior_now < senior_prev else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "price below SMA60 with senior MACD below zero and junior timing aligned",
            "price_vs_sma60": f"close {price_now:.2f} < SMA60 {sma_now:.2f}",
            "senior_state": f"MACD {senior_now:.3f} | signal {senior_sig:.3f} | hist {senior_hist_now:.3f}",
            "junior_state": f"MACD {junior_prev:.3f}->{junior_now:.3f} | hist {junior_hist_now:.3f}",
            "entry_price": float(tick.bid),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while senior MACD stays aligned",
            "trade_state": f"setup valid | H4 {h4_state}",
            "summary": (
                f"{profile['display']} SHORT | Doppio MACD bearish | close below SMA60 | "
                f"senior {senior_now:.3f} | junior {junior_prev:.3f}->{junior_now:.3f} | "
                f"entry {tick.bid:.2f} | SL {sl_price:.2f} | 1R partial + trailing | setup valid"
            ),
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_d1,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_sell,
            "signal_details": details,
        }

    if not long_context and not short_context:
        if price_now > sma_now and senior_now <= 0:
            return blocked_signal(branch_name, "SENIOR OPPOSITE", f"S {senior_now:.3f}")
        if price_now < sma_now and senior_now >= 0:
            return blocked_signal(branch_name, "SENIOR OPPOSITE", f"S {senior_now:.3f}")
        return blocked_signal(branch_name, "NO TREND ALIGN", f"SMA60 {sma_now:.2f}")
    if long_context and not junior_long:
        return blocked_signal(branch_name, "JUNIOR NOT READY", f"J {junior_prev:.3f}->{junior_now:.3f}")
    if short_context and not junior_short:
        return blocked_signal(branch_name, "JUNIOR NOT READY", f"J {junior_prev:.3f}->{junior_now:.3f}")
    return blocked_signal(branch_name, "DOUBLE MACD WAIT", confirm["time"].strftime("%Y-%m-%d"))


def strategy_us500_double_macd(df, symbol):
    return strategy_double_macd(df, symbol, "US500_DOUBLE_MACD", "US500")


def strategy_btc_double_macd(df, symbol):
    return strategy_double_macd(df, symbol, "BTC_DOUBLE_MACD", "BTCUSD")


def strategy_eth_double_macd(df, symbol):
    return strategy_double_macd(df, symbol, "ETH_DOUBLE_MACD", "ETHUSD")


HEIKEN_TDI_PROFILES = {
    "USTECH": {
        "display": "US100",
        "session": (13, 23),
        "tdi_clear": 0.55,
        "yellow_slope": 0.16,
        "yellow_pad": 0.25,
        "tdi_relax": 0.20,
        "compression": 0.35,
        "body_floor": 0.10,
        "key_level_pad": 0.45,
        "key_level_exit": 0.22,
        "daily_sl_mult": 0.22,
        "swing_lookback": 8,
        "whipsaw_limit": 2,
        "base_conf": 76,
        "boost_conf": 84,
    },
    "BTCUSD": {
        "display": "BTC",
        "session": (0, 23),
        "tdi_clear": 0.85,
        "yellow_slope": 0.22,
        "yellow_pad": 0.38,
        "tdi_relax": 0.26,
        "compression": 0.50,
        "body_floor": 0.08,
        "key_level_pad": 0.60,
        "key_level_exit": 0.28,
        "daily_sl_mult": 0.34,
        "swing_lookback": 10,
        "whipsaw_limit": 2,
        "base_conf": 78,
        "boost_conf": 86,
    },
    "ETHUSD": {
        "display": "ETH",
        "session": (0, 23),
        "tdi_clear": 0.95,
        "yellow_slope": 0.24,
        "yellow_pad": 0.42,
        "tdi_relax": 0.28,
        "compression": 0.56,
        "body_floor": 0.09,
        "key_level_pad": 0.58,
        "key_level_exit": 0.26,
        "daily_sl_mult": 0.32,
        "swing_lookback": 10,
        "whipsaw_limit": 2,
        "base_conf": 77,
        "boost_conf": 85,
    },
}


def _heiken_tdi_higher_state(df_tf):
    if df_tf is None or len(df_tf) < 90:
        return "N/A"
    ha_open, _, _, ha_close = compute_heiken_ashi_smoothed(df_tf, 2, 6, 3, 2)
    green, red, yellow, _ = compute_tdi(df_tf["close"], 13, 34, 2, 7)
    blue_now = bool(ha_close.iloc[-2] > ha_open.iloc[-2])
    yellow_slope = float(yellow.iloc[-2] - yellow.iloc[-3])
    green_now = float(green.iloc[-2])
    red_now = float(red.iloc[-2])
    yellow_now = float(yellow.iloc[-2])
    if blue_now and green_now > red_now and green_now > yellow_now and yellow_slope > 0:
        return "UP"
    if (not blue_now) and green_now < red_now and green_now < yellow_now and yellow_slope < 0:
        return "DOWN"
    return "MIXED"


def get_heiken_tdi_exit_reason(symbol, strategy_cfg, signal):
    family = strategy_cfg.get("symbol_family")
    profile = HEIKEN_TDI_PROFILES.get(family)
    if not profile:
        return None

    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    if df_h1 is None or len(df_h1) < 100 or df_h4 is None or len(df_h4) < 70:
        return None

    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    atr_h4 = float(df_h4["atr"].iloc[-2] or 0.0)
    if atr_h1 <= 0 or atr_h4 <= 0:
        return None

    ha_open_h1, _, _, ha_close_h1 = compute_heiken_ashi_smoothed(df_h1, 2, 6, 3, 2)
    h1_blue = ha_close_h1 > ha_open_h1
    same_color_count, same_color_state = count_consecutive_state(h1_blue, end_offset=2)
    green_h4, red_h4, yellow_h4, _ = compute_tdi(df_h4["close"], 13, 34, 2, 7)
    green_now = float(green_h4.iloc[-2])
    green_prev = float(green_h4.iloc[-3])
    yellow_now = float(yellow_h4.iloc[-2])
    yellow_prev = float(yellow_h4.iloc[-3])
    h4_resistance = float(df_h4["high"].iloc[-22:-2].max())
    h4_support = float(df_h4["low"].iloc[-22:-2].min())
    close_now = float(df_h4["close"].iloc[-2])

    if signal == "BUY":
        if green_prev >= yellow_prev and green_now < yellow_now - profile["yellow_pad"] * 0.4:
            return "TDI green rolled below yellow on H4"
        if bool(same_color_state) and same_color_count > 10:
            return "HAS trend extended beyond 10 bars on H1"
        if 0 < (h4_resistance - close_now) < atr_h4 * profile["key_level_exit"]:
            return "price approaching strong H4 resistance"
    else:
        if green_prev <= yellow_prev and green_now > yellow_now + profile["yellow_pad"] * 0.4:
            return "TDI green rolled above yellow on H4"
        if (not bool(same_color_state)) and same_color_count > 10:
            return "HAS trend extended beyond 10 bars on H1"
        if 0 < (close_now - h4_support) < atr_h4 * profile["key_level_exit"]:
            return "price approaching strong H4 support"
    return None


def strategy_heiken_tdi(df, symbol, branch_name, family):
    profile = HEIKEN_TDI_PROFILES[family]
    if is_crypto_family(family) and not crypto_enabled:
        return blocked_signal(branch_name, "CRYPTO OFF")

    hour = get_broker_hour()
    session_start, session_end = profile["session"]
    if not _session_open(hour, session_start, session_end):
        return blocked_signal(branch_name, "OUT SESSION")

    df_h1 = get_h1_cached(symbol)
    df_h4 = get_h4_cached(symbol)
    df_d1 = get_d1_cached(symbol)
    if df_h1 is None or len(df_h1) < 130:
        return blocked_signal(branch_name, "NO H1 DATA")
    if df_h4 is None or len(df_h4) < 90:
        return blocked_signal(branch_name, "NO H4 DATA")
    if df_d1 is None or len(df_d1) < 40:
        return blocked_signal(branch_name, "NO D1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    atr_h1 = float(df_h1["atr"].iloc[-2] or 0.0)
    atr_d1 = float(df_d1["atr"].iloc[-2] or 0.0)
    if atr_h1 <= 0 or atr_d1 <= 0:
        return blocked_signal(branch_name, "NO ATR")

    ha_open, _, ha_low, ha_close = compute_heiken_ashi_smoothed(df_h1, 2, 6, 3, 2)
    green, red, yellow, _ = compute_tdi(df_h1["close"], 13, 34, 2, 7)

    confirm = df_h1.iloc[-2]
    current = df_h1.iloc[-1]
    body = abs(float(confirm["close"] - confirm["open"]))
    blue_series = ha_close > ha_open
    same_color_count, same_color_state = count_consecutive_state(blue_series, end_offset=2)
    blue_now = bool(blue_series.iloc[-2])
    blue_prev = bool(blue_series.iloc[-3])
    blue_prev2 = bool(blue_series.iloc[-4])

    green_now = float(green.iloc[-2])
    green_prev = float(green.iloc[-3])
    green_prev2 = float(green.iloc[-4])
    red_now = float(red.iloc[-2])
    red_prev = float(red.iloc[-3])
    red_prev2 = float(red.iloc[-4])
    yellow_now = float(yellow.iloc[-2])
    yellow_prev = float(yellow.iloc[-3])
    tdi_spread = abs(green_now - red_now)
    whipsaw_flips = _count_sign_flips(green - red, lookback=7, end_offset=2)

    if same_color_count <= 0:
        return blocked_signal(branch_name, "NO HAS STATE")
    if body < atr_h1 * profile["body_floor"]:
        return blocked_signal(branch_name, "WEAK H1 BODY", pretrigger_text("BODY", atr_h1 * profile["body_floor"] - body, atr_h1))
    if same_color_count > 2:
        return blocked_signal(branch_name, "HAS EXTENDED", f"{same_color_count} bars")
    if whipsaw_flips > profile["whipsaw_limit"]:
        return blocked_signal(branch_name, "TDI WHIPSAW", f"FLIPS {whipsaw_flips}")
    if tdi_spread < profile["compression"] and abs(yellow_now - yellow_prev) < profile["yellow_slope"]:
        return blocked_signal(branch_name, "TDI COMPRESSED", f"G/R {tdi_spread:.2f}")

    long_has_ready = blue_now and ((not blue_prev) or (same_color_count == 2 and not blue_prev2))
    short_has_ready = (not blue_now) and (blue_prev or (same_color_count == 2 and blue_prev2))
    yellow_rising = yellow_now > yellow_prev + profile["yellow_slope"]
    yellow_falling = yellow_now < yellow_prev - profile["yellow_slope"]

    long_cross_now = green_prev <= red_prev and green_now > red_now + profile["tdi_clear"]
    short_cross_now = green_prev >= red_prev and green_now < red_now - profile["tdi_clear"]
    long_cross_prev = (
        green_prev2 <= red_prev2
        and green_prev > red_prev + profile["tdi_clear"] * 0.45
        and green_now > red_now
        and green_now >= green_prev - profile["tdi_relax"]
    )
    short_cross_prev = (
        green_prev2 >= red_prev2
        and green_prev < red_prev - profile["tdi_clear"] * 0.45
        and green_now < red_now
        and green_now <= green_prev + profile["tdi_relax"]
    )
    long_tdi_ready = (long_cross_now or long_cross_prev) and yellow_rising and green_now > yellow_now + profile["yellow_pad"]
    short_tdi_ready = (short_cross_now or short_cross_prev) and yellow_falling and green_now < yellow_now - profile["yellow_pad"]

    h4_state = _heiken_tdi_higher_state(df_h4)
    if long_has_ready and h4_state == "DOWN":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 bearish structure")
    if short_has_ready and h4_state == "UP":
        return blocked_signal(branch_name, "H4 OPPOSITE", "H4 bullish structure")

    h4_resistance = float(df_h4["high"].iloc[-22:-2].max())
    h4_support = float(df_h4["low"].iloc[-22:-2].min())
    if 0 < (h4_resistance - float(confirm["close"])) < atr_h1 * profile["key_level_pad"] and long_has_ready:
        return blocked_signal(branch_name, "NEAR RESISTANCE", pretrigger_text("H4", atr_h1 * profile["key_level_pad"] - (h4_resistance - float(confirm["close"])), atr_h1))
    if 0 < (float(confirm["close"]) - h4_support) < atr_h1 * profile["key_level_pad"] and short_has_ready:
        return blocked_signal(branch_name, "NEAR SUPPORT", pretrigger_text("H4", atr_h1 * profile["key_level_pad"] - (float(confirm["close"]) - h4_support), atr_h1))

    signal_key_buy = f"{family}|HTDI|{confirm['time'].isoformat()}|BUY"
    signal_key_sell = f"{family}|HTDI|{confirm['time'].isoformat()}|SELL"
    last_signal_key = strategy_runtime.get(branch_name, {}).get("last_signal_key_by_symbol", {}).get(symbol)
    if last_signal_key in {signal_key_buy, signal_key_sell}:
        return blocked_signal(branch_name, "DUPLICATE LEG", confirm["time"].strftime("%Y-%m-%d %H:%M"))

    if family == "ETHUSD":
        long_tdi_ready = long_tdi_ready and blue_series.iloc[-3] == blue_series.iloc[-2]
        short_tdi_ready = short_tdi_ready and blue_series.iloc[-3] == blue_series.iloc[-2]

    if long_has_ready and long_tdi_ready:
        swing_low = float(min(df_h1["low"].iloc[-profile["swing_lookback"]:-1].min(), ha_low.iloc[-2]))
        sl_price = swing_low - atr_d1 * profile["daily_sl_mult"]
        conf = profile["boost_conf"] if h4_state == "UP" and green_now > green_prev else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "LONG",
            "reason": "new bullish HeikenAshi Smoothed bar with TDI green crossing above red and yellow rising",
            "has_color": "BLUE",
            "tdi_state": f"green {green_prev:.2f}->{green_now:.2f} vs red {red_prev:.2f}->{red_now:.2f}",
            "yellow_direction": f"UP {yellow_prev:.2f}->{yellow_now:.2f}",
            "has_bars": same_color_count,
            "entry_price": float(tick.ask),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while TDI stays above yellow",
            "trend_h1": f"BLUE | {same_color_count} bars",
            "trend_h4": h4_state,
            "summary": (
                f"{profile['display']} LONG | HAS blue fresh | TDI green above red | yellow up | "
                f"{same_color_count} bars | entry {tick.ask:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | H1 BLUE | H4 {h4_state}"
            ),
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_h1,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_buy,
            "signal_details": details,
        }

    if short_has_ready and short_tdi_ready:
        swing_high = float(max(df_h1["high"].iloc[-profile["swing_lookback"]:-1].max(), df_h1["high"].iloc[-2]))
        sl_price = swing_high + atr_d1 * profile["daily_sl_mult"]
        conf = profile["boost_conf"] if h4_state == "DOWN" and green_now < green_prev else profile["base_conf"]
        details = {
            "instrument": profile["display"],
            "direction": "SHORT",
            "reason": "new bearish HeikenAshi Smoothed bar with TDI green crossing below red and yellow falling",
            "has_color": "RED",
            "tdi_state": f"green {green_prev:.2f}->{green_now:.2f} vs red {red_prev:.2f}->{red_now:.2f}",
            "yellow_direction": f"DOWN {yellow_prev:.2f}->{yellow_now:.2f}",
            "has_bars": same_color_count,
            "entry_price": float(tick.bid),
            "stop_loss": float(sl_price),
            "take_profit": "1R partial + trailing while TDI stays below yellow",
            "trend_h1": f"RED | {same_color_count} bars",
            "trend_h4": h4_state,
            "summary": (
                f"{profile['display']} SHORT | HAS red fresh | TDI green below red | yellow down | "
                f"{same_color_count} bars | entry {tick.bid:.2f} | SL {sl_price:.2f} | "
                f"1R partial + trailing | H1 RED | H4 {h4_state}"
            ),
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_h1,
            "sl_price": round(sl_price, 5),
            "signal_key": signal_key_sell,
            "signal_details": details,
        }

    if blue_now and not long_has_ready:
        return blocked_signal(branch_name, "HAS TOO OLD", f"{same_color_count} blue bars")
    if (not blue_now) and not short_has_ready:
        return blocked_signal(branch_name, "HAS TOO OLD", f"{same_color_count} red bars")
    if long_has_ready and not long_tdi_ready:
        return blocked_signal(branch_name, "TDI NOT READY", f"G/R {green_now:.2f}/{red_now:.2f}")
    if short_has_ready and not short_tdi_ready:
        return blocked_signal(branch_name, "TDI NOT READY", f"G/R {green_now:.2f}/{red_now:.2f}")
    return blocked_signal(branch_name, "HEIKEN TDI WAIT", confirm["time"].strftime("%H:%M"))


def strategy_ustech_heiken_tdi(df, symbol):
    return strategy_heiken_tdi(df, symbol, "USTECH_HEIKEN_TDI", "USTECH")


def strategy_btc_heiken_tdi(df, symbol):
    return strategy_heiken_tdi(df, symbol, "BTC_HEIKEN_TDI", "BTCUSD")


def strategy_eth_heiken_tdi(df, symbol):
    return strategy_heiken_tdi(df, symbol, "ETH_HEIKEN_TDI", "ETHUSD")


def strategy_jpn225_orb(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("JPN225_ORB", "NO M15 DATA")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 600)
    if df_m1 is None or len(df_m1) < 180:
        return blocked_signal("JPN225_ORB", "NO M1 DATA")

    range_start, range_end = broker_window_from_local_df(df_m1, 2, 0, 2, 20)
    session_start, session_end = broker_window_from_local_df(df_m1, 2, 20, 9, 0)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("JPN225_ORB", "BUILDING RANGE", "ORB 02:00-02:20")
    if broker_now > session_end:
        return blocked_signal("JPN225_ORB", "OUT SESSION", "ORB 02:20-09:00")

    orb_window = df_m1[(df_m1["time"] >= range_start) & (df_m1["time"] < range_end)]
    if len(orb_window) < 10:
        return blocked_signal("JPN225_ORB", "RANGE INCOMPLETE", f"{len(orb_window)} bars")

    post_orb = df_m1[(df_m1["time"] >= session_start) & (df_m1["time"] <= session_end)]
    if len(post_orb) < 5:
        return blocked_signal("JPN225_ORB", "WAIT POST OPEN", "POST 02:20")

    orb_high = orb_window["high"].max()
    orb_low = orb_window["low"].min()
    orb_range = orb_high - orb_low
    last = post_orb.iloc[-1]
    prev = post_orb.iloc[-2]
    atr_m1 = post_orb["atr"].iloc[-1]
    atr_orb = orb_window["atr"].median() if "atr" in orb_window else atr_m1
    atr_m5 = df["atr"].iloc[-1] if "atr" in df else atr_m1
    if atr_m1 <= 0:
        return blocked_signal("JPN225_ORB", "NO ATR")
    trend_strength = compute_trend_strength(df)
    range_ref_atr = max(atr_m1, atr_orb, atr_m5 * 0.55)
    min_range_mult = 0.85 if attack else 0.95
    max_range_mult = 8.60 if attack else 7.10
    if trend_strength > (0.85 if attack else 0.70):
        max_range_mult += 1.20
    if orb_range < range_ref_atr * min_range_mult:
        return blocked_signal("JPN225_ORB", "RANGE TOO NARROW", pretrigger_text("ORB", range_ref_atr * min_range_mult - orb_range, range_ref_atr))
    if orb_range > range_ref_atr * max_range_mult:
        return blocked_signal("JPN225_ORB", "RANGE TOO WIDE", pretrigger_text("ORB", orb_range - range_ref_atr * max_range_mult, range_ref_atr))

    ema20 = post_orb["close"].ewm(span=20).mean()
    ema50 = post_orb["close"].ewm(span=50).mean()
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = last["close"]
    vwap = post_orb["vwap"].iloc[-1]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)

    long_trigger = orb_high + atr_m1 * 0.02
    short_trigger = orb_low - atr_m1 * 0.02
    long_hold = min(prev["low"], last["low"]) >= orb_high - atr_m1 * 0.22
    short_hold = max(prev["high"], last["high"]) <= orb_low + atr_m1 * 0.22
    strong_body = body >= atr_m1 * 0.08
    clean_long_candle = wick_up <= max(body * 1.20, atr_m1 * 0.22)
    clean_short_candle = wick_down <= max(body * 1.20, atr_m1 * 0.22)

    if (
        ema20.iloc[-1] > ema50.iloc[-1]
        and ema20_m15 > ema50_m15
        and m15_strength >= 0.36
        and price > vwap
        and price >= long_trigger
        and last["close"] >= max(prev["high"] - atr_m1 * 0.10, orb_high - atr_m1 * 0.02)
        and long_hold
        and strong_body
        and clean_long_candle
    ):
        return {"signal": "BUY", "confidence": 81, "name": "JPN225_ORB", "execution_df": post_orb}

    if (
        ema20.iloc[-1] < ema50.iloc[-1]
        and ema20_m15 < ema50_m15
        and m15_strength >= 0.36
        and price < vwap
        and price <= short_trigger
        and last["close"] <= min(prev["low"] + atr_m1 * 0.10, orb_low + atr_m1 * 0.02)
        and short_hold
        and strong_body
        and clean_short_candle
    ):
        return {"signal": "SELL", "confidence": 81, "name": "JPN225_ORB", "execution_df": post_orb}

    if ema20_m15 <= ema50_m15 and ema20.iloc[-1] > ema50.iloc[-1]:
        return blocked_signal("JPN225_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr_m1))
    if ema20_m15 >= ema50_m15 and ema20.iloc[-1] < ema50.iloc[-1]:
        return blocked_signal("JPN225_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr_m1))
    if m15_strength < 0.36:
        return blocked_signal("JPN225_ORB", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if price > vwap and price < long_trigger:
        reason = "BREAKOUT READY" if (long_trigger - price) <= atr_m1 * 1.80 else "NO BREAKOUT"
        return blocked_signal("JPN225_ORB", reason, pretrigger_text("BRK", long_trigger - price, atr_m1))
    if price < vwap and price > short_trigger:
        reason = "BREAKDOWN READY" if (price - short_trigger) <= atr_m1 * 1.80 else "NO BREAKDOWN"
        return blocked_signal("JPN225_ORB", reason, pretrigger_text("BRK", price - short_trigger, atr_m1))
    if not strong_body:
        return blocked_signal("JPN225_ORB", "WEAK OPEN DRIVE", pretrigger_text("BODY", atr_m1 * 0.16 - body, atr_m1))
    if price > vwap and not long_hold:
        return blocked_signal("JPN225_ORB", "RETEST LOST", pretrigger_text("ORB", (orb_high - atr_m1 * 0.22) - min(prev["low"], last["low"]), atr_m1))
    if price < vwap and not short_hold:
        return blocked_signal("JPN225_ORB", "RETEST LOST", pretrigger_text("ORB", max(prev["high"], last["high"]) - (orb_low + atr_m1 * 0.22), atr_m1))
    if price > vwap and not clean_long_candle:
        return blocked_signal("JPN225_ORB", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 1.05, atr_m1 * 0.18), atr_m1))
    if price < vwap and not clean_short_candle:
        return blocked_signal("JPN225_ORB", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 1.05, atr_m1 * 0.18), atr_m1))
    return blocked_signal("JPN225_ORB", "TREND MIXED", pretrigger_text("EMA", abs(ema20.iloc[-1] - ema50.iloc[-1]), atr_m1))


def strategy_us500_orb(df, symbol):
    attack = is_attack_runtime()
    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("US500_ORB", "NO M15 DATA")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 600)
    if df_m1 is None or len(df_m1) < 120:
        return blocked_signal("US500_ORB", "NO M1 DATA")

    range_start, range_end = broker_window_from_local_df(df_m1, 15, 30, 15, 45)
    session_start, session_end = broker_window_from_local_df(df_m1, 15, 45, 23, 0)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("US500_ORB", "BUILDING RANGE", "ORB 15:30-15:45")
    if broker_now > session_end:
        return blocked_signal("US500_ORB", "OUT SESSION", "ORB 15:45-23:00")

    orb_window = df_m1[(df_m1["time"] >= range_start) & (df_m1["time"] < range_end)]
    if len(orb_window) < 8:
        return blocked_signal("US500_ORB", "RANGE INCOMPLETE", f"{len(orb_window)} bars")

    post_orb = df_m1[(df_m1["time"] >= session_start) & (df_m1["time"] <= session_end)]
    if len(post_orb) < 4:
        return blocked_signal("US500_ORB", "WAIT POST OPEN", "POST 15:45")

    orb_high = orb_window["high"].max()
    orb_low = orb_window["low"].min()
    orb_range = orb_high - orb_low
    last = post_orb.iloc[-1]
    prev = post_orb.iloc[-2]
    atr_m1 = post_orb["atr"].iloc[-1]
    atr_m5 = df["atr"].iloc[-1]
    if atr_m1 <= 0 or atr_m5 <= 0:
        return blocked_signal("US500_ORB", "NO ATR")

    min_range = 0.26 if attack else 0.45
    max_range = 2.80 if attack else 2.10
    if orb_range < atr_m5 * min_range:
        return blocked_signal("US500_ORB", "RANGE TOO TIGHT", pretrigger_text("ORB", atr_m5 * min_range - orb_range, atr_m5))
    if orb_range > atr_m5 * max_range:
        return blocked_signal("US500_ORB", "RANGE TOO WIDE", pretrigger_text("ORB", orb_range - atr_m5 * max_range, atr_m5))

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    trend_strength = compute_trend_strength(df)
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    recent_high = post_orb["high"].tail(8).max()
    recent_low = post_orb["low"].tail(8).min()

    trend_min = 0.22 if attack else 1.05
    long_trigger = orb_high + atr_m5 * (0.02 if attack else 0.06)
    short_trigger = orb_low - atr_m5 * (0.04 if attack else 0.08)
    long_hold = min(prev["low"], last["low"]) >= orb_high - atr_m5 * (0.14 if attack else 0.08)
    short_hold = max(prev["high"], last["high"]) <= orb_low + atr_m5 * (0.10 if attack else 0.06)
    strong_body = body >= atr_m5 * (0.06 if attack else 0.09)
    clean_long_candle = wick_up <= max(body * (1.00 if attack else 0.82), atr_m5 * (0.18 if attack else 0.12))
    clean_short_candle = wick_down <= max(body * (1.00 if attack else 0.82), atr_m5 * (0.18 if attack else 0.12))
    short_vwap_buffer = atr_m5 * (0.03 if attack else 0.05)

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and trend_strength >= trend_min
        and price > vwap
        and price >= long_trigger
        and last["close"] >= prev["high"]
        and long_hold
        and strong_body
        and clean_long_candle
        and recent_high >= orb_high
    ):
        confidence = 77
        if price >= orb_high + atr_m5 * 0.14 and trend_strength > 1.05:
            confidence = 81
        return {"signal": "BUY", "confidence": confidence, "name": "US500_ORB", "execution_df": post_orb}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and trend_strength >= trend_min
        and price < vwap - short_vwap_buffer
        and price <= short_trigger
        and last["close"] <= prev["low"] - atr_m5 * (0.02 if attack else 0.01)
        and short_hold
        and strong_body
        and clean_short_candle
        and recent_low <= orb_low
    ):
        confidence = 77
        if price <= orb_low - atr_m5 * 0.14 and trend_strength > 1.05:
            confidence = 81
        return {"signal": "SELL", "confidence": confidence, "name": "US500_ORB", "execution_df": post_orb}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("US500_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr_m5))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("US500_ORB", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr_m5))
    if trend_strength < trend_min:
        return blocked_signal("US500_ORB", "TREND WEAK", f"STR {round(trend_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("US500_ORB", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr_m5))
    if ema20 < ema50 and price >= vwap - short_vwap_buffer:
        return blocked_signal("US500_ORB", "VWAP LOST", pretrigger_text("VWAP", price - (vwap - short_vwap_buffer), atr_m5))
    if ema20 > ema50 and price < long_trigger:
        return blocked_signal("US500_ORB", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr_m5))
    if ema20 < ema50 and price > short_trigger:
        return blocked_signal("US500_ORB", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr_m5))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("US500_ORB", "NO HOLD ABOVE ORB", pretrigger_text("HOLD", (orb_high - atr_m5 * (0.14 if attack else 0.08)) - min(prev["low"], last["low"]), atr_m5))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("US500_ORB", "NO HOLD BELOW ORB", pretrigger_text("HOLD", max(prev["high"], last["high"]) - (orb_low + atr_m5 * (0.14 if attack else 0.08)), atr_m5))
    if not strong_body:
        return blocked_signal("US500_ORB", "WEAK OPEN DRIVE", pretrigger_text("BODY", atr_m5 * (0.06 if attack else 0.09) - body, atr_m5))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("US500_ORB", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.00 if attack else 0.82), atr_m5 * (0.18 if attack else 0.12)), atr_m5))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("US500_ORB", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.00 if attack else 0.82), atr_m5 * (0.18 if attack else 0.12)), atr_m5))
    return blocked_signal("US500_ORB", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr_m5))


def strategy_ustech_scalping(df, symbol):
    if not scalping_enabled:
        return blocked_signal("USTECH_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 8 or hour > 22:
        return blocked_signal("USTECH_SCALP", "OUT SESSION")

    df_m1 = get_m1_cached(symbol)
    if df_m1 is None or len(df_m1) < 60:
        return blocked_signal("USTECH_SCALP", "NO M1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("USTECH_SCALP", "NO TICK")

    spread = tick.ask - tick.bid
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0 or spread > atr_m1 * 0.20:
        return blocked_signal("USTECH_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    ema9 = close_m1.ewm(span=9).mean()
    ema21 = close_m1.ewm(span=21).mean()
    ema50 = close_m1.ewm(span=50).mean()
    rsi = compute_rsi(close_m1, 14)

    trend_up = df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1]
    trend_down = df["close"].ewm(span=20).mean().iloc[-1] < df["close"].ewm(span=50).mean().iloc[-1]

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]

    long_pullback_band = ema21.iloc[-1] + atr_m1 * 0.14
    short_pullback_band = ema21.iloc[-1] - atr_m1 * 0.14
    long_break_trigger = high_m1.iloc[-2] - atr_m1 * 0.16
    short_break_trigger = low_m1.iloc[-2] + atr_m1 * 0.16

    long_setup = (
        trend_up
        and ema9.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema9.iloc[-1]
        and last["close"] >= long_break_trigger
        and 50 <= rsi.iloc[-1] <= 75
    )
    if long_setup:
        return {"signal": "BUY", "confidence": 73, "name": "USTECH_SCALP", "execution_df": df_m1}

    short_setup = (
        trend_down
        and ema9.iloc[-1] < ema21.iloc[-1] < ema50.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema9.iloc[-1]
        and last["close"] <= short_break_trigger
        and 25 <= rsi.iloc[-1] <= 50
    )
    if short_setup:
        return {"signal": "SELL", "confidence": 73, "name": "USTECH_SCALP", "execution_df": df_m1}

    if trend_up:
        if not (ema9.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1]):
            return blocked_signal("USTECH_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema21.iloc[-1] - ema9.iloc[-1], ema50.iloc[-1] - ema21.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("USTECH_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema9.iloc[-1]):
            return blocked_signal("USTECH_SCALP", "EMA9 NOT HELD", pretrigger_text("EMA9", ema9.iloc[-1] - last["close"], atr_m1))
        if not (last["close"] >= long_break_trigger):
            return blocked_signal("USTECH_SCALP", "NO BREAKOUT", pretrigger_text("BRK", long_break_trigger - last["close"], atr_m1))
        return blocked_signal("USTECH_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if trend_down:
        if not (ema9.iloc[-1] < ema21.iloc[-1] < ema50.iloc[-1]):
            return blocked_signal("USTECH_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema9.iloc[-1] - ema21.iloc[-1], ema21.iloc[-1] - ema50.iloc[-1]), atr_m1))
        if not (max(prev["high"], last["high"]) >= short_pullback_band):
            return blocked_signal("USTECH_SCALP", "NO PULLBACK", pretrigger_text("PB", short_pullback_band - max(prev["high"], last["high"]), atr_m1))
        if not (last["close"] < ema9.iloc[-1]):
            return blocked_signal("USTECH_SCALP", "EMA9 NOT LOST", pretrigger_text("EMA9", last["close"] - ema9.iloc[-1], atr_m1))
        if not (last["close"] <= short_break_trigger):
            return blocked_signal("USTECH_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - short_break_trigger, atr_m1))
        return blocked_signal("USTECH_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    return blocked_signal("USTECH_SCALP", "TREND NOT READY", pretrigger_text("EMA", abs(df['close'].ewm(span=20).mean().iloc[-1] - df['close'].ewm(span=50).mean().iloc[-1]), df['atr'].iloc[-1]))


def strategy_uk100_trend(df, symbol):
    if detect_market_regime(df) != "TREND":
        return blocked_signal("UK100_TREND", "REGIME RANGE", f"STR {round(compute_trend_strength(df), 2)}")

    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("UK100_TREND", "NO M15 DATA")

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    breakout_high = df["high"].rolling(18).max().iloc[-2]
    breakout_low = df["low"].rolling(18).min().iloc[-2]

    long_trigger = breakout_high - atr * 0.28
    short_trigger = breakout_low + atr * 0.28

    if ema20 > ema50 and ema20_m15 > ema50_m15 and price > vwap and price > long_trigger:
        return {"signal": "BUY", "confidence": 77, "name": "UK100_TREND"}

    if ema20 < ema50 and ema20_m15 < ema50_m15 and price < vwap and price < short_trigger:
        return {"signal": "SELL", "confidence": 77, "name": "UK100_TREND"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("UK100_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("UK100_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("UK100_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("UK100_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if ema20 > ema50:
        return blocked_signal("UK100_TREND", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr))
    if ema20 < ema50:
        return blocked_signal("UK100_TREND", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr))
    return blocked_signal("UK100_TREND", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_uk100_scalping(df, symbol):
    if not scalping_enabled:
        return blocked_signal("UK100_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 8 or hour > 17:
        return blocked_signal("UK100_SCALP", "OUT SESSION")

    df_m1 = get_m1_cached(symbol)
    if df_m1 is None or len(df_m1) < 60:
        return blocked_signal("UK100_SCALP", "NO M1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("UK100_SCALP", "NO TICK")

    spread = tick.ask - tick.bid
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0 or spread > atr_m1 * 0.16:
        return blocked_signal("UK100_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    ema8 = close_m1.ewm(span=8).mean()
    ema18 = close_m1.ewm(span=18).mean()
    ema34 = close_m1.ewm(span=34).mean()
    rsi = compute_rsi(close_m1, 14)

    trend_up = df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1]
    trend_down = df["close"].ewm(span=20).mean().iloc[-1] < df["close"].ewm(span=50).mean().iloc[-1]
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]

    long_pullback_band = ema18.iloc[-1] + atr_m1 * 0.10
    short_pullback_band = ema18.iloc[-1] - atr_m1 * 0.10
    long_break_trigger = high_m1.iloc[-2] - atr_m1 * 0.12
    short_break_trigger = low_m1.iloc[-2] + atr_m1 * 0.12

    long_setup = (
        trend_up
        and ema8.iloc[-1] > ema18.iloc[-1] > ema34.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema8.iloc[-1]
        and (last["close"] - last["open"]) > atr_m1 * 0.08
        and last["close"] >= long_break_trigger
        and 51 <= rsi.iloc[-1] <= 72
    )
    if long_setup:
        return {"signal": "BUY", "confidence": 75, "name": "UK100_SCALP", "execution_df": df_m1}

    short_setup = (
        trend_down
        and ema8.iloc[-1] < ema18.iloc[-1] < ema34.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema8.iloc[-1]
        and (last["open"] - last["close"]) > atr_m1 * 0.08
        and last["close"] <= short_break_trigger
        and 28 <= rsi.iloc[-1] <= 49
    )
    if short_setup:
        return {"signal": "SELL", "confidence": 75, "name": "UK100_SCALP", "execution_df": df_m1}

    if trend_up:
        if not (ema8.iloc[-1] > ema18.iloc[-1] > ema34.iloc[-1]):
            return blocked_signal("UK100_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema18.iloc[-1] - ema8.iloc[-1], ema34.iloc[-1] - ema18.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("UK100_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema8.iloc[-1]):
            return blocked_signal("UK100_SCALP", "EMA8 NOT HELD", pretrigger_text("EMA8", ema8.iloc[-1] - last["close"], atr_m1))
        if not ((last["close"] - last["open"]) > atr_m1 * 0.08):
            return blocked_signal("UK100_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * 0.08 - (last["close"] - last["open"]), atr_m1))
        if not (last["close"] >= long_break_trigger):
            return blocked_signal("UK100_SCALP", "NO BREAKOUT", pretrigger_text("BRK", long_break_trigger - last["close"], atr_m1))
        return blocked_signal("UK100_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if trend_down:
        if not (ema8.iloc[-1] < ema18.iloc[-1] < ema34.iloc[-1]):
            return blocked_signal("UK100_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema8.iloc[-1] - ema18.iloc[-1], ema18.iloc[-1] - ema34.iloc[-1]), atr_m1))
        if not (max(prev["high"], last["high"]) >= short_pullback_band):
            return blocked_signal("UK100_SCALP", "NO PULLBACK", pretrigger_text("PB", short_pullback_band - max(prev["high"], last["high"]), atr_m1))
        if not (last["close"] < ema8.iloc[-1]):
            return blocked_signal("UK100_SCALP", "EMA8 NOT LOST", pretrigger_text("EMA8", last["close"] - ema8.iloc[-1], atr_m1))
        if not ((last["open"] - last["close"]) > atr_m1 * 0.08):
            return blocked_signal("UK100_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * 0.08 - (last["open"] - last["close"]), atr_m1))
        if not (last["close"] <= short_break_trigger):
            return blocked_signal("UK100_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - short_break_trigger, atr_m1))
        return blocked_signal("UK100_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    return blocked_signal("UK100_SCALP", "TREND NOT READY", pretrigger_text("EMA", abs(df['close'].ewm(span=20).mean().iloc[-1] - df['close'].ewm(span=50).mean().iloc[-1]), df['atr'].iloc[-1]))


def strategy_us500_trend(df, symbol):
    trend_strength = compute_trend_strength(df)
    attack = is_attack_runtime()
    min_trend = 0.10 if attack else 1.02
    if detect_market_regime(df) == "CHAOS" or trend_strength < min_trend:
        return blocked_signal("US500_TREND", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("US500_TREND", "NO M15 DATA")

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    breakout_high = df["high"].rolling(16).max().iloc[-2]
    breakout_low = df["low"].rolling(16).min().iloc[-2]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    vwap_dist = abs(price - vwap)
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)

    long_trigger = breakout_high + atr * (0.02 if attack else 0.03)
    short_trigger = breakout_low - atr * (0.02 if attack else 0.03)
    long_hold = last["low"] >= (ema20 - atr * (0.22 if attack else 0.10))
    short_hold = last["high"] <= (ema20 + atr * (0.22 if attack else 0.10))
    strong_body = body >= atr * (0.05 if attack else 0.10)
    clean_long_candle = wick_up <= max(body * (1.22 if attack else 0.98), atr * (0.18 if attack else 0.10))
    clean_short_candle = wick_down <= max(body * (1.22 if attack else 0.98), atr * (0.18 if attack else 0.10))
    long_close_confirm = last["close"] >= long_trigger and last["close"] > prev["high"] - atr * (0.08 if attack else 0.02)
    short_close_confirm = last["close"] <= short_trigger and last["close"] < prev["low"] + atr * (0.08 if attack else 0.02)

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and vwap_dist <= atr * (1.95 if attack else 1.45)
        and long_close_confirm
        and long_hold
        and strong_body
        and clean_long_candle
        and m15_strength >= (0.30 if attack else 0.48)
    ):
        return {"signal": "BUY", "confidence": 79, "name": "US500_TREND"}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and price < vwap
        and vwap_dist <= atr * (1.95 if attack else 1.45)
        and short_close_confirm
        and short_hold
        and strong_body
        and clean_short_candle
        and m15_strength >= (0.30 if attack else 0.48)
    ):
        return {"signal": "SELL", "confidence": 79, "name": "US500_TREND"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("US500_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("US500_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if m15_strength < (0.30 if attack else 0.48):
        return blocked_signal("US500_TREND", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("US500_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("US500_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if vwap_dist > atr * (1.95 if attack else 1.45):
        return blocked_signal("US500_TREND", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * (1.95 if attack else 1.45), atr))
    if not strong_body:
        return blocked_signal("US500_TREND", "WEAK BODY", pretrigger_text("BODY", atr * 0.10 - body, atr))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("US500_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", (ema20 - atr * 0.10) - last["low"], atr))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("US500_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", last["high"] - (ema20 + atr * 0.10), atr))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("US500_TREND", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * 0.98, atr * 0.10), atr))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("US500_TREND", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * 0.98, atr * 0.10), atr))
    if ema20 > ema50:
        if price <= long_trigger:
            return blocked_signal("US500_TREND", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr))
        return blocked_signal("US500_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(long_trigger - last["close"], prev["high"] - last["close"]), atr))
    if ema20 < ema50:
        if price >= short_trigger:
            return blocked_signal("US500_TREND", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr))
        return blocked_signal("US500_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(last["close"] - short_trigger, last["close"] - prev["low"]), atr))
    if price <= vwap and ema20 > ema50:
        return blocked_signal("US500_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if price >= vwap and ema20 < ema50:
        return blocked_signal("US500_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    return blocked_signal("US500_TREND", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_us500_scalping(df, symbol):
    if not scalping_enabled:
        return blocked_signal("US500_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 10 or hour > 23:
        return blocked_signal("US500_SCALP", "OUT SESSION")

    df_m1 = get_m1_cached(symbol)
    if df_m1 is None or len(df_m1) < 60:
        return blocked_signal("US500_SCALP", "NO M1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("US500_SCALP", "NO TICK")

    spread = tick.ask - tick.bid
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0 or spread > atr_m1 * 0.18:
        return blocked_signal("US500_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    ema8 = close_m1.ewm(span=8).mean()
    ema18 = close_m1.ewm(span=18).mean()
    ema40 = close_m1.ewm(span=40).mean()
    rsi = compute_rsi(close_m1, 14)

    trend_up = df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1]
    trend_down = df["close"].ewm(span=20).mean().iloc[-1] < df["close"].ewm(span=50).mean().iloc[-1]

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]

    long_pullback_band = ema18.iloc[-1] + atr_m1 * 0.10
    short_pullback_band = ema18.iloc[-1] - atr_m1 * 0.10
    long_break_trigger = high_m1.iloc[-2] - atr_m1 * 0.14
    short_break_trigger = low_m1.iloc[-2] + atr_m1 * 0.14

    long_setup = (
        trend_up
        and ema8.iloc[-1] > ema18.iloc[-1] > ema40.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema8.iloc[-1]
        and last["close"] >= long_break_trigger
        and 51 <= rsi.iloc[-1] <= 74
    )
    if long_setup:
        return {"signal": "BUY", "confidence": 74, "name": "US500_SCALP", "execution_df": df_m1}

    short_setup = (
        trend_down
        and ema8.iloc[-1] < ema18.iloc[-1] < ema40.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema8.iloc[-1]
        and last["close"] <= short_break_trigger
        and 26 <= rsi.iloc[-1] <= 49
    )
    if short_setup:
        return {"signal": "SELL", "confidence": 74, "name": "US500_SCALP", "execution_df": df_m1}

    if trend_up:
        if not (ema8.iloc[-1] > ema18.iloc[-1] > ema40.iloc[-1]):
            return blocked_signal("US500_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema18.iloc[-1] - ema8.iloc[-1], ema40.iloc[-1] - ema18.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("US500_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema8.iloc[-1]):
            return blocked_signal("US500_SCALP", "EMA8 NOT HELD", pretrigger_text("EMA8", ema8.iloc[-1] - last["close"], atr_m1))
        if not (last["close"] >= long_break_trigger):
            return blocked_signal("US500_SCALP", "NO BREAKOUT", pretrigger_text("BRK", long_break_trigger - last["close"], atr_m1))
        return blocked_signal("US500_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if trend_down:
        if not (ema8.iloc[-1] < ema18.iloc[-1] < ema40.iloc[-1]):
            return blocked_signal("US500_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema8.iloc[-1] - ema18.iloc[-1], ema18.iloc[-1] - ema40.iloc[-1]), atr_m1))
        if not (max(prev["high"], last["high"]) >= short_pullback_band):
            return blocked_signal("US500_SCALP", "NO PULLBACK", pretrigger_text("PB", short_pullback_band - max(prev["high"], last["high"]), atr_m1))
        if not (last["close"] < ema8.iloc[-1]):
            return blocked_signal("US500_SCALP", "EMA8 NOT LOST", pretrigger_text("EMA8", last["close"] - ema8.iloc[-1], atr_m1))
        if not (last["close"] <= short_break_trigger):
            return blocked_signal("US500_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - short_break_trigger, atr_m1))
        return blocked_signal("US500_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    return blocked_signal("US500_SCALP", "TREND NOT READY", pretrigger_text("EMA", abs(df['close'].ewm(span=20).mean().iloc[-1] - df['close'].ewm(span=50).mean().iloc[-1]), df['atr'].iloc[-1]))


def strategy_ger40_trend(df, symbol):
    hour = get_broker_hour()
    if hour < 8 or hour > 17:
        return blocked_signal("GER40_TREND", "OUT SESSION", "SESSION 08-17")

    trend_strength = compute_trend_strength(df)
    if detect_market_regime(df) != "TREND" or trend_strength < 1.28:
        return blocked_signal("GER40_TREND", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    df_m15 = get_m15_cached(symbol)
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("GER40_TREND", "NO M15 DATA")

    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    ema20_m15 = df_m15["close"].ewm(span=20).mean().iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    atr = df["atr"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    breakout_high = df["high"].rolling(18).max().iloc[-2]
    breakout_low = df["low"].rolling(18).min().iloc[-2]
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    vwap_dist = abs(price - vwap)
    m15_strength = abs(ema20_m15 - ema50_m15) / max(df_m15["atr"].iloc[-1], 1e-6)

    long_trigger = breakout_high + atr * 0.05
    short_trigger = breakout_low - atr * 0.05
    long_hold = last["low"] >= (ema20 - atr * 0.06)
    short_hold = last["high"] <= (ema20 + atr * 0.06)
    strong_body = body >= atr * 0.16
    long_close_confirm = last["close"] >= long_trigger and last["close"] > prev["high"]
    short_close_confirm = last["close"] <= short_trigger and last["close"] < prev["low"]
    clean_long_candle = wick_up <= body * 0.75
    clean_short_candle = wick_down <= body * 0.75

    if (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and vwap_dist <= atr * 1.15
        and long_close_confirm
        and long_hold
        and strong_body
        and clean_long_candle
        and m15_strength >= 0.65
    ):
        return {"signal": "BUY", "confidence": 82, "name": "GER40_TREND"}

    if (
        ema20 < ema50
        and ema20_m15 < ema50_m15
        and price < vwap
        and vwap_dist <= atr * 1.15
        and short_close_confirm
        and short_hold
        and strong_body
        and clean_short_candle
        and m15_strength >= 0.65
    ):
        return {"signal": "SELL", "confidence": 82, "name": "GER40_TREND"}

    if ema20_m15 <= ema50_m15 and ema20 > ema50:
        return blocked_signal("GER40_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema50_m15 - ema20_m15, atr))
    if ema20_m15 >= ema50_m15 and ema20 < ema50:
        return blocked_signal("GER40_TREND", "M15 NOT ALIGNED", pretrigger_text("M15 EMA", ema20_m15 - ema50_m15, atr))
    if m15_strength < 0.65:
        return blocked_signal("GER40_TREND", "M15 WEAK", f"STR {round(m15_strength, 2)}")
    if ema20 > ema50 and price <= vwap:
        return blocked_signal("GER40_TREND", "VWAP LOST", pretrigger_text("VWAP", vwap - price, atr))
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("GER40_TREND", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr))
    if vwap_dist > atr * 1.15:
        return blocked_signal("GER40_TREND", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * 1.15, atr))
    if not strong_body:
        return blocked_signal("GER40_TREND", "WEAK BODY", pretrigger_text("BODY", atr * 0.16 - body, atr))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("GER40_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", (ema20 - atr * 0.06) - last["low"], atr))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("GER40_TREND", "EMA20 NOT HELD", pretrigger_text("EMA20", last["high"] - (ema20 + atr * 0.06), atr))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("GER40_TREND", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - body * 0.75, atr))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("GER40_TREND", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - body * 0.75, atr))
    if ema20 > ema50:
        if price <= long_trigger:
            return blocked_signal("GER40_TREND", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr))
        return blocked_signal("GER40_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(long_trigger - last["close"], prev["high"] - last["close"]), atr))
    if ema20 < ema50:
        if price >= short_trigger:
            return blocked_signal("GER40_TREND", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr))
        return blocked_signal("GER40_TREND", "NO CLOSE CONFIRM", pretrigger_text("CLOSE", max(last["close"] - short_trigger, last["close"] - prev["low"]), atr))
    return blocked_signal("GER40_TREND", "TREND MIXED", pretrigger_text("EMA", abs(ema20 - ema50), atr))


def strategy_ger40_scalping(df, symbol):
    if not scalping_enabled:
        return blocked_signal("GER40_SCALP", "SCALP OFF")

    hour = get_broker_hour()
    if hour < 8 or hour > 17:
        return blocked_signal("GER40_SCALP", "OUT SESSION")

    df_m1 = get_m1_cached(symbol)
    df_m15 = get_m15_cached(symbol)
    df_h1 = get_h1_cached(symbol)
    if df_m1 is None or len(df_m1) < 60:
        return blocked_signal("GER40_SCALP", "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("GER40_SCALP", "NO M15 DATA")
    if df_h1 is None or len(df_h1) < 80:
        return blocked_signal("GER40_SCALP", "NO H1 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal("GER40_SCALP", "NO TICK")

    spread = tick.ask - tick.bid
    atr_m1 = df_m1["atr"].iloc[-1]
    if atr_m1 <= 0 or spread > atr_m1 * 0.18:
        return blocked_signal("GER40_SCALP", "SPREAD HIGH")

    close_m1 = df_m1["close"]
    high_m1 = df_m1["high"]
    low_m1 = df_m1["low"]
    ema9 = close_m1.ewm(span=9).mean()
    ema20 = close_m1.ewm(span=20).mean()
    ema50 = close_m1.ewm(span=50).mean()
    rsi = compute_rsi(close_m1, 14)
    m15_close = df_m15["close"]
    ema20_m15 = m15_close.ewm(span=20).mean()
    ema50_m15 = m15_close.ewm(span=50).mean()
    h1_close = df_h1["close"]
    ema20_h1 = h1_close.ewm(span=20).mean()
    ema50_h1 = h1_close.ewm(span=50).mean()

    trend_up = df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1]
    trend_down = df["close"].ewm(span=20).mean().iloc[-1] < df["close"].ewm(span=50).mean().iloc[-1]
    m15_bull = ema20_m15.iloc[-1] >= ema50_m15.iloc[-1] and ema20_m15.iloc[-1] - ema20_m15.iloc[-4] >= -atr_m1 * 0.01
    m15_bear = (
        ema20_m15.iloc[-1] < ema50_m15.iloc[-1]
        and ema20_m15.iloc[-1] - ema20_m15.iloc[-4] <= -atr_m1 * 0.03
        and m15_close.iloc[-1] <= ema50_m15.iloc[-1] - atr_m1 * 0.06
    )
    h1_bull = ema20_h1.iloc[-1] >= ema50_h1.iloc[-1]
    h1_bear = ema20_h1.iloc[-1] < ema50_h1.iloc[-1] and ema20_h1.iloc[-1] - ema20_h1.iloc[-4] <= -atr_m1 * 0.02

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]

    long_pullback_band = ema20.iloc[-1] + atr_m1 * 0.13
    short_pullback_band = ema20.iloc[-1] - atr_m1 * 0.13
    long_break_trigger = high_m1.iloc[-2] - atr_m1 * 0.16
    short_break_trigger = low_m1.iloc[-2] + atr_m1 * 0.16

    long_setup = (
        trend_up
        and m15_bull
        and h1_bull
        and ema9.iloc[-1] > ema20.iloc[-1] > ema50.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema9.iloc[-1]
        and (last["close"] - last["open"]) > atr_m1 * 0.08
        and last["close"] >= long_break_trigger
        and 50 <= rsi.iloc[-1] <= 76
    )
    if long_setup:
        return {"signal": "BUY", "confidence": 76, "name": "GER40_SCALP", "execution_df": df_m1}

    short_setup = (
        trend_down
        and m15_bear
        and h1_bear
        and ema9.iloc[-1] < ema20.iloc[-1] < ema50.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema9.iloc[-1]
        and (last["open"] - last["close"]) > atr_m1 * 0.10
        and last["close"] <= short_break_trigger - atr_m1 * 0.02
        and 24 <= rsi.iloc[-1] <= 45
    )
    if short_setup:
        return {"signal": "SELL", "confidence": 76, "name": "GER40_SCALP", "execution_df": df_m1}

    if trend_up:
        if not (m15_bull and h1_bull):
            return blocked_signal("GER40_SCALP", "HTF LONG BLOCK", pretrigger_text("HTF", abs(ema20_m15.iloc[-1] - ema50_m15.iloc[-1]), atr_m1))
        if not (ema9.iloc[-1] > ema20.iloc[-1] > ema50.iloc[-1]):
            return blocked_signal("GER40_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema20.iloc[-1] - ema9.iloc[-1], ema50.iloc[-1] - ema20.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("GER40_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema9.iloc[-1]):
            return blocked_signal("GER40_SCALP", "EMA9 NOT HELD", pretrigger_text("EMA9", ema9.iloc[-1] - last["close"], atr_m1))
        if not ((last["close"] - last["open"]) > atr_m1 * 0.08):
            return blocked_signal("GER40_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * 0.08 - (last["close"] - last["open"]), atr_m1))
        if not (last["close"] >= long_break_trigger):
            return blocked_signal("GER40_SCALP", "NO BREAKOUT", pretrigger_text("BRK", long_break_trigger - last["close"], atr_m1))
        return blocked_signal("GER40_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if trend_down:
        if not (m15_bear and h1_bear):
            return blocked_signal("GER40_SCALP", "HTF SHORT BLOCK", pretrigger_text("HTF", abs(ema20_m15.iloc[-1] - ema50_m15.iloc[-1]), atr_m1))
        if not (ema9.iloc[-1] < ema20.iloc[-1] < ema50.iloc[-1]):
            return blocked_signal("GER40_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema9.iloc[-1] - ema20.iloc[-1], ema20.iloc[-1] - ema50.iloc[-1]), atr_m1))
        if not (max(prev["high"], last["high"]) >= short_pullback_band):
            return blocked_signal("GER40_SCALP", "NO PULLBACK", pretrigger_text("PB", short_pullback_band - max(prev["high"], last["high"]), atr_m1))
        if not (last["close"] < ema9.iloc[-1]):
            return blocked_signal("GER40_SCALP", "EMA9 NOT LOST", pretrigger_text("EMA9", last["close"] - ema9.iloc[-1], atr_m1))
        if not ((last["open"] - last["close"]) > atr_m1 * 0.08):
            return blocked_signal("GER40_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * 0.08 - (last["open"] - last["close"]), atr_m1))
        if not (last["close"] <= short_break_trigger):
            return blocked_signal("GER40_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - short_break_trigger, atr_m1))
        return blocked_signal("GER40_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    return blocked_signal("GER40_SCALP", "TREND NOT READY", pretrigger_text("EMA", abs(df['close'].ewm(span=20).mean().iloc[-1] - df['close'].ewm(span=50).mean().iloc[-1]), df['atr'].iloc[-1]))


TITANYX_PROFILES = {
    "US500": {
        "display": "S&P 500",
        "session_tz": "US/Eastern",
        "cash_open": "09:30",
        "entry_windows": [("09:40", "15:15")],
        "flat_time": "15:45",
        "news_windows": [],
        "priority_rank": 1,
        "grid_step": 0.30,
        "grid_mult": 1.35,
        "max_orders": 4,
        "tp_atr": 0.22,
        "basket_stop_atr": 1.8,
        "z_entry": 2.0,
        "z_readd": 1.55,
        "rsi_long": 30,
        "rsi_short": 70,
        "rsi_readd_long": 38,
        "rsi_readd_short": 62,
        "atr_spike_mult": 1.8,
        "bar_range_mult": 1.5,
        "gap_atr_mult": 1.1,
        "ema_divergence": 0.95,
        "first_hour_pad": 0.18,
        "risk_scales": [1.00, 0.82, 0.68, 0.58],
        "daily_equity_dd": 0.012,
        "weekly_equity_dd": 0.022,
        "score_base": 70,
    },
    "EUSTX50": {
        "display": "Euro Stoxx 50",
        "session_tz": "Europe/Paris",
        "cash_open": "09:00",
        "entry_windows": [("09:05", "16:40")],
        "flat_time": "17:10",
        "news_windows": [],
        "priority_rank": 2,
        "grid_step": 0.35,
        "grid_mult": 1.35,
        "max_orders": 4,
        "tp_atr": 0.25,
        "basket_stop_atr": 1.8,
        "z_entry": 2.0,
        "z_readd": 1.60,
        "rsi_long": 30,
        "rsi_short": 70,
        "rsi_readd_long": 39,
        "rsi_readd_short": 61,
        "atr_spike_mult": 1.8,
        "bar_range_mult": 1.5,
        "gap_atr_mult": 1.0,
        "ema_divergence": 0.90,
        "first_hour_pad": 0.16,
        "risk_scales": [1.00, 0.84, 0.70, 0.60],
        "daily_equity_dd": 0.011,
        "weekly_equity_dd": 0.020,
        "score_base": 69,
    },
    "UK100": {
        "display": "FTSE 100",
        "session_tz": "Europe/London",
        "cash_open": "08:00",
        "entry_windows": [("08:05", "15:50")],
        "flat_time": "16:20",
        "news_windows": [],
        "priority_rank": 3,
        "grid_step": 0.35,
        "grid_mult": 1.35,
        "max_orders": 4,
        "tp_atr": 0.25,
        "basket_stop_atr": 1.8,
        "z_entry": 2.0,
        "z_readd": 1.60,
        "rsi_long": 30,
        "rsi_short": 70,
        "rsi_readd_long": 39,
        "rsi_readd_short": 61,
        "atr_spike_mult": 1.8,
        "bar_range_mult": 1.5,
        "gap_atr_mult": 1.0,
        "ema_divergence": 0.88,
        "first_hour_pad": 0.16,
        "risk_scales": [1.00, 0.84, 0.70, 0.60],
        "daily_equity_dd": 0.011,
        "weekly_equity_dd": 0.020,
        "score_base": 68,
    },
    "FRA40": {
        "display": "CAC 40",
        "session_tz": "Europe/Paris",
        "cash_open": "09:00",
        "entry_windows": [("09:05", "16:40")],
        "flat_time": "17:10",
        "news_windows": [],
        "priority_rank": 4,
        "grid_step": 0.40,
        "grid_mult": 1.35,
        "max_orders": 4,
        "tp_atr": 0.28,
        "basket_stop_atr": 1.8,
        "z_entry": 2.05,
        "z_readd": 1.65,
        "rsi_long": 30,
        "rsi_short": 70,
        "rsi_readd_long": 40,
        "rsi_readd_short": 60,
        "atr_spike_mult": 1.8,
        "bar_range_mult": 1.5,
        "gap_atr_mult": 1.0,
        "ema_divergence": 0.92,
        "first_hour_pad": 0.18,
        "risk_scales": [1.00, 0.82, 0.68, 0.58],
        "daily_equity_dd": 0.010,
        "weekly_equity_dd": 0.019,
        "score_base": 67,
    },
    "US30": {
        "display": "Dow Jones 30",
        "session_tz": "US/Eastern",
        "cash_open": "09:30",
        "entry_windows": [("09:40", "15:10")],
        "flat_time": "15:40",
        "news_windows": [],
        "priority_rank": 5,
        "grid_step": 0.40,
        "grid_mult": 1.35,
        "max_orders": 3,
        "tp_atr": 0.30,
        "basket_stop_atr": 1.8,
        "z_entry": 2.05,
        "z_readd": 1.70,
        "rsi_long": 30,
        "rsi_short": 70,
        "rsi_readd_long": 40,
        "rsi_readd_short": 60,
        "atr_spike_mult": 1.8,
        "bar_range_mult": 1.5,
        "gap_atr_mult": 1.15,
        "ema_divergence": 1.00,
        "first_hour_pad": 0.18,
        "risk_scales": [1.00, 0.80, 0.66],
        "daily_equity_dd": 0.012,
        "weekly_equity_dd": 0.022,
        "score_base": 66,
    },
}


def _titanyx_exchange_state(profile):
    now_exchange = get_exchange_now(profile["session_tz"])
    now_minutes = now_exchange.hour * 60 + now_exchange.minute
    flat_minutes = _time_hhmm_to_minutes(profile["flat_time"])
    weekday = now_exchange.weekday()
    entry_ok = _time_in_exchange_windows(profile["session_tz"], profile["entry_windows"]) and weekday < 5 and now_minutes < flat_minutes
    force_flat = weekday >= 5 or now_minutes >= flat_minutes
    return {
        "now": now_exchange,
        "weekday": weekday,
        "entry_ok": entry_ok,
        "force_flat": force_flat,
        "label": now_exchange.strftime("%a %H:%M"),
    }


def _titanyx_news_blocked(profile):
    news_windows = list(profile.get("news_windows") or [])
    if not news_windows:
        return False
    now = get_exchange_now(profile["session_tz"])
    now_minutes = now.hour * 60 + now.minute
    for center_text in news_windows:
        center_minutes = _time_hhmm_to_minutes(center_text)
        if abs(now_minutes - center_minutes) <= 30:
            return True
    return False


def _titanyx_equity_state(profile):
    global titanyx_week_state
    acc = mt5.account_info()
    ensure_session_balance_anchor()
    if not acc:
        return {"blocked": False, "alert": False, "daily_dd": 0.0, "weekly_dd": 0.0, "label": "NO ACCOUNT"}

    now_local = datetime.now()
    iso_week = f"{now_local.isocalendar().year}-{now_local.isocalendar().week}"
    if titanyx_week_state.get("week") != iso_week or not titanyx_week_state.get("balance"):
        titanyx_week_state = {"week": iso_week, "balance": float(acc.balance)}

    day_anchor = max(float(session_balance_start or acc.balance), 1e-6)
    week_anchor = max(float(titanyx_week_state.get("balance") or acc.balance), 1e-6)
    daily_dd = max(0.0, (day_anchor - float(acc.equity)) / day_anchor)
    weekly_dd = max(0.0, (week_anchor - float(acc.equity)) / week_anchor)
    blocked = daily_dd >= profile["daily_equity_dd"] or weekly_dd >= profile["weekly_equity_dd"]
    alert = daily_dd >= profile["daily_equity_dd"] * 0.75 or weekly_dd >= profile["weekly_equity_dd"] * 0.75
    return {
        "blocked": blocked,
        "alert": alert,
        "daily_dd": daily_dd,
        "weekly_dd": weekly_dd,
        "label": f"D {daily_dd*100:.2f}% | W {weekly_dd*100:.2f}%",
    }


def _titanyx_cluster_snapshot(symbol, strategy_tag):
    positions = get_strategy_cluster_positions(symbol, strategy_tag)
    if not positions:
        return None
    total_volume = sum(float(p.volume) for p in positions)
    if total_volume <= 0:
        return None
    avg_entry = sum(float(p.price_open) * float(p.volume) for p in positions) / total_volume
    open_profit = sum(float(p.profit) for p in positions)
    direction = "BUY" if positions[0].type == 0 else "SELL"
    return {
        "positions": positions,
        "orders": len(positions),
        "avg_entry": float(avg_entry),
        "direction": direction,
        "open_profit": float(open_profit),
        "total_volume": float(total_volume),
    }


def _titanyx_first_hour_breakout_active(df_m5, profile):
    if df_m5 is None or len(df_m5) < 40:
        return False
    tz_name = profile["session_tz"]
    open_minutes = _time_hhmm_to_minutes(profile["cash_open"])
    first_hour_end = open_minutes + 60
    rows = []
    for _, row in df_m5.tail(120).iterrows():
        exchange_time = broker_time_to_exchange(row["time"], tz_name)
        if exchange_time is None:
            continue
        rows.append((exchange_time, row))
    if len(rows) < 20:
        return False
    current_day = rows[-2][0].date()
    day_rows = [(ts, row) for ts, row in rows if ts.date() == current_day]
    first_hour = []
    for ts, row in day_rows:
        minutes = ts.hour * 60 + ts.minute
        if open_minutes <= minutes < first_hour_end:
            first_hour.append(row)
    if len(first_hour) < 3:
        return False
    first_hour_df = pd.DataFrame(first_hour)
    recent_df = pd.DataFrame([row for _, row in day_rows])
    atr_now = float(recent_df["atr"].iloc[-2] or 0.0)
    if atr_now <= 0:
        return False
    upper = float(first_hour_df["high"].max())
    lower = float(first_hour_df["low"].min())
    close_now = float(recent_df["close"].iloc[-2])
    if close_now > upper + atr_now * profile["first_hour_pad"]:
        return True
    if close_now < lower - atr_now * profile["first_hour_pad"]:
        return True
    return False


def _titanyx_volatility_state(symbol, df_m5, df_m15, profile):
    if df_m5 is None or len(df_m5) < 80 or df_m15 is None or len(df_m15) < 60:
        return {"blocked": True, "add_blocked": True, "label": "NO VOL DATA"}

    atr_now = float(df_m5["atr"].iloc[-2] or 0.0)
    atr_mean = float(df_m5["atr"].iloc[-22:-2].mean() or 0.0)
    current_bar = df_m5.iloc[-2]
    current_range = float(current_bar["high"] - current_bar["low"])
    if atr_now <= 0 or atr_mean <= 0:
        return {"blocked": True, "add_blocked": True, "label": "NO ATR"}

    ema20_m15 = df_m15["close"].ewm(span=20).mean()
    ema50_m15 = df_m15["close"].ewm(span=50).mean()
    divergence = abs(float(ema20_m15.iloc[-2] - ema50_m15.iloc[-2])) / max(atr_now, 1e-6)

    day_slice = df_m5.iloc[-60:].copy()
    latest_day = day_slice["time"].dt.date.iloc[-1]
    day_slice = day_slice[day_slice["time"].dt.date == latest_day]
    session_gap = 0.0
    if len(day_slice) >= 2 and len(df_m5) > len(day_slice):
        session_gap = abs(float(day_slice["open"].iloc[0] - df_m5["close"].iloc[-len(day_slice)-1]))

    breakout_active = _titanyx_first_hour_breakout_active(df_m5, profile)
    atr_spike = atr_now > atr_mean * profile["atr_spike_mult"]
    range_spike = current_range > atr_now * profile["bar_range_mult"]
    gap_spike = session_gap > atr_now * profile["gap_atr_mult"]
    trend_impulsive = divergence > profile["ema_divergence"]
    blocked = atr_spike or range_spike or gap_spike or breakout_active or trend_impulsive
    add_blocked = blocked or atr_now > atr_mean * 1.35 or current_range > atr_now * 1.15
    reasons = []
    if atr_spike:
        reasons.append("ATR SPIKE")
    if range_spike:
        reasons.append("BAR RANGE SPIKE")
    if gap_spike:
        reasons.append("GAP ACTIVE")
    if breakout_active:
        reasons.append("FIRST HOUR BREAKOUT")
    if trend_impulsive:
        reasons.append("EMA DIVERGENCE")
    return {
        "blocked": blocked,
        "add_blocked": add_blocked,
        "label": " | ".join(reasons) if reasons else "NORMAL",
        "atr_now": atr_now,
        "atr_mean": atr_mean,
        "trend_divergence": divergence,
    }


def _titanyx_basket_levels(cluster_snapshot, atr_value, profile):
    avg_entry = float(cluster_snapshot["avg_entry"])
    if cluster_snapshot["direction"] == "BUY":
        tp_price = avg_entry + atr_value * profile["tp_atr"]
        stop_price = avg_entry - atr_value * profile["basket_stop_atr"]
    else:
        tp_price = avg_entry - atr_value * profile["tp_atr"]
        stop_price = avg_entry + atr_value * profile["basket_stop_atr"]
    return float(tp_price), float(stop_price)


def _titanyx_update_stop_streak(strategy_name, was_stop):
    runtime = strategy_runtime.setdefault(strategy_name, {})
    if was_stop:
        runtime["titanyx_stop_streak"] = int(runtime.get("titanyx_stop_streak", 0) or 0) + 1
    else:
        runtime["titanyx_stop_streak"] = 0
    runtime["titanyx_last_cluster_exit"] = datetime.now()


def get_titanyx_exit_reason(symbol, strategy_cfg, cluster_state, cluster_snapshot):
    if not cluster_snapshot:
        return None, None, None, None
    family = strategy_cfg.get("symbol_family")
    profile = TITANYX_PROFILES.get(family)
    if not profile:
        return None, None, None, None
    df_m5 = get_m5_cached(symbol)
    df_m15 = get_m15_cached(symbol)
    if df_m5 is None or len(df_m5) < 80 or df_m15 is None or len(df_m15) < 40:
        return None, None, None, None
    tick = safe_tick(symbol)
    if not tick:
        return None, None, None, None

    atr_now = float(df_m5["atr"].iloc[-2] or 0.0)
    if atr_now <= 0:
        return None, None, None, None

    tp_price, stop_price = _titanyx_basket_levels(cluster_snapshot, atr_now, profile)
    price_mid = (float(tick.ask) + float(tick.bid)) / 2.0
    session_state = _titanyx_exchange_state(profile)
    equity_state = _titanyx_equity_state(profile)

    if equity_state["blocked"]:
        return "EQUITY PROTECTION", "STOP", stop_price, tp_price
    if session_state["force_flat"]:
        return "SESSION FLAT", "TIME", stop_price, tp_price

    if cluster_snapshot["direction"] == "BUY":
        if price_mid >= tp_price:
            return "BASKET TP", "TP", stop_price, tp_price
        if price_mid <= stop_price:
            return "BASKET STOP", "STOP", stop_price, tp_price
    else:
        if price_mid <= tp_price:
            return "BASKET TP", "TP", stop_price, tp_price
        if price_mid >= stop_price:
            return "BASKET STOP", "STOP", stop_price, tp_price
    return None, None, stop_price, tp_price


def strategy_titanyx(df, symbol, branch_name, family):
    profile = TITANYX_PROFILES[family]
    strategy_cfg = get_strategy_config(branch_name) or {}
    strategy_tag = strategy_cfg.get("tag", branch_name[:8])
    session_state = _titanyx_exchange_state(profile)
    if not session_state["entry_ok"]:
        return blocked_signal(branch_name, "OUT SESSION", session_state["label"])
    if _titanyx_news_blocked(profile):
        return blocked_signal(branch_name, "NEWS LOCK")

    equity_state = _titanyx_equity_state(profile)
    if equity_state["blocked"]:
        return blocked_signal(branch_name, "EQUITY LOCK", equity_state["label"])

    runtime = strategy_runtime.setdefault(branch_name, {})
    last_cluster_exit = runtime.get("titanyx_last_cluster_exit")
    if isinstance(last_cluster_exit, datetime) and last_cluster_exit.date() < datetime.now().date():
        runtime["titanyx_stop_streak"] = 0
    if int(runtime.get("titanyx_stop_streak", 0) or 0) >= 2:
        return blocked_signal(branch_name, "STOP STREAK", f"{runtime.get('titanyx_stop_streak', 0)} baskets")

    df_m5 = get_m5_cached(symbol)
    df_m15 = get_m15_cached(symbol)
    if df_m5 is None or len(df_m5) < 100:
        return blocked_signal(branch_name, "NO M5 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal(branch_name, "NO M15 DATA")

    tick = safe_tick(symbol)
    if not tick:
        return blocked_signal(branch_name, "NO TICK")

    vol_state = _titanyx_volatility_state(symbol, df_m5, df_m15, profile)
    if vol_state["blocked"]:
        return blocked_signal(branch_name, "VOL BLOCK", vol_state["label"])

    close_m5 = df_m5["close"]
    ema20_m5 = close_m5.ewm(span=20).mean()
    ema50_m5 = close_m5.ewm(span=50).mean()
    ema20_m15 = df_m15["close"].ewm(span=20).mean()
    ema50_m15 = df_m15["close"].ewm(span=50).mean()
    rsi_m5 = compute_rsi(close_m5, 14)
    spread_series = close_m5 - ema20_m5
    zscore = compute_zscore(spread_series, 50)

    atr_now = float(df_m5["atr"].iloc[-2] or 0.0)
    if atr_now <= 0:
        return blocked_signal(branch_name, "NO ATR")

    confirm = df_m5.iloc[-2]
    prev = df_m5.iloc[-3]
    price_mid = (float(tick.ask) + float(tick.bid)) / 2.0
    vwap = float(df_m5["vwap"].iloc[-2] or price_mid)
    close_now = float(confirm["close"])
    close_prev = float(prev["close"])
    rsi_now = float(rsi_m5.iloc[-2])
    z_now = float(zscore.iloc[-2])
    ema20_now = float(ema20_m5.iloc[-2])
    trend_divergence = abs(float(ema20_m15.iloc[-2] - ema50_m15.iloc[-2])) / max(atr_now, 1e-6)

    cluster_snapshot = _titanyx_cluster_snapshot(symbol, strategy_tag)
    if cluster_snapshot:
        if vol_state["add_blocked"]:
            return blocked_signal(branch_name, "GRID LOCK", vol_state["label"])
        next_index = cluster_snapshot["orders"]
        if next_index >= profile["max_orders"]:
            return blocked_signal(branch_name, "MAX BASKET", f"{cluster_snapshot['orders']}/{profile['max_orders']}")
        next_step = atr_now * profile["grid_step"] * (profile["grid_mult"] ** max(0, cluster_snapshot["orders"] - 1))
        if cluster_snapshot["direction"] == "BUY":
            adverse_distance = max(0.0, cluster_snapshot["avg_entry"] - price_mid)
            absorb_ok = close_now >= close_prev and close_now >= float(confirm["open"])
            if adverse_distance < next_step:
                return blocked_signal(branch_name, "GRID NOT READY", pretrigger_text("GRID", next_step - adverse_distance, atr_now))
            if not (price_mid < min(vwap, ema20_now) and z_now <= -profile["z_readd"] and rsi_now <= profile["rsi_readd_long"]):
                return blocked_signal(branch_name, "MEAN LOST", f"Z {z_now:.2f} RSI {rsi_now:.1f}")
            if cluster_snapshot["orders"] >= 2 and not absorb_ok:
                return blocked_signal(branch_name, "NO ABSORB", f"Z {z_now:.2f}")
            sl_price = cluster_snapshot["avg_entry"] - atr_now * profile["basket_stop_atr"]
            tp_price = cluster_snapshot["avg_entry"] + atr_now * profile["tp_atr"]
            risk_scale = profile["risk_scales"][min(next_index, len(profile["risk_scales"]) - 1)]
            conf = min(88, profile["score_base"] + 5 + min(10, adverse_distance / max(atr_now, 1e-6) * 2.0))
            details = {
                "signal": "BUY",
                "direction": "LONG",
                "entry_level": float(tick.ask),
                "max_orders": profile["max_orders"],
                "tp_basket": float(tp_price),
                "stop_basket": float(sl_price),
                "volatility_state": vol_state["label"],
                "equity_protection": equity_state["label"],
                "summary": f"{profile['display']} LONG | TITANYX add #{cluster_snapshot['orders']+1} | entry {tick.ask:.2f} | max {profile['max_orders']} | TP basket {tp_price:.2f} | stop basket {sl_price:.2f} | vol {vol_state['label']} | equity {equity_state['label']}",
            }
            return {
                "signal": "BUY",
                "confidence": conf,
                "name": branch_name,
                "execution_df": df_m5,
                "sl_price": round(sl_price, 5),
                "tp_price": round(tp_price, 5),
                "signal_key": f"{family}|TITANYX|{confirm['time'].isoformat()}|BUY|{cluster_snapshot['orders']+1}",
                "signal_details": details,
                "risk_multiplier_override": risk_scale,
            }
        adverse_distance = max(0.0, price_mid - cluster_snapshot["avg_entry"])
        absorb_ok = close_now <= close_prev and close_now <= float(confirm["open"])
        if adverse_distance < next_step:
            return blocked_signal(branch_name, "GRID NOT READY", pretrigger_text("GRID", next_step - adverse_distance, atr_now))
        if not (price_mid > max(vwap, ema20_now) and z_now >= profile["z_readd"] and rsi_now >= profile["rsi_readd_short"]):
            return blocked_signal(branch_name, "MEAN LOST", f"Z {z_now:.2f} RSI {rsi_now:.1f}")
        if cluster_snapshot["orders"] >= 2 and not absorb_ok:
            return blocked_signal(branch_name, "NO ABSORB", f"Z {z_now:.2f}")
        sl_price = cluster_snapshot["avg_entry"] + atr_now * profile["basket_stop_atr"]
        tp_price = cluster_snapshot["avg_entry"] - atr_now * profile["tp_atr"]
        risk_scale = profile["risk_scales"][min(next_index, len(profile["risk_scales"]) - 1)]
        conf = min(88, profile["score_base"] + 5 + min(10, adverse_distance / max(atr_now, 1e-6) * 2.0))
        details = {
            "signal": "SELL",
            "direction": "SHORT",
            "entry_level": float(tick.bid),
            "max_orders": profile["max_orders"],
            "tp_basket": float(tp_price),
            "stop_basket": float(sl_price),
            "volatility_state": vol_state["label"],
            "equity_protection": equity_state["label"],
            "summary": f"{profile['display']} SHORT | TITANYX add #{cluster_snapshot['orders']+1} | entry {tick.bid:.2f} | max {profile['max_orders']} | TP basket {tp_price:.2f} | stop basket {sl_price:.2f} | vol {vol_state['label']} | equity {equity_state['label']}",
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m5,
            "sl_price": round(sl_price, 5),
            "tp_price": round(tp_price, 5),
            "signal_key": f"{family}|TITANYX|{confirm['time'].isoformat()}|SELL|{cluster_snapshot['orders']+1}",
            "signal_details": details,
            "risk_multiplier_override": risk_scale,
        }

    mean_long = price_mid < min(vwap, ema20_now) and z_now <= -profile["z_entry"] and rsi_now <= profile["rsi_long"]
    mean_short = price_mid > max(vwap, ema20_now) and z_now >= profile["z_entry"] and rsi_now >= profile["rsi_short"]
    revert_long = close_now >= close_prev and close_now >= float(confirm["open"])
    revert_short = close_now <= close_prev and close_now <= float(confirm["open"])
    if trend_divergence > profile["ema_divergence"]:
        return blocked_signal(branch_name, "IMPULSIVE TREND", f"DIV {trend_divergence:.2f}")

    if mean_long and revert_long:
        sl_price = float(tick.ask) - atr_now * profile["basket_stop_atr"]
        tp_price = float(tick.ask) + atr_now * profile["tp_atr"]
        conf = min(86, profile["score_base"] + int(abs(z_now) * 4))
        details = {
            "signal": "BUY",
            "direction": "LONG",
            "entry_level": float(tick.ask),
            "max_orders": profile["max_orders"],
            "tp_basket": float(tp_price),
            "stop_basket": float(sl_price),
            "volatility_state": vol_state["label"],
            "equity_protection": equity_state["label"],
            "summary": f"{profile['display']} LONG | TITANYX init | entry {tick.ask:.2f} | max {profile['max_orders']} | TP basket {tp_price:.2f} | stop basket {sl_price:.2f} | vol {vol_state['label']} | equity {equity_state['label']}",
        }
        return {
            "signal": "BUY",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m5,
            "sl_price": round(sl_price, 5),
            "tp_price": round(tp_price, 5),
            "signal_key": f"{family}|TITANYX|{confirm['time'].isoformat()}|BUY|1",
            "signal_details": details,
            "risk_multiplier_override": profile["risk_scales"][0],
        }

    if mean_short and revert_short:
        sl_price = float(tick.bid) + atr_now * profile["basket_stop_atr"]
        tp_price = float(tick.bid) - atr_now * profile["tp_atr"]
        conf = min(86, profile["score_base"] + int(abs(z_now) * 4))
        details = {
            "signal": "SELL",
            "direction": "SHORT",
            "entry_level": float(tick.bid),
            "max_orders": profile["max_orders"],
            "tp_basket": float(tp_price),
            "stop_basket": float(sl_price),
            "volatility_state": vol_state["label"],
            "equity_protection": equity_state["label"],
            "summary": f"{profile['display']} SHORT | TITANYX init | entry {tick.bid:.2f} | max {profile['max_orders']} | TP basket {tp_price:.2f} | stop basket {sl_price:.2f} | vol {vol_state['label']} | equity {equity_state['label']}",
        }
        return {
            "signal": "SELL",
            "confidence": conf,
            "name": branch_name,
            "execution_df": df_m5,
            "sl_price": round(sl_price, 5),
            "tp_price": round(tp_price, 5),
            "signal_key": f"{family}|TITANYX|{confirm['time'].isoformat()}|SELL|1",
            "signal_details": details,
            "risk_multiplier_override": profile["risk_scales"][0],
        }

    if price_mid < ema20_now and z_now <= -profile["z_entry"]:
        return blocked_signal(branch_name, "NO RECLAIM", f"Z {z_now:.2f} RSI {rsi_now:.1f}")
    if price_mid > ema20_now and z_now >= profile["z_entry"]:
        return blocked_signal(branch_name, "NO REJECT", f"Z {z_now:.2f} RSI {rsi_now:.1f}")
    return blocked_signal(branch_name, "MEAN WAIT", f"Z {z_now:.2f} RSI {rsi_now:.1f}")


def strategy_us500_titanyx(df, symbol):
    return strategy_titanyx(df, symbol, "US500_TITANYX", "US500")


def strategy_eustx50_titanyx(df, symbol):
    return strategy_titanyx(df, symbol, "EUSTX50_TITANYX", "EUSTX50")


def strategy_uk100_titanyx(df, symbol):
    return strategy_titanyx(df, symbol, "UK100_TITANYX", "UK100")


def strategy_fra40_titanyx(df, symbol):
    return strategy_titanyx(df, symbol, "FRA40_TITANYX", "FRA40")


def strategy_us30_titanyx(df, symbol):
    return strategy_titanyx(df, symbol, "US30_TITANYX", "US30")


LIQUIDITY_SNIPER_REGISTRY = [
    {"name": "USTECH_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "U1CL", "family": "USTECH", "kind": "CONTINUATION", "strategy_id": "SND_CLP_USTECH", "risk": 0.18, "score": 0.62, "cooldown": 1800},
    {"name": "USTECH_REVERSAL_SWEEP", "tag": "U1RV", "family": "USTECH", "kind": "REVERSAL", "strategy_id": "SND_REV_USTECH", "risk": 0.17, "score": 0.64, "cooldown": 2100},
    {"name": "GER40_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "G4CL", "family": "GER40", "kind": "CONTINUATION", "strategy_id": "SND_CLP_GER40", "risk": 0.12, "score": 0.64, "cooldown": 1800},
    {"name": "GER40_REVERSAL_SWEEP", "tag": "G4RV", "family": "GER40", "kind": "REVERSAL", "strategy_id": "SND_REV_GER40", "risk": 0.11, "score": 0.66, "cooldown": 2100},
    {"name": "US500_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "U5CL", "family": "US500", "kind": "CONTINUATION", "strategy_id": "SND_CLP_US500", "risk": 0.17, "score": 0.62, "cooldown": 1800},
    {"name": "US500_REVERSAL_SWEEP", "tag": "U5RV", "family": "US500", "kind": "REVERSAL", "strategy_id": "SND_REV_US500", "risk": 0.16, "score": 0.64, "cooldown": 2100},
    {"name": "JPN225_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "J2CL", "family": "JPN225", "kind": "CONTINUATION", "strategy_id": "SND_CLP_JPN225", "risk": 0.16, "score": 0.61, "cooldown": 2100},
    {"name": "JPN225_REVERSAL_SWEEP", "tag": "J2RV", "family": "JPN225", "kind": "REVERSAL", "strategy_id": "SND_REV_JPN225", "risk": 0.15, "score": 0.63, "cooldown": 2400},
    {"name": "EUSTX50_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "E5CL", "family": "EUSTX50", "kind": "CONTINUATION", "strategy_id": "SND_CLP_EUSTX50", "risk": 0.16, "score": 0.61, "cooldown": 1800},
    {"name": "EUSTX50_REVERSAL_SWEEP", "tag": "E5RV", "family": "EUSTX50", "kind": "REVERSAL", "strategy_id": "SND_REV_EUSTX50", "risk": 0.15, "score": 0.63, "cooldown": 2100},
    {"name": "US30_REVERSAL_SWEEP", "tag": "U3RV", "family": "US30", "kind": "REVERSAL", "strategy_id": "SND_REV_US30", "risk": 0.17, "score": 0.64, "cooldown": 2100},
    {"name": "US30_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "U3CL", "family": "US30", "kind": "CONTINUATION", "strategy_id": "SND_CLP_US30", "risk": 0.16, "score": 0.62, "cooldown": 1800},
    {"name": "UK100_REVERSAL_SWEEP", "tag": "K1RV", "family": "UK100", "kind": "REVERSAL", "strategy_id": "SND_REV_UK100", "risk": 0.15, "score": 0.64, "cooldown": 2100},
    {"name": "UK100_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "K1CL", "family": "UK100", "kind": "CONTINUATION", "strategy_id": "SND_CLP_UK100", "risk": 0.14, "score": 0.62, "cooldown": 1800},
    {"name": "US2000_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "U2CL", "family": "US2000", "kind": "CONTINUATION", "strategy_id": "SND_CLP_US2000", "risk": 0.16, "score": 0.61, "cooldown": 1800},
    {"name": "US2000_REVERSAL_SWEEP", "tag": "U2RV", "family": "US2000", "kind": "REVERSAL", "strategy_id": "SND_REV_US2000", "risk": 0.15, "score": 0.63, "cooldown": 2100},
    {"name": "FRA40_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "F4CL", "family": "FRA40", "kind": "CONTINUATION", "strategy_id": "SND_CLP_FRA40", "risk": 0.14, "score": 0.61, "cooldown": 1800},
    {"name": "FRA40_REVERSAL_SWEEP", "tag": "F4RV", "family": "FRA40", "kind": "REVERSAL", "strategy_id": "SND_REV_FRA40", "risk": 0.14, "score": 0.63, "cooldown": 2100},
    {"name": "SMI20_REVERSAL_SWEEP", "tag": "S2RV", "family": "SMI20", "kind": "REVERSAL", "strategy_id": "SND_REV_SMI20", "risk": 0.14, "score": 0.64, "cooldown": 2100},
    {"name": "SMI20_CONTINUATION_LIQUIDITY_PULLBACK", "tag": "S2CL", "family": "SMI20", "kind": "CONTINUATION", "strategy_id": "SND_CLP_SMI20", "risk": 0.13, "score": 0.62, "cooldown": 1800},
]

for spec in LIQUIDITY_SNIPER_REGISTRY:
    runner = _make_liquidity_runner(spec["kind"], spec["name"], spec["family"])
    register_strategy(
        spec["name"],
        runner,
        tag=spec["tag"],
        symbol_family=spec["family"],
        execution_mode="STD_ONLY",
        risk_multiplier=spec["risk"],
        min_score=spec["score"],
        cooldown_sec=spec["cooldown"],
        allowed_regimes={"TREND", "RANGE"},
        filter_func=default_strategy_filter,
        market_filter_func=fast_market_filter,
        sl_atr_multiplier=1.0,
        tp_rr_multiplier=2.4 if spec["kind"] == "CONTINUATION" else 2.6,
        auto_disable=False,
        be_trigger_progress=0.18,
        be_buffer_progress=0.10,
        trail_steps=[(0.28, 0.10), (0.52, 0.30), (0.86, 0.58)],
        session_label="S&D",
        exclusive_symbol=True,
    )
    cfg = get_strategy_config(spec["name"])
    if cfg:
        cfg["suite"] = "SND_LIQUIDITY_SNIPER"
        cfg["strategy_id"] = spec["strategy_id"]
        cfg["priority_rank"] = LIQUIDITY_SNIPER_PROFILES[spec["family"]]["priority_rank"]
        cfg["setup_kind"] = spec["kind"]
        LIQUIDITY_SNIPER_STRATEGY_NAMES.add(spec["name"])


register_strategy(
    "XAU_TREND",
    strategy_trend_vwap,
    tag="XTRD",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.30,
    min_score=0.46,
    cooldown_sec=120,
    allowed_regimes={"TREND"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.96,
    tp_rr_multiplier=2.05,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.12,
    trail_steps=[(0.18, 0.24), (0.30, 0.46), (0.44, 0.70)],
    session_label="24H",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.50,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.16,
    multi_tp_curve=[1.0, 1.65, 2.35, 3.25, 4.45, 5.95],
    multi_tp_tp2_gap_eur=0.42,
    multi_tp_step_gap_eur=0.22,
    min_splits=4,
    max_splits=4,
)
register_strategy(
    "XAU_M1_SCALP",
    strategy_xau_m1_scalp,
    enabled=True,
    toggle_arm=True,
    tag="XM1S",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.50,
    cooldown_sec=60,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.70,
    tp_rr_multiplier=1.54,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.08,
    trail_steps=[(0.24, 0.12), (0.40, 0.28), (0.58, 0.46)],
    session_label="13-03",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.80, 2.75, 3.9, 5.2, 6.9, 8.9, 11.2],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("XAU_M1_SCALP")["disabled_reason"] = ""
register_strategy(
    "XAU_BREAK_RETEST",
    strategy_xau_break_retest,
    tag="XBRT",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.20,
    min_score=0.47,
    cooldown_sec=70,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.78,
    tp_rr_multiplier=1.84,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.22, 0.14), (0.36, 0.30), (0.54, 0.50)],
    session_label="13-03",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.45,
    multi_tp_target_eur=0.96,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.82, 2.80, 4.10, 5.70, 7.80, 10.20, 13.10],
    multi_tp_tp2_gap_eur=0.42,
    multi_tp_step_gap_eur=0.22,
    min_splits=4,
    max_splits=5,
)
register_strategy(
    "XAU_VWAP_RECLAIM",
    strategy_xau_vwap_reclaim,
    enabled=True,
    toggle_arm=True,
    tag="XVRC",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.48,
    cooldown_sec=65,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.74,
    tp_rr_multiplier=1.62,
    auto_disable=False,
    be_trigger_progress=0.11,
    be_buffer_progress=0.08,
    trail_steps=[(0.22, 0.12), (0.36, 0.28), (0.54, 0.48)],
    session_label="13-03",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.35,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.75, 2.65, 3.85, 5.25, 7.05, 9.20, 11.80],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("XAU_VWAP_RECLAIM")["disabled_reason"] = ""
register_strategy(
    "XAU_LUKE",
    strategy_xau_luke,
    enabled=True,
    toggle_arm=True,
    tag="XLUK",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.55,
    min_score=0.54,
    cooldown_sec=40,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=luke_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.0,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.02,
    trail_steps=[],
    session_label="09:30-22",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.60,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 2.0, 3.0, 3.75, 4.75, 6.5],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.18,
    min_splits=6,
    max_splits=6,
    exclusive_symbol=True,
)
get_strategy_config("XAU_LUKE").update(
    {
        "suite": "LUKE",
        "strategy_id": "LUKE_XAUUSD",
        "selection_priority": 16,
        "fixed_tp_splits": 6,
        "fixed_tp_points": list(LUKE_PROFILES["XAUUSD"]["tp_points"]),
        "tp_point_buffer": float(LUKE_PROFILES["XAUUSD"]["spread_buffer_points"]),
        "point_value_price": gold_points_to_price(safe_info(resolve_broker_symbol("XAUUSD") or "XAUUSD"), 1.0) if safe_info(resolve_broker_symbol("XAUUSD") or "XAUUSD") else 0.1,
        "lot_weights_override": [1.0] * 6,
        "runner_enabled": bool(LUKE_PROFILES["XAUUSD"]["runner_enabled"]),
        "runner_points": float(LUKE_PROFILES["XAUUSD"]["runner_points"]),
    }
)
register_strategy(
    "BTC_TREND",
    strategy_btc_trend,
    enabled=True,
    toggle_arm=True,
    tag="BTCR",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.24,
    min_score=0.46,
    cooldown_sec=32,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.92,
    tp_rr_multiplier=2.10,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.10,
    trail_steps=[(0.24, 0.14), (0.40, 0.30), (0.58, 0.54)],
    session_label="24/7",
    multi_tp_eur_min=0.80,
    multi_tp_eur_max=1.50,
    multi_tp_target_eur=1.12,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.80, 2.65, 3.85, 5.35, 7.25, 9.50, 12.20],
    multi_tp_tp2_gap_eur=0.44,
    multi_tp_step_gap_eur=0.22,
    min_splits=4,
    max_splits=8,
)
get_strategy_config("BTC_TREND")["disabled_reason"] = ""
register_strategy(
    "BTC_SANTO_GRAAL",
    strategy_btc_santo_graal,
    tag="BTSG",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.48,
    cooldown_sec=600,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.4,
    auto_disable=False,
    be_trigger_progress=0.22,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.55, 0.34), (0.90, 0.64)],
    session_label="24/7",
    multi_tp_eur_min=0.85,
    multi_tp_eur_max=1.70,
    multi_tp_target_eur=1.18,
    multi_tp_first_atr=0.12,
    multi_tp_curve=[1.0, 2.1, 3.6, 5.6, 8.2, 11.6, 15.2, 19.4],
    multi_tp_tp2_gap_eur=0.48,
    multi_tp_step_gap_eur=0.26,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_KUMO_BREAKOUT",
    strategy_btc_kumo_breakout,
    tag="BTKB",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.50,
    cooldown_sec=600,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.2,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.58)],
    session_label="24/7",
    multi_tp_eur_min=0.82,
    multi_tp_eur_max=1.65,
    multi_tp_target_eur=1.14,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.95, 3.20, 4.95, 7.30, 10.20, 13.80, 18.00],
    multi_tp_tp2_gap_eur=0.46,
    multi_tp_step_gap_eur=0.24,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_PURIA",
    strategy_btc_puria,
    tag="BTPU",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.17,
    min_score=0.48,
    cooldown_sec=420,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.20,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.54, 0.32), (0.88, 0.60)],
    session_label="24/7",
    multi_tp_eur_min=0.82,
    multi_tp_eur_max=1.65,
    multi_tp_target_eur=1.16,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 2.0, 3.20, 4.95, 7.15, 10.00, 13.60, 17.50],
    multi_tp_tp2_gap_eur=0.46,
    multi_tp_step_gap_eur=0.24,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_DOUBLE_MACD",
    strategy_btc_double_macd,
    tag="BTDM",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.17,
    min_score=0.48,
    cooldown_sec=7200,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.30,
    auto_disable=False,
    be_trigger_progress=0.22,
    be_buffer_progress=0.10,
    trail_steps=[(0.32, 0.10), (0.56, 0.34), (0.90, 0.62)],
    session_label="24/7",
    multi_tp_eur_min=0.82,
    multi_tp_eur_max=1.65,
    multi_tp_target_eur=1.18,
    multi_tp_first_atr=0.12,
    multi_tp_curve=[1.0, 2.05, 3.35, 5.15, 7.55, 10.55, 14.40, 18.70],
    multi_tp_tp2_gap_eur=0.48,
    multi_tp_step_gap_eur=0.25,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_HEIKEN_TDI",
    strategy_btc_heiken_tdi,
    tag="BTHT",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.17,
    min_score=0.52,
    cooldown_sec=4200,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.20,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.54, 0.32), (0.86, 0.60)],
    session_label="24/7",
    multi_tp_eur_min=0.82,
    multi_tp_eur_max=1.65,
    multi_tp_target_eur=1.14,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.98, 3.15, 4.75, 6.95, 9.55, 12.95, 16.90],
    multi_tp_tp2_gap_eur=0.46,
    multi_tp_step_gap_eur=0.24,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_SIDUS",
    strategy_btc_sidus,
    tag="BTSD",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.17,
    min_score=0.48,
    cooldown_sec=420,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.25,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.54, 0.32), (0.88, 0.60)],
    session_label="24/7",
    multi_tp_eur_min=0.82,
    multi_tp_eur_max=1.65,
    multi_tp_target_eur=1.16,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 2.0, 3.25, 5.0, 7.35, 10.25, 14.0, 18.1],
    multi_tp_tp2_gap_eur=0.46,
    multi_tp_step_gap_eur=0.24,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "BTC_BREAK_RETEST",
    strategy_btc_break_retest,
    tag="BBRT",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.48,
    cooldown_sec=38,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.84,
    tp_rr_multiplier=1.88,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.14), (0.40, 0.30), (0.58, 0.52)],
    session_label="24/7",
    multi_tp_eur_min=0.80,
    multi_tp_eur_max=1.55,
    multi_tp_target_eur=1.10,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.82, 2.75, 4.00, 5.55, 7.55, 9.95, 12.85],
    multi_tp_tp2_gap_eur=0.44,
    multi_tp_step_gap_eur=0.22,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "BTC_VWAP_RECLAIM",
    strategy_btc_vwap_reclaim,
    enabled=False,
    toggle_arm=False,
    tag="BVRC",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.50,
    cooldown_sec=34,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.80,
    tp_rr_multiplier=1.68,
    auto_disable=False,
    be_trigger_progress=0.11,
    be_buffer_progress=0.09,
    trail_steps=[(0.22, 0.12), (0.38, 0.28), (0.56, 0.48)],
    session_label="24/7",
    multi_tp_eur_min=0.75,
    multi_tp_eur_max=1.45,
    multi_tp_target_eur=1.02,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.78, 2.62, 3.78, 5.15, 6.95, 9.15, 11.80],
    multi_tp_tp2_gap_eur=0.42,
    multi_tp_step_gap_eur=0.21,
    min_splits=4,
    max_splits=6,
)
get_strategy_config("BTC_VWAP_RECLAIM")["disabled_reason"] = "TEST BENCH"
register_strategy(
    "ETH_PULSE",
    strategy_eth_pulse,
    enabled=True,
    toggle_arm=True,
    tag="ETHP",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.20,
    min_score=0.46,
    cooldown_sec=36,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.88,
    tp_rr_multiplier=1.96,
    auto_disable=False,
    be_trigger_progress=0.13,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.12), (0.38, 0.28), (0.56, 0.50)],
    session_label="24/7",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.75, 2.55, 3.65, 4.95, 6.55, 8.45, 10.80],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=6,
)
get_strategy_config("ETH_PULSE")["disabled_reason"] = ""
register_strategy(
    "ETH_KUMO_BREAKOUT",
    strategy_eth_kumo_breakout,
    tag="ETKB",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.50,
    cooldown_sec=660,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.1,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="24/7",
    multi_tp_eur_min=0.72,
    multi_tp_eur_max=1.36,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.90, 3.05, 4.65, 6.75, 9.35, 12.45, 16.10],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.21,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "ETH_PURIA",
    strategy_eth_puria,
    tag="ETPU",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.48,
    cooldown_sec=480,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.05,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="24/7",
    multi_tp_eur_min=0.72,
    multi_tp_eur_max=1.36,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.88, 2.90, 4.35, 6.20, 8.60, 11.35, 14.60],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.21,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "ETH_DOUBLE_MACD",
    strategy_eth_double_macd,
    tag="ETDM",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.48,
    cooldown_sec=7200,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.10,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.52, 0.32), (0.84, 0.58)],
    session_label="24/7",
    multi_tp_eur_min=0.72,
    multi_tp_eur_max=1.36,
    multi_tp_target_eur=1.02,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.92, 3.00, 4.55, 6.35, 8.80, 11.70, 15.00],
    multi_tp_tp2_gap_eur=0.42,
    multi_tp_step_gap_eur=0.22,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "ETH_HEIKEN_TDI",
    strategy_eth_heiken_tdi,
    tag="ETHT",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.52,
    cooldown_sec=4200,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.15,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.52, 0.32), (0.84, 0.58)],
    session_label="24/7",
    multi_tp_eur_min=0.72,
    multi_tp_eur_max=1.36,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.92, 3.00, 4.55, 6.35, 8.85, 11.85, 15.25],
    multi_tp_tp2_gap_eur=0.41,
    multi_tp_step_gap_eur=0.21,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "ETH_BREAK_RETEST",
    strategy_eth_break_retest,
    tag="EBRT",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.48,
    cooldown_sec=40,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.82,
    tp_rr_multiplier=1.82,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.14), (0.40, 0.30), (0.58, 0.52)],
    session_label="24/7",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.35,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.80, 2.72, 3.92, 5.30, 7.05, 9.10, 11.70],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "ETH_VWAP_RECLAIM",
    strategy_eth_vwap_reclaim,
    enabled=False,
    toggle_arm=False,
    tag="EVRC",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.50,
    cooldown_sec=36,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.78,
    tp_rr_multiplier=1.62,
    auto_disable=False,
    be_trigger_progress=0.11,
    be_buffer_progress=0.09,
    trail_steps=[(0.22, 0.12), (0.38, 0.28), (0.56, 0.48)],
    session_label="24/7",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.25,
    multi_tp_target_eur=0.92,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.76, 2.58, 3.72, 5.02, 6.70, 8.70, 11.10],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.19,
    min_splits=4,
    max_splits=6,
)
get_strategy_config("ETH_VWAP_RECLAIM")["disabled_reason"] = "TEST BENCH"
register_strategy(
    "USTECH_TREND",
    strategy_ustech_trend,
    enabled=False,
    toggle_arm=False,
    tag="USTR",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.22,
    min_score=0.50,
    cooldown_sec=42,
    allowed_regimes={"TREND"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.76,
    tp_rr_multiplier=1.78,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.14), (0.38, 0.30), (0.56, 0.50)],
    session_label="07-23",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.65, 2.35, 3.30, 4.45, 5.95],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("USTECH_TREND")["disabled_reason"] = "LOSS REVIEW"
register_strategy(
    "USTECH_ORB",
    strategy_ustech_orb,
    enabled=False,
    toggle_arm=False,
    tag="UORB",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.54,
    cooldown_sec=95,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.62,
    tp_rr_multiplier=1.62,
    auto_disable=False,
    be_trigger_progress=0.08,
    be_buffer_progress=0.09,
    trail_steps=[(0.18, 0.22), (0.28, 0.44), (0.42, 0.68)],
    session_label="15:45-22:15",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=1.05,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.75, 2.65, 3.8, 5.1, 6.8, 8.9, 11.4],
    multi_tp_tp2_gap_eur=0.42,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=4,
)
get_strategy_config("USTECH_ORB")["disabled_reason"] = "LOSS REVIEW"
register_strategy(
    "USTECH_BREAK_RETEST",
    strategy_ustech_break_retest,
    tag="UBRT",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.24,
    min_score=0.52,
    cooldown_sec=55,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.74,
    tp_rr_multiplier=2.00,
    auto_disable=False,
    be_trigger_progress=0.14,
    be_buffer_progress=0.10,
    trail_steps=[(0.24, 0.14), (0.40, 0.32), (0.58, 0.54)],
    session_label="07-23",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.50,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.40, 4.60, 6.10],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "USTECH_KUMO_BREAKOUT",
    strategy_ustech_kumo_breakout,
    tag="U1KB",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.50,
    cooldown_sec=540,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.05,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="13-23",
    multi_tp_eur_min=0.68,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.88, 3.00, 4.55, 6.55, 9.05, 12.00, 15.50],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.21,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "USTECH_HEIKEN_TDI",
    strategy_ustech_heiken_tdi,
    tag="U1HT",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.50,
    cooldown_sec=3600,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.10,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="13-23",
    multi_tp_eur_min=0.68,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.88, 2.95, 4.45, 6.35, 8.85, 11.75, 15.15],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "USTECH_PURIA",
    strategy_ustech_puria,
    tag="U1PU",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.47,
    cooldown_sec=360,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.05,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="13-23",
    multi_tp_eur_min=0.68,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.90, 3.00, 4.55, 6.45, 8.90, 11.75, 15.00],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "USTECH_SIDUS",
    strategy_ustech_sidus,
    tag="U1SD",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.47,
    cooldown_sec=360,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.10,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="13-23",
    multi_tp_eur_min=0.68,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.92, 3.05, 4.60, 6.55, 9.00, 11.85, 15.10],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "USTECH_TOP",
    strategy_ustech_top,
    tag="UTOP",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.12,
    min_score=0.56,
    cooldown_sec=28,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.64,
    tp_rr_multiplier=1.34,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.20, 0.12), (0.34, 0.26), (0.50, 0.44)],
    session_label="13-23",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.75, 2.55, 3.55, 4.80, 6.35, 8.20, 10.40],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "USTECH_SANTO_GRAAL",
    strategy_ustech_santo_graal,
    tag="U1SG",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.47,
    cooldown_sec=540,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.2,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.52, 0.32), (0.84, 0.58)],
    session_label="13-23",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.45,
    multi_tp_target_eur=1.00,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 2.0, 3.3, 5.0, 7.2, 10.0, 13.2, 17.0],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.22,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "USTECH_PULLBACK_SCALP",
    strategy_ustech_pullback_scalp,
    enabled=False,
    toggle_arm=False,
    tag="U1PB",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.10,
    min_score=0.64,
    cooldown_sec=34,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.66,
    tp_rr_multiplier=1.34,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.20, 0.12), (0.34, 0.26), (0.50, 0.44)],
    session_label="08-23",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.75, 2.55, 3.55, 4.80, 6.35, 8.20, 10.40],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=6,
)
get_strategy_config("USTECH_PULLBACK_SCALP")["disabled_reason"] = "SUPERSEDED BY USTECH_TOP"
register_strategy(
    "JPN225_TOP",
    strategy_jpn225_top,
    tag="JTOP",
    symbol_family="JPN225",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.54,
    cooldown_sec=30,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.70,
    tp_rr_multiplier=1.36,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.22, 0.12), (0.36, 0.26), (0.54, 0.44)],
    session_label="02-10",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.35, 4.55, 6.05, 7.90, 10.10],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "JPN225_VWAP_TREND",
    strategy_jpn225_vwap_trend,
    tag="JPVT",
    symbol_family="JPN225",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.20,
    min_score=0.50,
    cooldown_sec=46,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.80,
    tp_rr_multiplier=1.76,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.14), (0.40, 0.30), (0.58, 0.52)],
    session_label="02-10",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.35,
    multi_tp_target_eur=0.96,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.78, 2.60, 3.72, 5.02, 6.72, 8.92, 11.32],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=5,
)
register_strategy(
    "JPN225_BREAK_RETEST",
    strategy_jpn225_break_retest,
    enabled=False,
    toggle_arm=False,
    tag="JPBR",
    symbol_family="JPN225",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.52,
    cooldown_sec=42,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.78,
    tp_rr_multiplier=1.74,
    auto_disable=False,
    be_trigger_progress=0.11,
    be_buffer_progress=0.09,
    trail_steps=[(0.22, 0.12), (0.38, 0.28), (0.56, 0.48)],
    session_label="02-10",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.35,
    multi_tp_target_eur=0.94,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.76, 2.55, 3.65, 4.95, 6.55, 8.55, 10.90],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("JPN225_BREAK_RETEST")["disabled_reason"] = "TEST BENCH"
register_strategy(
    "US500_ORB",
    strategy_us500_orb,
    enabled=False,
    toggle_arm=False,
    tag="U5OB",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.13,
    min_score=0.56,
    cooldown_sec=105,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.60,
    tp_rr_multiplier=1.58,
    auto_disable=False,
    be_trigger_progress=0.08,
    be_buffer_progress=0.09,
    trail_steps=[(0.18, 0.22), (0.28, 0.44), (0.42, 0.68)],
    session_label="15:45-22:15",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.88,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.70, 2.50, 3.55, 4.80, 6.30],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=4,
)
get_strategy_config("US500_ORB")["disabled_reason"] = "LOSS REVIEW"
register_strategy(
    "US500_TREND",
    strategy_us500_trend,
    tag="U5TR",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.42,
    min_score=0.53,
    cooldown_sec=55,
    allowed_regimes={"TREND"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.90,
    tp_rr_multiplier=1.90,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.12), (0.46, 0.28), (0.64, 0.48)],
    session_label="13-23",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.12,
    multi_tp_curve=[1.0, 1.60, 2.30, 3.25, 4.40, 5.90],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "US500_SANTO_GRAAL",
    strategy_us500_santo_graal,
    tag="U5SG",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.48,
    cooldown_sec=720,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.1,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="13-23",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.96,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.95, 3.15, 4.85, 6.95, 9.45, 12.40, 15.90],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.20,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "US500_DOUBLE_MACD",
    strategy_us500_double_macd,
    tag="U5DM",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.48,
    cooldown_sec=5400,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.05,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="24H",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.98,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.90, 2.95, 4.40, 6.10, 8.30, 11.00, 14.00],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.19,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "US500_TOP",
    strategy_us500_top,
    tag="P5TP",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.13,
    min_score=0.55,
    cooldown_sec=30,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.66,
    tp_rr_multiplier=1.34,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.20, 0.12), (0.34, 0.26), (0.50, 0.44)],
    session_label="13-23",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.88,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.35, 4.55, 6.05, 7.90, 10.10],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "US30_VWAP_TREND",
    strategy_us30_vwap_trend,
    tag="U3VT",
    symbol_family="US30",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.50,
    cooldown_sec=44,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.82,
    tp_rr_multiplier=1.80,
    auto_disable=False,
    be_trigger_progress=0.12,
    be_buffer_progress=0.09,
    trail_steps=[(0.24, 0.14), (0.40, 0.30), (0.58, 0.52)],
    session_label="13-23",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.94,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.80, 2.62, 3.78, 5.12, 6.85, 9.05, 11.55],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=5,
)
register_strategy(
    "US30_BREAK_RETEST",
    strategy_us30_break_retest,
    enabled=False,
    toggle_arm=False,
    tag="U3BR",
    symbol_family="US30",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.52,
    cooldown_sec=40,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.80,
    tp_rr_multiplier=1.74,
    auto_disable=False,
    be_trigger_progress=0.11,
    be_buffer_progress=0.09,
    trail_steps=[(0.22, 0.12), (0.38, 0.28), (0.56, 0.48)],
    session_label="13-23",
    multi_tp_eur_min=0.65,
    multi_tp_eur_max=1.30,
    multi_tp_target_eur=0.92,
    multi_tp_first_atr=0.09,
    multi_tp_curve=[1.0, 1.76, 2.55, 3.68, 5.00, 6.65, 8.70, 11.10],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("US30_BREAK_RETEST")["disabled_reason"] = "TEST BENCH"
register_strategy(
    "GER40_TREND",
    strategy_ger40_trend,
    enabled=False,
    toggle_arm=False,
    tag="G4TR",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.14,
    min_score=0.58,
    cooldown_sec=52,
    allowed_regimes={"TREND"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.76,
    tp_rr_multiplier=1.76,
    auto_disable=False,
    be_trigger_progress=0.14,
    be_buffer_progress=0.09,
    trail_steps=[(0.28, 0.12), (0.44, 0.28), (0.62, 0.46)],
    session_label="08-17",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.12,
    multi_tp_curve=[1.0, 1.7, 2.5, 3.6, 4.9, 6.5],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=5,
)
get_strategy_config("GER40_TREND")["disabled_reason"] = "LOSS REVIEW"
register_strategy(
    "GER40_SANTO_GRAAL",
    strategy_ger40_santo_graal,
    tag="G4SG",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.14,
    min_score=0.58,
    cooldown_sec=1800,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.1,
    auto_disable=False,
    be_trigger_progress=0.20,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="08-18",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.28,
    multi_tp_target_eur=0.94,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.95, 3.10, 4.75, 6.80, 9.20, 12.00, 15.40],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.19,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "GER40_PURIA",
    strategy_ger40_puria,
    tag="G4PU",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.58,
    cooldown_sec=1800,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.00,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="08-18",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.28,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.90, 2.95, 4.45, 6.20, 8.45, 11.20, 14.30],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.19,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "GER40_SIDUS",
    strategy_ger40_sidus,
    tag="G4SD",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.58,
    cooldown_sec=1800,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=swing_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=1.0,
    tp_rr_multiplier=2.08,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.28, 0.10), (0.50, 0.30), (0.82, 0.56)],
    session_label="08-18",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.28,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.92, 3.00, 4.55, 6.35, 8.70, 11.55, 14.85],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.19,
    min_splits=3,
    max_splits=4,
    exclusive_symbol=True,
)
register_strategy(
    "GER40_BREAK_RETEST",
    strategy_ger40_break_retest,
    tag="GBRT",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.22,
    min_score=0.58,
    cooldown_sec=70,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.72,
    tp_rr_multiplier=1.95,
    auto_disable=False,
    be_trigger_progress=0.14,
    be_buffer_progress=0.10,
    trail_steps=[(0.24, 0.14), (0.40, 0.32), (0.58, 0.54)],
    session_label="08-18",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.40, 4.55, 5.95],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "GER40_TOP",
    strategy_ger40_top,
    tag="GTOP",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.54,
    cooldown_sec=16,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.64,
    tp_rr_multiplier=1.30,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.20, 0.12), (0.34, 0.26), (0.50, 0.44)],
    session_label="08-18",
    multi_tp_eur_min=0.55,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.85,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.35, 4.50, 5.95, 7.70, 9.80],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=8,
)
register_strategy(
    "UK100_TOP",
    strategy_uk100_top,
    tag="KTOP",
    symbol_family="UK100",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.15,
    min_score=0.55,
    cooldown_sec=18,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.64,
    tp_rr_multiplier=1.30,
    auto_disable=False,
    be_trigger_progress=0.10,
    be_buffer_progress=0.08,
    trail_steps=[(0.20, 0.12), (0.34, 0.26), (0.50, 0.44)],
    session_label="08-18",
    multi_tp_eur_min=0.55,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.86,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.35, 4.50, 5.95, 7.70, 9.80],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=8,
)
register_strategy(
    "GER40_PULLBACK_SCALP",
    strategy_ger40_pullback_scalp,
    enabled=True,
    toggle_arm=True,
    tag="G1PB",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.08,
    min_score=0.66,
    cooldown_sec=240,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.72,
    tp_rr_multiplier=1.24,
    auto_disable=False,
    be_trigger_progress=0.08,
    be_buffer_progress=0.08,
    trail_steps=[(0.16, 0.14), (0.28, 0.32), (0.42, 0.52)],
    session_label="08-17",
    multi_tp_eur_min=0.55,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.08,
    multi_tp_curve=[1.0, 1.70, 2.45, 3.35, 4.50, 5.95, 7.70, 9.80],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=4,
)
get_strategy_config("GER40_PULLBACK_SCALP").update(
    {
        "disabled_reason": "",
        "cluster_lock_bias": 0.10,
        "protect_bias": 0.12,
        "hard_lock_ratio": 0.24,
        "cascade_ratio": 0.22,
        "tp1_guard_ratio": 0.15,
        "weak_profit_floor": 0.70,
        "runner_peak_keep_ratio": 0.54,
        "runner_realized_lock_ratio": 0.88,
    }
)
register_strategy(
    "GER40_SCALP",
    strategy_ger40_scalping,
    enabled=True,
    toggle_arm=True,
    tag="G4SC",
    symbol_family="GER40",
    execution_mode="STD_ONLY",
    risk_multiplier=0.06,
    min_score=0.68,
    cooldown_sec=90,
    allowed_regimes=set(),
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.78,
    tp_rr_multiplier=1.18,
    auto_disable=True,
    min_winrate=0.54,
    min_profit_factor=1.20,
    be_trigger_progress=0.36,
    be_buffer_progress=0.06,
    trail_steps=[(0.52, 0.18), (0.70, 0.36), (0.86, 0.58)],
    session_label="08-17",
)

DEPRECATED_STRATEGY_NAMES = {
    "USTECH_TOP",
    "US500_TOP",
    "JPN225_TOP",
    "GER40_TOP",
    "UK100_TOP",
    "USTECH_PULLBACK_SCALP",
}


def _strategy_selection_priority(strategy):
    name = str(strategy.get("name", "") or "")
    if name == "XAU_LUKE":
        return 16
    if name == "GER40_PULLBACK_SCALP":
        return 10
    if name == "GER40_SCALP":
        return 12
    if name == "GER40_TREND":
        return 28
    if name in {"GER40_CONTINUATION_LIQUIDITY_PULLBACK", "GER40_REVERSAL_SWEEP"}:
        return 19
    if name.endswith("_TITANYX"):
        return 8
    if name in LIQUIDITY_SNIPER_STRATEGY_NAMES:
        return 12
    if name.endswith("_TREND"):
        return 18
    if name.endswith("_BREAK_RETEST"):
        return 20
    if name.endswith("_ORB"):
        return 22
    if name.endswith("_SCALP"):
        return 24
    if name.endswith("_VWAP_RECLAIM") or name.endswith("_VWAP_TREND"):
        return 26
    if name.endswith("_SANTO_GRAAL"):
        return 30
    if name.endswith("_KUMO_BREAKOUT"):
        return 32
    if name.endswith("_PURIA"):
        return 34
    if name.endswith("_SIDUS"):
        return 36
    if name.endswith("_DOUBLE_MACD"):
        return 38
    if name.endswith("_HEIKEN_TDI"):
        return 40
    return 50


def _shift_hhmm(value, delta_minutes):
    total = _time_hhmm_to_minutes(value) + int(delta_minutes)
    total = max(0, min(23 * 60 + 59, total))
    return f"{total // 60:02d}:{total % 60:02d}"


def _expand_exchange_windows(windows, before_minutes=0, after_minutes=0):
    return [(_shift_hhmm(start, -before_minutes), _shift_hhmm(end, after_minutes)) for start, end in list(windows or [])]


def _expand_session_tuple(session, start_pad=0, end_pad=0):
    start, end = session
    return max(0, int(start) - int(start_pad)), min(23, int(end) + int(end_pad))


def _derive_runtime_signal_key(symbol, strategy_name, signal, market_df=None, execution_df=None):
    source_df = execution_df if execution_df is not None else market_df
    candle_key = "NA"
    try:
        if source_df is not None and len(source_df) > 0:
            if hasattr(source_df, "columns") and "time" in source_df.columns:
                candle = source_df.iloc[-1]["time"]
            else:
                candle = source_df.index[-1]
            candle_key = candle.isoformat() if hasattr(candle, "isoformat") else str(candle)
    except Exception:
        candle_key = "NA"
    return f"{strategy_name}|{symbol}|{signal}|{candle_key}"


def _apply_v160_strategy_governance():
    global STRATEGIES
    kept = []
    for strategy in STRATEGIES:
        if strategy["name"] in DEPRECATED_STRATEGY_NAMES:
            continue
        kept.append(strategy)
    STRATEGIES = kept

    for deprecated_name in DEPRECATED_STRATEGY_NAMES:
        strategy_stats.pop(deprecated_name, None)
        strategy_runtime.pop(deprecated_name, None)

    for strategy in STRATEGIES:
        if strategy.get("disabled_reason") in {
            "S&D LIQUIDITY SNIPER CONTROL",
            "TEST BENCH",
            "SUPERSEDED BY USTECH_TOP",
            "SUPERSEDED BY GER40_TOP",
            "LOSS REVIEW",
        }:
            strategy["disabled_reason"] = ""
        if strategy.get("disabled_reason"):
            strategy["enabled"] = False
            strategy["toggle_arm"] = False
        else:
            strategy["enabled"] = True
            strategy["toggle_arm"] = True
        strategy["selection_priority"] = _strategy_selection_priority(strategy)


def _apply_v160_entry_push_governance():
    for strategy in STRATEGIES:
        if strategy.get("disabled_reason"):
            continue

        name = str(strategy.get("name", "") or "")
        suite = str(strategy.get("suite", "") or "").upper()
        filter_name = getattr(strategy.get("filter_func"), "__name__", "")
        min_score = float(strategy.get("min_score", 0.45) or 0.45)
        cooldown_sec = int(strategy.get("cooldown_sec", 60) or 60)
        risk_multiplier = float(strategy.get("risk_multiplier", 0.20) or 0.20)

        if suite == "SND_LIQUIDITY_SNIPER":
            strategy["min_score"] = round(clamp(min_score - 0.08, 0.52, 0.66), 3)
            strategy["cooldown_sec"] = max(420, int(cooldown_sec * 0.30))
            strategy["risk_multiplier"] = round(max(0.10, risk_multiplier * 0.92), 3)
        elif suite == "TITANYX":
            strategy["min_score"] = round(clamp(min_score - 0.05, 0.44, 0.56), 3)
            strategy["cooldown_sec"] = max(35, int(cooldown_sec * 0.60))
        elif filter_name == "luke_filter":
            strategy["min_score"] = round(clamp(min_score - 0.05, 0.48, 0.60), 3)
            strategy["cooldown_sec"] = max(40, int(cooldown_sec * 0.55))
        elif filter_name == "scalping_filter":
            strategy["min_score"] = round(clamp(min_score - 0.05, 0.42, 0.58), 3)
            strategy["cooldown_sec"] = max(15, int(cooldown_sec * 0.50))
        elif filter_name == "swing_filter":
            strategy["min_score"] = round(clamp(min_score - 0.05, 0.38, 0.56), 3)
            strategy["cooldown_sec"] = max(120, int(cooldown_sec * 0.55))
        else:
            strategy["min_score"] = round(clamp(min_score - 0.06, 0.44, 0.62), 3)
            strategy["cooldown_sec"] = max(180, int(cooldown_sec * 0.50))

        if name.endswith("_DOUBLE_MACD"):
            strategy["cooldown_sec"] = max(1800, int(cooldown_sec * 0.33))
            strategy["min_score"] = round(clamp(strategy["min_score"] - 0.02, 0.36, 0.52), 3)
        elif name.endswith("_SANTO_GRAAL"):
            strategy["cooldown_sec"] = max(180, int(cooldown_sec * 0.35))
            strategy["min_score"] = round(clamp(strategy["min_score"] - 0.02, 0.38, 0.52), 3)
        elif name.endswith("_KUMO_BREAKOUT"):
            strategy["cooldown_sec"] = max(240, int(cooldown_sec * 0.40))
            strategy["min_score"] = round(clamp(strategy["min_score"] - 0.02, 0.40, 0.54), 3)
        elif name.endswith("_PURIA") or name.endswith("_SIDUS"):
            strategy["cooldown_sec"] = max(180, int(cooldown_sec * 0.40))
            strategy["min_score"] = round(clamp(strategy["min_score"] - 0.02, 0.38, 0.52), 3)
        elif name.endswith("_ORB"):
            strategy["cooldown_sec"] = max(45, int(cooldown_sec * 0.50))
            strategy["min_score"] = round(clamp(strategy["min_score"] - 0.02, 0.42, 0.58), 3)
        elif name.endswith("_TREND"):
            strategy["cooldown_sec"] = max(45, int(cooldown_sec * 0.55))
            strategy["min_score"] = round(clamp(strategy["min_score"], 0.36, 0.58), 3)
        elif name.endswith("_PULLBACK_SCALP"):
            strategy["cooldown_sec"] = max(36, int(cooldown_sec * 0.55))
            strategy["min_score"] = round(clamp(strategy["min_score"], 0.38, 0.58), 3)
        elif name.endswith("_SCALP"):
            strategy["cooldown_sec"] = max(24, int(cooldown_sec * 0.55))
            strategy["min_score"] = round(clamp(strategy["min_score"], 0.38, 0.58), 3)
        elif name.endswith("_BREAK_RETEST") or name.endswith("_VWAP_RECLAIM") or name.endswith("_VWAP_TREND"):
            strategy["cooldown_sec"] = max(36, int(cooldown_sec * 0.55))
            strategy["min_score"] = round(clamp(strategy["min_score"], 0.36, 0.56), 3)

        strategy["enabled"] = True
        strategy["toggle_arm"] = True


def _apply_v163_stress_test_governance():
    if not STRESS_TEST_ACTIVE:
        return

    global MAX_NEW_TRADES_PER_CYCLE, MAX_TRADES_PER_CYCLE, MAX_PARALLEL_SYMBOL_SCANS
    MAX_NEW_TRADES_PER_CYCLE = max(MAX_NEW_TRADES_PER_CYCLE, 5)
    MAX_TRADES_PER_CYCLE = max(MAX_TRADES_PER_CYCLE, 10)
    MAX_PARALLEL_SYMBOL_SCANS = max(MAX_PARALLEL_SYMBOL_SCANS, 12)

    protected_live_branches = {
        "XAU_M1_SCALP",
        "US500_ORB",
        "GER40_SCALP",
        "GER40_PULLBACK_SCALP",
        "US30_BREAK_RETEST",
    }

    for strategy in STRATEGIES:
        if strategy.get("disabled_reason"):
            continue

        name = str(strategy.get("name", "") or "")
        if name in protected_live_branches:
            continue

        suite = str(strategy.get("suite", "") or "").upper()
        filter_name = getattr(strategy.get("filter_func"), "__name__", "")
        base_min_score = float(strategy.get("min_score", 0.45) or 0.45)
        base_cooldown = int(strategy.get("cooldown_sec", 60) or 60)
        base_risk = float(strategy.get("risk_multiplier", 0.20) or 0.20)

        strategy["min_score"] = round(clamp(base_min_score - 0.03, 0.30, 0.56), 3)
        strategy["cooldown_sec"] = max(12, int(base_cooldown * 0.72))

        if suite == "SND_LIQUIDITY_SNIPER" or name in LIQUIDITY_SNIPER_STRATEGY_NAMES:
            strategy["min_score"] = round(clamp(base_min_score - 0.10, 0.34, 0.52), 3)
            strategy["cooldown_sec"] = max(120, int(base_cooldown * 0.22))
            strategy["risk_multiplier"] = round(max(0.10, base_risk * 0.94), 3)
        elif suite == "TITANYX" or name.endswith("_TITANYX"):
            strategy["min_score"] = round(clamp(base_min_score - 0.08, 0.34, 0.50), 3)
            strategy["cooldown_sec"] = max(24, int(base_cooldown * 0.50))
        elif name.endswith("_SANTO_GRAAL"):
            strategy["min_score"] = round(clamp(base_min_score - 0.08, 0.34, 0.50), 3)
            strategy["cooldown_sec"] = max(120, int(base_cooldown * 0.24))
        elif name.endswith("_KUMO_BREAKOUT"):
            strategy["min_score"] = round(clamp(base_min_score - 0.08, 0.34, 0.50), 3)
            strategy["cooldown_sec"] = max(150, int(base_cooldown * 0.28))
        elif name.endswith("_PURIA") or name.endswith("_SIDUS"):
            strategy["min_score"] = round(clamp(base_min_score - 0.08, 0.32, 0.50), 3)
            strategy["cooldown_sec"] = max(120, int(base_cooldown * 0.28))
        elif name.endswith("_DOUBLE_MACD"):
            strategy["min_score"] = round(clamp(base_min_score - 0.10, 0.30, 0.48), 3)
            strategy["cooldown_sec"] = max(900, int(base_cooldown * 0.20))
        elif name.endswith("_HEIKEN_TDI"):
            strategy["min_score"] = round(clamp(base_min_score - 0.08, 0.32, 0.50), 3)
            strategy["cooldown_sec"] = max(900, int(base_cooldown * 0.24))
        elif name.endswith("_ORB"):
            strategy["min_score"] = round(clamp(base_min_score - 0.04, 0.40, 0.56), 3)
            strategy["cooldown_sec"] = max(42, int(base_cooldown * 0.52))
        elif name.endswith("_PULLBACK_SCALP"):
            strategy["min_score"] = round(clamp(base_min_score - 0.04, 0.40, 0.56), 3)
            strategy["cooldown_sec"] = max(34, int(base_cooldown * 0.55))
        elif name.endswith("_SCALP"):
            strategy["min_score"] = round(clamp(base_min_score - 0.04, 0.38, 0.54), 3)
            strategy["cooldown_sec"] = max(26, int(base_cooldown * 0.55))
        elif name.endswith("_BREAK_RETEST") or name.endswith("_VWAP_RECLAIM") or name.endswith("_VWAP_TREND"):
            strategy["min_score"] = round(clamp(base_min_score - 0.06, 0.36, 0.54), 3)
            strategy["cooldown_sec"] = max(30, int(base_cooldown * 0.48))
        elif name.endswith("_TREND") or filter_name == "luke_filter":
            strategy["min_score"] = round(clamp(base_min_score - 0.05, 0.36, 0.56), 3)
            strategy["cooldown_sec"] = max(36, int(base_cooldown * 0.52))

        strategy["enabled"] = True
        strategy["toggle_arm"] = True

    xau_overrides = {
        "XAU_TREND": {"min_score": 0.50, "cooldown_sec": 150, "risk_multiplier": 0.24, "allowed_regimes": {"TREND"}},
        "XAU_M1_SCALP": {"min_score": 0.45, "cooldown_sec": 34, "risk_multiplier": 0.15, "allowed_regimes": {"TREND", "RANGE"}},
        "XAU_BREAK_RETEST": {"min_score": 0.46, "cooldown_sec": 48, "risk_multiplier": 0.16, "allowed_regimes": {"TREND", "RANGE"}},
        "XAU_VWAP_RECLAIM": {"min_score": 0.43, "cooldown_sec": 28, "risk_multiplier": 0.17, "allowed_regimes": {"TREND", "RANGE"}},
        "XAU_LUKE": {"min_score": 0.44, "cooldown_sec": 28, "risk_multiplier": 0.48, "allowed_regimes": {"TREND", "RANGE"}},
    }
    for name, override in xau_overrides.items():
        strategy = get_strategy_config(name)
        if not strategy:
            continue
        strategy.update(override)
        strategy["enabled"] = True
        strategy["toggle_arm"] = True
        if name == "XAU_VWAP_RECLAIM":
            strategy["disabled_reason"] = ""

    branch_overrides = {
        "US500_ORB": {"min_score": 0.50, "cooldown_sec": 96, "risk_multiplier": 0.10},
        "ETH_BREAK_RETEST": {"min_score": 0.44, "cooldown_sec": 60, "risk_multiplier": 0.13},
        "GER40_PULLBACK_SCALP": {"min_score": 0.64, "cooldown_sec": 120, "risk_multiplier": 0.07},
        "GER40_SCALP": {"min_score": 0.56, "cooldown_sec": 42},
        "BTC_TREND": {"min_score": 0.42, "cooldown_sec": 54, "risk_multiplier": 0.20},
        "ETH_PULSE": {"min_score": 0.40, "cooldown_sec": 34},
        "US500_TREND": {"min_score": 0.42, "cooldown_sec": 42},
        "USTECH_TREND": {"min_score": 0.42, "cooldown_sec": 42},
    }
    for name, override in branch_overrides.items():
        strategy = get_strategy_config(name)
        if strategy:
            strategy.update(override)
            strategy["enabled"] = True
            strategy["toggle_arm"] = True

    for profile in SANTO_GRAAL_PROFILES.values():
        profile["session"] = _expand_session_tuple(profile["session"], 1, 1)
        profile["adx_min"] = max(22.0, float(profile["adx_min"]) - 3.0)
        profile["d1_adx_min"] = max(18.0, float(profile["d1_adx_min"]) - 2.5)
        profile["ema_pad"] = max(0.02, float(profile["ema_pad"]) - 0.01)
        profile["pullback_depth"] = min(0.62, float(profile["pullback_depth"]) + 0.08)
        profile["confirm_pad"] = min(0.18, float(profile["confirm_pad"]) + 0.03)
        profile["adx_drift_tol"] = float(profile.get("adx_drift_tol", 0.0)) + 0.35
        profile["d1_adx_drift_tol"] = float(profile.get("d1_adx_drift_tol", 0.0)) + 0.25

    for profile in KUMO_BREAKOUT_PROFILES.values():
        profile["session"] = _expand_session_tuple(profile["session"], 1, 1)
        profile["breakout_pad"] = max(0.03, float(profile["breakout_pad"]) - 0.02)
        profile["max_extension"] = float(profile["max_extension"]) + 0.35
        profile["hold_bars"] = max(1, int(profile.get("hold_bars", 1)) - 1)

    for profile in SIDUS_PROFILES.values():
        profile["session"] = _expand_session_tuple(profile["session"], 1, 1)
        profile["cross_pad"] = max(0.015, float(profile["cross_pad"]) - 0.01)
        profile["close_pad"] = max(0.01, float(profile["close_pad"]) - 0.008)
        profile["compression"] = max(0.10, float(profile["compression"]) - 0.05)
        profile["flat_slope"] = max(0.035, float(profile["flat_slope"]) - 0.02)
        profile["max_extension"] = float(profile["max_extension"]) + 0.35
        profile["body_floor"] = max(0.04, float(profile["body_floor"]) - 0.02)
        profile["wick_ratio"] = float(profile["wick_ratio"]) + 0.18
        profile["whipsaw_limit"] = int(profile.get("whipsaw_limit", 3)) + 1

    for profile in PURIA_PROFILES.values():
        profile["session"] = _expand_session_tuple(profile["session"], 1, 1)
        profile["cross_pad"] = max(0.015, float(profile["cross_pad"]) - 0.01)
        profile["close_pad"] = max(0.01, float(profile["close_pad"]) - 0.008)
        profile["compression"] = max(0.10, float(profile["compression"]) - 0.04)
        profile["flat_slope"] = max(0.03, float(profile["flat_slope"]) - 0.015)
        profile["body_floor"] = max(0.04, float(profile["body_floor"]) - 0.02)
        profile["wick_ratio"] = float(profile["wick_ratio"]) + 0.12
        profile["macd_floor"] = max(0.020, float(profile["macd_floor"]) - 0.010)
        profile["macd_expand"] = max(0.008, float(profile["macd_expand"]) - 0.004)
        profile["whipsaw_limit"] = int(profile.get("whipsaw_limit", 3)) + 1

    for profile in DOUBLE_MACD_PROFILES.values():
        profile["sma_sep"] = max(0.14, float(profile["sma_sep"]) - 0.05)
        profile["senior_floor"] = max(0.035, float(profile["senior_floor"]) - 0.015)
        profile["senior_hist_floor"] = max(0.008, float(profile["senior_hist_floor"]) - 0.004)
        profile["junior_floor"] = max(0.020, float(profile["junior_floor"]) - 0.010)
        profile["junior_near_zero"] = float(profile["junior_near_zero"]) + 0.05
        profile["junior_expand"] = max(0.008, float(profile["junior_expand"]) - 0.004)
        profile["whipsaw_limit"] = int(profile.get("whipsaw_limit", 3)) + 1

    for profile in HEIKEN_TDI_PROFILES.values():
        profile["session"] = _expand_session_tuple(profile["session"], 1, 1)
        profile["tdi_clear"] = max(0.35, float(profile["tdi_clear"]) - 0.12)
        profile["yellow_slope"] = max(0.08, float(profile["yellow_slope"]) - 0.05)
        profile["yellow_pad"] = max(0.14, float(profile["yellow_pad"]) - 0.08)
        profile["tdi_relax"] = float(profile["tdi_relax"]) + 0.05
        profile["compression"] = max(0.24, float(profile["compression"]) - 0.08)
        profile["body_floor"] = max(0.05, float(profile["body_floor"]) - 0.02)
        profile["key_level_pad"] = float(profile["key_level_pad"]) + 0.08
        profile["whipsaw_limit"] = int(profile.get("whipsaw_limit", 2)) + 1

    for profile in LIQUIDITY_SNIPER_PROFILES.values():
        profile["session_windows"] = _expand_exchange_windows(profile["session_windows"], before_minutes=20, after_minutes=45)
        profile["max_symbol_trades"] = max(int(profile.get("max_symbol_trades", 2)), 3)
        profile["max_total_trades"] = max(int(profile.get("max_total_trades", 4)), 6)
        profile["max_daily_drawdown_eur"] = round(float(profile.get("max_daily_drawdown_eur", 4.0)) * 1.20, 2)

    for profile in TITANYX_PROFILES.values():
        profile["entry_windows"] = _expand_exchange_windows(profile["entry_windows"], before_minutes=15, after_minutes=55)
        profile["z_entry"] = max(1.70, float(profile["z_entry"]) - 0.20)
        profile["z_readd"] = max(1.35, float(profile["z_readd"]) - 0.12)
        profile["rsi_long"] = min(36, int(profile["rsi_long"]) + 3)
        profile["rsi_short"] = max(64, int(profile["rsi_short"]) - 3)
        profile["rsi_readd_long"] = min(46, int(profile["rsi_readd_long"]) + 3)
        profile["rsi_readd_short"] = max(54, int(profile["rsi_readd_short"]) - 3)
        profile["atr_spike_mult"] = float(profile["atr_spike_mult"]) + 0.25
        profile["bar_range_mult"] = float(profile["bar_range_mult"]) + 0.20
        profile["gap_atr_mult"] = float(profile["gap_atr_mult"]) + 0.12
        profile["ema_divergence"] = float(profile["ema_divergence"]) + 0.18
        profile["score_base"] = int(profile.get("score_base", 70)) - 4


_apply_v160_strategy_governance()
_apply_v160_entry_push_governance()
_apply_v163_stress_test_governance()


def _restore_runtime_switches():
    refresh_symbols()
    for strategy in STRATEGIES:
        if strategy.get("disabled_reason"):
            strategy["enabled"] = False
            continue
        family = strategy.get("symbol_family")
        name = str(strategy.get("name", "") or "")
        if is_crypto_family(family):
            strategy["enabled"] = bool(crypto_enabled and strategy.get("toggle_arm", True))
        elif name.endswith("_SCALP"):
            strategy["enabled"] = bool(scalping_enabled and strategy.get("toggle_arm", True))
        else:
            strategy["enabled"] = True


_restore_runtime_switches()


def _asset_weight_for_symbol(symbol):
    for family, weight in ASSET_WEIGHTS.items():
        if symbol_in_family(symbol, family):
            return float(weight)
    return 0.50


def _strategy_archetype(strategy_cfg):
    strategy_cfg = strategy_cfg or {}
    name = str(strategy_cfg.get("name", "") or "")
    suite = str(strategy_cfg.get("suite", "") or "").upper()
    setup_kind = str(strategy_cfg.get("setup_kind", "") or "").upper()

    if suite == "TITANYX":
        return "GRID_MEAN_REVERSION"
    if suite == "LUKE":
        return "PULLBACK_SCALP"
    if suite == "SND_LIQUIDITY_SNIPER":
        return "SND_REVERSAL" if setup_kind == "REVERSAL" else "SND_CONTINUATION"
    if name.endswith("_PULLBACK_SCALP"):
        return "PULLBACK_SCALP"
    if name.endswith("_SCALP"):
        return "SCALP"
    if name.endswith("_ORB"):
        return "OPENING_REVERSION"
    if name.endswith("_BREAK_RETEST"):
        return "BREAK_RETEST"
    if name.endswith("_VWAP_RECLAIM"):
        return "VWAP_RECLAIM"
    if name.endswith("_VWAP_TREND"):
        return "VWAP_TREND"
    if name.endswith("_TREND"):
        return "TREND"
    if name.endswith("_SANTO_GRAAL"):
        return "SANTO_GRAAL"
    if name.endswith("_KUMO_BREAKOUT"):
        return "KUMO_BREAKOUT"
    if name.endswith("_PURIA"):
        return "PURIA"
    if name.endswith("_SIDUS"):
        return "SIDUS"
    if name.endswith("_DOUBLE_MACD"):
        return "DOUBLE_MACD"
    if name.endswith("_HEIKEN_TDI"):
        return "HEIKEN_TDI"
    return name or "GENERIC"


def _strategies_compatible(strategy_a, signal_a, strategy_b, signal_b):
    if not strategy_a or not strategy_b:
        return False
    if str(signal_a).upper() != str(signal_b).upper():
        return False
    if strategy_a.get("tag") == strategy_b.get("tag"):
        return False

    arch_a = _strategy_archetype(strategy_a)
    arch_b = _strategy_archetype(strategy_b)
    if arch_a == arch_b:
        return False

    mean_reversion_archetypes = {"GRID_MEAN_REVERSION", "OPENING_REVERSION", "SND_REVERSAL"}
    fast_archetypes = {"SCALP", "PULLBACK_SCALP"}
    breakout_archetypes = {"BREAK_RETEST", "KUMO_BREAKOUT"}

    if arch_a in mean_reversion_archetypes and arch_b in mean_reversion_archetypes:
        return False
    if arch_a in fast_archetypes and arch_b in fast_archetypes:
        return False
    if arch_a in breakout_archetypes and arch_b in breakout_archetypes:
        return False
    return True


def _get_open_symbol_clusters(symbol, live_positions=None):
    clusters = {}
    for p in (live_positions if live_positions is not None else safe_positions()):
        if p.magic != MAGIC_ID or p.symbol != symbol:
            continue
        p_tag = position_strategy_map.get(p.ticket) or extract_strategy_tag(getattr(p, "comment", ""))
        if not p_tag:
            continue
        clusters.setdefault(p_tag, "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL")
    return clusters


def _can_share_symbol_slot(symbol, strategy_cfg, signal, live_positions=None):
    open_clusters = _get_open_symbol_clusters(symbol, live_positions)
    strategy_tag = strategy_cfg.get("tag")

    if strategy_tag in open_clusters:
        return True, "SAME CLUSTER"
    if not open_clusters:
        return True, "FREE"

    symbol_limit = min(get_max_clusters_for_symbol(symbol), MAX_COMPATIBLE_STRATEGIES_PER_SYMBOL)
    if len(open_clusters) >= symbol_limit:
        return False, "SYMBOL SLOT FULL"

    for open_tag, open_signal in open_clusters.items():
        open_cfg = get_strategy_by_tag(open_tag)
        if not open_cfg:
            return False, "SYMBOL ACTIVE"
        if not _strategies_compatible(strategy_cfg, signal, open_cfg, open_signal):
            return False, f"INCOMPATIBLE {open_tag}"
    return True, "COMPATIBLE"


TITANYX_REGISTRY = [
    {"name": "US500_TITANYX", "func": strategy_us500_titanyx, "tag": "U5TX", "family": "US500", "risk": 0.15, "score": 0.52, "cooldown": 75},
    {"name": "EUSTX50_TITANYX", "func": strategy_eustx50_titanyx, "tag": "E5TX", "family": "EUSTX50", "risk": 0.14, "score": 0.51, "cooldown": 75},
    {"name": "UK100_TITANYX", "func": strategy_uk100_titanyx, "tag": "K1TX", "family": "UK100", "risk": 0.13, "score": 0.51, "cooldown": 75},
    {"name": "FRA40_TITANYX", "func": strategy_fra40_titanyx, "tag": "F4TX", "family": "FRA40", "risk": 0.13, "score": 0.50, "cooldown": 90},
    {"name": "US30_TITANYX", "func": strategy_us30_titanyx, "tag": "U3TX", "family": "US30", "risk": 0.14, "score": 0.50, "cooldown": 90},
]

for spec in TITANYX_REGISTRY:
    register_strategy(
        spec["name"],
        spec["func"],
        tag=spec["tag"],
        symbol_family=spec["family"],
        execution_mode="STD_ONLY",
        risk_multiplier=spec["risk"],
        min_score=spec["score"],
        cooldown_sec=spec["cooldown"],
        allowed_regimes={"RANGE", "TREND"},
        filter_func=titanyx_filter,
        market_filter_func=fast_market_filter,
        sl_atr_multiplier=1.0,
        tp_rr_multiplier=1.0,
        auto_disable=False,
        be_trigger_progress=0.08,
        be_buffer_progress=0.04,
        trail_steps=[],
        session_label="TITANYX",
        exclusive_symbol=True,
    )
    cfg = get_strategy_config(spec["name"])
    if cfg:
        profile = TITANYX_PROFILES[spec["family"]]
        cfg["suite"] = "TITANYX"
        cfg["strategy_id"] = f"TITANYX_{spec['family']}"
        cfg["priority_rank"] = profile["priority_rank"]
        cfg["allow_scale_in_cluster"] = True
        cfg["max_basket_orders"] = int(profile["max_orders"])
        cfg["titanyx_spread_atr"] = 0.18
        cfg["selection_priority"] = _strategy_selection_priority(cfg)


_DORMANT_STRESS_RELAX_OVERRIDES = {
    "GBRT": {"min_score": 0.38, "cooldown_sec": 30},
    "G4CL": {"min_score": 0.42, "cooldown_sec": 90},
    "G4PU": {"min_score": 0.39, "cooldown_sec": 150},
    "G4RV": {"min_score": 0.45, "cooldown_sec": 105},
    "G4SG": {"min_score": 0.39, "cooldown_sec": 120},
    "G4SC": {"min_score": 0.56, "cooldown_sec": 42},
    "G4SD": {"min_score": 0.39, "cooldown_sec": 150},
    "G4TR": {"min_score": 0.46, "cooldown_sec": 48},
    "U5CL": {"min_score": 0.40, "cooldown_sec": 90},
    "U5DM": {"min_score": 0.28, "cooldown_sec": 600},
    "U5RV": {"min_score": 0.42, "cooldown_sec": 96},
    "U5SG": {"min_score": 0.30, "cooldown_sec": 84},
    "U5TX": {"min_score": 0.47, "cooldown_sec": 54},
    "U5TR": {"min_score": 0.42, "cooldown_sec": 42},
    "UBRT": {"min_score": 0.38, "cooldown_sec": 30},
    "U1CL": {"min_score": 0.40, "cooldown_sec": 90},
    "U1HT": {"min_score": 0.33, "cooldown_sec": 600},
    "U1KB": {"min_score": 0.31, "cooldown_sec": 105},
    "U1PU": {"min_score": 0.29, "cooldown_sec": 84},
    "U1RV": {"min_score": 0.42, "cooldown_sec": 96},
    "U1SG": {"min_score": 0.30, "cooldown_sec": 84},
    "U1SD": {"min_score": 0.29, "cooldown_sec": 84},
    "USTR": {"min_score": 0.42, "cooldown_sec": 42},
    "XBRT": {"min_score": 0.46, "cooldown_sec": 48},
    "XLUK": {"min_score": 0.44, "cooldown_sec": 28},
    "XTRD": {"min_score": 0.50, "cooldown_sec": 150},
    "XVRC": {"min_score": 0.43, "cooldown_sec": 28},
}


def _apply_dormant_stress_relaxation():
    for cfg in STRATEGIES:
        override = _DORMANT_STRESS_RELAX_OVERRIDES.get(cfg.get("tag"))
        if not override:
            continue
        cfg["min_score"] = float(override["min_score"])
        cfg["cooldown_sec"] = int(override["cooldown_sec"])
        cfg["selection_priority"] = _strategy_selection_priority(cfg)


_apply_dormant_stress_relaxation()


def compute_asset_score(symbol, df, sig, conf, strategy_cfg=None):
    if sig == "NEUTRAL":
        return 0
    atr = df["atr"].iloc[-1]
    close_price = df["close"].iloc[-1]
    momentum = abs(df["close"].iloc[-1] - df["close"].iloc[-5])
    vol_score = min(1.0, (atr / close_price) * 100)
    mom_score = min(1.0, momentum / max(atr, 1e-6))
    base_score = conf / 100.0
    if strategy_cfg and str(strategy_cfg.get("name", "")).endswith("_TREND"):
        trend_score = min(1.0, compute_trend_strength(df) / 1.5)
        raw_score = (base_score * 0.45) + (vol_score * 0.10) + (mom_score * 0.15) + (trend_score * 0.30)
    else:
        raw_score = (base_score + vol_score + mom_score) / 3.0
    entry_push_bonus = 0.05 if str(CURRENT_OPTIMIZER_PROFILE).upper() == "ATTACK" else 0.03
    return round(min(1.0, raw_score + entry_push_bonus), 3)


# ==============================================================================
# EXECUTION LAYER PRO (MULTI-TP)
# ==============================================================================
def open_scaled_trade(symbol, signal, df, strat_name, score=1.0, confidence=None, strategy_cfg=None, execution_df=None, signal_payload=None):
    acc, info, tick = mt5.account_info(), safe_info(symbol), safe_tick(symbol)
    if not acc or not info or not tick or info.trade_tick_size <= 0:
        set_live_log(symbol, "❌ EXEC CANCEL: Missing Info")
        return False, "MISSING INFO"

    strategy_cfg = strategy_cfg or {}
    strategy_tag = strategy_cfg.get("tag", strat_name[:8])
    execution_mode = strategy_cfg.get("execution_mode", "AUTO")
    risk_multiplier = strategy_cfg.get("risk_multiplier", 1.0)
    sl_atr_multiplier = strategy_cfg.get("sl_atr_multiplier", 1.2)
    tp_rr_multiplier = strategy_cfg.get("tp_rr_multiplier", 2.0)
    signal_payload = signal_payload or {}
    risk_multiplier = float(signal_payload.get("risk_multiplier_override", risk_multiplier) or risk_multiplier)
    risk_percent_override = signal_payload.get("risk_percent_override")
    allow_scale_in_cluster = bool(strategy_cfg.get("allow_scale_in_cluster"))
    max_basket_orders = int(strategy_cfg.get("max_basket_orders", 1) or 1)

    live_pos = safe_positions()
    open_clusters = {
        extract_strategy_tag(getattr(p, "comment", ""))
        for p in live_pos
        if p.symbol == symbol and p.magic == MAGIC_ID
    }
    open_clusters.discard(None)
    existing_cluster_positions = get_strategy_cluster_positions(symbol, strategy_tag) if allow_scale_in_cluster else []
    if strategy_tag in open_clusters:
        if not allow_scale_in_cluster:
            set_live_log(symbol, f"⚠️ EXEC CANCEL: {strategy_tag} already active")
            return False, "ALREADY ACTIVE"
        if len(existing_cluster_positions) >= max_basket_orders:
            set_live_log(symbol, f"⚠️ EXEC CANCEL: {strategy_tag} max basket orders")
            return False, "MAX BASKET"
        existing_side = "BUY" if existing_cluster_positions and existing_cluster_positions[0].type == 0 else "SELL"
        if existing_cluster_positions and existing_side != signal:
            set_live_log(symbol, f"⚠️ EXEC CANCEL: {strategy_tag} opposite basket active")
            return False, "OPPOSITE BASKET"
    can_share_symbol, share_reason = _can_share_symbol_slot(symbol, strategy_cfg, signal, live_pos)
    if not (allow_scale_in_cluster and existing_cluster_positions) and not can_share_symbol:
        set_live_log(symbol, f"⚠️ EXEC CANCEL: {strategy_tag} {share_reason}")
        return False, share_reason
    symbol_cluster_cap = get_max_clusters_for_symbol(symbol)
    if len(open_clusters) >= symbol_cluster_cap:
        set_live_log(symbol, "⚠️ EXEC CANCEL: Max strategy clusters reached")
        return False, "CLUSTER LIMIT"

    price = tick.ask if signal == "BUY" else tick.bid
    working_df = execution_df if execution_df is not None else df
    atr = working_df["atr"].iloc[-1]
    custom_sl_price = signal_payload.get("sl_price")
    custom_tp_price = signal_payload.get("tp_price")
    if custom_sl_price is not None:
        custom_sl_price = float(custom_sl_price)
        sl_dist = abs(price - custom_sl_price)
    else:
        sl_dist = atr * sl_atr_multiplier
    sl_dist = max(sl_dist, max(info.point * 20, atr * 0.35))

    if risk_percent_override is not None:
        risk_money = acc.equity * (float(risk_percent_override) / 100.0)
    else:
        risk_money = acc.equity * compute_dynamic_risk() * risk_multiplier
    projected_risk = get_total_open_risk() + (risk_money / max(acc.equity, 1e-6))
    if projected_risk > MAX_TOTAL_RISK:
        set_live_log(symbol, f"🚫 {strategy_tag}: MAX TOTAL RISK")
        return False, "RISK LIMIT"

    total_lot = risk_money / ((sl_dist / info.trade_tick_size) * info.trade_tick_value)
    total_lot = round(max(info.volume_min, min(total_lot, info.volume_max * 0.3)) / info.volume_step) * info.volume_step

    if total_lot <= 0 or np.isnan(total_lot):
        return False, "LOT INVALID"

    if execution_mode == "MULTI_ONLY":
        if not USE_MULTI_TP:
            set_live_log(symbol, f"🚫 {strategy_tag}: MULTI-TP DISABLED")
            return False, "MULTI-TP DISABLED"
        use_multi = True
    elif execution_mode == "STD_ONLY":
        use_multi = False
    else:
        use_multi = USE_MULTI_TP and should_use_multi_tp(df, symbol)

    if not use_multi:
        broker_tp_disabled = bool(signal_payload.get("broker_tp_disabled"))
        tp = 0.0 if broker_tp_disabled else (float(custom_tp_price) if custom_tp_price is not None else (price + sl_dist * tp_rr_multiplier if signal == "BUY" else price - sl_dist * tp_rr_multiplier))
        sl = custom_sl_price if custom_sl_price is not None else (price - sl_dist if signal == "BUY" else price + sl_dist)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(total_lot),
            "type": mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "sl": round(sl, info.digits),
            "tp": round(tp, info.digits),
            "magic": MAGIC_ID,
            "comment": f"{strategy_tag}_STD",
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        set_live_log(symbol, f"📤 SEND STD ORDER | Lot: {total_lot}")
        with mt5_lock:
            res = mt5.order_send(request)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            cluster_key = (symbol, strategy_tag)
            cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
            cluster_state.update(
                {
                    "opened_at": datetime.now(),
                    "signal": signal,
                    "score": round(float(score or 0.0), 3),
                    "confidence": round(float(confidence if confidence is not None else score * 100.0), 1),
                    "signal_meta": signal_payload,
                    "tp_splits": max(1, int(cluster_state.get("tp_splits", 0) or 1)),
                    "tp_prices": [round(float(tp), info.digits)] if tp else list(cluster_state.get("tp_prices", []) or []),
                    "probability_below_count": 0,
                    "probability_cached": {
                        "mode": _infer_probability_mode(strategy_cfg),
                        "profile": _infer_probability_profile(strategy_cfg),
                        "probability": 50.0,
                        "threshold": PROBABILITY_MIN_THRESHOLD,
                        "hard_threshold": PROBABILITY_HARD_THRESHOLD,
                        "reason": "INIT",
                        "should_exit": False,
                        "below_count": 0,
                    },
                    "probability_last_eval_at": None,
                }
            )
            if strategy_cfg.get("suite") == "TITANYX":
                cluster_state.update(
                    {
                        "opened_at": datetime.now(),
                        "signal": signal,
                        "suite": "TITANYX",
                        "titanyx_last_add_price": float(price),
                        "titanyx_last_add_time": datetime.now(),
                        "titanyx_last_volatility": signal_payload.get("signal_details", {}).get("volatility_state", "---"),
                        "titanyx_last_equity": signal_payload.get("signal_details", {}).get("equity_protection", "---"),
                    }
                )
            last_trade_time[symbol] = time.time()
            last_signal[symbol] = signal
            return True, "EXECUTED"
        else:
            set_live_log(symbol, f"❌ ORDER FAIL: {res.retcode if res else 'NULL'}")
            return False, f"ORDER FAIL {res.retcode if res else 'NULL'}"

    confidence_value = confidence if confidence is not None else score * 100.0
    plan_cfg, splits, plan_text = build_dynamic_multi_tp_plan(
        strategy_cfg,
        score,
        confidence_value,
        working_df,
        info,
        abs(tick.ask - tick.bid),
    )
    forced_splits = signal_payload.get("force_splits")
    if forced_splits is not None:
        plan_cfg["fixed_tp_splits"] = max(1, min(MAX_SPLITS, int(forced_splits)))
        splits = int(plan_cfg["fixed_tp_splits"])
    if signal_payload.get("tp_point_targets"):
        plan_cfg["fixed_tp_points"] = list(signal_payload.get("tp_point_targets") or [])
    if signal_payload.get("tp_point_buffer") is not None:
        plan_cfg["tp_point_buffer"] = float(signal_payload.get("tp_point_buffer"))
    if signal_payload.get("point_value_price") is not None:
        plan_cfg["point_value_price"] = float(signal_payload.get("point_value_price"))
    if signal_payload.get("lot_weights_override"):
        plan_cfg["lot_weights_override"] = list(signal_payload.get("lot_weights_override") or [])
    strategy_cfg = plan_cfg
    record_strategy_tp_plan(strat_name, plan_text)
    lot_plan = build_dynamic_multi_tp_lot_plan(
        total_lot,
        info,
        strategy_cfg,
        splits,
        float(strategy_cfg.get("effective_tp_quality", 0.5) or 0.5),
    )
    if not lot_plan:
        set_live_log(symbol, "❌ MULTI-TP CANCEL: Lot too small")
        return False, "LOT TOO SMALL"
    splits = len(lot_plan)
    lot_per_trade = round((sum(lot_plan) / max(splits, 1)) / info.volume_step) * info.volume_step
    lot_per_trade = max(info.volume_min, lot_per_trade)

    success_count = 0
    successful_tp_prices = []
    successful_projected_profits = []
    successful_lots = []
    spread_price = abs(tick.ask - tick.bid)
    tp_prices, projected_profits = build_dynamic_multi_tp_prices(
        symbol, signal, price, info, lot_per_trade, atr, spread_price, strategy_cfg, splits
    )
    set_live_log(symbol, f"📤 SEND MULTI ORDER | {splits} legs | avg {lot_per_trade} | TP1~{projected_profits[0]}€")

    for i in range(splits):
        tp_price = tp_prices[i]
        leg_lot = float(lot_plan[i])
        projected_profit = round(price_distance_to_profit(info, leg_lot, abs(tp_price - price)), 2)
        sl_price = custom_sl_price if custom_sl_price is not None else (price - sl_dist if signal == "BUY" else price + sl_dist)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": leg_lot,
            "type": mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "sl": round(sl_price, info.digits),
            "tp": tp_price,
            "magic": MAGIC_ID,
            "comment": f"{strategy_tag}_TP{i+1}",
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        with mt5_lock:
            res = mt5.order_send(request)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            success_count += 1
            successful_tp_prices.append(float(tp_price))
            successful_projected_profits.append(projected_profit)
            successful_lots.append(leg_lot)
        time.sleep(0.05)

    if success_count > 0:
        cluster_key = (symbol, strategy_tag)
        cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
        cluster_profile = get_cluster_profile(strategy_cfg, cluster_state)
        effective_quality = float(strategy_cfg.get("effective_tp_quality", 0.5) or 0.5)
        tp_high_conf = bool(effective_quality >= 0.62 or confidence_value >= 80.0)
        tp_be_after_legs_raw = signal_payload.get("tp_be_after_legs_override")
        tp_lock_offset_raw = signal_payload.get("tp_lock_offset_override")
        tp_be_after_legs = int(tp_be_after_legs_raw if tp_be_after_legs_raw is not None else (2 if tp_high_conf else 1))
        tp_lock_offset = int(tp_lock_offset_raw if tp_lock_offset_raw is not None else (2 if tp_high_conf else 1))
        early_target = sum(successful_projected_profits[: min(2, len(successful_projected_profits))]) if successful_projected_profits else 0.0
        weak_floor = max(cluster_profile["weak_profit_floor"], early_target * clamp(0.52 + (0.5 - cluster_profile["quality"]) * 0.30, 0.34, 0.72))
        cluster_state.update(
            {
                "opened_at": datetime.now(),
                "signal": signal,
                "score": round(score, 3),
                "confidence": round(confidence_value, 1),
                "signal_meta": signal_payload,
                "tp_quality": round(strategy_cfg.get("effective_tp_quality", 0.5), 3),
                "be_trigger_progress": strategy_cfg.get("effective_be_trigger", cluster_profile["be_trigger_progress"]),
                "be_buffer_progress": strategy_cfg.get("effective_be_buffer_progress", cluster_profile["be_buffer_progress"]),
                "cluster_lock_bias": cluster_profile["cluster_lock_bias"],
                "protect_bias": cluster_profile["protect_bias"],
                "early_cashout_ratio": cluster_profile["early_cashout_ratio"],
                "orb_cashout_ratio": cluster_profile["orb_cashout_ratio"],
                "hard_lock_ratio": cluster_profile["hard_lock_ratio"],
                "cascade_ratio": cluster_profile["cascade_ratio"],
                "weak_profit_floor": round(weak_floor, 2),
                "tp_splits": success_count,
                "tp_prices": [round(tp, info.digits) for tp in successful_tp_prices],
                "tp_projected_profits": [round(x, 2) for x in successful_projected_profits],
                "tp_lot_plan": [round(x, 4) for x in successful_lots],
                "tp_high_conf": tp_high_conf,
                "tp_be_after_legs": tp_be_after_legs,
                "tp_lock_offset": tp_lock_offset,
                "tp3_bridge_ratio": cluster_profile["tp3_bridge_ratio"],
                "runner_peak_keep_ratio": cluster_profile["runner_peak_keep_ratio"],
                "runner_realized_lock_ratio": cluster_profile["runner_realized_lock_ratio"],
                "tp1_projected": successful_projected_profits[0] if successful_projected_profits else 0.0,
                "tp2_projected": successful_projected_profits[1] if len(successful_projected_profits) > 1 else (successful_projected_profits[0] if successful_projected_profits else 0.0),
                "peak_open_profit": 0.0,
                "probability_below_count": 0,
                "probability_cached": {
                    "mode": _infer_probability_mode(strategy_cfg),
                    "profile": _infer_probability_profile(strategy_cfg),
                    "probability": 50.0,
                    "threshold": PROBABILITY_MIN_THRESHOLD,
                    "hard_threshold": PROBABILITY_HARD_THRESHOLD,
                    "reason": "INIT",
                    "should_exit": False,
                    "below_count": 0,
                },
                "probability_last_eval_at": None,
                "abort_cluster": False,
                "abort_reason": "",
            }
        )
        if strategy_cfg.get("suite") == "LUKE":
            luke_details = signal_payload.get("signal_details", {}) or {}
            cluster_state.update(
                {
                    "luke_setup": luke_details.get("setup", "CONTINUATION"),
                    "luke_reentry": bool(luke_details.get("reentry")),
                    "luke_zone_low": luke_details.get("zone_low"),
                    "luke_zone_high": luke_details.get("zone_high"),
                    "luke_sl_price": round(float(custom_sl_price if custom_sl_price is not None else sl_price), info.digits),
                    "luke_entry_price": round(float(price), info.digits),
                    "luke_reentry_tp_points": list(signal_payload.get("tp_point_targets") or []),
                    "luke_reentry_after_legs": int(tp_be_after_legs_raw if tp_be_after_legs_raw is not None else 2),
                }
            )
            luke_state = _luke_runtime(symbol)
            if bool(luke_details.get("reentry")):
                luke_state["used"] = True
            else:
                luke_state["used"] = False
        last_trade_time[symbol] = time.time()
        last_signal[symbol] = signal
        return True, "EXECUTED"

    set_live_log(symbol, f"❌ {strategy_tag}: MULTI ORDER FAIL")
    return False, "ORDER FAIL MULTI"


# ==============================================================================
# TICK MANAGEMENT (BE PROGRESSIVO)
# ==============================================================================
def tick_management():
    positions = safe_positions()
    cluster_live = {}
    cluster_positions = {}
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        comment_tag = extract_strategy_tag(getattr(p, "comment", ""))
        if comment_tag:
            position_strategy_map[p.ticket] = comment_tag
        strategy_tag = position_strategy_map.get(p.ticket) or comment_tag
        if not strategy_tag:
            continue
        cluster_key = (p.symbol, strategy_tag)
        meta = cluster_live.setdefault(cluster_key, {"open_profit": 0.0, "open_count": 0})
        meta["open_profit"] += p.profit
        meta["open_count"] += 1
        cluster_positions.setdefault(cluster_key, []).append(p)

    for cluster_key, live_positions in cluster_positions.items():
        cluster_state = hydrate_cluster_tp_state(cluster_key, live_positions)
        open_legs = sorted(
            idx for idx in (
                extract_leg_index(getattr(p, "comment", "") or "")
                for p in live_positions
            )
            if idx is not None
        )
        if open_legs:
            cluster_state["open_legs"] = open_legs
            stopped_leg_indexes = sorted(
                idx for idx, leg in cluster_state.get("legs", {}).items()
                if leg.get("reason") == "SL"
            )
            if stopped_leg_indexes:
                highest_stopped_leg = max(stopped_leg_indexes)
                if highest_stopped_leg >= 3 and any(idx > highest_stopped_leg for idx in open_legs):
                    cluster_state["abort_cluster"] = True
                    cluster_state["abort_reason"] = f"TP{highest_stopped_leg} runner stop cascade"

    cluster_probability_cache = {}
    for cluster_key, live_positions in cluster_positions.items():
        strategy_cfg = get_strategy_by_tag(cluster_key[1])
        if not strategy_cfg:
            continue
        cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
        cluster_probability_cache[cluster_key] = evaluate_cluster_probability(
            cluster_key[0],
            strategy_cfg,
            cluster_state,
            live_positions,
            cluster_live.get(cluster_key, {}),
        )

    cluster_abort_logged = set()
    probability_exit_logged = set()
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        strategy_tag = position_strategy_map.get(p.ticket) or extract_strategy_tag(getattr(p, "comment", ""))
        if not strategy_tag:
            continue
        cluster_state = trade_memory.get((p.symbol, strategy_tag), {})
        if cluster_state.get("abort_cluster"):
            closed = close_strategy_cluster(p.symbol, strategy_tag)
            if closed > 0 and (p.symbol, strategy_tag) not in cluster_abort_logged:
                strategy_cfg = get_strategy_by_tag(strategy_tag)
                strategy_name = strategy_cfg["name"] if strategy_cfg else strategy_tag
                abort_reason = cluster_state.get("abort_reason", "cluster invalidated")
                set_live_log(p.symbol, f"🛑 CLUSTER ABORT {strategy_tag} | {abort_reason}")
                record_strategy_event(strategy_name, "CLUSTER ABORT")
                if strategy_cfg and strategy_cfg["name"].endswith("_LUKE"):
                    luke_state = _luke_runtime(p.symbol)
                    luke_state["loss_streak"] = min(int(luke_state.get("loss_streak", 0) or 0) + 1, 5)
                    luke_state["eligible"] = False
                cluster_abort_logged.add((p.symbol, strategy_tag))
            continue

        positive_legs = cluster_state.get("positive_legs", 0)
        early_profit = cluster_state.get("early_profit", 0.0)
        cluster_meta = cluster_live.get((p.symbol, strategy_tag), {})
        strategy_cfg = get_strategy_by_tag(strategy_tag)
        cluster_profile = get_cluster_profile(strategy_cfg, cluster_state) if strategy_cfg else get_cluster_profile({}, cluster_state)
        is_dynamic_tp_cluster = bool(cluster_state.get("tp_prices")) or int(cluster_state.get("tp_splits", 0) or 0) > 1
        tp_rules = get_tp_management_rules(cluster_state, cluster_profile)
        guard_after_legs = tp_rules["be_after_legs"] if is_dynamic_tp_cluster else 1
        weak_floor = max(cluster_state.get("weak_profit_floor", 0.0), cluster_profile["weak_profit_floor"])
        if strategy_cfg and strategy_cfg["name"].endswith("_LUKE") and positive_legs >= 2 and not cluster_state.get("luke_reentry"):
            luke_state = _luke_runtime(p.symbol)
            if not luke_state.get("used"):
                luke_state.update(
                    {
                        "eligible": True,
                        "used": False,
                        "direction": cluster_state.get("signal"),
                        "zone_low": cluster_state.get("luke_zone_low"),
                        "zone_high": cluster_state.get("luke_zone_high"),
                        "sl_price": cluster_state.get("luke_sl_price"),
                        "expires_at": datetime.now() + timedelta(minutes=int(LUKE_PROFILES["XAUUSD"]["max_minutes_from_first_entry"])),
                        "tp_points": list(LUKE_PROFILES["XAUUSD"]["reentry_tp_points"]),
                        "tp_be_after_legs": int(LUKE_PROFILES["XAUUSD"]["reentry_be_after_tp"]),
                        "entry_time": cluster_state.get("opened_at"),
                        "source_setup": cluster_state.get("luke_setup", "CONTINUATION"),
                        "loss_streak": 0,
                    }
                )
        if positive_legs >= 2 and early_profit < weak_floor and cluster_meta.get("open_profit", 0.0) <= max(0.15, early_profit * cluster_profile["early_cashout_ratio"]):
            closed = close_strategy_cluster(p.symbol, strategy_tag)
            if closed > 0 and (p.symbol, strategy_tag) not in cluster_abort_logged:
                strategy_cfg = get_strategy_by_tag(strategy_tag)
                strategy_name = strategy_cfg["name"] if strategy_cfg else strategy_tag
                set_live_log(p.symbol, f"💸 CLUSTER CASHOUT {strategy_tag} | weak TP1/TP2")
                record_strategy_event(strategy_name, "WEAK EARLY CASHOUT")
                if strategy_cfg and strategy_cfg["name"].endswith("_LUKE"):
                    _luke_runtime(p.symbol)["loss_streak"] = 0 if positive_legs >= 2 else min(int(_luke_runtime(p.symbol).get("loss_streak", 0) or 0) + 1, 5)
                cluster_abort_logged.add((p.symbol, strategy_tag))
        if strategy_cfg and strategy_cfg["name"].endswith("_ORB"):
            orb_floor = max(weak_floor, strategy_cfg.get("multi_tp_eur_min", 0.50) * clamp(1.22 + (0.5 - cluster_profile["quality"]) * 0.30, 0.90, 1.40))
            if positive_legs >= guard_after_legs and early_profit < orb_floor and cluster_meta.get("open_profit", 0.0) <= max(0.22, early_profit * cluster_profile["orb_cashout_ratio"]):
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in cluster_abort_logged:
                    strategy_name = strategy_cfg["name"]
                    set_live_log(p.symbol, f"💸 ORB CASHOUT {strategy_tag} | early edge too weak")
                    record_strategy_event(strategy_name, "ORB EARLY CASHOUT")
                    cluster_abort_logged.add((p.symbol, strategy_tag))

    positions = safe_positions()
    sg_exit_logged = set()
    titanyx_managed = set()
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        comment_tag = extract_strategy_tag(getattr(p, "comment", ""))
        if comment_tag:
            position_strategy_map[p.ticket] = comment_tag

        info = safe_info(p.symbol)
        tick = safe_tick(p.symbol)
        if not info or not tick:
            continue

        price = tick.bid if p.type == 0 else tick.ask
        entry = p.price_open
        profit_points = (price - entry) / info.point if p.type == 0 else (entry - price) / info.point
        strategy_tag = position_strategy_map.get(p.ticket) or comment_tag
        strategy_cfg = get_strategy_by_tag(strategy_tag) if strategy_tag else None
        strategy_name = strategy_cfg["name"] if strategy_cfg else strategy_tag
        cluster_state = trade_memory.get((p.symbol, strategy_tag), {}) if strategy_tag else {}
        probability_eval = cluster_probability_cache.get((p.symbol, strategy_tag))
        if strategy_cfg and probability_eval:
            record_strategy_probability(
                strategy_name,
                p.symbol,
                probability_eval.get("probability", 0.0),
                probability_eval.get("reason", "---"),
                probability_eval.get("mode", "OFF"),
                probability_eval.get("threshold", PROBABILITY_MIN_THRESHOLD),
            )
        is_orb_cluster = bool(strategy_cfg and strategy_cfg["name"].endswith("_ORB"))
        if strategy_cfg and strategy_cfg["name"].endswith("_LUKE"):
            luke_reason = get_luke_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if luke_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    luke_state = _luke_runtime(p.symbol)
                    if cluster_state.get("positive_legs", 0) >= 2:
                        luke_state["loss_streak"] = 0
                    else:
                        luke_state["loss_streak"] = min(int(luke_state.get("loss_streak", 0) or 0) + 1, 5)
                    set_live_log(p.symbol, f"🪙 LUKE EXIT {strategy_tag} | {luke_reason}")
                    record_strategy_event(strategy_name, "LUKE EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_TITANYX"):
            cluster_key = (p.symbol, strategy_tag)
            if cluster_key in titanyx_managed:
                continue
            titanyx_managed.add(cluster_key)
            cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
            cluster_snapshot = _titanyx_cluster_snapshot(p.symbol, strategy_tag)
            titanyx_reason, titanyx_exit_kind, titanyx_sl, titanyx_tp = get_titanyx_exit_reason(p.symbol, strategy_cfg, cluster_state, cluster_snapshot)
            live_cluster_positions = cluster_positions.get(cluster_key, [])
            if cluster_snapshot and titanyx_sl is not None and titanyx_tp is not None:
                for basket_pos in live_cluster_positions:
                    info_basket = safe_info(basket_pos.symbol)
                    tick_basket = safe_tick(basket_pos.symbol)
                    if not info_basket or not tick_basket:
                        continue
                    price_basket = tick_basket.bid if basket_pos.type == 0 else tick_basket.ask
                    min_dist = max((info_basket.trade_stops_level + 5) * info_basket.point, info_basket.point * 20)
                    sl_ok = titanyx_sl < price_basket - min_dist if basket_pos.type == 0 else titanyx_sl > price_basket + min_dist
                    tp_ok = titanyx_tp > price_basket + min_dist if basket_pos.type == 0 else titanyx_tp < price_basket - min_dist
                    if sl_ok and tp_ok:
                        update_position_sltp_levels(basket_pos, info_basket, titanyx_sl, titanyx_tp)
            if titanyx_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    was_stop = titanyx_exit_kind == "STOP"
                    _titanyx_update_stop_streak(strategy_name, was_stop)
                    set_live_log(p.symbol, f"🧲 TITANYX EXIT {strategy_tag} | {titanyx_reason}")
                    record_strategy_event(strategy_name, f"TITANYX {titanyx_reason}")
                continue
            continue
        if strategy_cfg and (
            strategy_cfg["name"].endswith("_CONTINUATION_LIQUIDITY_PULLBACK")
            or strategy_cfg["name"].endswith("_REVERSAL_SWEEP")
        ):
            sniper_reason = get_liquidity_sniper_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if sniper_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"🎯 S&D EXIT {strategy_tag} | {sniper_reason}")
                    record_strategy_event(strategy_name, "S&D EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_HEIKEN_TDI"):
            htdi_reason = get_heiken_tdi_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if htdi_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"🟦 HEIKEN+TDI EXIT {strategy_tag} | {htdi_reason}")
                    record_strategy_event(strategy_name, "HEIKEN+TDI EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_DOUBLE_MACD"):
            dmacd_reason = get_double_macd_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if dmacd_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"📊 DOUBLE MACD EXIT {strategy_tag} | {dmacd_reason}")
                    record_strategy_event(strategy_name, "DOUBLE MACD EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_PURIA"):
            puria_reason = get_puria_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if puria_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"🧪 PURIA EXIT {strategy_tag} | {puria_reason}")
                    record_strategy_event(strategy_name, "PURIA EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_SIDUS"):
            sidus_reason = get_sidus_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if sidus_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"🧭 SIDUS EXIT {strategy_tag} | {sidus_reason}")
                    record_strategy_event(strategy_name, "SIDUS EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_KUMO_BREAKOUT"):
            kumo_reason = get_kumo_breakout_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if kumo_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"☁️ KUMO EXIT {strategy_tag} | {kumo_reason}")
                    record_strategy_event(strategy_name, "KUMO EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and strategy_cfg["name"].endswith("_SANTO_GRAAL"):
            sg_reason = get_santo_graal_exit_reason(p.symbol, strategy_cfg, cluster_state.get("signal", "BUY"))
            if sg_reason:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0 and (p.symbol, strategy_tag) not in sg_exit_logged:
                    set_live_log(p.symbol, f"🕊️ SANTO GRAAL EXIT {strategy_tag} | {sg_reason}")
                    record_strategy_event(strategy_name, "SANTO GRAAL EXIT")
                    sg_exit_logged.add((p.symbol, strategy_tag))
                continue
        if strategy_cfg and probability_eval and probability_eval.get("mode") == "LIVE":
            cluster_key = (p.symbol, strategy_tag)
            if probability_eval.get("should_exit") and cluster_key not in probability_exit_logged:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    reason = probability_eval.get("reason", "PROBABILITY COLLAPSE")
                    prob_value = round(float(probability_eval.get("probability", 0.0) or 0.0), 1)
                    set_live_log(p.symbol, f"🧠 PROB EXIT {strategy_tag} | {prob_value}% | {reason}")
                    record_strategy_event(strategy_name, f"PROB EXIT {int(round(prob_value))}%")
                    probability_exit_logged.add(cluster_key)
                continue

        if int(time.time()) % 10 == 0:
            prob_text = ""
            if probability_eval:
                prob_text = f" | Prob {round(float(probability_eval.get('probability', 0.0) or 0.0), 1)}%"
            set_live_log(p.symbol, f"📌 MANAGING | PnL: {round(p.profit,2)}€ ({round(profit_points,1)} pts){prob_text}")

        new_sl = p.sl
        target_points = abs(p.tp - entry) / info.point if p.tp else 0
        progress = (profit_points / target_points) if target_points > 0 else 0
        hit_label = f"{round(progress * 100)}%"
        cluster_state = trade_memory.get((p.symbol, strategy_tag), {})
        cluster_meta = cluster_live.get((p.symbol, strategy_tag), {})
        positive_legs = cluster_state.get("positive_legs", 0)
        stopped_legs = cluster_state.get("sl_legs", 0)

        if strategy_cfg and target_points > 0:
            cluster_profile = get_cluster_profile(strategy_cfg, cluster_state)
            is_dynamic_tp_cluster = bool(cluster_state.get("tp_prices")) or int(cluster_state.get("tp_splits", 0) or 0) > 1
            tp_rules = get_tp_management_rules(cluster_state, cluster_profile)
            guard_after_legs = tp_rules["be_after_legs"] if is_dynamic_tp_cluster else 1
            be_trigger = cluster_profile["be_trigger_progress"]
            buffer_points = max(info.trade_stops_level + 2, target_points * cluster_profile["be_buffer_progress"])
            ladder_sl = None
            ladder_label = None

            if is_dynamic_tp_cluster:
                ladder_sl, ladder_label = get_cluster_tp_ladder_stop(p.type, entry, buffer_points, info, cluster_state, positive_legs, tp_rules)
                if ladder_sl is not None:
                    if p.type == 0:
                        new_sl = max(new_sl, ladder_sl)
                    else:
                        new_sl = ladder_sl if new_sl == 0 else min(new_sl, ladder_sl)
                    hit_label = ladder_label

            if (not is_dynamic_tp_cluster) and progress >= be_trigger:
                be_sl = entry + buffer_points * info.point if p.type == 0 else entry - buffer_points * info.point
                new_sl = be_sl

            if (not is_dynamic_tp_cluster) and positive_legs >= 1:
                if positive_legs == 1:
                    cluster_lock = 0.48 if is_orb_cluster else 0.24
                elif positive_legs == 2:
                    cluster_lock = 0.70 if is_orb_cluster else 0.40
                elif positive_legs == 3:
                    cluster_lock = 0.82 if is_orb_cluster else 0.56
                else:
                    cluster_lock = 0.90 if is_orb_cluster else 0.72
                cluster_lock = clamp(cluster_lock + cluster_profile["cluster_lock_bias"], 0.12, 0.96)
                if stopped_legs >= 1:
                    cluster_lock = min(0.96, cluster_lock + (0.12 if is_orb_cluster else 0.10))
                cluster_points = max(buffer_points, target_points * cluster_lock)
                cluster_sl = entry + cluster_points * info.point if p.type == 0 else entry - cluster_points * info.point
                if p.type == 0:
                    new_sl = max(new_sl, cluster_sl)
                else:
                    new_sl = cluster_sl if new_sl == 0 else min(new_sl, cluster_sl)
                hit_label = f"C{positive_legs}"

            realized_profit = cluster_state.get("realized_profit", 0.0)
            peak_open_profit = max(float(cluster_state.get("peak_open_profit", 0.0) or 0.0), float(cluster_meta.get("open_profit", 0.0) or 0.0))
            cluster_state["peak_open_profit"] = round(peak_open_profit, 2)
            if positive_legs >= guard_after_legs and realized_profit > 0:
                tp1_guard_floor = 0.10 if symbol_in_family(p.symbol, "XAUUSD") else 0.14
                tp1_guard_limit = max(tp1_guard_floor, realized_profit * cluster_profile["tp1_guard_ratio"])
                if cluster_meta.get("open_profit", 0.0) <= -tp1_guard_limit:
                    closed = close_strategy_cluster(p.symbol, strategy_tag)
                    if closed > 0:
                        set_live_log(p.symbol, f"🛡️ TP1 GUARD EXIT {strategy_tag} | protect first take")
                        record_strategy_event(strategy_name, "TP1 GUARD EXIT")
                    continue

            if positive_legs <= 1:
                protect_ratio = 0.42 if is_orb_cluster else 0.24
            elif positive_legs == 2:
                protect_ratio = 0.68 if is_orb_cluster else 0.42
            elif positive_legs == 3:
                protect_ratio = 0.80 if is_orb_cluster else 0.60
            else:
                protect_ratio = 0.90 if is_orb_cluster else 0.74
            protect_ratio = clamp(protect_ratio + cluster_profile["protect_bias"], 0.10, 0.95)
            if stopped_legs >= 1:
                protect_ratio = max(protect_ratio, 0.84 if is_orb_cluster else (0.60 if positive_legs <= 2 else 0.72))
            min_cluster_open = max(0.28 if is_orb_cluster else 0.22, realized_profit * protect_ratio)
            if positive_legs >= 2 and cluster_meta.get("open_profit", 0.0) <= min_cluster_open and realized_profit > 0:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    set_live_log(p.symbol, f"💼 CLUSTER EXIT {strategy_tag} | protect realized")
                    record_strategy_event(strategy_name, "CLUSTER PROFIT LOCK")
                continue
            if positive_legs >= 3 and realized_profit > 0 and peak_open_profit > 0:
                runner_open_floor = max(0.18 if symbol_in_family(p.symbol, "XAUUSD") else 0.14, peak_open_profit * cluster_profile["runner_peak_keep_ratio"])
                runner_realized_floor = realized_profit * cluster_profile["runner_realized_lock_ratio"]
                runner_floor = max(runner_open_floor, runner_realized_floor)
                if cluster_meta.get("open_profit", 0.0) <= runner_floor:
                    closed = close_strategy_cluster(p.symbol, strategy_tag)
                    if closed > 0:
                        set_live_log(p.symbol, f"🎯 TP3 RUNNER LOCK {strategy_tag} | keep extension")
                        record_strategy_event(strategy_name, "TP3 RUNNER LOCK")
                    continue
            if is_orb_cluster and positive_legs >= guard_after_legs and realized_profit > 0 and cluster_meta.get("open_profit", 0.0) <= max(0.18, realized_profit * cluster_profile["hard_lock_ratio"]):
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    set_live_log(p.symbol, f"🧲 ORB HARD LOCK {strategy_tag} | keep realized")
                    record_strategy_event(strategy_name, "ORB HARD LOCK")
                continue
            if is_orb_cluster and positive_legs >= guard_after_legs and stopped_legs >= 1 and realized_profit > 0 and cluster_meta.get("open_profit", 0.0) <= max(0.16, realized_profit * cluster_profile["cascade_ratio"]):
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    set_live_log(p.symbol, f"🧷 ORB STOP CASCADE {strategy_tag} | lock cluster")
                    record_strategy_event(strategy_name, "ORB STOP CASCADE")
                continue
            if positive_legs >= guard_after_legs and stopped_legs >= 1 and realized_profit > 0 and cluster_meta.get("open_profit", 0.0) <= 0:
                closed = close_strategy_cluster(p.symbol, strategy_tag)
                if closed > 0:
                    set_live_log(p.symbol, f"🧯 CLUSTER FLAT EXIT {strategy_tag} | open profit gone")
                    record_strategy_event(strategy_name, "CLUSTER FLAT EXIT")
                continue

            allow_trailing = (not is_dynamic_tp_cluster) or positive_legs >= guard_after_legs
            for trigger_progress, lock_progress in strategy_cfg.get("trail_steps", []):
                if allow_trailing and progress >= trigger_progress:
                    lock_points = max(buffer_points, target_points * lock_progress)
                    step_sl = entry + lock_points * info.point if p.type == 0 else entry - lock_points * info.point
                    if p.type == 0:
                        new_sl = max(new_sl, step_sl)
                    else:
                        new_sl = step_sl if new_sl == 0 else min(new_sl, step_sl)
        else:
            hit_level = 0
            for i, tp in enumerate(TP_POINTS):
                if profit_points >= tp:
                    hit_level = i + 1

            hit_label = f"L{hit_level}"
            if hit_level >= 2:
                buffer = points_to_price(p.symbol, SPREAD_BUFFER_POINTS)
                new_sl = entry + buffer if p.type == 0 else entry - buffer

            if hit_level >= 3:
                new_sl = entry + points_to_price(p.symbol, TP_POINTS[0]) if p.type == 0 else entry - points_to_price(p.symbol, TP_POINTS[0])
            if hit_level >= 4:
                new_sl = entry + points_to_price(p.symbol, TP_POINTS[1]) if p.type == 0 else entry - points_to_price(p.symbol, TP_POINTS[1])
            if hit_level >= 5:
                new_sl = entry + points_to_price(p.symbol, TP_POINTS[2]) if p.type == 0 else entry - points_to_price(p.symbol, TP_POINTS[2])
            if hit_level >= 6:
                new_sl = entry + points_to_price(p.symbol, TP_POINTS[3]) if p.type == 0 else entry - points_to_price(p.symbol, TP_POINTS[3])
            if hit_level >= 7:
                new_sl = entry + points_to_price(p.symbol, TP_POINTS[4]) if p.type == 0 else entry - points_to_price(p.symbol, TP_POINTS[4])

        min_dist = max((info.trade_stops_level + 5) * info.point, info.point * 20)

        if p.type == 0:
            if new_sl > p.sl and new_sl < (price - min_dist):
                updated, retcode = update_position_sltp(p, info, new_sl)
                if updated:
                    set_live_log(p.symbol, f"🔒 SL UPDATED {strategy_tag or ''} {hit_label}".strip())
                elif is_dynamic_tp_cluster and positive_legs >= max(3, guard_after_legs + 1):
                    closed = close_strategy_cluster(p.symbol, strategy_tag)
                    if closed > 0:
                        set_live_log(p.symbol, f"🧷 RUNNER SYNC EXIT {strategy_tag} | SL sync failed rc={retcode}")
                        record_strategy_event(strategy_name, "RUNNER SYNC EXIT")
                    else:
                        set_live_log(p.symbol, f"⚠️ SL UPDATE FAIL {strategy_tag} rc={retcode}")

        elif p.type == 1:
            if (p.sl == 0 or new_sl < p.sl) and new_sl > (price + min_dist):
                updated, retcode = update_position_sltp(p, info, new_sl)
                if updated:
                    set_live_log(p.symbol, f"🔒 SL UPDATED {strategy_tag or ''} {hit_label}".strip())
                elif is_dynamic_tp_cluster and positive_legs >= max(3, guard_after_legs + 1):
                    closed = close_strategy_cluster(p.symbol, strategy_tag)
                    if closed > 0:
                        set_live_log(p.symbol, f"🧷 RUNNER SYNC EXIT {strategy_tag} | SL sync failed rc={retcode}")
                        record_strategy_event(strategy_name, "RUNNER SYNC EXIT")
                    else:
                        set_live_log(p.symbol, f"⚠️ SL UPDATE FAIL {strategy_tag} rc={retcode}")


# ==============================================================================
# MAIN ENGINE
# ==============================================================================
def learn_from_history():
    global last_history_check, session_stats, performance_memory, legacy_stats, liquidity_sniper_day_state
    global session_trade_ledger, portfolio_snapshot, strategy_matrix_snapshot, session_ignored_position_ids, session_baseline_ready
    if not session_active:
        with history_state_lock:
            _rebuild_visual_snapshots(acc=mt5.account_info(), live_pnl=0.0)
        return
    if not session_baseline_ready:
        if not _seed_session_baseline():
            with history_state_lock:
                _rebuild_visual_snapshots(acc=mt5.account_info(), live_pnl=0.0)
            return
    with history_state_lock:
        now = datetime.now()
        broker_offset = get_live_broker_offset()
        broker_now = now + broker_offset
        seed_start = now - timedelta(days=5)
        seed_deals = mt5.history_deals_get(seed_start, broker_now) or []

        fresh_position_map = {}
        fresh_position_comment_map = {}
        for d in seed_deals:
            if d.magic != MAGIC_ID:
                continue
            position_id = getattr(d, "position_id", None) or getattr(d, "position", None)
            if d.entry == mt5.DEAL_ENTRY_IN and d.comment and position_id:
                strategy_tag = extract_strategy_tag(d.comment)
                if not strategy_tag:
                    continue
                fresh_position_map[position_id] = strategy_tag
                fresh_position_comment_map[position_id] = d.comment

        deals = [
            d
            for d in seed_deals
            if d.magic == MAGIC_ID and _is_realized_exit_deal(d)
        ]
        deals = sorted(deals, key=lambda d: (getattr(d, "time", 0), _deal_ticket(d) or 0))
        liquidity_sniper_day_state = _build_liquidity_sniper_day_state(deals, fresh_position_map)

    def apply_closed_deal(d, session_acc, perf_acc, strategy_acc, legacy_acc, trade_acc, latest_acc, ledger_acc):
        realized_pnl = get_deal_realized_pnl(d)
        if realized_pnl > 0:
            session_acc["trades"] += 1
            session_acc["wins"] += 1
            perf_acc["wins"] += 1
        elif realized_pnl < 0:
            session_acc["trades"] += 1
            session_acc["losses"] += 1
            perf_acc["losses"] += 1
        else:
            session_acc["flat"] += 1
        session_acc["profit"] += realized_pnl

        position_id = getattr(d, "position_id", None) or getattr(d, "position", None)
        strategy_tag = fresh_position_map.get(position_id) or extract_strategy_tag(getattr(d, "comment", ""))
        deal_time = datetime.fromtimestamp(getattr(d, "time", 0)) if getattr(d, "time", 0) else now

        if strategy_tag:
            strategy_cfg = get_strategy_by_tag(strategy_tag)
            strategy_family_ok = bool(strategy_cfg and symbol_in_family(getattr(d, "symbol", ""), strategy_cfg.get("symbol_family")))
            leg_comment = fresh_position_comment_map.get(position_id, "")
            exit_comment = str(getattr(d, "comment", "") or "").upper()
            if "[TP" in exit_comment:
                exit_reason = "TP"
            elif "[SL" in exit_comment:
                exit_reason = "SL"
            else:
                exit_reason = "OTHER"
            ledger_acc.append(
                _build_trade_ledger_entry(
                    d,
                    strategy_cfg,
                    strategy_tag,
                    realized_pnl,
                    deal_time,
                    leg_comment,
                    exit_reason,
                    strategy_family_ok,
                )
            )
            if strategy_cfg and strategy_family_ok:
                stats = strategy_acc[strategy_cfg["name"]]
                if realized_pnl > 0:
                    stats["wins"] += 1
                    stats["gross_profit"] += realized_pnl
                    stats["closed"] += 1
                elif realized_pnl < 0:
                    stats["losses"] += 1
                    stats["gross_loss"] += realized_pnl
                    stats["closed"] += 1
                else:
                    stats["flat"] += 1
                stats["net_profit"] += realized_pnl
                latest_acc[strategy_cfg["name"]] = (realized_pnl, deal_time)
            else:
                if realized_pnl > 0:
                    legacy_acc["wins"] += 1
                    legacy_acc["gross_profit"] += realized_pnl
                    legacy_acc["closed"] += 1
                elif realized_pnl < 0:
                    legacy_acc["losses"] += 1
                    legacy_acc["gross_loss"] += realized_pnl
                    legacy_acc["closed"] += 1
                else:
                    legacy_acc["flat"] += 1
                legacy_acc["net_profit"] += realized_pnl

            if strategy_cfg and strategy_family_ok:
                cluster_key = (d.symbol, strategy_tag)
                cluster_state = trade_acc.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
                leg_index = extract_leg_index(leg_comment)

                if leg_index is not None:
                    cluster_state["legs"][leg_index] = {
                        "reason": exit_reason,
                        "profit": realized_pnl,
                        "ticket": position_id,
                    }
                    tp1 = cluster_state["legs"].get(1)
                    tp2 = cluster_state["legs"].get(2)
                    positive_legs = sorted(idx for idx, leg in cluster_state["legs"].items() if leg.get("profit", 0.0) > 0)
                    early_profit = sum(cluster_state["legs"][idx]["profit"] for idx in (1, 2) if idx in cluster_state["legs"])
                    cluster_state["positive_legs"] = len(positive_legs)
                    cluster_state["realized_profit"] = round(sum(leg.get("profit", 0.0) for leg in cluster_state["legs"].values()), 2)
                    weak_profit_floor = max(0.35, strategy_cfg.get("multi_tp_eur_min", 0.50) * 0.85)
                    cluster_state["early_profit"] = round(early_profit, 2)
                    cluster_state["weak_profit_floor"] = round(weak_profit_floor, 2)
                    sl_legs = sorted(idx for idx, leg in cluster_state["legs"].items() if leg.get("reason") == "SL")
                    cluster_state["sl_legs"] = len(sl_legs)
                    cluster_state["sl_profit"] = round(sum(cluster_state["legs"][idx].get("profit", 0.0) for idx in sl_legs), 2)
                    if tp1 and tp1["reason"] == "SL":
                        cluster_state["abort_cluster"] = True
                        cluster_state["abort_reason"] = "TP1 stopped"
                    elif tp1 and tp2 and tp1["reason"] != "TP" and tp2["reason"] != "TP":
                        cluster_state["abort_cluster"] = True
                        cluster_state["abort_reason"] = "TP1/TP2 rejected"
                    elif tp1 and tp2 and early_profit < weak_profit_floor:
                        cluster_state["abort_cluster"] = True
                        cluster_state["abort_reason"] = "TP1/TP2 too weak"

                    sorted_legs = sorted(cluster_state["legs"])
                    for idx in range(len(sorted_legs) - 1):
                        current_leg = sorted_legs[idx]
                        next_leg = sorted_legs[idx + 1]
                        if next_leg != current_leg + 1:
                            continue
                        current_reason = cluster_state["legs"][current_leg].get("reason")
                        next_reason = cluster_state["legs"][next_leg].get("reason")
                        if current_reason == "SL" and next_reason == "SL":
                            cluster_state["abort_cluster"] = True
                            cluster_state["abort_reason"] = f"TP{current_leg}/TP{next_leg} stopped"
                            break
                    if not cluster_state.get("abort_cluster") and positive_legs and len(sl_legs) >= 2:
                        cluster_state["abort_cluster"] = True
                        cluster_state["abort_reason"] = f"{len(sl_legs)} TP legs stopped"
        else:
            legacy_acc["wins"] += 1 if realized_pnl > 0 else 0
            legacy_acc["losses"] += 1 if realized_pnl < 0 else 0
            legacy_acc["flat"] += 1 if realized_pnl == 0 else 0
            legacy_acc["gross_profit"] += realized_pnl if realized_pnl > 0 else 0.0
            legacy_acc["gross_loss"] += realized_pnl if realized_pnl < 0 else 0.0
            legacy_acc["net_profit"] += realized_pnl
            if realized_pnl != 0:
                legacy_acc["closed"] += 1
            ledger_acc.append(
                _build_trade_ledger_entry(
                    d,
                    None,
                    None,
                    realized_pnl,
                    deal_time,
                    "",
                    "OTHER",
                    False,
                )
            )

    latest_strategy_result = {}
    with history_state_lock:
        position_strategy_map.update(fresh_position_map)
        for d in deals:
            deal_ticket = _deal_ticket(d)
            if deal_ticket is None or deal_ticket in processed_deals:
                continue
            position_id = getattr(d, "position_id", None) or getattr(d, "position", None)
            if position_id in session_ignored_position_ids:
                processed_deals.add(deal_ticket)
                continue

            apply_closed_deal(
                d,
                session_stats,
                performance_memory,
                strategy_stats,
                legacy_stats,
                trade_memory,
                latest_strategy_result,
                session_trade_ledger,
            )
            processed_deals.add(deal_ticket)

        for strategy_name, result_data in latest_strategy_result.items():
            profit, result_time = result_data
            record_strategy_result(strategy_name, profit, result_time)

        _rebuild_visual_snapshots()
        evaluate_strategy_health()
        last_history_check = datetime.now()


def _scan_symbol_candidates(symbol, strategies_snapshot):
    result = {
        "symbol": symbol,
        "df": None,
        "regime": "UNKNOWN",
        "active_signals": [],
        "blocked_signals": [],
        "logs": ["🔄 SCAN START"],
        "debug": {
            "data": False,
            "signal": "NEUTRAL",
            "blocked_by": "NONE",
            "stage": "INIT",
            "final": "WAITING",
        },
        "radar": {"sig": "SCAN", "conf": 0, "strat": "---", "status": "SCANNING"},
    }

    if not allow_trading(symbol):
        for strategy in strategies_snapshot:
            symbol_family = strategy.get("symbol_family")
            if symbol_family and symbol_in_family(symbol, symbol_family):
                set_strategy_pretrigger(strategy["name"], f"SESSION {strategy.get('session_label', '24H')}")
                record_strategy_event(strategy["name"], "NO SETUP OUT SESSION")
        result["debug"].update({"stage": "PRE-CHECK", "blocked_by": "OUT OF SESSION", "final": "SKIPPED"})
        result["radar"] = {"sig": "WAIT", "conf": 0, "strat": "---", "status": "OUT OF SESSION"}
        result["logs"].append("🚫 BLOCKED: Out of Session")
        return result

    df = get_data(symbol, mt5.TIMEFRAME_M5, 300)
    if df is None:
        result["debug"].update({"stage": "DATA", "blocked_by": "NO M5 DATA", "final": "NO DATA"})
        result["radar"] = {"sig": "WAIT", "conf": 0, "strat": "---", "status": "NO DATA"}
        result["logs"].append("🚫 BLOCKED: No Data")
        return result

    result["df"] = df
    result["debug"]["data"] = True
    result["debug"].update({"stage": "DATA", "blocked_by": "OK"})
    result["logs"].append("📊 DATA OK")

    regime = detect_market_regime(df)
    result["regime"] = regime
    result["debug"].update({"stage": "REGIME", "blocked_by": regime})
    result["logs"].append(f"📈 REGIME: {regime}")

    if regime == "RANGE":
        result["logs"].append("⚠️ RANGE MODE (Continuing)")

    if regime == "CHAOS":
        result["debug"]["final"] = "CHAOS"
        result["radar"] = {"sig": "WAIT", "conf": 0, "strat": "---", "status": "CHAOS"}
        result["logs"].append("🚫 BLOCKED: Chaos Market")
        return result

    mode_str = "MULTI" if should_use_multi_tp(df, symbol) else "STD"
    result["logs"].append(f"⚙️ MODE: {mode_str}")

    active_signals = []
    blocked_signals = []
    strategy_results = []
    strategy_worker_count = (
        len(strategies_snapshot)
        if USE_FULL_STRATEGY_EVAL_PARALLELISM
        else max(1, min(MAX_PARALLEL_STRATEGY_EVAL, len(strategies_snapshot)))
    )

    if strategy_worker_count <= 1:
        for strategy in strategies_snapshot:
            strategy_results.append(_run_strategy_eval(strategy, df, symbol))
    else:
        with ThreadPoolExecutor(max_workers=strategy_worker_count, thread_name_prefix=f"kryon-strat-{symbol}") as executor:
            future_map = {
                strategy["name"]: executor.submit(_run_strategy_eval, strategy, df, symbol)
                for strategy in strategies_snapshot
            }
            resolved = {}
            for strategy in strategies_snapshot:
                resolved[strategy["name"]] = future_map[strategy["name"]].result()
            strategy_results = [resolved[strategy["name"]] for strategy in strategies_snapshot]

    for evaluation in strategy_results:
        strategy = evaluation["strategy"]
        symbol_family = strategy.get("symbol_family")
        if symbol_family and not symbol_in_family(symbol, symbol_family):
            continue

        error = evaluation.get("error")
        if error is not None:
            error_reason = f"ERROR {type(error).__name__}"
            set_strategy_pretrigger(strategy["name"], "---")
            record_strategy_event(strategy["name"], error_reason)
            blocked_signals.append(
                {
                    "name": strategy["name"],
                    "reason": error_reason,
                    "pretrigger": "---",
                    "priority": int(strategy.get("selection_priority", 999)),
                }
            )
            continue

        res = evaluation.get("result")

        if res is None:
            set_strategy_pretrigger(strategy["name"], "---")
            record_strategy_event(strategy["name"], "NO SETUP")
            blocked_signals.append(
                {
                    "name": strategy["name"],
                    "reason": "NO SETUP",
                    "pretrigger": "---",
                    "priority": int(strategy.get("selection_priority", 999)),
                }
            )
            continue

        if res.get("blocked"):
            set_strategy_pretrigger(strategy["name"], res.get("pretrigger", "---"))
            record_strategy_event(strategy["name"], f"NO SETUP {res['blocked']}")
            blocked_signals.append(
                {
                    "name": strategy["name"],
                    "reason": str(res.get("blocked", "NO SETUP")),
                    "pretrigger": res.get("pretrigger", "---"),
                    "priority": int(strategy.get("selection_priority", 999)),
                }
            )
            continue

        sig = res["signal"]
        conf = res["confidence"]
        strat_name = res["name"]
        record_strategy_signal(strat_name, symbol, sig, conf)
        active_signals.append(
            {
                "signal": sig,
                "confidence": conf,
                "name": strat_name,
                "cfg": strategy,
                "execution_df": res.get("execution_df", df),
                "signal_key": res.get("signal_key"),
                "sl_price": res.get("sl_price"),
                "tp_price": res.get("tp_price"),
                "risk_multiplier_override": res.get("risk_multiplier_override"),
                "risk_percent_override": res.get("risk_percent_override"),
                "force_splits": res.get("force_splits"),
                "tp_point_targets": res.get("tp_point_targets"),
                "tp_point_buffer": res.get("tp_point_buffer"),
                "point_value_price": res.get("point_value_price"),
                "lot_weights_override": res.get("lot_weights_override"),
                "tp_be_after_legs_override": res.get("tp_be_after_legs_override"),
                "tp_lock_offset_override": res.get("tp_lock_offset_override"),
                "signal_details": res.get("signal_details"),
            }
        )
        result["logs"].append(f"🧠 {strat_name} -> {sig} ({round(conf, 1)}%)")
        if res.get("signal_details"):
            result["logs"].append(f"📐 {format_santo_graal_signal_details(res['signal_details'])}")

    result["blocked_signals"] = blocked_signals
    result["active_signals"] = active_signals
    result["debug"]["signal"] = ",".join([x["name"] for x in active_signals]) if active_signals else "NONE"

    if not active_signals:
        primary_block = None
        meaningful = sorted(
            [b for b in blocked_signals if b.get("reason") not in {"NO SETUP", "SCALP OFF", "OUT SESSION"}],
            key=lambda b: (int(b.get("priority", 999)), str(b.get("name", ""))),
        )
        if meaningful:
            primary_block = meaningful[0]
        if primary_block is None:
            secondary = sorted(
                [b for b in blocked_signals if b.get("reason") not in {"NO SETUP", "SCALP OFF"}],
                key=lambda b: (int(b.get("priority", 999)), str(b.get("name", ""))),
            )
            if secondary:
                primary_block = secondary[0]
        if primary_block is None and blocked_signals:
            primary_block = blocked_signals[0]

        radar_reason = primary_block["reason"] if primary_block else "MONITORING"
        radar_strat = primary_block["name"] if primary_block else "---"
        result["debug"].update({"stage": "SIGNAL", "blocked_by": radar_reason, "final": "NO SIGNAL"})
        result["radar"] = {"sig": "WAIT", "conf": 0, "strat": radar_strat, "status": radar_reason[:26]}
        result["logs"].append(f"⛔ NO SIGNAL | {radar_strat}: {radar_reason}")
        return result

    active_signals.sort(
        key=lambda item: (
            int(item["cfg"].get("selection_priority", 999)),
            -float(item.get("confidence", 0.0) or 0.0),
            -float(item["cfg"].get("min_score", 0.0) or 0.0),
            str(item.get("name", "")),
        )
    )
    top_signal = active_signals[0]
    result["debug"].update({"stage": "SIGNAL", "blocked_by": "MULTI SIGNAL", "final": "WAITING"})
    result["radar"] = {
        "sig": top_signal["signal"],
        "conf": int(top_signal["confidence"]),
        "strat": result["debug"]["signal"],
        "status": "MULTI SIGNAL",
    }
    return result


def run_cycle():
    global last_heartbeat_time, last_global_trade_time, debug_state, liquidity_sniper_cycle_symbols
    if bot_killed:
        return

    if time.time() - last_heartbeat_time > 15:
        last_heartbeat_time = time.time()
        set_log(f"💓 BOT ALIVE {datetime.now().strftime('%H:%M:%S')}")

    if not mt5.terminal_info():
        mt5.initialize()
        return

    ensure_session_balance_anchor()
    _refresh_runtime_modes()
    refresh_symbols()

    learn_from_history()
    tick_management()
    liquidity_sniper_cycle_symbols = []
    symbols_snapshot = list(symbols)
    strategies_snapshot = [strategy for strategy in STRATEGIES if strategy["enabled"]]
    strategies_by_symbol = {
        s: [
            strategy
            for strategy in strategies_snapshot
            if not strategy.get("symbol_family") or symbol_in_family(s, strategy.get("symbol_family"))
        ]
        for s in symbols_snapshot
    }
    scan_results = {}

    for s in symbols_snapshot:
        debug_state[s] = {
            "data": False,
            "signal": "NEUTRAL",
            "blocked_by": "NONE",
            "stage": "INIT",
            "final": "WAITING",
        }
        update_radar_state(s, "SCAN", 0, "---", "SCANNING")

    worker_count = len(symbols_snapshot) if USE_FULL_SYMBOL_SCAN_PARALLELISM else max(1, min(MAX_PARALLEL_SYMBOL_SCANS, len(symbols_snapshot)))
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="kryon-prime") as executor:
            future_map = {
                executor.submit(_prime_symbol_scan_context, s, strategies_by_symbol.get(s, [])): s
                for s in symbols_snapshot
            }
            for future in as_completed(future_map):
                s = future_map[future]
                try:
                    future.result()
                except Exception as exc:
                    set_live_log(s, f"⚠️ PRIME ERROR: {type(exc).__name__}")
    else:
        for s in symbols_snapshot:
            try:
                _prime_symbol_scan_context(s, strategies_by_symbol.get(s, []))
            except Exception as exc:
                set_live_log(s, f"⚠️ PRIME ERROR: {type(exc).__name__}")

    if worker_count == 1:
        for s in symbols_snapshot:
            scan_results[s] = _scan_symbol_candidates(s, strategies_by_symbol.get(s, []))
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="kryon-scan") as executor:
            future_map = {
                executor.submit(_scan_symbol_candidates, s, strategies_by_symbol.get(s, [])): s
                for s in symbols_snapshot
            }
            for future in as_completed(future_map):
                s = future_map[future]
                try:
                    scan_results[s] = future.result()
                except Exception as exc:
                    scan_results[s] = {
                        "symbol": s,
                        "df": None,
                        "regime": "ERROR",
                        "active_signals": [],
                        "blocked_signals": [],
                        "logs": [f"❌ SCAN ERROR: {type(exc).__name__}"],
                        "debug": {
                            "data": False,
                            "signal": "NONE",
                            "blocked_by": "SCAN ERROR",
                            "stage": "SCAN",
                            "final": "SCAN ERROR",
                        },
                        "radar": {"sig": "WAIT", "conf": 0, "strat": "---", "status": "SCAN ERROR"},
                    }

    execution_candidates = []
    symbol_exec_state = {}

    for s in symbols_snapshot:
        res = scan_results.get(s)
        if not res:
            continue

        for msg in res.get("logs", []):
            set_live_log(s, msg)

        debug_state[s] = res.get("debug", debug_state.get(s, {}))
        push_debug(s, debug_state[s].get("stage", "SCAN"), debug_state[s].get("blocked_by", "NONE"))

        radar = res.get("radar", {})
        update_radar_state(
            s,
            radar.get("sig", "WAIT"),
            int(radar.get("conf", 0) or 0),
            radar.get("strat", "---"),
            radar.get("status", "MONITORING"),
        )

        symbol_exec_state[s] = {
            "executed": 0,
            "last_reason": debug_state[s].get("final", "WAITING"),
            "planned": [],
        }

        active_signals = res.get("active_signals", [])
        if not active_signals:
            continue

        decision_stats["signals"] += len(active_signals)
        session_decision_stats["signals"] += len(active_signals)

        for signal_index, signal_data in enumerate(active_signals):
            execution_candidates.append(
                {
                    "symbol": s,
                    "regime": res.get("regime", "UNKNOWN"),
                    "df": res.get("df"),
                    "signal_index": signal_index,
                    **signal_data,
                }
            )

    execution_candidates.sort(
        key=lambda item: (
            int(item["cfg"].get("selection_priority", 999)),
            item.get("signal_index", 0),
            -float(item.get("confidence", 0.0) or 0.0),
            -_asset_weight_for_symbol(item["symbol"]),
            -float(item["cfg"].get("min_score", 0.0) or 0.0),
            str(item.get("name", "")),
        )
    )

    cycle_trade_cap = max(
        MAX_NEW_TRADES_PER_CYCLE,
        min(MAX_TRADES_PER_CYCLE, len(symbols_snapshot) * MAX_COMPATIBLE_STRATEGIES_PER_SYMBOL),
    )
    new_trades_executed = 0
    for candidate in execution_candidates:
        if new_trades_executed >= cycle_trade_cap:
            break

        s = candidate["symbol"]
        state = symbol_exec_state.setdefault(s, {"executed": 0, "last_reason": "WAITING", "planned": []})
        if state["executed"] >= MAX_COMPATIBLE_STRATEGIES_PER_SYMBOL:
            continue

        sig = candidate["signal"]
        conf = candidate["confidence"]
        strat_name = candidate["name"]
        strategy_cfg = candidate["cfg"]
        df = candidate["df"]
        execution_df = candidate.get("execution_df", df)
        regime = candidate.get("regime", "UNKNOWN")
        cooldown_key = (s, strat_name)

        incompatible_slot = False
        for planned in state["planned"]:
            if not _strategies_compatible(strategy_cfg, sig, planned["cfg"], planned["signal"]):
                set_live_log(s, f"🚫 {strat_name}: INCOMPATIBLE SLOT")
                state["last_reason"] = "INCOMPATIBLE SLOT"
                incompatible_slot = True
                break
        if incompatible_slot:
            continue

        allowed_regimes = strategy_cfg.get("allowed_regimes", set())
        if allowed_regimes and regime not in allowed_regimes:
            record_strategy_event(strat_name, f"REGIME {regime}")
            set_live_log(s, f"🚫 {strat_name}: REGIME {regime}")
            state["last_reason"] = f"REGIME {regime}"
            continue

        cooldown_sec = strategy_cfg.get("cooldown_sec", 60)
        if time.time() - last_strategy_trade_time.get(cooldown_key, 0) < cooldown_sec:
            record_strategy_event(strat_name, "COOLDOWN")
            set_live_log(s, f"🚫 {strat_name}: COOLDOWN {cooldown_sec}s")
            state["last_reason"] = f"{strat_name} COOLDOWN"
            continue

        runtime = strategy_runtime.get(strat_name, {})
        signal_key = candidate.get("signal_key") or _derive_runtime_signal_key(
            s,
            strat_name,
            sig,
            market_df=df,
            execution_df=execution_df,
        )
        candidate["signal_key"] = signal_key
        if signal_key and runtime.get("last_signal_key_by_symbol", {}).get(s) == signal_key:
            record_strategy_event(strat_name, "DUPLICATE LEG")
            set_live_log(s, f"🚫 {strat_name}: DUPLICATE LEG")
            state["last_reason"] = f"{strat_name} DUPLICATE"
            continue

        last_loss_time = runtime.get("last_loss_time")
        last_loss_profit = float(runtime.get("last_loss_profit", 0.0) or 0.0)
        if (
            strat_name.endswith("_ORB")
            and isinstance(last_loss_time, datetime)
            and last_loss_profit <= -1.0
            and (datetime.now() - last_loss_time).total_seconds() < 75
        ):
            record_strategy_event(strat_name, "LOSS BRAKE")
            set_live_log(s, f"🚫 {strat_name}: LOSS BRAKE 75s")
            state["last_reason"] = f"{strat_name} LOSS BRAKE"
            continue

        market_filter = strategy_cfg.get("market_filter_func") or fast_market_filter
        if not market_filter(s):
            record_strategy_event(strat_name, "FAST MARKET")
            push_debug(s, "FILTER", "FAST_MKT")
            decision_stats["filtered"] += 1
            session_decision_stats["filtered"] += 1
            debug_state[s]["final"] = "FAST MKT"
            update_radar_state(s, sig, int(conf), strat_name, "FAST MARKET")
            set_live_log(s, f"🚫 {strat_name}: FAST MARKET")
            state["last_reason"] = "FAST MARKET"
            continue

        score = compute_asset_score(s, execution_df if execution_df is not None else df, sig, conf, strategy_cfg)
        if score < strategy_cfg.get("min_score", 0.45):
            record_strategy_event(strat_name, f"LOW SCORE {round(score,2)}")
            push_debug(s, "FILTER", f"{strat_name}_LOW_SCORE")
            decision_stats["filtered"] += 1
            session_decision_stats["filtered"] += 1
            debug_state[s]["final"] = "FILTERED"
            set_live_log(s, f"🚫 {strat_name}: LOW SCORE ({round(score, 2)})")
            update_radar_state(s, sig, int(score * 100), strat_name, "FILTERED: STRAT SCORE")
            state["last_reason"] = "LOW SCORE"
            continue

        filter_func = strategy_cfg.get("filter_func") or default_strategy_filter
        valid, reason = filter_func(s, sig, score, df, execution_df, strategy_cfg)
        if not valid:
            record_strategy_event(strat_name, f"FILTER {reason}")
            push_debug(s, "FILTER", reason)
            decision_stats["filtered"] += 1
            session_decision_stats["filtered"] += 1
            debug_state[s]["final"] = "FILTERED"
            update_radar_state(s, sig, int(score * 100), strat_name, f"FILTERED: {reason}")
            set_live_log(s, f"🚫 {strat_name}: {reason}")
            state["last_reason"] = reason
            continue

        set_live_log(s, f"✅ {strat_name}: ENTRY {sig}")
        record_strategy_event(strat_name, f"ENTRY {sig}")
        push_debug(s, "EXECUTION", f"ATTEMPTING {strat_name}")
        debug_state[s]["final"] = "READY"

        trade_ok, trade_status = open_scaled_trade(s, sig, df, strat_name, score, conf, strategy_cfg, execution_df, candidate)
        if trade_ok:
            state["executed"] += 1
            state["planned"].append(candidate)
            new_trades_executed += 1
            record_strategy_entry(strat_name, s, sig)
            last_global_trade_time = time.time()
            last_strategy_trade_time[cooldown_key] = time.time()
            if signal_key:
                runtime.setdefault("last_signal_key_by_symbol", {})[s] = signal_key
            if candidate.get("signal_details"):
                runtime["last_signal_details"] = candidate["signal_details"]
            decision_stats["executed"] += 1
            session_decision_stats["executed"] += 1
            record_strategy_event(strat_name, "LIVE")
            set_live_log(s, f"🚀 {strat_name}: EXECUTED")
            push_debug(s, "DONE", "CLEARED")
            debug_state[s]["final"] = "EXECUTED"
            update_radar_state(s, sig, int(score * 100), strat_name, "EXECUTED")
            state["last_reason"] = "EXECUTED"
            continue

        normalized_status = str(trade_status or "ORDER FAIL").upper()
        if normalized_status == "ALREADY ACTIVE":
            record_strategy_event(strat_name, "LIVE")
            push_debug(s, "DONE", "LIVE")
            debug_state[s]["final"] = "LIVE"
            update_radar_state(s, sig, int(score * 100), strat_name, "LIVE")
            state["last_reason"] = "LIVE"
            continue

        record_strategy_event(strat_name, normalized_status)
        push_debug(s, "DONE", normalized_status)
        debug_state[s]["final"] = normalized_status
        update_radar_state(s, sig, int(score * 100), strat_name, normalized_status)
        state["last_reason"] = normalized_status

    for s in symbols_snapshot:
        state = symbol_exec_state.get(s)
        if not state:
            continue
        if debug_state[s]["final"] == "WAITING":
            debug_state[s]["final"] = state.get("last_reason", "MONITORING")
