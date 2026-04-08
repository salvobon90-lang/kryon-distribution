import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
import os
import json
import copy
from datetime import datetime, timedelta
import threading
import warnings
import traceback

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# CONFIGURAZIONE (KRYON ULTIMATE PRO V 15.4.2 - CLIENT DELIVERY & MONTHLY LICENSING)
# ==============================================================================
PROFIT_MODE = True
PRIMARY_SYMBOL = "XAUUSD"
SECONDARY_SYMBOLS = ["USTECH", "US500", "GER40", "JPN225"]
CRYPTO_SYMBOLS = ["BTCUSD", "ETHUSD"]

symbols_list = [PRIMARY_SYMBOL, *SECONDARY_SYMBOLS]
symbols = symbols_list.copy()

TIMEFRAME_MAIN = mt5.TIMEFRAME_M5
MAX_UNIQUE_SYMBOLS = 7
MAX_CLUSTERS_PER_SYMBOL = 3
SYMBOL_CLUSTER_CAPS = {"USTECH": 4, "GER40": 4}

MAX_NEW_TRADES_PER_CYCLE = 2
MAX_TRADES_PER_CYCLE = 5
RISK_PER_TRADE_PERCENT = 0.5
MAX_TOTAL_RISK = 0.08
CURRENT_OPTIMIZER_PROFILE = "BALANCED"

OPTIMIZER_PROFILES = {
    "SAFE": {"risk_per_trade_percent": 0.30, "max_total_risk": 0.05, "max_trades_per_cycle": 3},
    "BALANCED": {"risk_per_trade_percent": 0.50, "max_total_risk": 0.08, "max_trades_per_cycle": 5},
    "ATTACK": {"risk_per_trade_percent": 0.75, "max_total_risk": 0.12, "max_trades_per_cycle": 7},
}

ASSET_WEIGHTS = {"XAUUSD": 1.0, "USTECH": 0.8, "US500": 0.7, "GER40": 0.65, "JPN225": 0.58, "BTCUSD": 0.62, "ETHUSD": 0.54}

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
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100."],
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
equity_peak = 0.0
equity_mode = "NORMAL"
ai_mode = "STABLE"

SESSION_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kryon_session_state.json")

processed_deals = set()
trade_memory = {}
position_strategy_map = {}
resolved_symbol_map = {}

last_history_check = datetime.now()

m1_cache = {}
m1_cache_time = {}
m15_cache = {}
m15_cache_time = {}
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
):
    cfg = {
        "name": name,
        "func": func,
        "enabled": enabled,
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


def _deal_ticket(deal):
    return getattr(deal, "ticket", None) or getattr(deal, "deal", None)


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


def record_strategy_signal(name, symbol, signal, confidence):
    runtime = strategy_runtime.setdefault(name, {})
    runtime["last_signal"] = signal
    runtime["last_signal_conf"] = round(confidence, 1)
    runtime["last_signal_symbol"] = symbol
    runtime["last_signal_time"] = datetime.now()
    runtime["last_event"] = f"SIGNAL {signal}"
    runtime["live_block"] = "READY"
    runtime["pretrigger"] = "TRIGGER READY"


def set_strategy_pretrigger(name, pretrigger_text_value="---"):
    runtime = strategy_runtime.setdefault(name, {})
    runtime["pretrigger"] = pretrigger_text_value or "---"


def record_strategy_tp_plan(name, plan_text):
    runtime = strategy_runtime.setdefault(name, {})
    runtime["last_tp_plan"] = plan_text or "---"


def record_strategy_event(name, event_text):
    runtime = strategy_runtime.setdefault(name, {})
    runtime["last_event"] = event_text
    event_upper = str(event_text).upper()
    if event_upper.startswith("NO SETUP "):
        runtime["live_block"] = event_upper.replace("NO SETUP ", "")[:24]
    elif "NO SETUP" in event_upper:
        runtime["live_block"] = "NO SETUP"
    elif "COOLDOWN" in event_upper:
        runtime["live_block"] = "COOLDOWN"
    elif "OUT SESSION" in event_upper:
        runtime["live_block"] = "OUT SESSION"
    elif "FAST" in event_upper:
        runtime["live_block"] = "FAST"
    elif "SPREAD" in event_upper:
        runtime["live_block"] = "SPREAD"
    elif "LOW SCORE" in event_upper:
        runtime["live_block"] = "LOW SCORE"
    elif "REGIME" in event_upper:
        runtime["live_block"] = "REGIME"
    elif "LIVE" in event_upper or "ENTRY" in event_upper:
        runtime["live_block"] = "LIVE"
    elif "FAIL" in event_upper:
        runtime["live_block"] = "FAIL"
    elif "FILTER" in event_upper:
        runtime["live_block"] = event_upper.replace("FILTER ", "")[:18]
    else:
        runtime["live_block"] = event_upper[:18]


def record_strategy_result(name, profit, result_time=None):
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
        active_symbols = active_symbols[:MAX_UNIQUE_SYMBOLS]
    symbols_list = active_symbols
    symbols = active_symbols
    for symbol in active_symbols:
        _ensure_symbol_state(symbol)
    return symbols


refresh_symbols()


def _default_session_start():
    now = datetime.now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _load_session_state():
    try:
        with open(SESSION_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        started = datetime.fromisoformat(str(data.get("session_started_at")))
        broker_started_raw = data.get("session_broker_started_at")
        broker_started = datetime.fromisoformat(str(broker_started_raw)) if broker_started_raw else None
        balance = data.get("session_balance_start")
        return started, broker_started, float(balance) if balance is not None else None
    except Exception:
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


session_started_at, session_broker_started_at, session_balance_start = _load_session_state()

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

    lock_bias = cluster_state.get("cluster_lock_bias")
    if lock_bias is None:
        lock_bias = (0.5 - quality) * (0.20 if is_orb_cluster else 0.14)

    protect_bias = cluster_state.get("protect_bias")
    if protect_bias is None:
        protect_bias = (0.5 - quality) * (0.26 if is_orb_cluster else 0.18)

    early_cashout_ratio = cluster_state.get("early_cashout_ratio")
    if early_cashout_ratio is None:
        early_cashout_ratio = clamp(0.10 + (0.5 - quality) * (0.16 if is_orb_cluster else 0.10), 0.04, 0.24)

    orb_cashout_ratio = cluster_state.get("orb_cashout_ratio")
    if orb_cashout_ratio is None:
        orb_cashout_ratio = clamp(0.18 + (0.5 - quality) * 0.20, 0.08, 0.32)

    hard_lock_ratio = cluster_state.get("hard_lock_ratio")
    if hard_lock_ratio is None:
        hard_lock_ratio = clamp(0.14 + (0.5 - quality) * 0.18, 0.06, 0.30)

    cascade_ratio = cluster_state.get("cascade_ratio")
    if cascade_ratio is None:
        cascade_ratio = clamp(0.10 + (0.5 - quality) * 0.16, 0.04, 0.26)

    tp1_guard_ratio = cluster_state.get("tp1_guard_ratio")
    if tp1_guard_ratio is None:
        tp1_guard_ratio = clamp(0.08 + (0.5 - quality) * (0.12 if is_orb_cluster else 0.08), 0.03, 0.18)

    weak_floor = cluster_state.get("weak_profit_floor")
    if weak_floor is None:
        weak_floor = max(0.35, strategy_cfg.get("multi_tp_eur_min", 0.50) * clamp(1.00 + (0.5 - quality) * 0.40, 0.70, 1.20))

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
    }


def build_dynamic_multi_tp_prices(symbol, signal, entry_price, info, lot_per_trade, atr, spread_price, strategy_cfg, splits):
    eur_min = strategy_cfg.get("multi_tp_eur_min", 0.50)
    eur_max = strategy_cfg.get("multi_tp_eur_max", 1.50)
    target_eur = min(max(strategy_cfg.get("effective_multi_tp_target_eur", strategy_cfg.get("multi_tp_target_eur", 1.00)), eur_min), eur_max)
    curve = list(strategy_cfg.get("effective_multi_tp_curve") or strategy_cfg.get("multi_tp_curve") or [1.0, 1.8, 2.8, 4.0, 5.6, 7.6, 10.0, 13.0])
    tp2_gap_eur = max(0.25, strategy_cfg.get("multi_tp_tp2_gap_eur", 0.35))
    step_gap_eur = max(0.10, strategy_cfg.get("multi_tp_step_gap_eur", 0.18))

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
    min_splits = max(4, int(strategy_cfg.get("min_splits", 4)))
    max_splits = max(min_splits, min(MAX_SPLITS, int(strategy_cfg.get("max_splits", MAX_SPLITS))))

    split_span = max_splits - min_splits
    splits = min_splits + int(round(quality * split_span)) if split_span > 0 else min_splits
    splits = max(min_splits, min(max_splits, splits))

    eur_min = max(0.50, strategy_cfg.get("multi_tp_eur_min", 0.50))
    eur_max = max(eur_min, strategy_cfg.get("multi_tp_eur_max", 1.50))
    base_target = min(max(strategy_cfg.get("multi_tp_target_eur", eur_min), eur_min), eur_max)
    dynamic_target = eur_min + (eur_max - eur_min) * clamp(quality * 1.04, 0.0, 1.0)
    target_floor = min(eur_max, eur_min + max(0.12, (eur_max - eur_min) * 0.12))
    target_eur = min(eur_max, max(target_floor, (base_target * 0.22) + (dynamic_target * 0.78)))

    base_first_atr = strategy_cfg.get("multi_tp_first_atr", 0.12)
    first_atr = base_first_atr * (0.88 + quality * 0.34)
    base_curve = list(strategy_cfg.get("multi_tp_curve") or [1.0, 1.8, 2.8, 4.0, 5.6, 7.6, 10.0, 13.0])
    curve_scale = 0.90 + quality * 0.26
    effective_curve = [round(max(1.0, factor * curve_scale), 3) for factor in base_curve]

    plan_cfg = dict(strategy_cfg)
    plan_cfg["effective_multi_tp_target_eur"] = round(target_eur, 2)
    plan_cfg["effective_multi_tp_first_atr"] = round(first_atr, 4)
    plan_cfg["effective_multi_tp_curve"] = effective_curve
    plan_cfg["effective_tp_quality"] = round(quality, 3)
    plan_cfg["effective_be_trigger"] = round(clamp(strategy_cfg.get("be_trigger_progress", 0.45) + (quality - 0.5) * 0.18, 0.06, 0.82), 4)
    plan_cfg["effective_be_buffer_progress"] = round(clamp(strategy_cfg.get("be_buffer_progress", 0.05) * (0.86 + quality * 0.28), 0.02, 0.25), 4)
    plan_text = f"{splits}TP | C{int(round(confidence))} | S{int(round(score * 100))} | TP1~{round(target_eur, 2)}€"
    return plan_cfg, splits, plan_text


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


def atr_gap_pct(gap, atr):
    return round(max(0.0, gap) / max(atr, 1e-6) * 100)


def pretrigger_text(label, gap, atr):
    return f"{label} {atr_gap_pct(gap, atr)}% ATR"


def should_use_multi_tp(df, symbol):
    atr = df["atr"].iloc[-1]
    price = df["close"].iloc[-1]
    atr_ratio = atr / price
    trend_strength = compute_trend_strength(df)

    if atr_ratio < 0.001:
        return False
    if trend_strength < MIN_TREND_STRENGTH:
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
    debug_state[symbol]["stage"] = stage
    debug_state[symbol]["blocked_by"] = reason


def update_radar_state(symbol, sig, conf, strat, status):
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
    radar_state[symbol].update({"sig": sig, "conf": conf, "strat": strat, "status": status})


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
    if mt5.terminal_info():
        try:
            tracked = performance_memory["wins"] + performance_memory["losses"]
            if tracked == 0 or (datetime.now() - last_history_check).total_seconds() > 2:
                learn_from_history()
        except Exception:
            pass
    _refresh_runtime_modes()
    acc = mt5.account_info()
    ensure_session_balance_anchor()
    total = performance_memory["wins"] + performance_memory["losses"]
    session_closed_profit = session_stats["profit"]
    session_open_pnl = get_live_bot_pnl()
    session_profit = session_closed_profit + session_open_pnl
    broker_session_start = get_broker_session_start()
    return {
        "ai_mode": ai_mode,
        "equity_mode": equity_mode,
        "optimizer_profile": CURRENT_OPTIMIZER_PROFILE,
        "drawdown": round(get_drawdown() * 100, 2),
        "winrate": round(_get_session_winrate() * 100, 1),
        "total_winrate": round(_get_winrate() * 100, 1),
        "signals": session_decision_stats["signals"],
        "filtered": session_decision_stats["filtered"],
        "executed": session_decision_stats["executed"],
        "equity": round(acc.equity, 2) if acc else 0.0,
        "balance": round(acc.balance, 2) if acc else 0.0,
        "session_profit": round(session_profit, 2),
        "session_closed_profit": round(session_closed_profit, 2),
        "session_open_pnl": round(session_open_pnl, 2),
        "closed_trades": session_stats["trades"],
        "flat_trades": session_stats.get("flat", 0),
        "session_started_at_label": f"{session_started_at.strftime('%H:%M')} | MT5 {broker_session_start.strftime('%H:%M') if broker_session_start else '--:--'}",
        "tracked_results": total,
    }


def reset_session_tracking():
    global session_stats, session_decision_stats, decision_stats, session_started_at, session_broker_started_at, session_balance_start, last_history_check
    session_started_at = datetime.now()
    session_broker_started_at = session_started_at + get_live_broker_offset(force=True)
    session_stats = {"profit": 0.0, "trades": 0, "wins": 0, "losses": 0, "flat": 0}
    session_decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
    decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
    processed_deals.clear()
    trade_memory.clear()
    position_strategy_map.clear()
    last_history_check = session_started_at
    acc = mt5.account_info()
    session_balance_start = acc.balance if acc else None
    _save_session_state()


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
    if mt5.terminal_info():
        try:
            tracked = performance_memory["wins"] + performance_memory["losses"]
            if tracked == 0 or (datetime.now() - last_history_check).total_seconds() > 2:
                learn_from_history()
        except Exception:
            pass
    dashboard = []
    for strategy in STRATEGIES:
        stats = strategy_stats.get(strategy["name"], {})
        runtime = strategy_runtime.get(strategy["name"], {})
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        total = wins + losses
        winrate = (wins / total) * 100 if total > 0 else 0.0
        profit_factor = compute_profit_factor(stats)
        if profit_factor == float("inf"):
            profit_factor_display = "INF"
        else:
            profit_factor_display = round(profit_factor, 2)

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

        dashboard.append(
            {
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
                "closed": stats.get("closed", 0),
                "net_profit": round(stats.get("net_profit", 0.0), 2),
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
            }
        )
    legacy_total = legacy_stats.get("wins", 0) + legacy_stats.get("losses", 0)
    if legacy_stats.get("closed", 0) > 0 or abs(legacy_stats.get("net_profit", 0.0)) > 0:
        legacy_pf = compute_profit_factor(legacy_stats)
        dashboard.append(
            {
                "name": "LEGACY",
                "family": "MIGRATED",
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
                "disabled_reason": "UNMAPPED TAGS",
                "auto_disable": False,
                "last_signal": "---",
                "last_signal_conf": 0.0,
                "last_signal_symbol": "---",
                "last_signal_time": "---",
                "last_result": "---",
                "last_result_profit": 0.0,
                "last_result_time": "---",
                "last_event": "LEGACY HISTORY",
                "live_block": "UNMAPPED",
                "pretrigger": "---",
                "tp_plan": "---",
            }
        )
    return dashboard


def toggle_crypto(state):
    global crypto_enabled
    crypto_enabled = bool(state)
    armed = []
    for strategy in STRATEGIES:
        if not is_crypto_family(strategy.get("symbol_family")):
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


def toggle_scalping(state):
    global scalping_enabled
    scalping_enabled = bool(state)
    armed = []
    for strategy in STRATEGIES:
        if not str(strategy.get("name", "")).endswith("_SCALP"):
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
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID or p.symbol != symbol:
            continue
        p_tag = position_strategy_map.get(p.ticket) or (p.comment.rsplit("_", 1)[0] if p.comment else None)
        if p_tag != strategy_tag:
            continue
        if close_position(p):
            closed += 1
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


def get_m15_cached(symbol):
    global m15_cache, m15_cache_time
    if symbol not in m15_cache or time.time() - m15_cache_time.get(symbol, 0) > 60:
        m15_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M15, 100)
        m15_cache_time[symbol] = time.time()
    return m15_cache[symbol]


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
    late_gold = symbol_in_family(symbol, "XAUUSD") and (broker_hour >= 21 or broker_hour <= 2)
    min_score = 0.40 if attack else 0.45
    min_momentum = atr * (0.06 if attack else 0.10)
    min_vol = 0.00035 if attack else 0.0005
    max_spread = atr * (0.14 if attack else 0.10)
    if is_crypto:
        min_score = 0.36 if attack else 0.40
        min_momentum = atr * (0.04 if attack else 0.07)
        min_vol = 0.00018 if attack else 0.00028
        max_spread = atr * (0.22 if attack else 0.16)
    elif late_gold:
        min_score = 0.38 if attack else 0.42
        min_momentum = atr * (0.05 if attack else 0.08)
        min_vol = 0.00025 if attack else 0.00038
        max_spread = atr * (0.20 if attack else 0.14)
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


def allow_trading(symbol):
    hour = get_broker_hour()
    if symbol_in_family(symbol, "XAUUSD"):
        return 0 <= hour <= 23
    if symbol_in_family(symbol, "BTCUSD") or symbol_in_family(symbol, "ETHUSD"):
        return 0 <= hour <= 23
    if symbol_in_family(symbol, "JPN225"):
        return 2 <= hour <= 12
    if symbol_in_family(symbol, "USTECH"):
        return 7 <= hour <= 23
    if symbol_in_family(symbol, "US500"):
        return 10 <= hour <= 23
    if symbol_in_family(symbol, "GER40") or symbol_in_family(symbol, "UK100"):
        return 8 <= hour <= 18
    return False


def fast_market_filter(symbol):
    t1 = safe_tick(symbol)
    time.sleep(0.02)
    t2 = safe_tick(symbol)
    if not t1 or not t2:
        return False
    move = abs(t2.bid - t1.bid)
    return move <= t1.bid * 0.004


def fast_market_filter_scalping(symbol):
    t1 = safe_tick(symbol)
    time.sleep(0.01)
    t2 = safe_tick(symbol)
    if not t1 or not t2:
        return False
    move = abs(t2.bid - t1.bid)
    return move <= t1.bid * 0.006


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
    late_gold = symbol_in_family(symbol, "XAUUSD") and (broker_hour >= 21 or broker_hour <= 2)
    min_score = strategy_cfg.get("min_score", 0.55)
    spread_limit = atr_m1 * 0.16
    body_floor = atr_m1 * 0.12
    momentum_floor = atr_m1 * 0.22
    if late_gold:
        min_score = max(0.40 if attack else 0.42, min_score - 0.03)
        spread_limit = atr_m1 * (0.22 if attack else 0.18)
        body_floor = atr_m1 * (0.08 if attack else 0.10)
        momentum_floor = atr_m1 * (0.16 if attack else 0.18)
    if score < min_score:
        return False, f"LOW SCORE ({round(score,2)})"
    if spread > spread_limit:
        return False, "SCALP SPREAD"
    if candle_body < body_floor:
        return False, "WEAK CANDLE"
    if momentum < momentum_floor:
        return False, "WEAK SCALP MOM"
    return True, "OK"


def get_correlation_group(symbol):
    if any(symbol_in_family(symbol, family) for family in ["USTECH", "US500", "GER40", "UK100"]):
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


def strategy_trend_vwap(df, symbol):
    trend_strength = compute_trend_strength(df)
    regime = detect_market_regime(df)
    attack = is_attack_runtime()
    hour = get_broker_hour()
    min_trend = 0.08 if attack else 0.22
    vwap_limit = 2.10 if attack else 1.45
    if symbol_in_family(symbol, "XAUUSD") and (hour >= 21 or hour <= 2):
        min_trend = min(min_trend, 0.05 if attack else 0.12)
        if trend_strength >= (0.85 if attack else 0.65):
            vwap_limit = max(vwap_limit, 5.60 if attack else 4.80)
    confirm_pad = 0.24 if attack else 0.12
    ema_hold_pad = 0.30 if attack else 0.18
    body_floor = 0.02 if attack else 0.04
    wick_mult = 1.70 if attack else 1.35
    wick_atr = 0.26 if attack else 0.18
    if symbol_in_family(symbol, "XAUUSD") and (hour >= 21 or hour <= 2):
        confirm_pad = 0.34 if attack else 0.20
        ema_hold_pad = 0.42 if attack else 0.26
        body_floor = 0.015 if attack else 0.028
        wick_mult = 1.95 if attack else 1.55
        wick_atr = 0.32 if attack else 0.22
    if regime == "CHAOS" or trend_strength < min_trend:
        return blocked_signal("XAU_TREND", "REGIME RANGE", f"STR {round(trend_strength, 2)}")

    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    ema20 = df["close"].ewm(span=20).mean().iloc[-1]
    ema50 = df["close"].ewm(span=50).mean().iloc[-1]
    atr = df["atr"].iloc[-1]
    last = df.iloc[-1]
    prev = df.iloc[-2]
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
            return {"signal": "BUY", "confidence": 72, "name": "XAU_TREND"}
        if (
            ema20 < ema50
            and price < vwap
            and distance <= atr * vwap_limit
            and price <= prev["low"] + atr * confirm_pad
            and last["high"] <= ema20 + atr * ema_hold_pad
            and strong_body
            and clean_short_candle
        ):
            return {"signal": "SELL", "confidence": 72, "name": "XAU_TREND"}
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

    return {"signal": sig, "confidence": conf, "name": "XAU_TREND"}


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
    if not (now.hour >= 13 or now.hour <= 3):
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
    if spread > atr_m1 * (0.34 if attack else 0.26):
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
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    recent_high = high_m1.tail(6).max()
    recent_low = low_m1.tail(6).min()
    price = df["close"].iloc[-1]
    vwap = df["vwap"].iloc[-1]
    trend_strength = compute_trend_strength(df)

    min_trend = 0.06 if attack else 0.14
    long_bias = ema20_m5 > ema50_m5 and ema20_m15 > ema50_m15 and price > vwap and trend_strength >= min_trend
    short_bias = ema20_m5 < ema50_m5 and ema20_m15 < ema50_m15 and price < vwap and trend_strength >= min_trend

    long_stack = ema8.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1]
    short_stack = ema8.iloc[-1] < ema21.iloc[-1] < ema50.iloc[-1]
    long_pullback = min(prev["low"], last["low"]) <= (ema21.iloc[-1] + atr_m1 * (0.30 if attack else 0.22))
    short_pullback = max(prev["high"], last["high"]) >= (ema21.iloc[-1] - atr_m1 * (0.30 if attack else 0.22))
    long_reclaim = last["close"] > ema8.iloc[-1] and last["close"] > last["open"]
    short_reject = last["close"] < ema8.iloc[-1] and last["close"] < last["open"]
    long_break = last["close"] >= (recent_high - atr_m1 * (0.22 if attack else 0.10))
    short_break = last["close"] <= (recent_low + atr_m1 * (0.22 if attack else 0.10))
    strong_body = body >= atr_m1 * (0.04 if attack else 0.05)
    clean_long = wick_up <= max(body * (1.35 if attack else 1.18), atr_m1 * (0.22 if attack else 0.16))
    clean_short = wick_down <= max(body * (1.35 if attack else 1.18), atr_m1 * (0.22 if attack else 0.16))

    if long_bias and long_stack and long_pullback and long_reclaim and long_break and strong_body and clean_long and 42 <= rsi.iloc[-1] <= 82:
        conf = 78
        if price >= vwap + atr_m5 * 0.08 and trend_strength > 0.70:
            conf = 82
        return {"signal": "BUY", "confidence": conf, "name": "XAU_M1_SCALP", "execution_df": df_m1}

    if short_bias and short_stack and short_pullback and short_reject and short_break and strong_body and clean_short and 18 <= rsi.iloc[-1] <= 58:
        conf = 78
        if price <= vwap - atr_m5 * 0.08 and trend_strength > 0.70:
            conf = 82
        return {"signal": "SELL", "confidence": conf, "name": "XAU_M1_SCALP", "execution_df": df_m1}

    if not long_bias and not short_bias:
        return blocked_signal("XAU_M1_SCALP", "TREND NOT READY", f"STR {round(trend_strength, 2)}")
    if long_bias:
        if not long_stack:
            return blocked_signal("XAU_M1_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema21.iloc[-1] - ema8.iloc[-1], ema50.iloc[-1] - ema21.iloc[-1]), atr_m1))
        if not long_pullback:
            return blocked_signal("XAU_M1_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev['low'], last['low']) - (ema21.iloc[-1] + atr_m1 * (0.30 if attack else 0.22)), atr_m1))
        if not long_reclaim:
            return blocked_signal("XAU_M1_SCALP", "EMA8 NOT RECLAIMED", pretrigger_text("EMA8", ema8.iloc[-1] - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal("XAU_M1_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.04 if attack else 0.05) - body, atr_m1))
        if not clean_long:
            return blocked_signal("XAU_M1_SCALP", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.35 if attack else 1.18), atr_m1 * (0.22 if attack else 0.16)), atr_m1))
        if not long_break:
            return blocked_signal("XAU_M1_SCALP", "NO BREAKOUT", pretrigger_text("BRK", (recent_high - atr_m1 * (0.22 if attack else 0.10)) - last["close"], atr_m1))
        return blocked_signal("XAU_M1_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")

    if not short_stack:
        return blocked_signal("XAU_M1_SCALP", "EMA STACK MISS", pretrigger_text("STACK", max(ema8.iloc[-1] - ema21.iloc[-1], ema21.iloc[-1] - ema50.iloc[-1]), atr_m1))
    if not short_pullback:
        return blocked_signal("XAU_M1_SCALP", "NO PULLBACK", pretrigger_text("PB", (ema21.iloc[-1] - atr_m1 * (0.30 if attack else 0.22)) - max(prev['high'], last['high']), atr_m1))
    if not short_reject:
        return blocked_signal("XAU_M1_SCALP", "EMA8 NOT LOST", pretrigger_text("EMA8", last["close"] - ema8.iloc[-1], atr_m1))
    if not strong_body:
        return blocked_signal("XAU_M1_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.04 if attack else 0.05) - body, atr_m1))
    if not clean_short:
        return blocked_signal("XAU_M1_SCALP", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.35 if attack else 1.18), atr_m1 * (0.22 if attack else 0.16)), atr_m1))
    if not short_break:
        return blocked_signal("XAU_M1_SCALP", "NO BREAKDOWN", pretrigger_text("BRK", last["close"] - (recent_low + atr_m1 * (0.22 if attack else 0.10)), atr_m1))
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

    trend_min = 0.24 if attack else 0.42
    m15_min = 0.14 if attack else 0.26
    vwap_limit = 3.45 if attack else 2.95
    if trend_strength >= (0.90 if attack else 1.05):
        vwap_limit += 0.65
    if m15_strength >= (0.55 if attack else 0.42):
        vwap_limit += 0.45
    long_trigger = recent_high - atr * (0.10 if attack else 0.04)
    short_trigger = recent_low + atr * (0.10 if attack else 0.04)
    long_break_gap = max(0.0, long_trigger - price)
    short_break_gap = max(0.0, price - short_trigger)
    long_pullback = min(m1_prev["low"], m1_last["low"]) <= (ema20_m1 + atr_m1 * (0.34 if attack else 0.24))
    short_pullback = max(m1_prev["high"], m1_last["high"]) >= (ema20_m1 - atr_m1 * (0.34 if attack else 0.24))
    long_reclaim = m1_last["close"] > ema9_m1 and ema9_m1 > ema20_m1 > ema50_m1
    short_reclaim = m1_last["close"] < ema9_m1 and ema9_m1 < ema20_m1 < ema50_m1
    strong_body = body >= atr * (0.04 if attack else 0.06)
    clean_long_candle = wick_up <= max(body * (1.28 if attack else 1.06), atr * 0.16)
    clean_short_candle = wick_down <= max(body * (1.28 if attack else 1.06), atr * 0.16)
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
    trend_min = 0.22 if attack else 0.40
    m15_min = 0.15 if attack else 0.24

    long_setup = (
        ema20 > ema50
        and ema20_m15 > ema50_m15
        and price > vwap
        and vwap_dist <= atr * (2.85 if attack else 2.45)
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and ema9_m1.iloc[-1] > ema21_m1.iloc[-1] > ema55_m1.iloc[-1]
        and min(prev["low"], last["low"]) <= long_pullback_band
        and last["close"] > ema9_m1.iloc[-1]
        and (last["close"] - last["open"]) > atr_m1 * (0.07 if attack else 0.07)
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
        and vwap_dist <= atr * (2.85 if attack else 2.45)
        and trend_strength >= trend_min
        and m15_strength >= m15_min
        and ema9_m1.iloc[-1] < ema21_m1.iloc[-1] < ema55_m1.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema9_m1.iloc[-1]
        and (last["open"] - last["close"]) > atr_m1 * (0.07 if attack else 0.07)
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
    if vwap_dist > atr * (2.85 if attack else 2.45):
        return blocked_signal("ETH_PULSE", "VWAP TOO FAR", pretrigger_text("VWAP", vwap_dist - atr * (2.85 if attack else 2.45), atr))
    if ema20 > ema50:
        if not (ema9_m1.iloc[-1] > ema21_m1.iloc[-1] > ema55_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA STACK MISS", pretrigger_text("STACK", max(ema21_m1.iloc[-1] - ema9_m1.iloc[-1], ema55_m1.iloc[-1] - ema21_m1.iloc[-1]), atr_m1))
        if not (min(prev["low"], last["low"]) <= long_pullback_band):
            return blocked_signal("ETH_PULSE", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - long_pullback_band, atr_m1))
        if not (last["close"] > ema9_m1.iloc[-1]):
            return blocked_signal("ETH_PULSE", "EMA9 NOT HELD", pretrigger_text("EMA9", ema9_m1.iloc[-1] - last["close"], atr_m1))
        if not ((last["close"] - last["open"]) > atr_m1 * (0.07 if attack else 0.07)):
            return blocked_signal("ETH_PULSE", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.07 if attack else 0.07) - (last["close"] - last["open"]), atr_m1))
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
        if not ((last["open"] - last["close"]) > atr_m1 * (0.07 if attack else 0.07)):
            return blocked_signal("ETH_PULSE", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.07 if attack else 0.07) - (last["open"] - last["close"]), atr_m1))
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
    session_start, session_end = broker_window_from_local_df(df_m1, 14, 30, 22, 30)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("XAU_ORB", "BUILDING RANGE", "ORB 14:00-14:30")
    if broker_now > session_end:
        return blocked_signal("XAU_ORB", "OUT SESSION", "ORB 14:30-22:30")

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
    session_start, session_end = broker_window_from_local_df(df_m1, 15, 45, 22, 15)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("USTECH_ORB", "BUILDING RANGE", "ORB 15:30-15:45")
    if broker_now > session_end:
        return blocked_signal("USTECH_ORB", "OUT SESSION", "ORB 15:45-22:15")

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
    if spread > atr_m1 * (0.34 if attack else 0.24):
        return blocked_signal("USTECH_PULLBACK_SCALP", "SPREAD HIGH")

    m15_close = df_m15["close"].iloc[-1]
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    recent_high = df_m1["high"].tail(6).max()
    recent_low = df_m1["low"].tail(6).min()
    body = abs(last["close"] - last["open"])
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    strong_body = body >= atr_m1 * (0.05 if attack else 0.06)
    clean_long = wick_up <= max(body * (1.25 if attack else 1.05), atr_m1 * (0.24 if attack else 0.16))
    clean_short = wick_down <= max(body * (1.25 if attack else 1.05), atr_m1 * (0.24 if attack else 0.16))

    long_trend = ema50.iloc[-1] > ema200.iloc[-1] and m15_close >= ema50_m15
    short_trend = ema50.iloc[-1] < ema200.iloc[-1] and m15_close <= ema50_m15
    long_pullback = min(prev["low"], last["low"]) <= ema50.iloc[-1] + atr_m1 * (0.22 if attack else 0.16) and min(prev["low"], last["low"]) >= ema200.iloc[-1] - atr_m1 * 0.10
    short_pullback = max(prev["high"], last["high"]) >= ema50.iloc[-1] - atr_m1 * (0.22 if attack else 0.16) and max(prev["high"], last["high"]) <= ema200.iloc[-1] + atr_m1 * 0.10
    long_confirm = last["close"] > last["open"] and last["close"] > ema50.iloc[-1] and last["close"] >= recent_high - atr_m1 * (0.18 if attack else 0.08)
    short_confirm = last["close"] < last["open"] and last["close"] < ema50.iloc[-1] and last["close"] <= recent_low + atr_m1 * (0.18 if attack else 0.08)

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi.iloc[-1] > 50:
        conf = 76
        if rsi.iloc[-1] > 58 and last["close"] >= ema50.iloc[-1] + atr_m1 * 0.08:
            conf = 81
        return {"signal": "BUY", "confidence": conf, "name": "USTECH_PULLBACK_SCALP", "execution_df": df_m1}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi.iloc[-1] < 50:
        conf = 76
        if rsi.iloc[-1] < 42 and last["close"] <= ema50.iloc[-1] - atr_m1 * 0.08:
            conf = 81
        return {"signal": "SELL", "confidence": conf, "name": "USTECH_PULLBACK_SCALP", "execution_df": df_m1}

    if not long_trend and not short_trend:
        return blocked_signal("USTECH_PULLBACK_SCALP", "EMA TREND MIXED", pretrigger_text("EMA", abs(ema50.iloc[-1] - ema200.iloc[-1]), atr_m1))
    if long_trend:
        if not long_pullback:
            return blocked_signal("USTECH_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", min(prev["low"], last["low"]) - (ema50.iloc[-1] + atr_m1 * (0.22 if attack else 0.16)), atr_m1))
        if not long_confirm:
            return blocked_signal("USTECH_PULLBACK_SCALP", "NO BULL CONFIRM", pretrigger_text("CONF", (recent_high - atr_m1 * (0.18 if attack else 0.08)) - last["close"], atr_m1))
        if not strong_body:
            return blocked_signal("USTECH_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
        if not clean_long:
            return blocked_signal("USTECH_PULLBACK_SCALP", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - max(body * (1.25 if attack else 1.05), atr_m1 * (0.24 if attack else 0.16)), atr_m1))
        return blocked_signal("USTECH_PULLBACK_SCALP", "RSI BLOCKED", f"RSI {round(rsi.iloc[-1], 1)}")
    if not short_pullback:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO PULLBACK", pretrigger_text("PB", (ema50.iloc[-1] - atr_m1 * (0.22 if attack else 0.16)) - max(prev["high"], last["high"]), atr_m1))
    if not short_confirm:
        return blocked_signal("USTECH_PULLBACK_SCALP", "NO BEAR CONFIRM", pretrigger_text("CONF", last["close"] - (recent_low + atr_m1 * (0.18 if attack else 0.08)), atr_m1))
    if not strong_body:
        return blocked_signal("USTECH_PULLBACK_SCALP", "WEAK CANDLE", pretrigger_text("BODY", atr_m1 * (0.05 if attack else 0.06) - body, atr_m1))
    if not clean_short:
        return blocked_signal("USTECH_PULLBACK_SCALP", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - max(body * (1.25 if attack else 1.05), atr_m1 * (0.24 if attack else 0.16)), atr_m1))
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

    long_break = prev["close"] >= resistance + atr * (0.02 if attack else 0.04) and prev["close"] > prev["open"] and strong_break
    short_break = prev["close"] <= support - atr * (0.02 if attack else 0.04) and prev["close"] < prev["open"] and strong_break
    long_retest = last["low"] <= resistance + atr * (0.16 if attack else 0.10) and last["close"] >= resistance - atr * (0.03 if attack else 0.02)
    short_retest = last["high"] >= support - atr * (0.16 if attack else 0.10) and last["close"] <= support + atr * (0.03 if attack else 0.02)
    long_confirm = last["close"] > last["open"] and last["close"] >= max(resistance, prev["high"] - atr * (0.10 if attack else 0.03))
    short_confirm = last["close"] < last["open"] and last["close"] <= min(support, prev["low"] + atr * (0.10 if attack else 0.03))
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
    if hour < 8 or hour > 18:
        return blocked_signal("GER40_PULLBACK_SCALP", "OUT SESSION")

    df_m1 = get_data(symbol, mt5.TIMEFRAME_M1, 260)
    df_m15 = get_m15_cached(symbol)
    if df_m1 is None or len(df_m1) < 220:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO M1 DATA")
    if df_m15 is None or len(df_m15) < 60:
        return blocked_signal("GER40_PULLBACK_SCALP", "NO M15 DATA")

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
    ema50_m15 = df_m15["close"].ewm(span=50).mean().iloc[-1]
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

    long_trend = ema50.iloc[-1] > ema200.iloc[-1] and m15_close >= ema50_m15
    short_trend = ema50.iloc[-1] < ema200.iloc[-1] and m15_close <= ema50_m15
    long_pullback = min(prev["low"], last["low"]) <= ema50.iloc[-1] + atr_m1 * (0.20 if attack else 0.14) and min(prev["low"], last["low"]) >= ema200.iloc[-1] - atr_m1 * 0.10
    short_pullback = max(prev["high"], last["high"]) >= ema50.iloc[-1] - atr_m1 * (0.20 if attack else 0.14) and max(prev["high"], last["high"]) <= ema200.iloc[-1] + atr_m1 * 0.10
    long_confirm = last["close"] > last["open"] and last["close"] > ema50.iloc[-1] and last["close"] >= recent_high - atr_m1 * (0.16 if attack else 0.07)
    short_confirm = last["close"] < last["open"] and last["close"] < ema50.iloc[-1] and last["close"] <= recent_low + atr_m1 * (0.16 if attack else 0.07)

    if long_trend and long_pullback and long_confirm and strong_body and clean_long and rsi.iloc[-1] > 50:
        conf = 75
        if rsi.iloc[-1] > 57 and last["close"] >= ema50.iloc[-1] + atr_m1 * 0.07:
            conf = 80
        return {"signal": "BUY", "confidence": conf, "name": "GER40_PULLBACK_SCALP", "execution_df": df_m1}

    if short_trend and short_pullback and short_confirm and strong_body and clean_short and rsi.iloc[-1] < 50:
        conf = 75
        if rsi.iloc[-1] < 43 and last["close"] <= ema50.iloc[-1] - atr_m1 * 0.07:
            conf = 80
        return {"signal": "SELL", "confidence": conf, "name": "GER40_PULLBACK_SCALP", "execution_df": df_m1}

    if not long_trend and not short_trend:
        return blocked_signal("GER40_PULLBACK_SCALP", "EMA TREND MIXED", pretrigger_text("EMA", abs(ema50.iloc[-1] - ema200.iloc[-1]), atr_m1))
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
    session_start, session_end = broker_window_from_local_df(df_m1, 15, 45, 22, 15)
    broker_now = df_m1["time"].iloc[-1].to_pydatetime()

    if broker_now < session_start:
        return blocked_signal("US500_ORB", "BUILDING RANGE", "ORB 15:30-15:45")
    if broker_now > session_end:
        return blocked_signal("US500_ORB", "OUT SESSION", "ORB 15:45-22:15")

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

    trend_min = 0.12 if attack else 0.85
    long_trigger = orb_high + atr_m5 * (0.01 if attack else 0.04)
    short_trigger = orb_low - atr_m5 * (0.01 if attack else 0.04)
    long_hold = min(prev["low"], last["low"]) >= orb_high - atr_m5 * (0.16 if attack else 0.10)
    short_hold = max(prev["high"], last["high"]) <= orb_low + atr_m5 * (0.16 if attack else 0.10)
    strong_body = body >= atr_m5 * (0.05 if attack else 0.08)
    clean_long_candle = wick_up <= body * (1.05 if attack else 0.90)
    clean_short_candle = wick_down <= body * (1.05 if attack else 0.90)

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
        and price < vwap
        and price <= short_trigger
        and last["close"] <= prev["low"]
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
    if ema20 < ema50 and price >= vwap:
        return blocked_signal("US500_ORB", "VWAP LOST", pretrigger_text("VWAP", price - vwap, atr_m5))
    if ema20 > ema50 and price < long_trigger:
        return blocked_signal("US500_ORB", "NO BREAKOUT", pretrigger_text("BRK", long_trigger - price, atr_m5))
    if ema20 < ema50 and price > short_trigger:
        return blocked_signal("US500_ORB", "NO BREAKDOWN", pretrigger_text("BRK", price - short_trigger, atr_m5))
    if ema20 > ema50 and not long_hold:
        return blocked_signal("US500_ORB", "NO HOLD ABOVE ORB", pretrigger_text("HOLD", (orb_high - atr_m5 * 0.10) - min(prev["low"], last["low"]), atr_m5))
    if ema20 < ema50 and not short_hold:
        return blocked_signal("US500_ORB", "NO HOLD BELOW ORB", pretrigger_text("HOLD", max(prev["high"], last["high"]) - (orb_low + atr_m5 * 0.10), atr_m5))
    if not strong_body:
        return blocked_signal("US500_ORB", "WEAK OPEN DRIVE", pretrigger_text("BODY", atr_m5 * 0.08 - body, atr_m5))
    if ema20 > ema50 and not clean_long_candle:
        return blocked_signal("US500_ORB", "UPPER WICK HEAVY", pretrigger_text("WICK", wick_up - body * 0.90, atr_m5))
    if ema20 < ema50 and not clean_short_candle:
        return blocked_signal("US500_ORB", "LOWER WICK HEAVY", pretrigger_text("WICK", wick_down - body * 0.90, atr_m5))
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
    if df_m1 is None or len(df_m1) < 60:
        return blocked_signal("GER40_SCALP", "NO M1 DATA")

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

    trend_up = df["close"].ewm(span=20).mean().iloc[-1] > df["close"].ewm(span=50).mean().iloc[-1]
    trend_down = df["close"].ewm(span=20).mean().iloc[-1] < df["close"].ewm(span=50).mean().iloc[-1]

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]

    long_pullback_band = ema20.iloc[-1] + atr_m1 * 0.13
    short_pullback_band = ema20.iloc[-1] - atr_m1 * 0.13
    long_break_trigger = high_m1.iloc[-2] - atr_m1 * 0.16
    short_break_trigger = low_m1.iloc[-2] + atr_m1 * 0.16

    long_setup = (
        trend_up
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
        and ema9.iloc[-1] < ema20.iloc[-1] < ema50.iloc[-1]
        and max(prev["high"], last["high"]) >= short_pullback_band
        and last["close"] < ema9.iloc[-1]
        and (last["open"] - last["close"]) > atr_m1 * 0.08
        and last["close"] <= short_break_trigger
        and 24 <= rsi.iloc[-1] <= 50
    )
    if short_setup:
        return {"signal": "SELL", "confidence": 76, "name": "GER40_SCALP", "execution_df": df_m1}

    if trend_up:
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


register_strategy(
    "XAU_TREND",
    strategy_trend_vwap,
    tag="XTRD",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.60,
    min_score=0.43,
    cooldown_sec=35,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
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
    max_splits=5,
)
register_strategy(
    "XAU_ORB",
    strategy_xau_orb,
    tag="XORB",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.28,
    min_score=0.50,
    cooldown_sec=80,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.68,
    tp_rr_multiplier=1.82,
    auto_disable=False,
    be_trigger_progress=0.06,
    be_buffer_progress=0.10,
    trail_steps=[(0.14, 0.36), (0.24, 0.60), (0.36, 0.82)],
    session_label="14:30-22:30",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.50,
    multi_tp_target_eur=1.05,
    multi_tp_first_atr=0.11,
    multi_tp_curve=[1.0, 1.70, 2.50, 3.55, 4.90, 6.55, 8.55, 10.9],
    multi_tp_tp2_gap_eur=0.40,
    multi_tp_step_gap_eur=0.20,
    min_splits=4,
    max_splits=5,
)
register_strategy(
    "XAU_M1_SCALP",
    strategy_xau_m1_scalp,
    tag="XM1S",
    symbol_family="XAUUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.32,
    min_score=0.43,
    cooldown_sec=24,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=scalping_filter,
    market_filter_func=fast_market_filter_scalping,
    sl_atr_multiplier=0.74,
    tp_rr_multiplier=1.42,
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
    max_splits=8,
)
register_strategy(
    "BTC_TREND",
    strategy_btc_trend,
    enabled=False,
    tag="BTCR",
    symbol_family="BTCUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.24,
    min_score=0.48,
    cooldown_sec=40,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
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
register_strategy(
    "ETH_PULSE",
    strategy_eth_pulse,
    enabled=False,
    tag="ETHP",
    symbol_family="ETHUSD",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.20,
    min_score=0.46,
    cooldown_sec=36,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
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
    max_splits=8,
)
register_strategy(
    "USTECH_TREND",
    strategy_ustech_trend,
    tag="USTR",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.28,
    min_score=0.54,
    cooldown_sec=60,
    allowed_regimes={"TREND"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.80,
    tp_rr_multiplier=1.95,
    auto_disable=False,
    be_trigger_progress=0.15,
    be_buffer_progress=0.10,
    trail_steps=[(0.26, 0.16), (0.40, 0.34), (0.58, 0.56)],
    session_label="07-23",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.65, 2.35, 3.30, 4.45, 5.95],
    multi_tp_tp2_gap_eur=0.38,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=4,
)
register_strategy(
    "USTECH_ORB",
    strategy_ustech_orb,
    tag="UORB",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.22,
    min_score=0.58,
    cooldown_sec=125,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.62,
    tp_rr_multiplier=1.75,
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
    max_splits=5,
)
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
    "USTECH_PULLBACK_SCALP",
    strategy_ustech_pullback_scalp,
    tag="U1PB",
    symbol_family="USTECH",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.18,
    min_score=0.55,
    cooldown_sec=18,
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
    max_splits=8,
)
register_strategy(
    "JPN225_ORB",
    strategy_jpn225_orb,
    tag="JPOR",
    symbol_family="JPN225",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.30,
    min_score=0.50,
    cooldown_sec=70,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.84,
    tp_rr_multiplier=1.85,
    auto_disable=False,
    be_trigger_progress=0.16,
    be_buffer_progress=0.10,
    trail_steps=[(0.30, 0.12), (0.46, 0.26), (0.64, 0.44)],
    session_label="02:20-09:00",
    multi_tp_eur_min=0.70,
    multi_tp_eur_max=1.40,
    multi_tp_target_eur=0.95,
    multi_tp_first_atr=0.10,
    multi_tp_curve=[1.0, 1.65, 2.35, 3.30, 4.45, 5.95],
    multi_tp_tp2_gap_eur=0.36,
    multi_tp_step_gap_eur=0.18,
    min_splits=4,
    max_splits=6,
)
register_strategy(
    "US500_ORB",
    strategy_us500_orb,
    tag="U5OB",
    symbol_family="US500",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.20,
    min_score=0.57,
    cooldown_sec=120,
    allowed_regimes={"TREND", "RANGE"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.60,
    tp_rr_multiplier=1.72,
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
    "GER40_TREND",
    strategy_ger40_trend,
    tag="G4TR",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.28,
    min_score=0.64,
    cooldown_sec=75,
    allowed_regimes={"TREND"},
    filter_func=default_strategy_filter,
    market_filter_func=fast_market_filter,
    sl_atr_multiplier=0.82,
    tp_rr_multiplier=1.90,
    auto_disable=False,
    be_trigger_progress=0.18,
    be_buffer_progress=0.10,
    trail_steps=[(0.32, 0.10), (0.50, 0.24), (0.68, 0.42)],
    session_label="08-17",
    multi_tp_eur_min=0.60,
    multi_tp_eur_max=1.20,
    multi_tp_target_eur=0.90,
    multi_tp_first_atr=0.12,
    multi_tp_curve=[1.0, 1.7, 2.5, 3.6, 4.9, 6.5],
    multi_tp_tp2_gap_eur=0.34,
    multi_tp_step_gap_eur=0.17,
    min_splits=4,
    max_splits=6,
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
    "GER40_PULLBACK_SCALP",
    strategy_ger40_pullback_scalp,
    tag="G1PB",
    symbol_family="GER40",
    execution_mode="MULTI_ONLY",
    risk_multiplier=0.16,
    min_score=0.56,
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
    "GER40_SCALP",
    strategy_ger40_scalping,
    enabled=False,
    tag="G4SC",
    symbol_family="GER40",
    execution_mode="STD_ONLY",
    risk_multiplier=0.12,
    min_score=0.60,
    cooldown_sec=9,
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
    return round(min(1.0, raw_score), 3)


# ==============================================================================
# EXECUTION LAYER PRO (MULTI-TP)
# ==============================================================================
def open_scaled_trade(symbol, signal, df, strat_name, score=1.0, confidence=None, strategy_cfg=None, execution_df=None):
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

    live_pos = safe_positions()
    open_clusters = {
        (p.comment or "MANUAL").rsplit("_", 1)[0]
        for p in live_pos
        if p.symbol == symbol and p.magic == MAGIC_ID
    }
    if strategy_tag in open_clusters:
        set_live_log(symbol, f"⚠️ EXEC CANCEL: {strategy_tag} already active")
        return False, "ALREADY ACTIVE"
    symbol_cluster_cap = get_max_clusters_for_symbol(symbol)
    if len(open_clusters) >= symbol_cluster_cap:
        set_live_log(symbol, "⚠️ EXEC CANCEL: Max strategy clusters reached")
        return False, "CLUSTER LIMIT"

    price = tick.ask if signal == "BUY" else tick.bid
    working_df = execution_df if execution_df is not None else df
    atr = working_df["atr"].iloc[-1]
    sl_dist = atr * sl_atr_multiplier

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
        tp = price + sl_dist * tp_rr_multiplier if signal == "BUY" else price - sl_dist * tp_rr_multiplier
        sl = price - sl_dist if signal == "BUY" else price + sl_dist

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
    strategy_cfg = plan_cfg
    record_strategy_tp_plan(strat_name, plan_text)
    lot_per_trade = total_lot / splits

    if lot_per_trade < MIN_LOT_PER_SPLIT:
        splits = max(1, int(total_lot / MIN_LOT_PER_SPLIT))
        lot_per_trade = total_lot / splits

    lot_per_trade = round(lot_per_trade / info.volume_step) * info.volume_step

    if lot_per_trade < info.volume_min:
        set_live_log(symbol, "❌ MULTI-TP CANCEL: Lot too small")
        return False, "LOT TOO SMALL"

    success_count = 0
    successful_tp_prices = []
    successful_projected_profits = []
    spread_price = abs(tick.ask - tick.bid)
    tp_prices, projected_profits = build_dynamic_multi_tp_prices(
        symbol, signal, price, info, lot_per_trade, atr, spread_price, strategy_cfg, splits
    )
    set_live_log(symbol, f"📤 SEND MULTI ORDER | {splits} x {lot_per_trade} | TP1~{projected_profits[0]}€")

    for i in range(splits):
        tp_price = tp_prices[i]
        sl_price = price - sl_dist if signal == "BUY" else price + sl_dist

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot_per_trade),
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
            successful_projected_profits.append(float(projected_profits[i]))
        time.sleep(0.05)

    if success_count > 0:
        cluster_key = (symbol, strategy_tag)
        cluster_state = trade_memory.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
        cluster_profile = get_cluster_profile(strategy_cfg, cluster_state)
        effective_quality = float(strategy_cfg.get("effective_tp_quality", 0.5) or 0.5)
        tp_high_conf = bool(effective_quality >= 0.62 or confidence_value >= 80.0)
        tp_be_after_legs = 2 if tp_high_conf else 1
        tp_lock_offset = 2 if tp_high_conf else 1
        early_target = sum(successful_projected_profits[: min(2, len(successful_projected_profits))]) if successful_projected_profits else 0.0
        weak_floor = max(cluster_profile["weak_profit_floor"], early_target * clamp(0.52 + (0.5 - cluster_profile["quality"]) * 0.30, 0.34, 0.72))
        cluster_state.update(
            {
                "opened_at": datetime.now(),
                "signal": signal,
                "score": round(score, 3),
                "confidence": round(confidence_value, 1),
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
                "tp_high_conf": tp_high_conf,
                "tp_be_after_legs": tp_be_after_legs,
                "tp_lock_offset": tp_lock_offset,
                "tp1_projected": successful_projected_profits[0] if successful_projected_profits else 0.0,
                "tp2_projected": successful_projected_profits[1] if len(successful_projected_profits) > 1 else (successful_projected_profits[0] if successful_projected_profits else 0.0),
                "abort_cluster": False,
                "abort_reason": "",
            }
        )
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
        if p.comment:
            position_strategy_map[p.ticket] = p.comment.rsplit("_", 1)[0]
        strategy_tag = position_strategy_map.get(p.ticket) or (p.comment.rsplit("_", 1)[0] if p.comment else None)
        if not strategy_tag:
            continue
        cluster_key = (p.symbol, strategy_tag)
        meta = cluster_live.setdefault(cluster_key, {"open_profit": 0.0, "open_count": 0})
        meta["open_profit"] += p.profit
        meta["open_count"] += 1
        cluster_positions.setdefault(cluster_key, []).append(p)

    for cluster_key, live_positions in cluster_positions.items():
        hydrate_cluster_tp_state(cluster_key, live_positions)

    cluster_abort_logged = set()
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        strategy_tag = position_strategy_map.get(p.ticket) or (p.comment.rsplit("_", 1)[0] if p.comment else None)
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
        if positive_legs >= 2 and early_profit < weak_floor and cluster_meta.get("open_profit", 0.0) <= max(0.15, early_profit * cluster_profile["early_cashout_ratio"]):
            closed = close_strategy_cluster(p.symbol, strategy_tag)
            if closed > 0 and (p.symbol, strategy_tag) not in cluster_abort_logged:
                strategy_cfg = get_strategy_by_tag(strategy_tag)
                strategy_name = strategy_cfg["name"] if strategy_cfg else strategy_tag
                set_live_log(p.symbol, f"💸 CLUSTER CASHOUT {strategy_tag} | weak TP1/TP2")
                record_strategy_event(strategy_name, "WEAK EARLY CASHOUT")
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
    for p in positions:
        if p.magic != MAGIC_ID:
            continue
        if p.comment:
            position_strategy_map[p.ticket] = p.comment.rsplit("_", 1)[0]

        info = safe_info(p.symbol)
        tick = safe_tick(p.symbol)
        if not info or not tick:
            continue

        price = tick.bid if p.type == 0 else tick.ask
        entry = p.price_open
        profit_points = (price - entry) / info.point if p.type == 0 else (entry - price) / info.point
        strategy_tag = position_strategy_map.get(p.ticket) or (p.comment.rsplit("_", 1)[0] if p.comment else None)
        strategy_cfg = get_strategy_by_tag(strategy_tag) if strategy_tag else None
        strategy_name = strategy_cfg["name"] if strategy_cfg else strategy_tag
        is_orb_cluster = bool(strategy_cfg and strategy_cfg["name"].endswith("_ORB"))

        if int(time.time()) % 10 == 0:
            set_live_log(p.symbol, f"📌 MANAGING | PnL: {round(p.profit,2)}€ ({round(profit_points,1)} pts)")

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
                set_live_log(p.symbol, f"🔒 SL UPDATED {strategy_tag or ''} {hit_label}".strip())

        elif p.type == 1:
            if (p.sl == 0 or new_sl < p.sl) and new_sl > (price + min_dist):
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
                set_live_log(p.symbol, f"🔒 SL UPDATED {strategy_tag or ''} {hit_label}".strip())


# ==============================================================================
# MAIN ENGINE
# ==============================================================================
def learn_from_history():
    global last_history_check, session_stats, performance_memory, legacy_stats

    now = datetime.now()
    broker_offset = get_live_broker_offset()
    broker_session_started_at = get_broker_session_start()
    broker_now = now + broker_offset
    seed_start = broker_session_started_at - timedelta(days=2)
    seed_deals = mt5.history_deals_get(seed_start, broker_now) or []
    if not seed_deals:
        return

    fresh_position_map = {}
    fresh_position_comment_map = {}
    for d in seed_deals:
        if d.magic != MAGIC_ID:
            continue
        position_id = getattr(d, "position_id", None) or getattr(d, "position", None)
        if d.entry == mt5.DEAL_ENTRY_IN and d.comment and position_id:
            strategy_tag = TAG_ALIASES.get(d.comment.rsplit("_", 1)[0], d.comment.rsplit("_", 1)[0])
            fresh_position_map[position_id] = strategy_tag
            fresh_position_comment_map[position_id] = d.comment

    deals = [
        d
        for d in seed_deals
        if d.magic == MAGIC_ID and d.entry == mt5.DEAL_ENTRY_OUT and getattr(d, "time", 0) >= broker_session_started_at.timestamp()
    ]
    deals = sorted(deals, key=lambda d: (getattr(d, "time", 0), _deal_ticket(d) or 0))

    def apply_closed_deal(d, session_acc, perf_acc, strategy_acc, legacy_acc, trade_acc, latest_acc):
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
        strategy_tag = fresh_position_map.get(position_id)
        deal_time = datetime.fromtimestamp(getattr(d, "time", 0)) if getattr(d, "time", 0) else now

        if strategy_tag:
            strategy_cfg = get_strategy_by_tag(strategy_tag)
            if strategy_cfg:
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

            cluster_key = (d.symbol, strategy_tag)
            cluster_state = trade_acc.setdefault(cluster_key, {"legs": {}, "abort_cluster": False})
            leg_comment = fresh_position_comment_map.get(position_id, "")
            leg_index = extract_leg_index(leg_comment)
            exit_comment = str(getattr(d, "comment", "") or "").upper()
            if "[TP" in exit_comment:
                exit_reason = "TP"
            elif "[SL" in exit_comment:
                exit_reason = "SL"
            else:
                exit_reason = "OTHER"

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
                weak_profit_floor = 0.35
                strategy_cfg = get_strategy_by_tag(strategy_tag)
                if strategy_cfg:
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

    latest_strategy_result = {}
    if not processed_deals:
        fresh_session_stats = {"profit": 0.0, "trades": 0, "wins": 0, "losses": 0, "flat": 0}
        fresh_performance = {"wins": 0, "losses": 0}
        fresh_strategy_stats = _blank_strategy_stats()
        fresh_legacy_stats = _blank_stats_row()
        fresh_trade_memory = {}
        for d in deals:
            apply_closed_deal(d, fresh_session_stats, fresh_performance, fresh_strategy_stats, fresh_legacy_stats, fresh_trade_memory, latest_strategy_result)
            deal_ticket = _deal_ticket(d)
            if deal_ticket is not None:
                processed_deals.add(deal_ticket)
        session_stats = fresh_session_stats
        performance_memory = fresh_performance
        legacy_stats = fresh_legacy_stats
        for strategy_name, stats in fresh_strategy_stats.items():
            strategy_stats[strategy_name] = stats
        trade_memory.clear()
        trade_memory.update(fresh_trade_memory)
    else:
        fresh_session_stats = dict(session_stats)
        fresh_performance = dict(performance_memory)
        fresh_strategy_stats = _blank_strategy_stats()
        for strategy_name, stats in strategy_stats.items():
            if strategy_name in fresh_strategy_stats:
                fresh_strategy_stats[strategy_name].update(stats)
        fresh_legacy_stats = dict(legacy_stats)
        fresh_trade_memory = copy.deepcopy(trade_memory)
        for d in deals:
            deal_ticket = _deal_ticket(d)
            if deal_ticket is None or deal_ticket in processed_deals:
                continue
            apply_closed_deal(d, fresh_session_stats, fresh_performance, fresh_strategy_stats, fresh_legacy_stats, fresh_trade_memory, latest_strategy_result)
            processed_deals.add(deal_ticket)
        session_stats = fresh_session_stats
        performance_memory = fresh_performance
        legacy_stats = fresh_legacy_stats
        for strategy_name, stats in fresh_strategy_stats.items():
            strategy_stats[strategy_name] = stats
        trade_memory.clear()
        trade_memory.update(fresh_trade_memory)

    position_strategy_map.clear()
    position_strategy_map.update(fresh_position_map)
    for strategy_name, result_data in latest_strategy_result.items():
        profit, result_time = result_data
        record_strategy_result(strategy_name, profit, result_time)

    evaluate_strategy_health()
    last_history_check = datetime.now()


def run_cycle():
    global last_heartbeat_time, last_global_trade_time, debug_state
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

    for s in symbols:
        debug_state[s] = {
            "data": False,
            "signal": "NEUTRAL",
            "blocked_by": "NONE",
            "stage": "INIT",
            "final": "WAITING",
        }
        update_radar_state(s, "SCAN", 0, "---", "SCANNING")

        set_live_log(s, "🔄 SCAN START")
        push_debug(s, "PRE-CHECK", "OK")

        if not allow_trading(s):
            for strategy in STRATEGIES:
                symbol_family = strategy.get("symbol_family")
                if symbol_family and symbol_in_family(s, symbol_family):
                    set_strategy_pretrigger(strategy["name"], f"SESSION {strategy.get('session_label', '24H')}")
                    record_strategy_event(strategy["name"], "NO SETUP OUT SESSION")
            push_debug(s, "PRE-CHECK", "OUT OF SESSION")
            debug_state[s]["final"] = "SKIPPED"
            update_radar_state(s, "WAIT", 0, "---", "OUT OF SESSION")
            set_live_log(s, "🚫 BLOCKED: Out of Session")
            continue

        df = get_data(s, mt5.TIMEFRAME_M5, 300)
        if df is None:
            push_debug(s, "DATA", "NO M5 DATA")
            debug_state[s]["final"] = "NO DATA"
            update_radar_state(s, "WAIT", 0, "---", "NO DATA")
            set_live_log(s, "🚫 BLOCKED: No Data")
            continue

        debug_state[s]["data"] = True
        set_live_log(s, "📊 DATA OK")
        push_debug(s, "DATA", "OK")

        regime = detect_market_regime(df)
        set_live_log(s, f"📈 REGIME: {regime}")
        push_debug(s, "REGIME", regime)

        if regime == "RANGE":
            set_live_log(s, "⚠️ RANGE MODE (Continuing)")

        if regime == "CHAOS":
            push_debug(s, "REGIME", "CHAOS")
            debug_state[s]["final"] = "CHAOS"
            update_radar_state(s, "WAIT", 0, "---", "CHAOS")
            set_live_log(s, "🚫 BLOCKED: Chaos Market")
            continue

        mode_str = "MULTI" if should_use_multi_tp(df, s) else "STD"
        set_live_log(s, f"⚙️ MODE: {mode_str}")

        active_signals = []
        blocked_signals = []
        for strategy in STRATEGIES:
            if not strategy["enabled"]:
                continue
            symbol_family = strategy.get("symbol_family")
            if symbol_family and not symbol_in_family(s, symbol_family):
                continue

            res = strategy["func"](df, s)
            if res is None:
                set_strategy_pretrigger(strategy["name"], "---")
                record_strategy_event(strategy["name"], "NO SETUP")
                blocked_signals.append({"name": strategy["name"], "reason": "NO SETUP", "pretrigger": "---"})
                continue
            if res.get("blocked"):
                set_strategy_pretrigger(strategy["name"], res.get("pretrigger", "---"))
                record_strategy_event(strategy["name"], f"NO SETUP {res['blocked']}")
                blocked_signals.append(
                    {
                        "name": strategy["name"],
                        "reason": str(res.get("blocked", "NO SETUP")),
                        "pretrigger": res.get("pretrigger", "---"),
                    }
                )
                continue

            sig = res["signal"]
            conf = res["confidence"]
            strat_name = res["name"]
            record_strategy_signal(strat_name, s, sig, conf)

            active_signals.append(
                {
                    "signal": sig,
                    "confidence": conf,
                    "name": strat_name,
                    "cfg": strategy,
                    "execution_df": res.get("execution_df", df),
                }
            )
            set_live_log(s, f"🧠 {strat_name} -> {sig} ({round(conf, 1)}%)")

        debug_state[s]["signal"] = ",".join([x["name"] for x in active_signals]) if active_signals else "NONE"

        if not active_signals:
            primary_block = None
            for blocked in blocked_signals:
                if blocked["reason"] not in {"NO SETUP", "SCALP OFF"}:
                    primary_block = blocked
                    break
            if primary_block is None and blocked_signals:
                primary_block = blocked_signals[0]

            radar_reason = primary_block["reason"] if primary_block else "MONITORING"
            radar_strat = primary_block["name"] if primary_block else "---"
            push_debug(s, "SIGNAL", "NO SETUP")
            debug_state[s]["blocked_by"] = radar_reason
            debug_state[s]["final"] = "NO SIGNAL"
            set_live_log(s, f"⛔ NO SIGNAL | {radar_strat}: {radar_reason}")
            update_radar_state(s, "WAIT", 0, radar_strat, radar_reason[:26])
            continue

        decision_stats["signals"] += len(active_signals)
        session_decision_stats["signals"] += len(active_signals)
        push_debug(s, "SIGNAL", debug_state[s]["signal"])
        update_radar_state(
            s,
            active_signals[0]["signal"],
            int(active_signals[0]["confidence"]),
            debug_state[s]["signal"],
            "MULTI SIGNAL",
        )

        executed = 0
        last_reason = "WAITING"

        for signal_data in active_signals:
            if executed >= MAX_TRADES_PER_CYCLE:
                set_live_log(s, f"⏸️ LIMIT REACHED: {MAX_TRADES_PER_CYCLE} trades/cycle")
                last_reason = "CYCLE LIMIT"
                break

            sig = signal_data["signal"]
            conf = signal_data["confidence"]
            strat_name = signal_data["name"]
            strategy_cfg = signal_data["cfg"]
            execution_df = signal_data.get("execution_df", df)
            cooldown_key = (s, strat_name)
            allowed_regimes = strategy_cfg.get("allowed_regimes", set())

            if allowed_regimes and regime not in allowed_regimes:
                record_strategy_event(strat_name, f"REGIME {regime}")
                set_live_log(s, f"🚫 {strat_name}: REGIME {regime}")
                last_reason = f"REGIME {regime}"
                continue

            cooldown_sec = strategy_cfg.get("cooldown_sec", 60)
            if time.time() - last_strategy_trade_time.get(cooldown_key, 0) < cooldown_sec:
                record_strategy_event(strat_name, "COOLDOWN")
                set_live_log(s, f"🚫 {strat_name}: COOLDOWN {cooldown_sec}s")
                last_reason = f"{strat_name} COOLDOWN"
                continue

            runtime = strategy_runtime.get(strat_name, {})
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
                last_reason = f"{strat_name} LOSS BRAKE"
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
                last_reason = "FAST MARKET"
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
                last_reason = "LOW SCORE"
                continue

            filter_func = strategy_cfg.get("filter_func") or default_strategy_filter
            v, reason = filter_func(s, sig, score, df, execution_df, strategy_cfg)
            if not v:
                record_strategy_event(strat_name, f"FILTER {reason}")
                push_debug(s, "FILTER", reason)
                decision_stats["filtered"] += 1
                session_decision_stats["filtered"] += 1
                debug_state[s]["final"] = "FILTERED"
                update_radar_state(s, sig, int(score * 100), strat_name, f"FILTERED: {reason}")
                set_live_log(s, f"🚫 {strat_name}: {reason}")
                last_reason = reason
                continue

            set_live_log(s, f"✅ {strat_name}: ENTRY {sig}")
            record_strategy_event(strat_name, f"ENTRY {sig}")
            push_debug(s, "EXECUTION", f"ATTEMPTING {strat_name}")
            debug_state[s]["final"] = "READY"

            trade_ok, trade_status = open_scaled_trade(s, sig, df, strat_name, score, conf, strategy_cfg, execution_df)
            if trade_ok:
                executed += 1
                last_global_trade_time = time.time()
                last_strategy_trade_time[cooldown_key] = time.time()
                decision_stats["executed"] += 1
                session_decision_stats["executed"] += 1
                record_strategy_event(strat_name, "LIVE")
                set_live_log(s, f"🚀 {strat_name}: EXECUTED")
                push_debug(s, "DONE", "CLEARED")
                debug_state[s]["final"] = "EXECUTED"
                update_radar_state(s, sig, int(score * 100), strat_name, "EXECUTED")
                last_reason = "EXECUTED"
            else:
                normalized_status = str(trade_status or "ORDER FAIL").upper()
                if normalized_status == "ALREADY ACTIVE":
                    record_strategy_event(strat_name, "LIVE")
                    push_debug(s, "DONE", "LIVE")
                    debug_state[s]["final"] = "LIVE"
                    update_radar_state(s, sig, int(score * 100), strat_name, "LIVE")
                    last_reason = "LIVE"
                else:
                    record_strategy_event(strat_name, normalized_status)
                    push_debug(s, "DONE", normalized_status)
                    debug_state[s]["final"] = normalized_status
                    update_radar_state(s, sig, int(score * 100), strat_name, normalized_status)
                    last_reason = normalized_status

        if executed == 0 and debug_state[s]["final"] == "WAITING":
            debug_state[s]["final"] = last_reason

    time.sleep(0.3)
