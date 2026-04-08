import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
from datetime import datetime
import threading
import warnings
import traceback

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# CONFIGURAZIONE (V 52.0.0 - THE GLASS BOX - MULTI-TP PROGRESSIVE)
# ==============================================================================
PROFIT_MODE = True
PRIMARY_SYMBOL = "XAUUSD"
SECONDARY_SYMBOLS = []
CRYPTO_SYMBOLS = []

symbols_list = [PRIMARY_SYMBOL, *SECONDARY_SYMBOLS]
symbols = symbols_list.copy() 

TIMEFRAME_MAIN = mt5.TIMEFRAME_M5 
MAX_UNIQUE_SYMBOLS = 1
MAX_CLUSTERS_PER_SYMBOL = 1 

MAX_NEW_TRADES_PER_CYCLE = 1    
RISK_PER_TRADE_PERCENT = 0.5 
MAX_TOTAL_RISK = 0.08 

ASSET_WEIGHTS = {"XAUUSD": 1.0}

MAX_RETRIES = 3
BASE_DEVIATION = 10
MAX_DEVIATION = 50
RETRY_DELAY = 0.4
MAGIC_ID = 600000 

MAX_LOG_LINES = 50 

# ==============================================================================
# MULTI TP ENGINE CONFIG 🔴
# ==============================================================================
USE_MULTI_TP = True
TP_POINTS = [150, 200, 300, 500, 700, 1000, 1500, 2000]
SPLITS = len(TP_POINTS)
SPREAD_BUFFER_POINTS = 50

# Attivazione Intelligente
MIN_ATR_MULTIPLIER = 1.5
MIN_TREND_STRENGTH = 0.6

# Sicurezza Lotto
MIN_LOT_PER_SPLIT = 0.01
MAX_SPLITS = 8

# ==============================================================================
# GLOBALS & MEMORY
# ==============================================================================
latest_log_message = ""
latest_live_analysis_msgs = [] 
last_trade_time = {} 
last_signal = {s: "NONE" for s in symbols_list}
last_global_trade_time = 0 
last_heartbeat_time = 0

bot_killed = False 
crypto_enabled = True

mt5_lock = threading.Lock()

performance_memory = {"wins": 0, "losses": 0}
strategy_stats = {} 
asset_performance = {} 
session_stats = {"profit": 0.0, "trades": 0} 
equity_peak = 0.0
equity_mode = "NORMAL"
ai_mode = "STABLE"

processed_deals = set() 
trade_memory = {} 

last_history_check = datetime.now() 

m1_cache = {}
m1_cache_time = {}
m15_cache = {}
m15_cache_time = {}

radar_state = {s: {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "INIT", "live_conf": "N/A", "action": "---"} for s in symbols_list}
decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
debug_state = {s: {"data": False, "signal": "NONE", "blocked_by": "INIT", "stage": "INIT", "final": "NONE"} for s in symbols_list}
market_state = {"quality": 0.0, "pause_active": False, "pause_reason": "OK"}

RETCODE_RETRY = {
    mt5.TRADE_RETCODE_REQUOTE, mt5.TRADE_RETCODE_PRICE_OFF,
    mt5.TRADE_RETCODE_REJECT, mt5.TRADE_RETCODE_INVALID_PRICE,
    mt5.TRADE_RETCODE_PRICE_CHANGED, mt5.TRADE_RETCODE_CONNECTION
}

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
        radar_state[symbol] = {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "INIT", "live_conf": "N/A", "action": "---"}
    if symbol not in debug_state:
        debug_state[symbol] = {"data": False, "signal": "NONE", "blocked_by": "INIT", "stage": "INIT", "final": "NONE"}

def refresh_symbols():
    global symbols, symbols_list
    base_symbols = _unique_symbols([PRIMARY_SYMBOL, *SECONDARY_SYMBOLS])
    crypto_symbols = _unique_symbols(CRYPTO_SYMBOLS) if crypto_enabled else []
    active_symbols = _unique_symbols([*base_symbols, *crypto_symbols])
    if MAX_UNIQUE_SYMBOLS > 0:
        active_symbols = active_symbols[:MAX_UNIQUE_SYMBOLS]
    symbols_list = base_symbols
    symbols = active_symbols
    for symbol in active_symbols:
        _ensure_symbol_state(symbol)
    return symbols

refresh_symbols()

# ==============================================================================
# THREAD-SAFE DATA ACCESS & UTILS
# ==============================================================================
def safe_tick(symbol):
    with mt5_lock: return mt5.symbol_info_tick(symbol)

def safe_info(symbol):
    with mt5_lock: return mt5.symbol_info(symbol)

def safe_positions():
    with mt5_lock: return mt5.positions_get() or []

def points_to_price(symbol, points):
    info = safe_info(symbol)
    if not info: return 0
    return points * info.point

def compute_trend_strength(df):
    ema20 = df['close'].ewm(span=20).mean()
    ema50 = df['close'].ewm(span=50).mean()
    atr = df['atr'].iloc[-1]
    return abs(ema20.iloc[-1] - ema50.iloc[-1]) / max(atr, 1e-6)

def should_use_multi_tp(df, symbol):
    atr = df['atr'].iloc[-1]
    price = df['close'].iloc[-1]
    atr_ratio = atr / price
    trend_strength = compute_trend_strength(df)

    if atr_ratio < 0.001: return False
    if trend_strength < MIN_TREND_STRENGTH: return False
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
        radar_state[symbol] = {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "INIT", "live_conf": "N/A", "action": "---"}
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
    _refresh_runtime_modes()
    acc = mt5.account_info()
    total = performance_memory["wins"] + performance_memory["losses"]
    return {
        "ai_mode": ai_mode,
        "equity_mode": equity_mode,
        "drawdown": round(get_drawdown() * 100, 2),
        "winrate": round(_get_winrate() * 100, 1),
        "signals": decision_stats["signals"],
        "filtered": decision_stats["filtered"],
        "executed": decision_stats["executed"],
        "equity": round(acc.equity, 2) if acc else 0.0,
        "balance": round(acc.balance, 2) if acc else 0.0,
        "session_profit": round(session_stats["profit"], 2),
        "closed_trades": session_stats["trades"],
        "tracked_results": total
    }

def toggle_crypto(state):
    global crypto_enabled
    crypto_enabled = bool(state)
    refresh_symbols()
    if crypto_enabled and CRYPTO_SYMBOLS:
        set_log(f"🌐 CRYPTO ENABLED: {', '.join(CRYPTO_SYMBOLS)}")
    elif crypto_enabled:
        set_log("🌐 CRYPTO ENABLED: nessun simbolo crypto configurato")
    else:
        set_log("🌐 CRYPTO DISABLED")

# ==============================================================================
# MANUAL OVERRIDES 
# ==============================================================================
def close_only_profit():
    closed = 0
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID: continue
        if p.profit > 0:
            tick = safe_tick(p.symbol)
            if tick:
                with mt5_lock:
                    mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY, "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "magic": MAGIC_ID})
                closed += 1
    if closed > 0: set_log(f"💰 CLOSED PROFIT ONLY: {closed} trades")

def force_break_even_plus(buffer_ratio=0.2):
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID:
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
            new_sl = min(p.price_open + buffer, current_price - info.point * 10)
            if new_sl > p.sl:
                with mt5_lock:
                    mt5.order_send({
                        "action": mt5.TRADE_ACTION_SLTP,
                        "symbol": p.symbol,
                        "position": p.ticket,
                        "sl": round(new_sl, info.digits),
                        "tp": p.tp
                    })
        else:
            new_sl = max(p.price_open - buffer, current_price + info.point * 10)
            if p.sl == 0 or new_sl < p.sl:
                with mt5_lock:
                    mt5.order_send({
                        "action": mt5.TRADE_ACTION_SLTP,
                        "symbol": p.symbol,
                        "position": p.ticket,
                        "sl": round(new_sl, info.digits),
                        "tp": p.tp
                    })

    set_log("🛡️ BE+ APPLIED (MANUAL OVERRIDE)")

def close_all_now():
    set_log("⚠️ CHIUSURA TOTALE...")
    positions = safe_positions()
    for p in positions:
        if p.magic == MAGIC_ID: close_position(p)

def close_position(p):
    tick = safe_tick(p.symbol)
    if not tick: return
    with mt5_lock: 
        res = mt5.order_send({
            "action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume,
            "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY,
            "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "magic": MAGIC_ID
        })
    if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
        set_log(f"❌ CLOSE FAIL: {p.symbol}")

# ==============================================================================
# CORE DATA & REGIME
# ==============================================================================
def get_data(symbol, timeframe, n_bars=300):
    with mt5_lock: 
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
    if rates is None or len(rates) < 50: return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['vol_price'] = df['close'] * df['tick_volume']
    df['date'] = df['time'].dt.date
    df['cum_vol'] = df.groupby('date')['tick_volume'].cumsum()
    df['cum_vol_price'] = df.groupby('date')['vol_price'].cumsum()
    if df['cum_vol'].iloc[-1] == 0: return None
    df['vwap'] = df['cum_vol_price'] / df['cum_vol']
    df.drop(columns=['vol_price', 'date'], inplace=True)
    df['tr0'] = abs(df['high'] - df['low'])
    df['tr1'] = abs(df['high'] - df['close'].shift(1))
    df['tr2'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
    df['atr'] = df['tr'].rolling(14).mean()
    return df.dropna()

def get_m1_cached(symbol):
    global m1_cache, m1_cache_time
    if symbol not in m1_cache or time.time() - m1_cache_time.get(symbol, 0) > 2:
        m1_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M1, 50)
        m1_cache_time[symbol] = time.time()
    return m1_cache[symbol]

def get_m15_cached(symbol):
    global m15_cache, m15_cache_time
    if symbol not in m15_cache or time.time() - m15_cache_time.get(symbol, 0) > 60: 
        m15_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M15, 100)
        m15_cache_time[symbol] = time.time()
    return m15_cache[symbol]

def detect_market_regime(df):
    if df is None or len(df) < 50: return "UNKNOWN"
    atr = df['atr'].iloc[-1]
    price = df['close']
    ema20 = price.ewm(span=20).mean()
    ema50 = price.ewm(span=50).mean()
    trend_strength = abs(ema20.iloc[-1] - ema50.iloc[-1]) / max(atr, 1e-6)
    if trend_strength > 0.8: return "TREND"
    return "RANGE"

# ==============================================================================
# FILTERS & RISK
# ==============================================================================
def final_filter(symbol, df, score):
    atr = df['atr'].iloc[-1]
    price = df['close'].iloc[-1]
    momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5])
    vol = atr / price
    if score < 0.45: return False, f"LOW SCORE ({round(score,2)})"
    if momentum < atr * 0.1: return False, "WEAK MOMENTUM"
    if vol < 0.0005: return False, "ULTRA LOW VOL"
    tick = safe_tick(symbol)
    if tick and (tick.ask - tick.bid) > atr * 0.1: return False, "HIGH SPREAD"
    return True, "OK"

def allow_trading(symbol):
    hour = datetime.now().hour
    if symbol == PRIMARY_SYMBOL: 
        return 0 <= hour <= 23 
    return False

def fast_market_filter(symbol):
    t1 = safe_tick(symbol)
    time.sleep(0.02) 
    t2 = safe_tick(symbol)
    if not t1 or not t2: return False
    move = abs(t2.bid - t1.bid)
    return move <= t1.bid * 0.004

def compute_dynamic_risk():
    return RISK_PER_TRADE_PERCENT / 100.0

def get_total_open_risk():
    acc = mt5.account_info()
    if not acc: return 0
    p = safe_positions()
    risk_money = sum([abs(pos.price_open - pos.sl) * pos.volume for pos in p if pos.magic == MAGIC_ID and pos.sl != 0])
    return risk_money / acc.equity

def get_correlation_group(symbol):
    if symbol in ["US100", "GER40"]: return "INDEX"
    if symbol in ["EURUSD", "GBPUSD"]: return "FX_USD"
    return symbol

def correlated_directional_exposure(target_symbol, target_signal):
    risk_money = 0
    acc = mt5.account_info()
    if not acc: return 0
    target_group = get_correlation_group(target_symbol)
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID or get_correlation_group(p.symbol) != target_group: continue
        if target_signal == "BUY" and p.type != 0: continue
        if target_signal == "SELL" and p.type != 1: continue
        info = safe_info(p.symbol)
        if info and info.trade_tick_size > 0:
            loss_ticks = abs(p.price_open - p.sl) / info.trade_tick_size
            risk_money += loss_ticks * info.trade_tick_value * p.volume
    return risk_money / acc.equity

# ==============================================================================
# STRATEGIA
# ==============================================================================
def get_multi_tf_signal(symbol):
    df_m1, df_m5, df_m15 = get_m1_cached(symbol), get_data(symbol, mt5.TIMEFRAME_M5, 50), get_m15_cached(symbol)
    if any(d is None for d in [df_m1, df_m5, df_m15]): return "NEUTRAL", 0
    def trend(df): return 1 if df['close'].ewm(span=20).mean().iloc[-1] > df['close'].ewm(span=50).mean().iloc[-1] else -1
    
    t1 = trend(df_m1)
    t5 = trend(df_m5)
    t15 = trend(df_m15)
    alignment = t1 + t5 + t15
    
    if alignment >= 2: return "BUY", 3
    elif alignment <= -2: return "SELL", 3
    return "NEUTRAL", 0

def gold_profit_entry(df):
    price = df['close'].iloc[-1]
    vwap = df['vwap'].iloc[-1]
    prev_close = df['close'].iloc[-2]
    ema20 = df['close'].ewm(span=20).mean().iloc[-1]
    ema50 = df['close'].ewm(span=50).mean().iloc[-1]
    atr = df['atr'].iloc[-1]
    
    conf = 60 
    
    if ema20 > ema50 and price > vwap:
        if abs(price - vwap) < atr * 0.8:
            return "BUY", conf, "GLD_FLOW"

    if ema20 < ema50 and price < vwap:
        if abs(price - vwap) < atr * 0.8:
            return "SELL", conf, "GLD_FLOW"

    return "NEUTRAL", 0, "---"

def compute_asset_score(symbol, df, sig, conf):
    if sig == "NEUTRAL": return 0
    atr = df['atr'].iloc[-1]
    close_price = df['close'].iloc[-1]
    momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5])
    vol_score = min(1.0, (atr / close_price) * 100)
    mom_score = min(1.0, momentum / max(atr, 1e-6)) 
    base_score = conf / 100.0
    raw_score = (base_score + vol_score + mom_score) / 3.0
    return round(min(1.0, raw_score), 3)

# ==============================================================================
# EXECUTION LAYER PRO (MULTI-TP 🔴)
# ==============================================================================
def open_scaled_trade(symbol, signal, df, strat_name, score=1.0):
    acc, info, tick = mt5.account_info(), safe_info(symbol), safe_tick(symbol)
    if not acc or not info or not tick or info.trade_tick_size <= 0: 
        set_live_log(symbol, "❌ EXEC CANCEL: Missing Info")
        return False

    live_pos = safe_positions()
    if len([p for p in live_pos if p.symbol == symbol and p.magic == MAGIC_ID]) >= MAX_CLUSTERS_PER_SYMBOL:
        set_live_log(symbol, "⚠️ EXEC CANCEL: Pos already open")
        return False

    price = tick.ask if signal == "BUY" else tick.bid
    atr = df['atr'].iloc[-1]
    sl_dist = atr * 1.2
    
    risk_money = acc.equity * compute_dynamic_risk()
    total_lot = risk_money / ((sl_dist / info.trade_tick_size) * info.trade_tick_value)
    total_lot = round(max(info.volume_min, min(total_lot, info.volume_max * 0.3)) / info.volume_step) * info.volume_step

    if total_lot <= 0 or np.isnan(total_lot): return False

    # ===== DECISIONE MULTI TP =====
    use_multi = USE_MULTI_TP and should_use_multi_tp(df, symbol)

    if not use_multi:
        tp = price + sl_dist*2 if signal == "BUY" else price - sl_dist*2
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
            "comment": f"{strat_name[:8]}_STD",
            "type_filling": mt5.ORDER_FILLING_IOC
        }

        set_live_log(symbol, f"📤 SEND STD ORDER | Lot: {total_lot}")
        with mt5_lock: res = mt5.order_send(request)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            last_trade_time[symbol] = time.time()
            last_signal[symbol] = signal
            return True
        else:
            set_live_log(symbol, f"❌ ORDER FAIL: {res.retcode if res else 'NULL'}")
            return False

    # ===== MULTI TP (FAN OUT) =====
    splits = min(SPLITS, MAX_SPLITS)
    lot_per_trade = total_lot / splits

    if lot_per_trade < MIN_LOT_PER_SPLIT:
        splits = max(1, int(total_lot / MIN_LOT_PER_SPLIT))
        lot_per_trade = total_lot / splits

    lot_per_trade = round(lot_per_trade / info.volume_step) * info.volume_step
    
    if lot_per_trade < info.volume_min:
        set_live_log(symbol, "❌ MULTI-TP CANCEL: Lot too small")
        return False

    success_count = 0
    set_live_log(symbol, f"📤 SEND MULTI ORDER | {splits} Splits x {lot_per_trade} Lot")

    for i in range(splits):
        tp_points = TP_POINTS[i] if i < len(TP_POINTS) else TP_POINTS[-1]
        tp_price = price + points_to_price(symbol, tp_points) if signal == "BUY" else price - points_to_price(symbol, tp_points)
        sl_price = price - sl_dist if signal == "BUY" else price + sl_dist

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot_per_trade),
            "type": mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "sl": round(sl_price, info.digits),
            "tp": round(tp_price, info.digits),
            "magic": MAGIC_ID,
            "comment": f"{strat_name[:8]}_TP{i+1}",
            "type_filling": mt5.ORDER_FILLING_IOC
        }

        with mt5_lock: res = mt5.order_send(request)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            success_count += 1
        time.sleep(0.05)

    if success_count > 0:
        last_trade_time[symbol] = time.time()
        last_signal[symbol] = signal
        return True
    
    return False

# ==============================================================================
# TICK MANAGEMENT (BE PROGRESSIVO 🔴)
# ==============================================================================
def tick_management():
    positions = safe_positions()
    for p in positions:
        if p.magic != MAGIC_ID: continue

        info = safe_info(p.symbol)
        tick = safe_tick(p.symbol)
        if not info or not tick: continue

        price = tick.bid if p.type == 0 else tick.ask
        entry = p.price_open
        profit_points = (price - entry) / info.point if p.type == 0 else (entry - price) / info.point

        if int(time.time()) % 10 == 0: 
            set_live_log(p.symbol, f"📌 MANAGING | PnL: {round(p.profit,2)}€ ({round(profit_points,1)} pts)")

        # ===== LIVELLI TP RAGGIUNTI =====
        hit_level = 0
        for i, tp in enumerate(TP_POINTS):
            if profit_points >= tp:
                hit_level = i + 1

        new_sl = p.sl

        # Livello 2 (TP2 Colpito) -> Break-Even + Spread Buffer
        if hit_level >= 2:
            buffer = points_to_price(p.symbol, SPREAD_BUFFER_POINTS)
            new_sl = entry + buffer if p.type == 0 else entry - buffer

        # Livelli 3+ -> Stepped Trailing (Blocca il profitto sul TP precedente)
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

        # Bounds check per limitare l'errore Invalid Stops (10016)
        min_dist = max((info.trade_stops_level + 5) * info.point, info.point * 20)

        # Applica l'aggiornamento SL solo se lo migliora (Trailing stretto asimmetrico)
        if p.type == 0:
            if new_sl > p.sl and new_sl < (price - min_dist):
                with mt5_lock: mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})
                set_live_log(p.symbol, f"🔒 SL UPDATED L{hit_level}")

        elif p.type == 1:
            if (p.sl == 0 or new_sl < p.sl) and new_sl > (price + min_dist):
                with mt5_lock: mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})
                set_live_log(p.symbol, f"🔒 SL UPDATED L{hit_level}")

# ==============================================================================
# MAIN ENGINE
# ==============================================================================
def learn_from_history():
    global last_history_check
    deals = mt5.history_deals_get(last_history_check, datetime.now())
    if deals:
        for d in deals:
            if d.magic != MAGIC_ID or d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            if d.ticket in processed_deals:
                continue
            processed_deals.add(d.ticket)
            performance_memory["wins"] += 1 if d.profit > 0 else 0
            performance_memory["losses"] += 1 if d.profit < 0 else 0
            session_stats["profit"] += d.profit
            session_stats["trades"] += 1
        last_history_check = datetime.now()

def run_cycle():
    global last_heartbeat_time, last_global_trade_time, debug_state
    if bot_killed: return
    
    if time.time() - last_heartbeat_time > 15: 
        last_heartbeat_time = time.time()
        set_log(f"💓 BOT ALIVE {datetime.now().strftime('%H:%M:%S')}")
        
    if not mt5.terminal_info(): mt5.initialize(); return

    refresh_symbols()
        
    learn_from_history()
    tick_management()
    
    for s in symbols:
        debug_state[s] = {"data": False, "signal": "NEUTRAL", "blocked_by": "NONE", "stage": "INIT", "final": "WAITING"}
        update_radar_state(s, "SCAN", 0, "---", "SCANNING")
        
        set_live_log(s, "🔄 SCAN START")
        push_debug(s, "PRE-CHECK", "OK")

        if not allow_trading(s): 
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
            
        if s in last_trade_time and (time.time() - last_trade_time[s] < 60): 
            push_debug(s, "COOLDOWN", "ACTIVE")
            debug_state[s]["final"] = "COOLDOWN"
            update_radar_state(s, "WAIT", 0, "---", "COOLDOWN")
            set_live_log(s, "🚫 BLOCKED: Cooldown")
            continue

        sig, conf_base, strat = gold_profit_entry(df)
        conf = conf_base 
        
        if sig == "NEUTRAL": 
            push_debug(s, "SIGNAL", "NO SETUP")
            debug_state[s]["final"] = "NO SIGNAL"
            set_live_log(s, "⛔ NO SIGNAL")
            update_radar_state(s, "WAIT", conf, "---", "MONITORING")
            continue

        decision_stats["signals"] += 1
            
        set_live_log(s, f"🎯 SIGNAL: {sig} (Base Conf: {conf}%)")
        push_debug(s, "SIGNAL", sig)

        # 🔴 LOGICA MULTI-TP UI LOG
        mode_str = "MULTI" if should_use_multi_tp(df, s) else "STD"
        set_live_log(s, f"⚙️ MODE: {mode_str}")

        mtf_sig = get_multi_tf_signal(s)[0]
        if mtf_sig != sig: 
            set_live_log(s, "⚠️ MTF WEAK")
            conf *= 0.8 

        if not fast_market_filter(s): 
            push_debug(s, "FILTER", "FAST_MKT")
            decision_stats["filtered"] += 1
            debug_state[s]["final"] = "FAST MKT"
            update_radar_state(s, sig, int(conf), strat, "FAST MARKET")
            set_live_log(s, "🚫 BLOCKED: Fast Market")
            continue
            
        score = compute_asset_score(s, df, sig, conf)
        debug_state[s]["signal"] = f"{sig} (Score: {round(score,2)})"

        v, r = final_filter(s, df, score)
        if not v:
            push_debug(s, "FILTER", r)
            decision_stats["filtered"] += 1
            debug_state[s]["final"] = "FILTERED"
            update_radar_state(s, sig, int(score * 100), strat, f"FILTERED: {r}")
            set_live_log(s, f"🚫 BLOCKED: {r}")
            continue

        set_live_log(s, "✅ FILTER PASS")
        push_debug(s, "EXECUTION", "ATTEMPTING")
        debug_state[s]["final"] = "READY"

        set_live_log(s, f"🔥 READY TO TRADE | Score: {round(score,2)}")

        if open_scaled_trade(s, sig, df, strat, score):
            last_global_trade_time = time.time()
            decision_stats["executed"] += 1
            set_live_log(s, f"🚀 ENTRY {sig} | {strat}")
            push_debug(s, "DONE", "CLEARED")
            debug_state[s]["final"] = "EXECUTED"
            update_radar_state(s, sig, int(score*100), strat, "EXECUTED")
        else:
            push_debug(s, "DONE", "ORDER FAIL")
            debug_state[s]["final"] = "ORDER FAIL"
            update_radar_state(s, sig, int(score * 100), strat, "ORDER FAIL")
            
    time.sleep(0.3)
