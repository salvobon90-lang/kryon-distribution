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
# CONFIGURAZIONE (V 31.0.0 - SMART LOCK & THREAD SAFE)
# ==============================================================================
PROFIT_MODE = True
PRIMARY_SYMBOL = "XAUUSD"
SECONDARY_SYMBOLS = ["US100", "EURUSD", "GER40", "GBPUSD"]
CRYPTO_SYMBOLS = ["BTCUSD"]

symbols_list = [PRIMARY_SYMBOL] + SECONDARY_SYMBOLS + CRYPTO_SYMBOLS
symbols = symbols_list.copy() 

TIMEFRAME_MAIN = mt5.TIMEFRAME_M5 
MAX_UNIQUE_SYMBOLS = 4
MAX_CLUSTERS_PER_SYMBOL = 1 
MAX_NEW_TRADES_PER_CYCLE = 2    
RISK_PER_TRADE_PERCENT = 0.7 
MAX_TOTAL_RISK = 0.06 

ASSET_WEIGHTS = {
    "XAUUSD": 1.4,
    "US100": 1.2,
    "GER40": 1.1,
    "GBPUSD": 1.0,
    "EURUSD": 0.9,
    "BTCUSD": 0.7
}

MAX_RETRIES = 3
BASE_DEVIATION = 10
MAX_DEVIATION = 50
RETRY_DELAY = 0.4
MAGIC_ID = 600000 

CORRELATED_PAIRS = [
    ("US100", "GER40"),   
    ("EURUSD", "GBPUSD")  
]

latest_log_message = ""
latest_live_analysis_msgs = [] 
last_trade_time = {} 
last_global_trade_time = 0 
last_heartbeat_time = 0

bot_killed = False 
crypto_enabled = True

# 🔴 MULTI-THREADING LOCK (Race Condition Protection)
mt5_lock = threading.Lock()

# 🔴 AUTO-LEARNING MEMORIES & STATS
performance_memory = {"wins": 0, "losses": 0}
strategy_stats = {} 
asset_performance = {} 
session_stats = {"profit": 0.0, "trades": 0} 

processed_deals = set() 
partial_closed_tickets = set() 
trade_memory = {} 

last_history_check = datetime.now() 

m1_cache = {}
m1_cache_time = {}

radar_state = {s: {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "INIT", "live_conf": "N/A", "action": "---"} for s in symbols_list}

ai_mode = "STABLE"   
mode_last_update = 0
equity_peak = 0
equity_mode = "NORMAL"

decision_stats = {"signals": 0, "filtered": 0, "executed": 0}
debug_state = {s: {"data": False, "signal": "NONE", "blocked_by": "INIT", "final": "NONE"} for s in symbols_list}

market_state = {
    "quality": 0.0,
    "pause_active": False,
    "pause_reason": "OK"
}

RETCODE_RETRY = {
    mt5.TRADE_RETCODE_REQUOTE, mt5.TRADE_RETCODE_PRICE_OFF,
    mt5.TRADE_RETCODE_REJECT, mt5.TRADE_RETCODE_INVALID_PRICE,
    mt5.TRADE_RETCODE_PRICE_CHANGED, mt5.TRADE_RETCODE_CONNECTION
}
RETCODE_FATAL = {
    mt5.TRADE_RETCODE_NO_MONEY, mt5.TRADE_RETCODE_INVALID_VOLUME,
    mt5.TRADE_RETCODE_MARKET_CLOSED, mt5.TRADE_RETCODE_TRADE_DISABLED,
    mt5.TRADE_RETCODE_LIMIT_ORDERS, mt5.TRADE_RETCODE_LIMIT_VOLUME
}

# ==============================================================================
# MANUAL OVERRIDES 
# ==============================================================================
def close_only_profit():
    closed = 0
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        if p.profit > 0:
            close_position(p)
            closed += 1
    if closed > 0:
        set_log(f"💰 CLOSED PROFIT ONLY: {closed} trades")

def force_break_even_plus(buffer_ratio=0.2):
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue

        info = mt5.symbol_info(p.symbol)
        tick = mt5.symbol_info_tick(p.symbol)
        if not info or not tick: continue

        current_price = tick.bid if p.type == 0 else tick.ask
        if p.sl == 0: continue
        
        risk = abs(p.price_open - p.sl)
        if risk <= 0: continue

        buffer = risk * buffer_ratio

        if p.type == 0: 
            new_sl = p.price_open + buffer
            # 🔴 FIX: Simmetria BE+ sicura
            new_sl = min(new_sl, current_price - info.point * 10)
            if new_sl > p.sl:
                with mt5_lock:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})
        else: 
            new_sl = p.price_open - buffer
            new_sl = max(new_sl, current_price + info.point * 10)
            if p.sl == 0 or new_sl < p.sl:
                with mt5_lock:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})

    set_log("🛡️ BE+ APPLIED (MANUAL OVERRIDE)")

# ==============================================================================
# UTILITIES BASE
# ==============================================================================
def set_log(msg):
    global latest_log_message
    latest_log_message = msg
    print(msg)

def set_live_log(symbol, action_text):
    global latest_live_analysis_msgs
    timestamp = datetime.now().strftime("%H:%M:%S")
    latest_live_analysis_msgs.append(f"{str(symbol).ljust(10)} | {action_text}")

def get_latest_log():
    global latest_log_message
    msg = latest_log_message
    latest_log_message = ""
    return msg

def get_latest_live_logs():
    global latest_live_analysis_msgs
    msgs = latest_live_analysis_msgs.copy()
    latest_live_analysis_msgs.clear()
    return msgs

def get_debug_state():
    return debug_state.copy()

def toggle_crypto(state):
    global crypto_enabled
    crypto_enabled = state
    set_log(f"🔄 IMPOSTAZIONE: Crypto {'ABILITATE 🟢' if state else 'DISABILITATE 🔴'}")

if not mt5.initialize(): set_log("❌ ERRORE MT5: Connessione fallita in avvio.")

def check_and_fix_symbols():
    all_mkt_symbols = mt5.symbols_get()
    if all_mkt_symbols is None: return
    mkt_names = [s.name for s in all_mkt_symbols]
    for s in symbols_list:
        if s in mkt_names: mt5.symbol_select(s, True)
        else:
            for name in mkt_names:
                if s in name: mt5.symbol_select(name, True); break
check_and_fix_symbols()

def update_radar_state(symbol, sig, conf, strat, status):
    if symbol not in radar_state:
        radar_state[symbol] = {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "INIT", "live_conf": "N/A", "action": "---"}
    radar_state[symbol].update({"sig": sig, "conf": conf, "strat": strat, "status": status})

# ==============================================================================
# CORE DATA
# ==============================================================================
def get_data(symbol, timeframe, n_bars=300):
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

def detect_market_regime(df):
    if df is None or len(df) < 50: return "UNKNOWN"
    atr = df['atr'].iloc[-1]
    price = df['close']
    ema20 = price.ewm(span=20).mean()
    ema50 = price.ewm(span=50).mean()
    trend_strength = abs(ema20.iloc[-1] - ema50.iloc[-1]) / atr if atr > 0 else 0
    volatility = atr / price.iloc[-1]
    if volatility > 0.004: return "CHAOS"
    if trend_strength > 1.2: return "TREND"
    return "RANGE"

# ==============================================================================
# EQUITY ENGINE, AI METRICS & AUTO-LEARNING
# ==============================================================================
def get_drawdown():
    global equity_peak
    acc = mt5.account_info()
    if not acc: return 0
    if acc.equity > equity_peak: equity_peak = acc.equity
    if equity_peak == 0: return 0
    return (equity_peak - acc.equity) / equity_peak

def update_equity_mode():
    global equity_mode, ai_mode, mode_last_update
    dd = get_drawdown()
    
    if dd < 0.02: equity_mode = "AGGRESSIVE"
    elif dd < 0.05: equity_mode = "NORMAL"
    elif dd < 0.08: equity_mode = "DEFENSIVE"
    else: equity_mode = "RECOVERY"

    if time.time() - mode_last_update > 60:
        if dd > 0.05: ai_mode = "STABLE"
        else:
            total = performance_memory["wins"] + performance_memory["losses"]
            wr = (performance_memory["wins"] / total) if total > 0 else 0.5
            df_g = get_data(PRIMARY_SYMBOL, mt5.TIMEFRAME_M5, 50)
            vol = df_g['atr'].iloc[-1] if df_g is not None else 0
            if wr > 0.6 and equity_mode == "AGGRESSIVE" and vol > 1.0: ai_mode = "AGGRESSIVE"
            else: ai_mode = "STABLE"
        mode_last_update = time.time()

def get_mode_params():
    if ai_mode == "AGGRESSIVE": return {"risk": 1.2, "slices": 3}
    return {"risk": 0.6, "slices": 2}

def equity_risk_multiplier():
    if equity_mode == "AGGRESSIVE": return 1.5
    if equity_mode == "NORMAL": return 1.0
    if equity_mode == "DEFENSIVE": return 0.6
    if equity_mode == "RECOVERY": return 0.3
    return 1.0

def equity_protection():
    dd = get_drawdown()
    if dd > 0.08:
        for p in mt5.positions_get() or []:
            if p.magic == MAGIC_ID and p.profit < 0:
                close_position(p)

def learn_from_history():
    global last_history_check, session_stats
    
    current_time = datetime.now()
    deals = mt5.history_deals_get(last_history_check, current_time)
    
    if not deals: return
    for d in deals:
        # 🔴 FIX 3: Evita Overlap di conteggio
        if d.time < last_history_check.timestamp(): continue
        if d.ticket in processed_deals: continue
        if d.magic != MAGIC_ID: continue
        if d.entry != mt5.DEAL_ENTRY_OUT: continue
        
        # 🔴 FIX 2: Escludi i BE dal Winrate
        if d.profit == 0: 
            processed_deals.add(d.ticket)
            continue

        if d.profit > 0: performance_memory["wins"] += 1
        else: performance_memory["losses"] += 1

        if d.symbol not in asset_performance:
            asset_performance[d.symbol] = {"wins": 0, "losses": 0}
        if d.profit > 0: asset_performance[d.symbol]["wins"] += 1
        else: asset_performance[d.symbol]["losses"] += 1

        parts = d.comment.split("_")
        if len(parts) >= 2:
            strat = parts[0] 
            if strat not in strategy_stats: strategy_stats[strat] = {"wins": 0, "losses": 0}
            if d.profit > 0: strategy_stats[strat]["wins"] += 1
            else: strategy_stats[strat]["losses"] += 1

        session_stats["profit"] += d.profit
        session_stats["trades"] += 1
        processed_deals.add(d.ticket)
        
    last_history_check = current_time

def get_strategy_score(strat_name):
    strat_id = strat_name[:8]
    data = strategy_stats.get(strat_id, None)
    if not data: return 1.0
    total = data["wins"] + data["losses"]
    if total < 5: return 1.0 
    wr = data["wins"] / total
    return max(0.5, min(1.5, wr * 2))

def get_asset_score(symbol):
    data = asset_performance.get(symbol)
    if not data: return 1.0
    total = data["wins"] + data["losses"]
    if total < 5: return 1.0
    wr = data["wins"] / total
    return max(0.5, min(1.5, wr * 2))

def get_decision_data():
    acc = mt5.account_info()
    if not acc: return {}
    total = performance_memory["wins"] + performance_memory["losses"]
    winrate = (performance_memory["wins"] / total) if total > 0 else 0
    return {
        "ai_mode": ai_mode, "equity_mode": equity_mode,
        "drawdown": round(get_drawdown() * 100, 2), "winrate": round(winrate * 100, 1),
        "signals": decision_stats["signals"], "filtered": decision_stats["filtered"],
        "executed": decision_stats["executed"], "equity": acc.equity, "balance": acc.balance,
        "session_profit": round(session_stats["profit"], 2), 
        "closed_trades": session_stats["trades"]
    }

def get_system_snapshot():
    return {
        "equity_mode": equity_mode,
        "ai_mode": ai_mode,
        "market_quality": market_state["quality"],
        "pause": market_state["pause_active"],
        "open_risk": round(get_total_open_risk(), 4),
        "positions": len(mt5.positions_get() or [])
    }

# ==============================================================================
# CAPITAL ALLOCATION, RISK & CORRELATION ENGINE
# ==============================================================================
def compute_asset_score(symbol, df, sig, conf, strat_name):
    if sig == "NEUTRAL": return 0

    atr = df['atr'].iloc[-1]
    close_price = df['close'].iloc[-1]
    momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5])

    vol_score = min(1.0, (atr / close_price) * 100)
    mom_score = min(1.0, momentum / atr) if atr > 0 else 0
    base_score = conf / 100.0

    if symbol == PRIMARY_SYMBOL: base_score *= 1.2 

    strat_boost = get_strategy_score(strat_name)
    asset_boost = get_asset_score(symbol)

    # 🔴 FIX 6: Normalizzazione rigorosa (Max 1.0 = 100%)
    raw_score = (base_score + vol_score + mom_score) / 3.0
    final = raw_score * strat_boost * asset_boost
    return round(min(1.0, final), 3)

def is_correlated(symbol, active_symbols):
    for a, b in CORRELATED_PAIRS:
        if symbol == a and b in active_symbols: return True
        if symbol == b and a in active_symbols: return True
    return False

def compute_dynamic_risk(symbol, score):
    base = RISK_PER_TRADE_PERCENT / 100.0
    weight = ASSET_WEIGHTS.get(symbol, 1.0)
    for k, v in ASSET_WEIGHTS.items():
        if k in symbol: weight = v

    score_factor = min(1.5, max(0.5, score))

    if ai_mode == "AGGRESSIVE": ai_factor = 1.3
    elif ai_mode == "STABLE": ai_factor = 1.0
    else: ai_factor = 0.7

    eq_factor = equity_risk_multiplier()
    final_risk = base * weight * score_factor * ai_factor * eq_factor

    if symbol == PRIMARY_SYMBOL and ai_mode == "AGGRESSIVE": final_risk *= 1.2

    final_risk = min(final_risk, 0.02)   
    final_risk = max(final_risk, 0.002)  
    return final_risk

def get_total_open_risk():
    acc = mt5.account_info()
    if not acc: return 0

    open_risk_money = 0
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        info = mt5.symbol_info(p.symbol)
        if not info or info.trade_tick_size == 0: continue
        
        if p.type == 0 and p.sl >= p.price_open: continue
        if p.type == 1 and p.sl > 0 and p.sl <= p.price_open: continue
        
        loss_ticks = abs(p.price_open - p.sl) / info.trade_tick_size
        risk_cash = loss_ticks * info.trade_tick_value * p.volume
        open_risk_money += risk_cash

    return open_risk_money / acc.equity

def directional_exposure(symbol, signal):
    count = 0
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        if p.symbol != symbol: continue
        if signal == "BUY" and p.type == 0: count += 1
        if signal == "SELL" and p.type == 1: count += 1
    return count

# ==============================================================================
# GLOBAL REGIME FILTER & TRADE VALIDATION
# ==============================================================================
def evaluate_market_conditions(candidates, total_symbols_count):
    if not candidates: return 0.0
    avg_score = sum(c["score"] for c in candidates) / len(candidates)
    signal_ratio = len(candidates) / total_symbols_count
    
    vol_array = [(c["df"]["atr"].iloc[-1] / c["df"]["close"].iloc[-1]) * 100 for c in candidates if c["df"] is not None]
    vol_component = np.mean(vol_array) if vol_array else 0
    
    return round((avg_score * 0.5) + (signal_ratio * 0.2) + (vol_component * 0.3), 3)

def should_pause_trading(market_quality):
    dd = get_drawdown()
    total = performance_memory["wins"] + performance_memory["losses"]
    winrate = (performance_memory["wins"] / total) if total >= 10 else 0.5

    if dd > 0.06: return True, "HIGH DD (>6%)"
    if total >= 10 and winrate < 0.40: return True, "LOW WINRATE (<40%)"
    if market_quality > 0 and market_quality < 0.40: return True, "LOW MARKET QUALITY"
    
    return False, "OK"

def recovery_check():
    total = performance_memory["wins"] + performance_memory["losses"]
    if total < 10: return False
    winrate = performance_memory["wins"] / total
    return winrate > 0.55

def validate_trade(symbol, df, signal, score):
    reasons = []
    atr = df['atr'].iloc[-1]
    price = df['close'].iloc[-1]

    if atr / price < 0.0005: reasons.append("LOW VOL")
    momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5])
    if momentum < atr * 0.2: reasons.append("WEAK MOMENTUM")

    tick = mt5.symbol_info_tick(symbol)
    if tick:
        spread = tick.ask - tick.bid
        if spread > atr * 0.5: reasons.append("HIGH SPREAD")

    if score < 0.5: reasons.append("LOW SCORE") # Lowered since max is now 1.0

    if reasons: return False, reasons
    return True, []

# ==============================================================================
# FILTRI AVANZATI 
# ==============================================================================
def spread_filter(symbol, df):
    tick = mt5.symbol_info_tick(symbol)
    if not tick: return False
    spread = tick.ask - tick.bid
    atr = df['atr'].iloc[-1]

    if "XAU" in symbol: return spread < atr * 0.4
    elif "USD" in symbol: return spread < atr * 0.2
    else: return spread < atr * 0.5

def allow_trading(symbol):
    hour = datetime.now().hour
    if symbol == PRIMARY_SYMBOL: return True 
    if 2 <= hour <= 6: return False
    if 9 <= hour <= 21: return True
    return False

def crypto_kill_switch():
    if not crypto_enabled: return True
    acc = mt5.account_info()
    if not acc: return False
    if (acc.balance - acc.equity) / acc.balance > 0.05: return True
    return False

def news_filter(symbol):
    return True

def fast_market_filter(symbol):
    t1 = mt5.symbol_info_tick(symbol)
    time.sleep(0.05)
    t2 = mt5.symbol_info_tick(symbol)
    if not t1 or not t2: return False
    move = abs(t1.bid - t2.bid)
    if move > t1.bid * 0.0025: return False
    return True

def anomaly_filter(df):
    last = df.iloc[-1]
    candle = last['high'] - last['low']
    atr = df['atr'].iloc[-1]
    if candle > atr * 3: return False
    return True

def master_filter(symbol, sig, conf, df):
    if symbol == PRIMARY_SYMBOL:
        if conf < 60: return False
        return True
    
    if conf < 60: return False
    if not anomaly_filter(df): return False
    return True

def execution_precheck(symbol, df):
    tick = mt5.symbol_info_tick(symbol)
    if not tick: return False
    spread = tick.ask - tick.bid
    atr = df['atr'].iloc[-1]
    if spread > atr * 0.6: return False
    return True

# ==============================================================================
# STRATEGIE OPERATIVE
# ==============================================================================
def gold_profit_entry(df):
    global ai_mode
    last = df.iloc[-1]
    price = last['close']
    vwap = last['vwap']
    atr = df['atr'].iloc[-1]

    ema20 = df['close'].ewm(span=20).mean().iloc[-1]
    ema50 = df['close'].ewm(span=50).mean().iloc[-1]
    momentum = price - df['close'].iloc[-3]
    dist_vwap = abs(price - vwap)

    if price > vwap and ema20 > ema50:
        if dist_vwap < atr * 1.2: return "BUY", 75, "GLDPUL"
    if price < vwap and ema20 < ema50:
        if dist_vwap < atr * 1.2: return "SELL", 75, "GLDPUL"

    if momentum > atr * 0.3: return "BUY", 80, "GLDBRK"
    if momentum < -atr * 0.3: return "SELL", 80, "GLDBRK"

    if ema20 > ema50 and momentum > 0: return "BUY", 70, "GLDTRD"
    if ema20 < ema50 and momentum < 0: return "SELL", 70, "GLDTRD"

    candle_body = abs(last['close'] - last['open'])
    if candle_body > atr * 0.2:
        if last['close'] > last['open']: return "BUY", 65, "GLDSCL"
        else: return "SELL", 65, "GLDSCL"

    return "NEUTRAL", 0, "---"

def nasdaq_strategy(df):
    last = df.iloc[-1]
    high_break = df['high'].rolling(10).max().iloc[-2]
    low_break = df['low'].rolling(10).min().iloc[-2]

    if last['close'] > high_break: return "BUY", 75, "NASBRK"
    if last['close'] < low_break: return "SELL", 75, "NASBRK"
    return "NEUTRAL", 0, "---"

def eurusd_strategy(df):
    last = df.iloc[-1]
    vwap = last['vwap']
    atr = df['atr'].iloc[-1]
    dist = last['close'] - vwap

    if dist > atr * 0.7: return "SELL", 65, "EURREV"
    if dist < -atr * 0.7: return "BUY", 65, "EURREV"
    return "NEUTRAL", 0, "---"

def dax_strategy(df):
    ema20 = df['close'].ewm(span=20).mean().iloc[-1]
    ema50 = df['close'].ewm(span=50).mean().iloc[-1]
    price = df['close'].iloc[-1]

    if ema20 > ema50 and price > ema20: return "BUY", 70, "DAXTRD"
    if ema20 < ema50 and price < ema20: return "SELL", 70, "DAXTRD"
    return "NEUTRAL", 0, "---"

def gbp_strategy(df):
    last = df.iloc[-1]
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]

    if last['high'] > prev_high and last['close'] < prev_high: return "SELL", 70, "GBPTRP"
    if last['low'] < prev_low and last['close'] > prev_low: return "BUY", 70, "GBPTRP"
    return "NEUTRAL", 0, "---"

def crypto_profit_entry(df):
    global ai_mode
    if ai_mode != "AGGRESSIVE": return "NEUTRAL", 0, "---"
    last = df.iloc[-1]
    atr = df['atr'].iloc[-1]
    atr_avg = df['atr'].rolling(50).mean().iloc[-1]
    momentum = last['close'] - df['close'].iloc[-10]

    if atr > atr_avg * 1.5:
        if momentum > 0: return "BUY", 75, "BTCBRK"
        if momentum < 0: return "SELL", 75, "BTCBRK"
    return "NEUTRAL", 0, "---"

# ==============================================================================
# EXECUTION LAYER PRO 
# ==============================================================================
def get_dynamic_deviation(symbol, df):
    info = mt5.symbol_info(symbol)
    if not info or info.point == 0: return BASE_DEVIATION
    atr_points = df['atr'].iloc[-1] / info.point
    dev = int(min(MAX_DEVIATION, max(BASE_DEVIATION, atr_points * 0.1)))
    return dev

def split_volume(total_lot, info):
    l1 = max(info.volume_min, round((total_lot * 0.4) / info.volume_step) * info.volume_step)
    l2 = max(info.volume_min, round((total_lot * 0.35) / info.volume_step) * info.volume_step)
    l3 = max(info.volume_min, round((total_lot * 0.25) / info.volume_step) * info.volume_step)
    return [l1, l2, l3]

def compute_tp_levels(price, sl_dist, signal):
    if signal == "BUY": return [price + sl_dist*1.0, price + sl_dist*2.0, price + sl_dist*3.0]
    else: return [price - sl_dist*1.0, price - sl_dist*2.0, price - sl_dist*3.0]

def send_order_with_retry(request, symbol, df):
    deviation = get_dynamic_deviation(symbol, df)
    for attempt in range(MAX_RETRIES):
        request["deviation"] = deviation
        
        with mt5_lock: # 🔴 THREAD SAFE
            result = mt5.order_send(request)
        
        if result is None: 
            set_log(f"❌ MT5 NULL RESULT: {symbol}")
            time.sleep(RETRY_DELAY)
            continue
            
        if result.retcode == mt5.TRADE_RETCODE_DONE: 
            return True
            
        if result.retcode == mt5.TRADE_RETCODE_MARKET_CLOSED:
            set_log(f"💤 MERCATO CHIUSO: {symbol}. Sospeso.")
            last_trade_time[symbol] = time.time() + 3600
            return False

        if result.retcode in RETCODE_RETRY:
            set_log(f"⚠️ RETRY {symbol} | retcode: {result.retcode}")
            deviation = min(MAX_DEVIATION, deviation + 5)
            time.sleep(RETRY_DELAY)
            tick = mt5.symbol_info_tick(symbol)
            if tick: request["price"] = tick.ask if request["type"] == mt5.ORDER_TYPE_BUY else tick.bid
            continue
            
        set_log(f"❌ EXEC FAIL {symbol} | retcode: {result.retcode}")
        return False
        
    return False

def open_scaled_trade(symbol, signal, df, strat_name, score=1.0):
    acc = mt5.account_info()
    info = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not acc or not info or not tick: return False
    if info.trade_tick_size == 0 or info.trade_tick_value == 0: return False

    price = tick.ask if signal == "BUY" else tick.bid
    atr = df['atr'].iloc[-1]
    cluster_id = str(int(time.time()))[-6:]

    if signal == "BUY": sl_price = price - (atr * 2.0)
    else: sl_price = price + (atr * 2.0)

    sl_dist = abs(price - sl_price)
    stops_level = info.trade_stops_level if info.trade_stops_level else 0
    min_dist = max((stops_level + 20) * info.point, info.point * 100)
    sl_dist = max(sl_dist, min_dist)

    sl_price = price - sl_dist if signal == "BUY" else price + sl_dist

    risk_pct = compute_dynamic_risk(symbol, score)
    risk_money = acc.equity * risk_pct
    
    loss_ticks = sl_dist / info.trade_tick_size
    total_lot = risk_money / (loss_ticks * info.trade_tick_value)
    
    if sl_dist <= 0 or np.isnan(sl_dist): return False
    if total_lot <= 0 or np.isnan(total_lot): return False

    total_lot = max(info.volume_min, min(total_lot, info.volume_max * 0.3))
    
    lots = split_volume(total_lot, info)
    tps = compute_tp_levels(price, sl_dist, signal)
    
    success_count = 0
    params = get_mode_params()
    slices = params["slices"] if symbol == PRIMARY_SYMBOL else 1 
    
    short_strat = strat_name[:8]

    for i in range(slices):
        lot = lots[i]
        if lot < info.volume_min: continue
        
        tp = round(tps[i], info.digits)
        sl = round(sl_price, info.digits)

        if abs(price - sl) < min_dist or abs(tp - price) < min_dist: continue

        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": round(lot, 2),
            "type": mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": price, "sl": sl, "tp": tp, "magic": MAGIC_ID,
            "comment": f"{short_strat}_{cluster_id}_{i+1}", "type_filling": mt5.ORDER_FILLING_IOC
        }
        
        if send_order_with_retry(request, symbol, df): success_count += 1
        time.sleep(0.2)

    if success_count > 0:
        set_log(f"🎯 EXEC OK: {symbol} {signal} {strat_name} | Risk: {round(risk_pct*100,2)}%")
        last_trade_time[symbol] = time.time()
        return True
    else:
        last_trade_time[symbol] = time.time()
        return False

# ==============================================================================
# HFT TICK REFLEX ENGINE 🔴 (FIX 5 & SMART LOCK)
# ==============================================================================
def get_trailing_factor(symbol):
    if "XAU" in symbol: return 0.35   
    elif "US100" in symbol or "GER40" in symbol: return 0.6 
    elif "USD" in symbol: return 0.8  
    elif "BTC" in symbol: return 0.5  
    return 0.6

def partial_close(position, info, percentage=0.5):
    volume_to_close = position.volume * percentage
    volume_to_close = round(volume_to_close / info.volume_step) * info.volume_step
    volume_to_close = max(info.volume_min, volume_to_close)

    if volume_to_close >= position.volume: return False 

    tick = mt5.symbol_info_tick(position.symbol)
    if not tick: return False

    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": position.symbol, "volume": volume_to_close,
        "type": mt5.ORDER_TYPE_SELL if position.type == 0 else mt5.ORDER_TYPE_BUY,
        "position": position.ticket, "price": tick.bid if position.type == 0 else tick.ask,
        "magic": MAGIC_ID
    }
    with mt5_lock: # 🔴 THREAD SAFE
        res = mt5.order_send(request)
    return res is not None and res.retcode == mt5.TRADE_RETCODE_DONE

def tick_speed(symbol):
    t1 = mt5.symbol_info_tick(symbol)
    time.sleep(0.05)
    t2 = mt5.symbol_info_tick(symbol)
    if not t1 or not t2: return 0
    return abs(t2.bid - t1.bid)

def smart_profit_protection():
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        if p.ticket not in trade_memory: continue

        info = mt5.symbol_info(p.symbol)
        tick = mt5.symbol_info_tick(p.symbol)
        if not info or not tick: continue

        max_profit = trade_memory[p.ticket]["max_profit"]
        if max_profit <= 0: continue

        current_price = tick.bid if p.type == 0 else tick.ask
        
        lock_ratio = 0.3 if max_profit < 50 else (0.5 if max_profit < 150 else 0.7)
        lock_profit_val = max_profit * lock_ratio

        points_to_lock = (lock_profit_val / max(0.0001, p.volume * info.trade_tick_value)) * info.trade_tick_size

        if p.type == 0:
            new_sl = p.price_open + points_to_lock
            if new_sl > p.sl and new_sl < current_price:
                with mt5_lock:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})
        else:
            new_sl = p.price_open - points_to_lock
            if (p.sl == 0 or new_sl < p.sl) and new_sl > current_price:
                with mt5_lock:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, info.digits), "tp": p.tp})

def tick_management():
    global partial_closed_tickets, trade_memory
    positions = mt5.positions_get() or []
    
    current_tickets = set(p.ticket for p in positions)
    partial_closed_tickets = {t for t in partial_closed_tickets if t in current_tickets}
    
    keys_to_delete = [t for t in trade_memory if t not in current_tickets]
    for k in keys_to_delete:
        del trade_memory[k]

    for p in positions:
        if p.magic != MAGIC_ID: continue

        tick = mt5.symbol_info_tick(p.symbol)
        info = mt5.symbol_info(p.symbol)
        df = get_m1_cached(p.symbol)
        
        if not tick or not info or df is None: continue

        current_price = tick.bid if p.type == 0 else tick.ask
        spread = tick.ask - tick.bid
        
        # 🔴 FIX 5: INIZIALIZZAZIONE RISCHIO REALE ALL'APERTURA
        if p.ticket not in trade_memory:
            trade_memory[p.ticket] = {
                "max_profit": p.profit, 
                "entry_time": time.time(),
                "initial_risk": abs(p.price_open - p.sl) if p.sl != 0 else (df['atr'].iloc[-1] * 2.0)
            }
            
        trade_memory[p.ticket]["max_profit"] = max(trade_memory[p.ticket]["max_profit"], p.profit)
        initial_risk = trade_memory[p.ticket]["initial_risk"]
        
        if initial_risk <= 0: continue

        # 🔴 PROFITTO NORMALIZZATO
        if p.type == 0: profit_points = current_price - p.price_open
        else: profit_points = p.price_open - current_price

        # 🛡️ SPREAD GUARD 
        if spread > initial_risk * 0.3:
            continue 

        max_profit = trade_memory[p.ticket]["max_profit"]
        if max_profit > 0:
            dd = (max_profit - p.profit) / max_profit
            if dd > 0.4:
                close_position(p)
                set_log(f"🧠 EXIT DD (Peak Drop): {p.symbol} a +{round(p.profit,2)}€")
                continue

        # ✂️ PARTIAL CLOSE
        if p.ticket not in partial_closed_tickets and p.volume >= (info.volume_min * 2):
            if profit_points > initial_risk * 0.8:
                if partial_close(p, info, 0.5):
                    partial_closed_tickets.add(p.ticket)
                    set_log(f"✂️ PARTIAL CLOSE: {p.symbol}")
                    continue

        # ⚡ FAST TP DINAMICO
        if profit_points > initial_risk * 1.5:
            close_position(p)
            set_log(f"⚡ FAST CLOSE: {p.symbol}")
            continue

        # 🔒 TRAILING ADATTIVO
        factor = get_trailing_factor(p.symbol)
        min_dist = max((info.trade_stops_level + 10) * info.point, info.point * 50)
        
        speed = tick_speed(p.symbol)
        if speed > initial_risk * 0.2: factor *= 0.7  

        trail_distance = initial_risk * factor

        if p.type == 0:
            new_sl = current_price - trail_distance
            if new_sl <= p.sl: continue
            if new_sl >= current_price: continue
            if (current_price - new_sl) < min_dist: continue
        else:
            new_sl = current_price + trail_distance
            if p.sl != 0 and new_sl >= p.sl: continue
            if new_sl <= current_price: continue
            if (new_sl - current_price) < min_dist: continue

        with mt5_lock:
            mt5.order_send({
                "action": mt5.TRADE_ACTION_SLTP,
                "symbol": p.symbol,
                "position": p.ticket,
                "sl": round(new_sl, info.digits),
                "tp": p.tp
            })
            
    smart_profit_protection()

def get_clusters():
    clusters = {}
    positions = mt5.positions_get() or []
    for p in positions:
        if p.magic != MAGIC_ID: continue
        parts = p.comment.split("_")
        if len(parts) < 3: continue
        cid = parts[1] 
        if cid not in clusters: clusters[cid] = []
        clusters[cid].append(p)
    return clusters

def momentum_exit():
    for p in mt5.positions_get() or [] :
        if p.magic != MAGIC_ID: continue
        df = get_m1_cached(p.symbol)
        if df is None: continue

        momentum = df['close'].iloc[-1] - df['close'].iloc[-3]
        atr = df['atr'].iloc[-1]

        if abs(momentum) < atr * 0.1 and p.profit > 0:
            close_position(p)
            set_log(f"📉 EXIT MOMENTUM (Dead Market): {p.symbol}")

def manage_clusters():
    clusters = get_clusters()
    for cid, positions in clusters.items():
        if not positions: continue
        symbol = positions[0].symbol
        total_profit = sum(p.profit for p in positions)
        total_volume = sum(p.volume for p in positions)

        df = get_m1_cached(symbol)
        info = mt5.symbol_info(symbol)
        if df is None or not info: continue

        atr = df['atr'].iloc[-1]
        atr_monetary_per_lot = (atr / info.trade_tick_size) * info.trade_tick_value
        target_profit = total_volume * atr_monetary_per_lot * 1.5

        if total_profit > target_profit:
            for p in positions: close_position(p)
            set_log(f"💰 CLUSTER CLOSE [PROFIT] | ID: {cid} | PNL: +{round(total_profit,2)}€")
            continue

        if total_profit < -(total_volume * atr_monetary_per_lot * 2.0):
            for p in positions: close_position(p)
            set_log(f"❌ CLUSTER CLOSE [STOP] | ID: {cid} | PNL: {round(total_profit,2)}€")
            continue

def smart_exit_ai():
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        if "BTC" in p.symbol: continue 
        
        trade_age = time.time() - p.time
        if trade_age < 30: continue

        df = get_m1_cached(p.symbol)
        if df is None: continue
        
        momentum = df['close'].iloc[-1] - df['close'].iloc[-5]
        vwap_dist = abs(df['close'].iloc[-1] - df['vwap'].iloc[-1])
        atr = df['atr'].iloc[-1]
        
        score = 0
        if abs(momentum) > atr * 0.5: score += 1
        if vwap_dist < atr * 0.3: score += 1

        if score == 0 and p.profit < 0:
            close_position(p)
            set_log(f"⚠️ AI EXIT (Loss Protection): {p.symbol}")

def close_position(p):
    tick = mt5.symbol_info_tick(p.symbol)
    if not tick: return
    with mt5_lock: # 🔴 THREAD SAFE
        res = mt5.order_send({
            "action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume,
            "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY,
            "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "magic": MAGIC_ID
        })
    if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
        set_log(f"❌ CLOSE FAIL: {p.symbol}")

# ==============================================================================
# MAIN CYCLE 
# ==============================================================================
def run_cycle():
    global bot_killed, last_heartbeat_time, debug_state, market_state, last_global_trade_time
    if bot_killed: return
    
    if time.time() - last_heartbeat_time > 15:
        last_heartbeat_time = time.time()

    if not mt5.terminal_info():
        mt5.initialize()
        set_log("⚠️ MT5 DISCONNESSO. Tentativo di ripristino in corso...")
        return

    decision_stats["signals"] = 0
    decision_stats["filtered"] = 0
    decision_stats["executed"] = 0

    update_equity_mode()
    equity_protection()
    learn_from_history()
    
    tick_management()
    manage_clusters()
    momentum_exit()
    smart_exit_ai()
    
    if equity_mode == "RECOVERY": return
    
    positions = mt5.positions_get() or []
    active_symbols = set(p.symbol for p in positions if p.magic == MAGIC_ID)
    asset_candidates = []
    
    for s in symbols:
        debug_state[s] = {"data": False, "signal": "NEUTRAL", "blocked_by": None, "final": "WAITING"}

        if not allow_trading(s):
            debug_state[s]["blocked_by"] = "OUT OF SESSION"
            continue

        asset_score = get_asset_score(s)
        if asset_score < 0.7:
            debug_state[s]["blocked_by"] = f"WEAK ASSET ({round(asset_score, 2)})"
            continue

        tick = mt5.symbol_info_tick(s)
        if tick is None:
            debug_state[s]["blocked_by"] = "NO TICK DATA"
            continue

        if "BTC" in s and crypto_kill_switch():
            debug_state[s]["blocked_by"] = "CRYPTO DD PROTECT"
            continue

        df_m5 = get_data(s, mt5.TIMEFRAME_M5, 300)
        if df_m5 is None: 
            debug_state[s]["blocked_by"] = "NO M5 DATA"
            continue
            
        debug_state[s]["data"] = True

        if not spread_filter(s, df_m5):
            debug_state[s]["blocked_by"] = "SPREAD TOO HIGH"
            decision_stats["filtered"] += 1
            continue
        
        regime = detect_market_regime(df_m5)
        if regime == "CHAOS":
            debug_state[s]["signal"] = f"[CHAOS] ---"
            debug_state[s]["blocked_by"] = "CHAOS MARKET"
            continue

        cooldown = 45 if s == PRIMARY_SYMBOL else 30
        if s in last_trade_time and (time.time() - last_trade_time[s] < cooldown):
            debug_state[s]["blocked_by"] = "COOLDOWN"
            continue

        if s == "XAUUSD":
            if regime == "TREND": sig, conf, strat = gold_profit_entry(df_m5)
            elif regime == "RANGE": sig, conf, strat = eurusd_strategy(df_m5) 
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        elif s in ["US100", "NAS100"]:
            if regime == "TREND": sig, conf, strat = nasdaq_strategy(df_m5)
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        elif s == "EURUSD":
            if regime == "RANGE": sig, conf, strat = eurusd_strategy(df_m5)
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        elif s in ["GER40", "DE40", "DAX"]:
            if regime == "TREND": sig, conf, strat = dax_strategy(df_m5)
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        elif s == "GBPUSD":
            if regime == "RANGE": sig, conf, strat = gbp_strategy(df_m5)
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        elif s in CRYPTO_SYMBOLS:
            if regime == "TREND": sig, conf, strat = crypto_profit_entry(df_m5)
            else: sig, conf, strat = "NEUTRAL", 0, "---"
        else: 
            sig, conf, strat = "NEUTRAL", 0, "---"
        
        debug_state[s]["signal"] = f"[{regime}] {sig}" 

        if sig == "NEUTRAL":
            debug_state[s]["blocked_by"] = "NO ENTRY CONDITION"
            continue

        if directional_exposure(s, sig) >= 2:
            debug_state[s]["blocked_by"] = "DIRECTIONAL OVERLOAD"
            continue

        strat_score = get_strategy_score(strat)
        if strat_score < 0.7:
            debug_state[s]["blocked_by"] = f"LOW STRAT SCORE ({round(strat_score, 2)})"
            decision_stats["filtered"] += 1
            continue

        decision_stats["signals"] += 1
        set_live_log(s, f"🔍 CHECK: {sig} | conf:{conf}")
            
        if not master_filter(s, sig, conf, df_m5): 
            debug_state[s]["blocked_by"] = "MASTER FILTER"
            decision_stats["filtered"] += 1
            set_live_log(s, "🚫 BLOCK: MASTER FILTER")
            continue

        regime_bonus = 1.0
        strat_u = strat.upper()
        if regime == "TREND" and any(x in strat_u for x in ["TREND", "BRK", "BREAK", "PUL"]): regime_bonus = 1.3
        elif regime == "RANGE" and any(x in strat_u for x in ["REV", "TRAP", "SCL", "SCALP"]): regime_bonus = 1.3
        
        score = compute_asset_score(s, df_m5, sig, conf, strat) * regime_bonus
        debug_state[s]["blocked_by"] = "QUEUED FOR RANKING"
        
        asset_candidates.append({
            "symbol": s, "signal": sig, "score": score,
            "df": df_m5, "strat": strat
        })

    market_quality = evaluate_market_conditions(asset_candidates, len(symbols))
    pause_trading, pause_reason = should_pause_trading(market_quality)
    
    market_state["quality"] = market_quality
    market_state["pause_active"] = pause_trading
    market_state["pause_reason"] = pause_reason

    allowed_new_trades = MAX_NEW_TRADES_PER_CYCLE
    
    if pause_trading:
        if not recovery_check():
            decision_stats["filtered"] += len(asset_candidates)
            for c in asset_candidates: debug_state[c["symbol"]]["blocked_by"] = f"NO TRADE: {pause_reason}"
            asset_candidates = [] 
        else:
            allowed_new_trades = 1

    asset_candidates.sort(key=lambda x: x["score"], reverse=True)
    executed_this_cycle = 0

    for cand in asset_candidates:
        s = cand["symbol"]
        
        valid, reasons = validate_trade(s, cand["df"], cand["signal"], cand["score"])
        if not valid:
            debug_state[s]["blocked_by"] = "VALIDATION: " + ",".join(reasons)
            decision_stats["filtered"] += 1
            continue

        if time.time() - last_global_trade_time < 10:
            debug_state[s]["blocked_by"] = "GLOBAL COOLDOWN"
            continue

        if get_total_open_risk() > MAX_TOTAL_RISK:
            debug_state[s]["blocked_by"] = f"MAX GLOBAL RISK ({MAX_TOTAL_RISK*100}%)"
            decision_stats["filtered"] += 1
            continue

        if executed_this_cycle >= allowed_new_trades:
            debug_state[s]["blocked_by"] = "OUTRANKED"
            decision_stats["filtered"] += 1
            continue

        symbol_positions = [p for p in positions if p.symbol == s and p.magic == MAGIC_ID]
        active_clusters = len(set(p.comment.split("_")[1] for p in symbol_positions if p.comment.startswith(cand["strat"][:8])))
        if active_clusters >= MAX_CLUSTERS_PER_SYMBOL:
            debug_state[s]["blocked_by"] = "MAX CLUSTERS PER SYMBOL"
            decision_stats["filtered"] += 1
            continue

        if len(active_symbols) >= MAX_UNIQUE_SYMBOLS and s not in active_symbols:
            debug_state[s]["blocked_by"] = "MAX GLOBAL ASSETS"
            decision_stats["filtered"] += 1
            continue
            
        if is_correlated(s, active_symbols) and s not in active_symbols:
            debug_state[s]["blocked_by"] = "CORRELATION LOCK"
            decision_stats["filtered"] += 1
            set_live_log(s, "🚫 BLOCK: CORRELATION")
            continue

        # 🔴 FIX 6: UI Normalizzata correttamente a Max 100%
        conf_clamped = int(cand["score"] * 100) 
        update_radar_state(s, cand["signal"], conf_clamped, cand["strat"], "LANCIO!")
        
        if open_scaled_trade(s, cand["signal"], cand["df"], cand["strat"], cand["score"]):
            debug_state[s]["final"] = "EXECUTED"
            debug_state[s]["blocked_by"] = "CLEARED"
            decision_stats["executed"] += 1
            executed_this_cycle += 1
            active_symbols.add(s) 
            last_global_trade_time = time.time() 
        else:
            debug_state[s]["final"] = "EXEC_FAILED"
            debug_state[s]["blocked_by"] = "MT5 REJECT/LIMIT"
            decision_stats["filtered"] += 1

def close_all_now():
    set_log("⚠️ CHIUSURA TOTALE...")
    for p in mt5.positions_get() or []:
        if p.magic == MAGIC_ID: close_position(p)