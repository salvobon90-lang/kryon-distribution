import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
from datetime import datetime
import warnings
import traceback

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# CONFIGURAZIONE (V 21.0.0 - GLOBAL REGIME FILTER)
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
MAX_NEW_TRADES_PER_CYCLE = 2    # 🔴 ECCO LA VARIABILE MANCANTE
RISK_PER_TRADE_PERCENT = 0.7

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
last_heartbeat_time = 0

bot_killed = False 
crypto_enabled = True

performance_memory = {"wins": 0, "losses": 0}
processed_deals = set() 
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

# 🔴 MACRO STATE (Regime Filter)
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
    if symbol not in m1_cache or time.time() - m1_cache_time.get(symbol, 0) > 10:
        m1_cache[symbol] = get_data(symbol, mt5.TIMEFRAME_M1, 50)
        m1_cache_time[symbol] = time.time()
    return m1_cache[symbol]

# ==============================================================================
# EQUITY ENGINE & AI METRICS
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
    global last_history_check
    deals = mt5.history_deals_get(last_history_check, datetime.now())
    last_history_check = datetime.now()
    if not deals: return
    for d in deals:
        if d.ticket in processed_deals: continue
        if d.magic == MAGIC_ID and d.entry == 1:
            if d.profit > 0: performance_memory["wins"] += 1
            else: performance_memory["losses"] += 1
            processed_deals.add(d.ticket)

def get_decision_data():
    acc = mt5.account_info()
    if not acc: return {}
    total = performance_memory["wins"] + performance_memory["losses"]
    winrate = (performance_memory["wins"] / total) if total > 0 else 0
    return {
        "ai_mode": ai_mode, "equity_mode": equity_mode,
        "drawdown": round(get_drawdown() * 100, 2), "winrate": round(winrate * 100, 1),
        "signals": decision_stats["signals"], "filtered": decision_stats["filtered"],
        "executed": decision_stats["executed"], "equity": acc.equity, "balance": acc.balance
    }

# ==============================================================================
# CAPITAL ALLOCATION, RISK & CORRELATION ENGINE
# ==============================================================================
def compute_asset_score(symbol, df, sig, conf):
    if sig == "NEUTRAL": return 0

    atr = df['atr'].iloc[-1]
    close_price = df['close'].iloc[-1]
    momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5])

    vol_score = min(1.0, (atr / close_price) * 100)
    mom_score = min(1.0, momentum / atr) if atr > 0 else 0
    base_score = conf / 100.0

    if symbol == PRIMARY_SYMBOL: base_score *= 1.2 

    return round(base_score + vol_score + mom_score, 3)

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

    if symbol == PRIMARY_SYMBOL and ai_mode == "AGGRESSIVE":
        final_risk *= 1.2

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

# ==============================================================================
# GLOBAL REGIME FILTER (NO-TRADE MODE)
# ==============================================================================
def evaluate_market_conditions(candidates, total_symbols_count):
    if not candidates: return 0.0
    avg_score = sum(c["score"] for c in candidates) / len(candidates)
    # Ratio di quanti asset hanno generato un segnale rispetto al totale
    signal_ratio = len(candidates) / total_symbols_count
    return round((avg_score * 0.7) + (signal_ratio * 0.3), 3)

def should_pause_trading(market_quality):
    dd = get_drawdown()
    total = performance_memory["wins"] + performance_memory["losses"]
    winrate = (performance_memory["wins"] / total) if total >= 10 else 0.5

    if dd > 0.06: return True, "HIGH DD (>6%)"
    if total >= 10 and winrate < 0.40: return True, "LOW WINRATE (<40%)"
    
    # Se il mercato è apatico (score basso e pochi segnali)
    if market_quality > 0 and market_quality < 0.40: return True, "LOW MARKET QUALITY"
    
    return False, "OK"

def recovery_check():
    total = performance_memory["wins"] + performance_memory["losses"]
    if total < 10: return False
    winrate = performance_memory["wins"] / total
    return winrate > 0.55

# ==============================================================================
# FILTRI AVANZATI 
# ==============================================================================
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
    minute = datetime.now().minute
    if minute in [28, 29, 30, 31, 32, 58, 59, 0, 1]: return False
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
# STRATEGIE OPERATIVE (ARSENALE MULTI-ASSET)
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
        if dist_vwap < atr * 1.2: return "BUY", 75, "GOLD-PULLBACK"
    if price < vwap and ema20 < ema50:
        if dist_vwap < atr * 1.2: return "SELL", 75, "GOLD-PULLBACK"

    if momentum > atr * 0.3: return "BUY", 80, "GOLD-BREAKOUT"
    if momentum < -atr * 0.3: return "SELL", 80, "GOLD-BREAKOUT"

    if ema20 > ema50 and momentum > 0: return "BUY", 70, "GOLD-TREND"
    if ema20 < ema50 and momentum < 0: return "SELL", 70, "GOLD-TREND"

    candle_body = abs(last['close'] - last['open'])
    if candle_body > atr * 0.2:
        if last['close'] > last['open']: return "BUY", 65, "GOLD-SCALP"
        else: return "SELL", 65, "GOLD-SCALP"

    return "NEUTRAL", 0, "---"

def nasdaq_strategy(df):
    last = df.iloc[-1]
    high_break = df['high'].rolling(10).max().iloc[-2]
    low_break = df['low'].rolling(10).min().iloc[-2]

    if last['close'] > high_break: return "BUY", 75, "NAS_BREAK"
    if last['close'] < low_break: return "SELL", 75, "NAS_BREAK"
    return "NEUTRAL", 0, "---"

def eurusd_strategy(df):
    last = df.iloc[-1]
    vwap = last['vwap']
    atr = df['atr'].iloc[-1]
    dist = last['close'] - vwap

    if dist > atr * 0.7: return "SELL", 65, "EUR-REVERT"
    if dist < -atr * 0.7: return "BUY", 65, "EUR-REVERT"
    return "NEUTRAL", 0, "---"

def dax_strategy(df):
    ema20 = df['close'].ewm(span=20).mean().iloc[-1]
    ema50 = df['close'].ewm(span=50).mean().iloc[-1]
    price = df['close'].iloc[-1]

    if ema20 > ema50 and price > ema20: return "BUY", 70, "DAX-TREND"
    if ema20 < ema50 and price < ema20: return "SELL", 70, "DAX-TREND"
    return "NEUTRAL", 0, "---"

def gbp_strategy(df):
    last = df.iloc[-1]
    prev_high = df['high'].iloc[-2]
    prev_low = df['low'].iloc[-2]

    if last['high'] > prev_high and last['close'] < prev_high: return "SELL", 70, "GBP-TRAP"
    if last['low'] < prev_low and last['close'] > prev_low: return "BUY", 70, "GBP-TRAP"
    return "NEUTRAL", 0, "---"

def crypto_profit_entry(df):
    global ai_mode
    if ai_mode != "AGGRESSIVE": return "NEUTRAL", 0, "---"
    last = df.iloc[-1]
    atr = df['atr'].iloc[-1]
    atr_avg = df['atr'].rolling(50).mean().iloc[-1]
    momentum = last['close'] - df['close'].iloc[-10]

    if atr > atr_avg * 1.5:
        if momentum > 0: return "BUY", 75, "CRYPTO-BRK"
        if momentum < 0: return "SELL", 75, "CRYPTO-BRK"
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
        result = mt5.order_send(request)
        if result is None: time.sleep(RETRY_DELAY); continue
        if result.retcode == mt5.TRADE_RETCODE_DONE: return True
        if result.retcode in RETCODE_RETRY:
            deviation = min(MAX_DEVIATION, deviation + 5)
            time.sleep(RETRY_DELAY)
            tick = mt5.symbol_info_tick(symbol)
            if tick: request["price"] = tick.ask if request["type"] == mt5.ORDER_TYPE_BUY else tick.bid
            continue
        if result.retcode in RETCODE_FATAL: return False
    return False

def open_scaled_trade(symbol, signal, df, strat_name, score=1.0):
    acc = mt5.account_info()
    info = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not acc or not info or not tick: return False
    if info.trade_tick_size == 0 or info.trade_tick_value == 0: return False

    positions = mt5.positions_get() or []
    
    symbol_positions = [p for p in positions if p.symbol == symbol and p.magic == MAGIC_ID]
    active_clusters = len(set(p.comment.split("_")[1] for p in symbol_positions if p.comment.startswith("C_")))
    if active_clusters >= MAX_CLUSTERS_PER_SYMBOL: 
        return False

    price = tick.ask if signal == "BUY" else tick.bid
    atr = df['atr'].iloc[-1]
    cluster_id = str(int(time.time()))[-6:]

    if signal == "BUY": sl_price = price - (atr * 2.0)
    else: sl_price = price + (atr * 2.0)

    sl_dist = abs(price - sl_price)
    stops_level = info.trade_stops_level if info.trade_stops_level else 0
    min_dist = max((stops_level + 20) * info.point, info.point * 100)
    sl_dist = max(sl_dist, min_dist)

    risk_pct = compute_dynamic_risk(symbol, score)
    risk_money = acc.equity * risk_pct
    
    loss_ticks = sl_dist / info.trade_tick_size
    total_lot = risk_money / (loss_ticks * info.trade_tick_value)
    total_lot = max(info.volume_min, min(total_lot, info.volume_max * 0.3))
    
    lots = split_volume(total_lot, info)
    tps = compute_tp_levels(price, sl_dist, signal)
    
    success_count = 0
    params = get_mode_params()
    slices = params["slices"] if symbol == PRIMARY_SYMBOL else 1 
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
            "comment": f"C_{cluster_id}_{i+1}", "type_filling": mt5.ORDER_FILLING_IOC
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
# CLUSTER MANAGEMENT
# ==============================================================================
def get_clusters():
    clusters = {}
    positions = mt5.positions_get() or []
    for p in positions:
        if p.magic != MAGIC_ID: continue
        parts = p.comment.split("_")
        if len(parts) < 3 or parts[0] != "C": continue
        cid = parts[1]
        if cid not in clusters: clusters[cid] = []
        clusters[cid].append(p)
    return clusters

def manage_clusters():
    clusters = get_clusters()
    for cid, positions in clusters.items():
        total_profit = sum(p.profit for p in positions)
        total_volume = sum(p.volume for p in positions)

        if total_profit > (total_volume * 25):
            for p in positions: close_position(p)
            set_log(f"💰 CLUSTER CLOSE [PROFIT] | ID: {cid} | PNL: +{round(total_profit,2)}€")
            continue

        if total_profit < -(total_volume * 40):
            for p in positions: close_position(p)
            set_log(f"❌ CLUSTER CLOSE [STOP] | ID: {cid} | PNL: {round(total_profit,2)}€")
            continue

def cluster_trailing():
    clusters = get_clusters()
    for cid, positions in clusters.items():
        total_profit = sum(p.profit for p in positions)
        if total_profit > 0:
            for p in positions:
                if p.profit > 0:
                    if (p.type == 0 and p.sl < p.price_open) or (p.type == 1 and p.sl > p.price_open):
                        mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": p.price_open, "tp": p.tp})

def lock_profit():
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        risk = abs(p.price_open - p.sl)
        if risk <= 0: continue
        
        if p.type == 0: 
            if (p.price_current - p.price_open) > risk * 0.7:
                new_sl = p.price_open + (risk * 0.1) 
                if new_sl > p.sl:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, 5), "tp": p.tp})
        else: 
            if (p.price_open - p.price_current) > risk * 0.7:
                new_sl = p.price_open - (risk * 0.1)
                if new_sl < p.sl or p.sl == 0:
                    mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol, "position": p.ticket, "sl": round(new_sl, 5), "tp": p.tp})

def smart_exit_ai():
    for p in mt5.positions_get() or []:
        if p.magic != MAGIC_ID: continue
        if "BTC" in p.symbol: continue 
        df = get_data(p.symbol, mt5.TIMEFRAME_M5, 50)
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
    mt5.order_send({
        "action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume,
        "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY,
        "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "magic": MAGIC_ID
    })

# ==============================================================================
# MAIN CYCLE (RANKING & GLOBAL MARKET FILTER)
# ==============================================================================
def run_cycle():
    global bot_killed, last_heartbeat_time, debug_state, market_state
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
    
    if equity_mode == "RECOVERY": 
        manage_clusters()
        cluster_trailing()
        lock_profit()
        return
    
    positions = mt5.positions_get() or []
    active_symbols = set(p.symbol for p in positions if p.magic == MAGIC_ID)
    asset_candidates = []
    
    # 🔴 FASE 1: SCANSIONE E RANKING
    for s in symbols:
        debug_state[s] = {"data": False, "signal": "NEUTRAL", "blocked_by": None, "final": "WAITING"}

        if not allow_trading(s):
            debug_state[s]["blocked_by"] = "OUT OF SESSION"
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
        
        cooldown = 45 if s == PRIMARY_SYMBOL else 30
        if s in last_trade_time and (time.time() - last_trade_time[s] < cooldown):
            debug_state[s]["blocked_by"] = "COOLDOWN"
            continue

        if not execution_precheck(s, df_m5):
            debug_state[s]["blocked_by"] = "SPREAD ALTO"
            continue

        if s == "XAUUSD": sig, conf, strat = gold_profit_entry(df_m5)
        elif s == "US100" or s == "NAS100": sig, conf, strat = nasdaq_strategy(df_m5)
        elif s == "EURUSD": sig, conf, strat = eurusd_strategy(df_m5)
        elif s == "GER40" or s == "DE40" or s == "DAX": sig, conf, strat = dax_strategy(df_m5)
        elif s == "GBPUSD": sig, conf, strat = gbp_strategy(df_m5)
        elif s in CRYPTO_SYMBOLS: sig, conf, strat = crypto_profit_entry(df_m5)
        else: sig, conf, strat = "NEUTRAL", 0, "---"
        
        debug_state[s]["signal"] = sig

        if sig == "NEUTRAL":
            debug_state[s]["blocked_by"] = "NO ENTRY CONDITION"
            continue

        decision_stats["signals"] += 1
        set_live_log(s, f"🔍 CHECK: {sig} | conf:{conf}")

        if not news_filter(s): 
            debug_state[s]["blocked_by"] = "NEWS"
            decision_stats["filtered"] += 1
            set_live_log(s, "🚫 BLOCK: NEWS")
            continue
            
        if not master_filter(s, sig, conf, df_m5): 
            debug_state[s]["blocked_by"] = "MASTER FILTER"
            decision_stats["filtered"] += 1
            set_live_log(s, "🚫 BLOCK: MASTER FILTER")
            continue
            
        if not fast_market_filter(s): 
            debug_state[s]["blocked_by"] = "FAST MARKET"
            decision_stats["filtered"] += 1
            set_live_log(s, "🚫 BLOCK: FAST MARKET")
            continue

        score = compute_asset_score(s, df_m5, sig, conf)
        debug_state[s]["blocked_by"] = "QUEUED FOR RANKING"
        
        asset_candidates.append({
            "symbol": s, "signal": sig, "score": score,
            "df": df_m5, "strat": strat
        })

    # 🔴 FASE 2: GLOBAL MARKET FILTER E NO-TRADE MODE
    market_quality = evaluate_market_conditions(asset_candidates, len(symbols))
    pause_trading, pause_reason = should_pause_trading(market_quality)
    
    market_state["quality"] = market_quality
    market_state["pause_active"] = pause_trading
    market_state["pause_reason"] = pause_reason

    allowed_new_trades = MAX_NEW_TRADES_PER_CYCLE
    
    if pause_trading:
        if not recovery_check():
            # Blocco Hard
            decision_stats["filtered"] += len(asset_candidates)
            for c in asset_candidates: debug_state[c["symbol"]]["blocked_by"] = f"NO TRADE: {pause_reason}"
            asset_candidates = [] # Azzera per non eseguire nulla
            # set_log(f"⛔ NO TRADE MODE ATTIVO: {pause_reason} | MQ: {market_quality}") # Scommenta per loggare
        else:
            # Blocco Soft (Recovery)
            allowed_new_trades = 1
            # set_log(f"⚠️ SOFT RECOVERY MODE: {pause_reason} | Max Trades: 1") # Scommenta per loggare

    # 🔴 FASE 3: ALLOCAZIONE DEL CAPITALE (RANK & EXECUTE)
    asset_candidates.sort(key=lambda x: x["score"], reverse=True)
    executed_this_cycle = 0

    for cand in asset_candidates:
        s = cand["symbol"]
        
        if get_total_open_risk() > 0.05:
            debug_state[s]["blocked_by"] = "MAX GLOBAL RISK (5%)"
            decision_stats["filtered"] += 1
            continue

        if executed_this_cycle >= allowed_new_trades:
            debug_state[s]["blocked_by"] = "OUTRANKED"
            decision_stats["filtered"] += 1
            continue

        symbol_positions = [p for p in positions if p.symbol == s and p.magic == MAGIC_ID]
        active_clusters = len(set(p.comment.split("_")[1] for p in symbol_positions if p.comment.startswith("C_")))
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

        update_radar_state(s, cand["signal"], int(cand["score"]*100), cand["strat"], "LANCIO!")
        
        if open_scaled_trade(s, cand["signal"], cand["df"], cand["strat"], cand["score"]):
            debug_state[s]["final"] = "EXECUTED"
            debug_state[s]["blocked_by"] = "CLEARED"
            decision_stats["executed"] += 1
            executed_this_cycle += 1
            active_symbols.add(s) 
        else:
            debug_state[s]["final"] = "EXEC_FAILED"
            debug_state[s]["blocked_by"] = "MT5 REJECT/LIMIT"
            decision_stats["filtered"] += 1
            set_live_log(s, "❌ FALLITO AL LIVELLO EXECUTION (MT5 Reject/Limit)")

    manage_clusters()
    cluster_trailing()
    lock_profit()
    smart_exit_ai()

def close_all_now():
    set_log("⚠️ CHIUSURA TOTALE...")
    for p in mt5.positions_get() or []:
        if p.magic == MAGIC_ID: close_position(p)