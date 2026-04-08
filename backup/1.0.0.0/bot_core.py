import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# CONFIGURAZIONE (V 8.6.0 - M5 ARSENAL EXPANSION)
# ==============================================================================

TRADITIONAL_SYMBOLS = [
    "US30", "US100", "US500", "GER40", "UK100", "JPN225", "FRA40", "EUSTX50", 
    "XAUUSD", "XAGUSD", "WTI", "BRENT", 
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"
]

CRYPTO_SYMBOLS = [
    "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "ADAUSD", "LTCUSD", "DOTUSD", 
    "DOGEUSD", "LINKUSD", "AVAXUSD", "MATICUSD", "UNIUSD", "ATOMUSD",
    "BCHUSD", "EOSUSD", "TRXUSD", "XLMUSD", "XMRUSD", "DASHUSD", "ZECUSD",
    "SHIBUSD", "ALGOUSD", "FILUSD", "VETUSD", "ICPUSD", "AAVEUSD", "MKRUSD",
    "COMPUSD", "SNXUSD", "THETAUSD", "MANAUSD", "SANDUSD", "AXSUSD", "GALAUSD",
    "ENJUSD", "CHZUSD", "SUSHIUSD", "FTMUSD", "CRVUSD", "LUNAUSD", "NEARUSD"
]

symbols_list = TRADITIONAL_SYMBOLS + CRYPTO_SYMBOLS

TIMEFRAME_MAIN = mt5.TIMEFRAME_M5 
MAX_TRADES_TOTAL = 5  
MAX_TRADES_PER_SYMBOL = 1 
TARGET_EURO_PER_TRADE = 3.00 

MAGIC_ID = 600000 

latest_log_message = ""
latest_live_analysis_msgs = [] 

last_mod_time = {} 
last_trade_time = {} 
initial_risks = {}
partial_closed = set()

crypto_enabled = True

def set_log(msg):
    global latest_log_message
    latest_log_message = msg
    print(msg)

def get_latest_log():
    global latest_log_message
    msg = latest_log_message
    latest_log_message = ""
    return msg

def set_live_log(symbol, action_text):
    global latest_live_analysis_msgs
    timestamp = datetime.now().strftime("%H:%M:%S")
    latest_live_analysis_msgs.append(f"{str(symbol).ljust(10)} | {action_text}")

def get_latest_live_logs():
    global latest_live_analysis_msgs
    msgs = latest_live_analysis_msgs.copy()
    latest_live_analysis_msgs.clear()
    return msgs

def toggle_crypto(state):
    global crypto_enabled
    crypto_enabled = state
    msg = "ABILITATE 🟢" if state else "DISABILITATE 🔴"
    set_log(f"🔄 IMPOSTAZIONE: Operatività Crypto {msg}")

if not mt5.initialize(): set_log("❌ ERRORE MT5: Connessione fallita.")

def check_and_fix_symbols():
    available = []
    all_mkt_symbols = mt5.symbols_get()
    if all_mkt_symbols is None: return symbols_list 
    mkt_names = [s.name for s in all_mkt_symbols]
    for s in symbols_list:
        if s in mkt_names:
            mt5.symbol_select(s, True); info = mt5.symbol_info(s)
            if info is not None and info.visible: available.append(s)
        else:
            for name in mkt_names:
                if s in name:
                    mt5.symbol_select(name, True); info = mt5.symbol_info(name)
                    if info is not None and info.visible: available.append(name); break
    return list(set(available))

symbols = check_and_fix_symbols()
radar_state = {s: {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "ATTESA", "live_conf": "N/A", "action": "---"} for s in symbols}

def is_market_active(symbol):
    now = datetime.now()
    time_float = now.hour + now.minute / 60.0
    
    if "US30" in symbol or "US100" in symbol or "US500" in symbol:
        return 15.5 <= time_float <= 21.0
    if "GER40" in symbol or "UK100" in symbol or "FRA40" in symbol or "EUSTX50" in symbol:
        return (9.0 <= time_float <= 11.5) or (15.5 <= time_float <= 17.5)
    if "JPN225" in symbol:
        return 1.0 <= time_float <= 7.0
    if "XAU" in symbol or "XAG" in symbol or "WTI" in symbol or "BRENT" in symbol:
        return (9.0 <= time_float <= 11.5) or (15.5 <= time_float <= 18.0)
    if symbol in TRADITIONAL_SYMBOLS: 
        return 8.0 <= time_float <= 18.0
    
    return True 

def get_macro_trend(symbol):
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, 50)
    if rates is None or len(rates) < 50: return "NEUTRAL"
    df = pd.DataFrame(rates)
    sma_50 = df['close'].mean()
    last_close = df['close'].iloc[-1]
    
    if last_close > (sma_50 + (df['close'].std() * 0.5)): return "BULLISH"
    if last_close < (sma_50 - (df['close'].std() * 0.5)): return "BEARISH"
    return "NEUTRAL"

def calculate_poc(df, period=50):
    recent_df = df.tail(period).copy()
    if len(recent_df) < period: return df['close'].iloc[-1]
    recent_df['price_rounded'] = recent_df['close'].round(2)
    vol_profile = recent_df.groupby('price_rounded')['tick_volume'].sum()
    return vol_profile.idxmax()

def get_data(symbol, timeframe, n_bars=150):
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
    if rates is None or len(rates) < 50: return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    
    df['vwap'] = (df['close'] * df['tick_volume']).rolling(window=30).sum() / df['tick_volume'].rolling(window=30).sum()
    df['vwap_std'] = df['close'].rolling(window=30).std()
    df['vwap_up3'] = df['vwap'] + (3 * df['vwap_std'])
    df['vwap_dn3'] = df['vwap'] - (3 * df['vwap_std'])

    buy_vol = np.where(df['close'] > df['open'], df['tick_volume'], 0)
    sell_vol = np.where(df['close'] < df['open'], df['tick_volume'], 0)
    df['cvd'] = (buy_vol - sell_vol).cumsum()
    
    df['poc'] = df['close'].rolling(window=50).apply(lambda x: calculate_poc(df.loc[x.index]), raw=False).bfill()
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['std_20'] = df['close'].rolling(window=20).std()
    df['z_score'] = (df['close'] - df['sma_20']) / df['std_20']
    
    df['atr'] = (df['high'] - df['low']).rolling(window=14).mean()
    
    delta = df['close'].diff()
    up, down = delta.clip(lower=0).ewm(com=13).mean(), -1*delta.clip(upper=0).ewm(com=13).mean()
    df['rsi'] = 100 - (100 / (1 + up/down))
    
    return df.dropna()

def strategy_m1_traditional(symbol_name, df, spread, atr):
    if not is_market_active(symbol_name): return "NEUTRAL", 0, "FUORI ORARIO", "1M"
    
    macro = get_macro_trend(symbol_name)
    set_live_log(symbol_name, f"Macro: {macro} | Scan M1...")
    
    last, prev = df.iloc[-1], df.iloc[-2]
    rsi = last['rsi']
    is_green = last['close'] > last['open']
    
    spread_ratio = spread / atr if atr > 0 else 99
    if spread_ratio > 1.2: return "NEUTRAL", 0, "SPREAD ALTO", "1M"
        
    price_higher_high = last['high'] > prev['high']
    cvd_lower = last['cvd'] < prev['cvd']
    avg_vol = df['tick_volume'].rolling(20).mean().iloc[-2]

    # M1 DEPOTENZIATO: Solo se i volumi sono mostruosi (2.5x)
    if last['tick_volume'] > (avg_vol * 2.5): 
        if price_higher_high and cvd_lower and not is_green and macro == "BEARISH": 
            return "SELL", 88.0, "TRAD-CVD-DIV (A+)", "1M"
        price_lower_low = last['low'] < prev['low']
        cvd_higher = last['cvd'] > prev['cvd']
        if price_lower_low and cvd_higher and is_green and macro == "BULLISH": 
            return "BUY", 88.0, "TRAD-CVD-DIV (A+)", "1M"

    # VWAP e REJECT M1 rimosse per non soffocare M5!

    return "NEUTRAL", 0, "---", "1M"

def strategy_m5_traditional(symbol_name, df):
    if not is_market_active(symbol_name): return "NEUTRAL", 0, "FUORI ORARIO", "5M"
    
    macro = get_macro_trend(symbol_name)
    set_live_log(symbol_name, f"Macro: {macro} | Analisi Istituzionale M5...")
    
    last, prev = df.iloc[-1], df.iloc[-2]
    is_green = last['close'] > last['open']
    rsi = last['rsi']
    current_hour = datetime.now().hour
    avg_vol = df['tick_volume'].rolling(20).mean().iloc[-2]
    
    # 1. FVG & SILVER BULLET (Già presenti e ottime)
    bullish_fvg = prev['low'] > df.iloc[-4]['high']
    bearish_fvg = prev['high'] < df.iloc[-4]['low']
    if (9 <= current_hour <= 12) or (15 <= current_hour <= 17):
        if bullish_fvg and last['low'] < prev['low'] and is_green and macro == "BULLISH": return "BUY", 85.0, "TRAD-SILVER-BULLET", "5M"
        if bearish_fvg and last['high'] > prev['high'] and not is_green and macro == "BEARISH": return "SELL", 85.0, "TRAD-SILVER-BULLET", "5M"
    else:
        if bullish_fvg and last['low'] < prev['low'] and is_green and macro == "BULLISH": return "BUY", 75.0, "TRAD-FVG-RETEST", "5M"
        if bearish_fvg and last['high'] > prev['high'] and not is_green and macro == "BEARISH": return "SELL", 75.0, "TRAD-FVG-RETEST", "5M"

    # 2. POC REJECT (Già presente)
    if abs(last['low'] - last['poc']) < (last['atr'] * 0.2) and is_green and macro == "BULLISH": return "BUY", 82.0, "TRAD-POC-REJECT (A+)", "5M"
    if abs(last['high'] - last['poc']) < (last['atr'] * 0.2) and not is_green and macro == "BEARISH": return "SELL", 82.0, "TRAD-POC-REJECT (A+)", "5M"

    # 3. NUOVA STRATEGIA: VWAP TRAMPOLINE M5
    # Cerca un rimbalzo morbido sul VWAP a 5 minuti per cavalcare il trend principale
    if abs(last['low'] - last['vwap']) < (last['atr'] * 0.3) and is_green and macro == "BULLISH" and rsi < 55: 
        return "BUY", 80.0, "TRAD-VWAP-TRAMP", "5M"
    if abs(last['high'] - last['vwap']) < (last['atr'] * 0.3) and not is_green and macro == "BEARISH" and rsi > 45: 
        return "SELL", 80.0, "TRAD-VWAP-TRAMP", "5M"

    # 4. NUOVA STRATEGIA: M5 INSTITUTIONAL ENGULFING
    # Una candela M5 enorme e con alti volumi che si "mangia" la precedente, a favore di trend
    prev_is_green = prev['close'] > prev['open']
    if last['tick_volume'] > (avg_vol * 1.5):
        if is_green and not prev_is_green and last['close'] > prev['high'] and last['low'] < prev['low'] and macro == "BULLISH":
            return "BUY", 83.0, "TRAD-ENGULFING", "5M"
        if not is_green and prev_is_green and last['close'] < prev['low'] and last['high'] > prev['high'] and macro == "BEARISH":
            return "SELL", 83.0, "TRAD-ENGULFING", "5M"

    # 5. ASIA SWEEP & IB BREAKOUT (Già presenti)
    if 8 <= current_hour <= 11:
        asian_min = df['low'].tail(80).iloc[:-2].min()
        asian_max = df['high'].tail(80).iloc[:-2].max()
        if last['low'] < asian_min and last['close'] > asian_min and is_green and macro == "BULLISH": return "BUY", 80.0, "TRAD-ASIA-SWEEP", "5M"
        if last['high'] > asian_max and last['close'] < asian_max and not is_green and macro == "BEARISH": return "SELL", 80.0, "TRAD-ASIA-SWEEP", "5M"

    if 15 <= current_hour <= 18:
        ib_min = df['low'].tail(24).iloc[:-2].min()
        ib_max = df['high'].tail(24).iloc[:-2].max()
        vol_surge = last['tick_volume'] > (avg_vol * 1.5)
        if last['close'] > ib_max and vol_surge and is_green and macro == "BULLISH": return "BUY", 78.0, "TRAD-IB-BREAKOUT", "5M"
        if last['close'] < ib_min and vol_surge and not is_green and macro == "BEARISH": return "SELL", 78.0, "TRAD-IB-BREAKOUT", "5M"

    return "NEUTRAL", 0, "---", "5M"

def strategy_m1_crypto(symbol_name, df, spread, atr):
    last, prev = df.iloc[-1], df.iloc[-2]
    z, rsi = round(last['z_score'], 2), last['rsi']
    is_green = last['close'] > last['open']
    
    spread_ratio = spread / atr if atr > 0 else 99
    if spread_ratio > 1.2: return "NEUTRAL", 0, "SPREAD ALTO", "1M"

    macro = get_macro_trend(symbol_name)

    if last['low'] < last['vwap_dn3'] and is_green and rsi < 30 and macro != "BEARISH": 
        return "BUY", 88.0, "CRYPTO-VWAP-SNAP", "1M"
    if last['high'] > last['vwap_up3'] and not is_green and rsi > 70 and macro != "BULLISH": 
        return "SELL", 88.0, "CRYPTO-VWAP-SNAP", "1M"

    return "NEUTRAL", 0, "---", "1M"

def strategy_m5_crypto(symbol_name, df):
    last, prev = df.iloc[-1], df.iloc[-2]
    is_green = last['close'] > last['open']
    recent_df = df.tail(20).copy()
    macro = get_macro_trend(symbol_name)

    avg_vol_50 = df['tick_volume'].rolling(50).mean().iloc[-2]
    green_candles = recent_df[recent_df['close'] > recent_df['open']]
    if not green_candles.empty:
        max_vol_idx = green_candles['tick_volume'].idxmax()
        if max_vol_idx > recent_df.index[0]:
            ob_candle = recent_df.loc[max_vol_idx - 1]
            if ob_candle['tick_volume'] > avg_vol_50 * 2.0 and ob_candle['close'] < ob_candle['open']: 
                if ob_candle['low'] <= last['low'] <= ob_candle['high'] and is_green and macro == "BULLISH":
                    return "BUY", 85.0, "CRYPTO-OB-RETEST (A+)", "5M"

    red_candles = recent_df[recent_df['close'] < recent_df['open']]
    if not red_candles.empty:
        max_vol_idx = red_candles['tick_volume'].idxmax()
        if max_vol_idx > recent_df.index[0]:
            ob_candle = recent_df.loc[max_vol_idx - 1]
            if ob_candle['tick_volume'] > avg_vol_50 * 2.0 and ob_candle['close'] > ob_candle['open']: 
                if ob_candle['low'] <= last['high'] <= ob_candle['high'] and not is_green and macro == "BEARISH":
                    return "SELL", 85.0, "CRYPTO-OB-RETEST (A+)", "5M"

    return "NEUTRAL", 0, "---", "5M"

def open_trade(symbol, signal, atr_val, strat, timing):
    if symbol in last_trade_time and (time.time() - last_trade_time[symbol] < 120): return
    if len(mt5.positions_get()) >= MAX_TRADES_TOTAL: 
        return
    
    info, tick = mt5.symbol_info(symbol), mt5.symbol_info_tick(symbol)
    if info is None or tick is None: return
    spread = tick.ask - tick.bid
    
    if spread > (atr_val * 1.5): 
        set_log(f"🛡️ BLOCCO SPREAD: {symbol} ignorato. Spread broker eccessivo.")
        return 
    
    current_positions = mt5.positions_get(symbol=symbol)
    if current_positions:
        for p in current_positions:
            if p.magic != MAGIC_ID: continue
            if p.profit > 0:
                if (signal == "BUY" and p.type == 1) or (signal == "SELL" and p.type == 0):
                    close_price = tick.ask if p.type == 1 else tick.bid
                    mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": mt5.ORDER_TYPE_BUY if p.type == 1 else mt5.ORDER_TYPE_SELL, "position": p.ticket, "price": close_price, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
                    set_log(f"🔄 ANTI-CONFLITTO: Incassato profitto in controtendenza su {symbol}")
    
    def calculate_dynamic_lot(symbol, sl_dist_points):
        info = mt5.symbol_info(symbol)
        if not info or info.trade_tick_value == 0: return info.volume_min
        loss_ticks = sl_dist_points / info.trade_tick_size
        if loss_ticks > 0: lot = TARGET_EURO_PER_TRADE / (loss_ticks * info.trade_tick_value)
        else: lot = info.volume_min
        lot = round(lot / info.volume_step) * info.volume_step
        return max(info.volume_min, min(info.volume_max, lot))

    min_dist = max((info.trade_stops_level + 5) * info.point, info.point * 30)
    sl_dist = max((atr_val * 1.5) + spread, min_dist)
    
    lot = calculate_dynamic_lot(symbol, sl_dist)
    can_runner = lot >= (info.volume_min * 2)
    
    if can_runner:
        tp_dist = sl_dist * 5.0 
    else:
        tp_dist = max(sl_dist * 3.0, min_dist + (info.point * 20))
    
    price = tick.ask if signal == "BUY" else tick.bid
    sl = round(price - sl_dist if signal == "BUY" else price + sl_dist, info.digits)
    tp = round(price + tp_dist if signal == "BUY" else price - tp_dist, info.digits)
    
    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": round(lot, 2), 
        "type": mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL, 
        "price": price, "sl": sl, "tp": tp, "magic": MAGIC_ID, 
        "comment": f"K {timing} {strat}", "type_filling": mt5.ORDER_FILLING_IOC
    }
    
    res = mt5.order_send(request)
    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
        last_trade_time[symbol] = time.time()
        ticket = res.order
        if can_runner:
            initial_risks[ticket] = sl_dist
            set_log(f"🎯 SNIPER ESEGUITO: {symbol} {signal} {strat} | Lot: {lot} | TP RUNNER: {(tp_dist/info.point):.0f}pt")
        else:
            set_log(f"🎯 SNIPER ESEGUITO: {symbol} {signal} {strat} | Lot: {lot} | TP 1:3: {(tp_dist/info.point):.0f}pt")
    else:
        if res: set_log(f"⚠️ ORDINE RIFIUTATO: {symbol} (Codice MT5: {res.retcode})")

def get_live_confidence(symbol, p_type):
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, 15)
    if rates is None: return 50, "N/A"
    df = pd.DataFrame(rates)
    delta = df['close'].diff()
    up, down = delta.clip(lower=0).ewm(com=13).mean(), -1*delta.clip(upper=0).ewm(com=13).mean()
    rsi = 100 - (100 / (1 + up.iloc[-1]/down.iloc[-1]))
    
    if p_type == 0: # BUY
        if rsi < 35: return 10, "ABORT (Trend Inverso)"
        if rsi > 80: return 15, "MAX PROFIT (Iper-C)"
        if rsi > 65: return 80, "FORZA RIALZISTA"
        return 50, "STABILE"
    else: # SELL
        if rsi > 65: return 10, "ABORT (Trend Inverso)"
        if rsi < 20: return 15, "MAX PROFIT (Iper-V)"
        if rsi < 35: return 80, "FORZA RIBASSISTA"
        return 50, "STABILE"

def manage_positions():
    pos = mt5.positions_get()
    if pos is None: return
    now_ts = time.time()
    
    for p in pos:
        if p.magic != MAGIC_ID: continue
        info, tick = mt5.symbol_info(p.symbol), mt5.symbol_info_tick(p.symbol)
        if not info or not tick: continue
        if p.ticket in last_mod_time and (now_ts - last_mod_time[p.ticket] < 5): continue

        curr = tick.bid if p.type == 0 else tick.ask
        dist = (curr - p.price_open) if p.type == 0 else (p.price_open - curr)
        spread = tick.ask - tick.bid
        total_tp_dist = abs(p.tp - p.price_open)
        
        timing_strat = p.comment.split(" ")[1] if "K " in p.comment else "---"
        trade_duration_seconds = now_ts - p.time
        
        conf_value, action_msg = get_live_confidence(p.symbol, p.type)
        
        if p.symbol not in radar_state:
            radar_state[p.symbol] = {"sig": "IN TRADE", "timing": timing_strat, "conf": 0, "strat": "---", "status": "ATTESA", "live_conf": "N/A", "action": "---"}
            
        radar_state[p.symbol]["live_conf"] = f"{conf_value}%"
        radar_state[p.symbol]["action"] = action_msg

        if "ABORT" in action_msg and p.profit < -0.50:
            close_price = tick.bid if p.type == 0 else tick.ask
            close_type = mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY
            res = mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": close_type, "position": p.ticket, "price": close_price, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                set_log(f"✂️ EARLY CUT: Chiusura emergenza su {p.symbol}. Fuga prima dello Stop Loss.")
                continue

        if p.profit > (spread * info.trade_tick_value * 2) and ("MAX PROFIT" in action_msg or "ABORT" in action_msg):
            close_price = tick.bid if p.type == 0 else tick.ask
            close_type = mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY
            res = mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": close_type, "position": p.ticket, "price": close_price, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                set_log(f"💸 PROFIT CUT: Incasso immediato su {p.symbol}. Mercato in estensione/inversione.")
                continue

        if timing_strat == "1M" and trade_duration_seconds > 900: 
            if p.profit > -1.0 and p.profit < 1.0: 
                close_price = tick.bid if p.type == 0 else tick.ask
                close_type = mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY
                res = mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": close_type, "position": p.ticket, "price": close_price, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
                continue
        
        safe_stop_distance = max(info.trade_stops_level * info.point, spread * 1.5, info.point * 20)

        if p.ticket in initial_risks:
            target_dist = initial_risks[p.ticket] * 1.5 
            if dist >= target_dist and p.ticket not in partial_closed:
                close_vol = round((p.volume / 2.0) / info.volume_step) * info.volume_step
                if close_vol >= info.volume_min and (p.volume - close_vol) >= info.volume_min:
                    close_price = tick.bid if p.type == 0 else tick.ask
                    close_type = mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY
                    
                    res_pc = mt5.order_send({
                        "action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": close_vol, 
                        "type": close_type, "position": p.ticket, "price": close_price, 
                        "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC
                    })
                    if res_pc and res_pc.retcode == mt5.TRADE_RETCODE_DONE:
                        partial_closed.add(p.ticket)
                        set_live_log(p.symbol, "💰 PROFITTO MATURO INCASSATO!")
                        
                        new_sl = p.price_open + (spread + (info.point * 2)) if p.type == 0 else p.price_open - (spread + (info.point * 2))
                        if abs(curr - new_sl) > safe_stop_distance:
                            mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "position": p.ticket, "symbol": p.symbol, "sl": round(new_sl, info.digits), "tp": p.tp})
                        last_mod_time[p.ticket] = now_ts
                        continue
        
        if total_tp_dist > 0:
            new_sl = None
            if dist >= (total_tp_dist * 0.8):
                locked_profit = total_tp_dist * 0.5
                new_sl = p.price_open + locked_profit if p.type == 0 else p.price_open - locked_profit
            elif dist >= (total_tp_dist * 0.6): 
                locked_profit = total_tp_dist * 0.25 if p.ticket in initial_risks else spread + (info.point * 2)
                new_sl = p.price_open + locked_profit if p.type == 0 else p.price_open - locked_profit
                
            if new_sl is not None:
                is_better = (new_sl > p.sl) if p.type == 0 else (new_sl < p.sl or p.sl == 0)
                if is_better and abs(curr - new_sl) > safe_stop_distance:
                    res = mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "position": p.ticket, "symbol": p.symbol, "sl": round(new_sl, info.digits), "tp": p.tp})
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE: 
                        last_mod_time[p.ticket] = now_ts
                        radar_state[p.symbol]["action"] = "TRAILING ATTIVO"

def update_radar_state(symbol, sig, conf, strat, timing, status):
    if symbol not in radar_state:
        radar_state[symbol] = {"sig": "NEUTRAL", "timing": "---", "conf": 0, "strat": "---", "status": "ATTESA", "live_conf": "N/A", "action": "---"}
    radar_state[symbol]["sig"] = sig
    radar_state[symbol]["timing"] = timing
    radar_state[symbol]["conf"] = conf
    radar_state[symbol]["strat"] = strat
    radar_state[symbol]["status"] = status

def process_cascading_strategy(s, df, strategy_func, current_pos, tick=None):
    if df is not None:
        if strategy_func.__name__ in ['strategy_m1_traditional', 'strategy_m1_crypto'] and tick is not None:
            spread = tick.ask - tick.bid
            atr = df.iloc[-1]['atr']
            sig, conf, strat, timing = strategy_func(s, df, spread, atr) 
        else:
            sig, conf, strat, timing = strategy_func(s, df) 
            
        if sig != "NEUTRAL":
            if strat == "SPREAD ALTO" or strat == "FUORI ORARIO": return False 

            update_radar_state(s, sig, conf, strat, timing, f"LANCIO!")
            open_trade(s, sig, df.iloc[-1]['atr'], strat, timing)
            return True
    return False

def run_cycle():
    manage_positions()
    for s in symbols:
        info, tick = mt5.symbol_info(s), mt5.symbol_info_tick(s)
        
        if not tick or (time.time() - tick.time > 300):
            update_radar_state(s, "NEUTRAL", 0, "---", "---", "CHIUSO")
            continue
            
        pos = mt5.positions_get(symbol=s)
        bot_pos = [p for p in pos if p.magic == MAGIC_ID] if pos else []
        num_pos = len(bot_pos)
        
        is_crypto = s in CRYPTO_SYMBOLS
        
        if is_crypto and not crypto_enabled:
            if num_pos == 0:
                radar_state[s]["live_conf"] = "N/A"
                radar_state[s]["action"] = "---"
                update_radar_state(s, "NEUTRAL", 0, "---", "---", "OFF 🔴")
            else:
                update_radar_state(s, "IN TRADE", 100, "GESTIONE", "---", "OFF (IN CHIUSURA)")
            continue
            
        if num_pos >= MAX_TRADES_PER_SYMBOL:
            update_radar_state(s, "IN TRADE", 100, "ATTESA", "---", "1/1 (MAX REACHED)")
            continue
        
        strat_m1 = strategy_m1_crypto if is_crypto else strategy_m1_traditional
        strat_m5 = strategy_m5_crypto if is_crypto else strategy_m5_traditional

        if process_cascading_strategy(s, get_data(s, mt5.TIMEFRAME_M1, 60), strat_m1, num_pos, tick): continue
        if process_cascading_strategy(s, get_data(s, mt5.TIMEFRAME_M5, 80), strat_m5, num_pos): continue
        
        if num_pos == 0: 
            radar_state[s]["live_conf"] = "N/A"
            radar_state[s]["action"] = "---"
            update_radar_state(s, "NEUTRAL", 0, "---", "---", "ATTESA SETUP")

def close_all_now():
    set_log("⚠️ COMANDO: Chiusura di tutte le operazioni in corso...")
    for p in mt5.positions_get():
        if p.magic != MAGIC_ID: continue 
        tick = mt5.symbol_info_tick(p.symbol)
        if tick: mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY, "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
    set_log("💣 Portafoglio svuotato.")

def close_all_profitable():
    set_log("💰 COMANDO: Chiusura di tutte le operazioni in profitto...")
    closed_count = 0
    for p in mt5.positions_get():
        if p.magic == MAGIC_ID and p.profit > 0: 
            tick = mt5.symbol_info_tick(p.symbol)
            if tick: 
                res = mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume, "type": mt5.ORDER_TYPE_SELL if p.type == 0 else mt5.ORDER_TYPE_BUY, "position": p.ticket, "price": tick.bid if p.type == 0 else tick.ask, "deviation": 20, "magic": MAGIC_ID, "type_filling": mt5.ORDER_FILLING_IOC})
                if res and res.retcode == mt5.TRADE_RETCODE_DONE: closed_count += 1
    set_log(f"✅ Incassati {closed_count} trade in profitto.")

def force_be_on_all_profitable():
    for p in mt5.positions_get():
        if p.profit > 0 and p.magic == MAGIC_ID: 
            info, tick = mt5.symbol_info(p.symbol), mt5.symbol_info_tick(p.symbol)
            spread = tick.ask - tick.bid
            be_plus_margin = spread + (info.point * 15)
            min_be_price = p.price_open + be_plus_margin if p.type == 0 else p.price_open - be_plus_margin
            mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "position": p.ticket, "symbol": p.symbol, "sl": round(min_be_price, info.digits), "tp": p.tp})
    set_log("🛡️ BE manuale applicato a tutti i trade in positivo.")