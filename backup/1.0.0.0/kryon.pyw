import tkinter as tk
from tkinter import ttk
import threading
import MetaTrader5 as mt5
import pandas as pd
import time
from datetime import datetime
import os
import sys
from PIL import Image, ImageTk
import bot_core

VERSION = " 8.5.0"
AUTHOR_BRAND = "POWERED BY BONS ⚡ FOR MY DAUGHTER EMMA ❤️"
running = False

session_start_time = datetime.now()
initial_balance = None
crypto_active = True

def get_base_dir():
    if getattr(sys, 'frozen', False): return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
IMG_DIR = os.path.join(BASE_DIR, "immagini kryon")

BG = "#0f1117"           
CARD_BG = "#161b22"      
FG = "#00ffcc"           
BTN_START = "#22c55e"    
BTN_STOP = "#ef4444"     
BTN_ORANGE = "#f97316"   
BTN_BLUE = "#3b82f6"     
BTN_GOLD = "#eab308"     
LOG_BG = "#020617"       
ERR_BG = "#2a0a0a"       

def start_bot():
    global running, session_start_time, initial_balance
    running = True
    session_start_time = datetime.now()
    initial_balance = None 
    status_var.set("STATO: ON 🟢")
    status_label.config(fg=BTN_START)
    log_event("SISTEMA ONLINE: ACTIVE QUANTUM ENGINE 🟢.")

def stop_bot():
    global running
    running = False
    status_var.set("STATO: OFF 🔴")
    status_label.config(fg=BTN_STOP)
    log_event("SISTEMA OFFLINE: QUANTUM ENGINE PROCESSES OFF 🔴.")

def toggle_crypto_mode():
    global crypto_active
    crypto_active = not crypto_active
    bot_core.toggle_crypto(crypto_active)
    
    if crypto_active:
        crypto_status_var.set("CRYPTO: ON 🟢")
        crypto_status_label.config(fg=BTN_START)
        btn_crypto.change_style("🌐 CRYPTO: ON", BTN_BLUE)
    else:
        crypto_status_var.set("CRYPTO: OFF 🔴")
        crypto_status_label.config(fg=BTN_STOP)
        btn_crypto.change_style("🌐 CRYPTO: OFF", "#64748b")

def log_event(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_box.insert(tk.END, f"[{timestamp}] {msg}\n")
    log_box.see(tk.END)
    if int(float(log_box.index('end-1c'))) > 500: log_box.delete("1.0", "2.0")

def log_error(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    error_box.insert(tk.END, f"[{timestamp}] ⚠️ {msg}\n")
    error_box.see(tk.END)
    if int(float(error_box.index('end-1c'))) > 200: error_box.delete("1.0", "2.0")

def log_live_analysis(msg):
    live_analysis_box.insert(tk.END, f"{msg}\n")
    live_analysis_box.see(tk.END)
    if int(float(live_analysis_box.index('end-1c'))) > 500:
        live_analysis_box.delete("1.0", "2.0")

def update_logs():
    main_msg = bot_core.get_latest_log()
    if main_msg:
        if any(keyword in main_msg.upper() for keyword in ["❌", "ERRORE", "FALLITO", "⚠️", "INVALID", "⛔"]): 
            log_error(main_msg)
        else: 
            log_event(main_msg)
            
    live_msgs = bot_core.get_latest_live_logs()
    for msg in live_msgs:
        log_live_analysis(msg)
        
    root.after(400, update_logs)

def format_duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

def update_dashboard():
    global initial_balance
    if mt5.initialize():
        acc = mt5.account_info()
        if acc:
            if initial_balance is None:
                initial_balance = acc.balance

            balance_var.set(f"BILANCIO: {round(acc.balance, 2)} €")
            equity_var.set(f"EQUITY: {round(acc.equity, 2)} €")
            
            floating = round(acc.equity - acc.balance, 2)
            pnl_var.set(f"LIVE VALUE: {floating} €")
            lbl_pnl.config(fg="#22c55e" if floating >= 0 else "#ef4444")
            
            session_profit = round(acc.balance - initial_balance, 2)
            session_profit_var.set(f"PROFITTO SESSIONE: {session_profit} €")
            lbl_session.config(fg="#22c55e" if session_profit >= 0 else "#ef4444")
            
            closed_trades_count = 0
            safe_start = session_start_time + pd.Timedelta(seconds=1) 
            deals = mt5.history_deals_get(safe_start, datetime.now())
            if deals:
                unique_positions = set(d.position_id for d in deals if d.magic == bot_core.MAGIC_ID and d.entry == 1)
                closed_trades_count = len(unique_positions)
                
            trades_var.set(f"TRADE CONCLUSI: {closed_trades_count}")
        
        pos = mt5.positions_get()
        monitor_tree.delete(*monitor_tree.get_children())
        
        if pos:
            now_ts = time.time()
            for p in pos:
                if p.magic != bot_core.MAGIC_ID: continue 
                
                direction = "BUY 🟢" if p.type == 0 else "SELL 🔴"
                duration = format_duration(now_ts - p.time)
                profit_str = f"{round(p.profit, 2)} €"
                
                state = bot_core.radar_state.get(p.symbol, {})
                live_conf = state.get("live_conf", "---")
                action = state.get("action", "Holding")
                
                p_tag = "profit" if p.profit >= 0 else "loss"
                monitor_tree.insert("", "end", values=(p.symbol, direction, duration, profit_str, live_conf, action), tags=(p_tag,))

    root.after(2000, update_dashboard)

def update_analysis():
    if not running:
        radar_status_var.set("STAND-BY (Motore fermo) 📡")
        root.after(2000, update_analysis); return
    
    active_assets = bot_core.symbols
    current_time = datetime.now().strftime('%H:%M:%S')
    radar_status_var.set(f"{len(active_assets)} Asset Attivi | Ultimo aggiornamento: {current_time}")
    
    for s in active_assets:
        state = bot_core.radar_state.get(s, None)
        if not state: continue
        
        if state['status'] == "CHIUSO": st, tag = "🔴 CHIUSO", "closed_mkt"
        elif "SEGNALE" in state['status'] or "SHOT" in state['status'] or "LANCIO" in state['status']: 
            st, tag = f"🔥 {state['status']}", "entry"
        elif "GESTIONE" in state['status'] or "PYRAMID" in state['status'] or "ATTESA" in state['status']: 
            st, tag = f"🛡️ {state['status']}", "monitor"
        elif "OFF" in state['status']: 
            st, tag = f"🛑 {state['status']}", "closed_mkt"
        elif "RICERCA" in state['status']: 
            st, tag = f"📡 {state['status']}", "neutral"
        else: 
            st, tag = f"⏳ {state['status']}", "low_vol"

        exists = False
        for item in tree.get_children():
            if tree.item(item, "values")[0] == s:
                tree.item(item, values=(s, state['sig'], state['timing'], f"{state['conf']}%", st), tags=(tag,))
                exists = True; break
        if not exists: tree.insert("", "end", values=(s, state['sig'], state['timing'], f"{state['conf']}%", st), tags=(tag,))
        
    root.after(1000, update_analysis) 

def _on_main_mousewheel_y(event):
    main_canvas.yview_scroll(int(-1*(event.delta/120)), "units")

def _on_main_mousewheel_x(event):
    main_canvas.xview_scroll(int(-1*(event.delta/120)), "units")

def _on_inner_mousewheel(event):
    event.widget.yview_scroll(int(-1*(event.delta/120)), "units")
    return "break"

def create_scrollable_text_box(parent, title, text_bg, text_fg, height=9, font=("Consolas", 9)):
    container = tk.LabelFrame(parent, text=f" {title} ", bg=BG, fg=FG, font=("Segoe UI", 10, "bold"), bd=0, highlightthickness=1, highlightbackground=CARD_BG)
    inner_frame = tk.Frame(container, bg=BG)
    inner_frame.pack(fill="both", expand=True, padx=10, pady=10)
    scroll = ttk.Scrollbar(inner_frame, style="Vertical.TScrollbar")
    scroll.pack(side="right", fill="y")
    box = tk.Text(inner_frame, height=height, bg=text_bg, fg=text_fg, font=font, relief="flat", yscrollcommand=scroll.set, wrap="word")
    box.pack(side="left", fill="both", expand=True)
    scroll.config(command=box.yview)
    box.bind("<MouseWheel>", _on_inner_mousewheel)
    return container, box

class RoundedButton(tk.Canvas):
    def __init__(self, parent, text, bg_color, command, width, height=35):
        super().__init__(parent, bg=BG, highlightthickness=0, width=width, height=height)
        self.command = command
        self.bg_color = bg_color
        self.hover_color = self._adjust_color(bg_color, 1.2) 
        
        self.rect = self.create_rounded_rect(2, 2, width-2, height-2, r=15, fill=bg_color, outline=bg_color)
        self.text = self.create_text(width/2, height/2, text=text, fill="white", font=("Segoe UI", 10, "bold"))
        
        self.bind("<Button-1>", self._on_click)
        self.bind("<Enter>", self._on_hover)
        self.bind("<Leave>", self._on_leave)

    def create_rounded_rect(self, x1, y1, x2, y2, r, **kwargs):
        points = [x1+r, y1, x1+r, y1, x2-r, y1, x2-r, y1, x2, y1, x2, y1+r, x2, y1+r, x2, y2-r, x2, y2-r, x2, y2, x2-r, y2, x2-r, y2, x1+r, y2, x1+r, y2, x1, y2, x1, y2-r, x1, y2-r, x1, y1+r, x1, y1+r, x1, y1]
        return self.create_polygon(points, smooth=True, **kwargs)

    def change_style(self, text, bg_color):
        self.bg_color = bg_color
        self.hover_color = self._adjust_color(bg_color, 1.2)
        self.itemconfig(self.rect, fill=bg_color, outline=bg_color)
        self.itemconfig(self.text, text=text)

    def _on_click(self, event):
        self.itemconfig(self.rect, fill=self._adjust_color(self.bg_color, 0.8))
        self.after(100, lambda: self.itemconfig(self.rect, fill=self.hover_color))
        if self.command: self.command()

    def _on_hover(self, event):
        self.itemconfig(self.rect, fill=self.hover_color)

    def _on_leave(self, event):
        self.itemconfig(self.rect, fill=self.bg_color)
        
    def _adjust_color(self, hex_color, factor):
        hex_color = hex_color.lstrip('#')
        r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        r = min(int(r * factor), 255)
        g = min(int(g * factor), 255)
        b = min(int(b * factor), 255)
        return f'#{r:02x}{g:02x}{b:02x}'

# ==============================================================================
# COSTRUZIONE INTERFACCIA V8.0.0
# ==============================================================================
root = tk.Tk()
root.title(f"KRYON PRO v{VERSION}")
root.geometry("1300x950")
root.configure(bg=BG)

path_logo = os.path.join(IMG_DIR, "logo2.png")
try:
    img_icon = Image.open(path_logo)
    photo_icon = ImageTk.PhotoImage(img_icon)
    root.iconphoto(True, photo_icon)
except: pass

style = ttk.Style()
style.theme_use("clam")
style.configure("Vertical.TScrollbar", background=CARD_BG, bordercolor=BG, arrowcolor=FG, troughcolor=BG, relief="flat")
style.configure("Horizontal.TScrollbar", background=CARD_BG, bordercolor=BG, arrowcolor=FG, troughcolor=BG, relief="flat")
style.configure("Treeview", background=LOG_BG, foreground="white", fieldbackground=LOG_BG, rowheight=28, font=("Segoe UI", 10), borderwidth=0)
style.configure("Treeview.Heading", background=CARD_BG, foreground=FG, font=("Segoe UI", 10, "bold"), borderwidth=0)
style.map("Treeview", background=[("selected", "#1e293b")])

main_canvas = tk.Canvas(root, bg=BG, highlightthickness=0)
main_scrollbar_y = ttk.Scrollbar(root, orient="vertical", command=main_canvas.yview, style="Vertical.TScrollbar")
main_scrollbar_x = ttk.Scrollbar(root, orient="horizontal", command=main_canvas.xview, style="Horizontal.TScrollbar")

scroll_frame = tk.Frame(main_canvas, bg=BG)
canvas_window = main_canvas.create_window((0, 0), window=scroll_frame, anchor="nw")

def on_canvas_configure(event):
    main_canvas.configure(scrollregion=main_canvas.bbox("all"))
    min_width = scroll_frame.winfo_reqwidth()
    if event.width > min_width:
        main_canvas.itemconfig(canvas_window, width=event.width)
    else:
        main_canvas.itemconfig(canvas_window, width=min_width)

scroll_frame.bind("<Configure>", lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all")))
main_canvas.bind('<Configure>', on_canvas_configure)

main_canvas.configure(yscrollcommand=main_scrollbar_y.set, xscrollcommand=main_scrollbar_x.set)

root.bind_all("<MouseWheel>", _on_main_mousewheel_y)
root.bind_all("<Shift-MouseWheel>", _on_main_mousewheel_x)

main_scrollbar_y.pack(side="right", fill="y")
main_scrollbar_x.pack(side="bottom", fill="x")
main_canvas.pack(side="left", fill="both", expand=True)

header_bar = tk.Frame(scroll_frame, bg=BG)
header_bar.pack(fill="x", padx=30, pady=(15, 5))
tk.Label(header_bar, text=f"KRYON PRO v{VERSION}", font=("Segoe UI", 12, "bold"), bg=BG, fg=FG).pack(side="left")
tk.Label(header_bar, text=AUTHOR_BRAND, font=("Segoe UI", 11, "bold", "italic"), bg=BG, fg=BTN_GOLD).pack(side="right")

logo_frame = tk.Frame(scroll_frame, bg=BG)
logo_frame.pack(pady=5, fill="x")
try:
    img_pil = Image.open(path_logo)
    img_res = img_pil.resize((300, 164), Image.Resampling.LANCZOS) 
    img_tk = ImageTk.PhotoImage(img_res)
    logo_lbl = tk.Label(logo_frame, image=img_tk, bg=BG)
    logo_lbl.image = img_tk
    logo_lbl.pack()
except: tk.Label(logo_frame, text="KRYON QUANT", font=("Impact", 36), bg=BG, fg=FG).pack()

# --- INDICATORI DI STATO APPAIATI ---
status_frame = tk.Frame(logo_frame, bg=BG)
status_frame.pack(pady=5)

status_var = tk.StringVar(value="SISTEMA IN ATTESA")
status_label = tk.Label(status_frame, textvariable=status_var, font=("Segoe UI", 14, "bold"), bg=BG, fg="red")
status_label.pack(side="left", padx=15)

crypto_status_var = tk.StringVar(value="CRYPTO: ON 🟢")
crypto_status_label = tk.Label(status_frame, textvariable=crypto_status_var, font=("Segoe UI", 14, "bold"), bg=BG, fg=BTN_START)
crypto_status_label.pack(side="left", padx=15)

# --- STATS CONTAINER ---
stats_container = tk.Frame(scroll_frame, bg=CARD_BG, bd=0, highlightthickness=1, highlightbackground="#1e293b")
stats_container.pack(pady=10, fill="x", padx=30, ipadx=10, ipady=10)

balance_var = tk.StringVar()
equity_var = tk.StringVar()
pnl_var = tk.StringVar()
session_profit_var = tk.StringVar()
trades_var = tk.StringVar()

lbl_balance = tk.Label(stats_container, textvariable=balance_var, bg=CARD_BG, fg="white", font=("Segoe UI", 11, "bold"))
lbl_equity = tk.Label(stats_container, textvariable=equity_var, bg=CARD_BG, fg="white", font=("Segoe UI", 11, "bold"))
lbl_pnl = tk.Label(stats_container, textvariable=pnl_var, bg=CARD_BG, font=("Segoe UI", 11, "bold"))
lbl_session = tk.Label(stats_container, textvariable=session_profit_var, bg=CARD_BG, font=("Segoe UI", 11, "bold"))
lbl_trades = tk.Label(stats_container, textvariable=trades_var, bg=CARD_BG, fg=BTN_GOLD, font=("Segoe UI", 11, "bold"))

lbl_balance.pack(side="left", expand=True)
lbl_equity.pack(side="left", expand=True)
lbl_pnl.pack(side="left", expand=True)
lbl_session.pack(side="left", expand=True)
lbl_trades.pack(side="left", expand=True)

# --- BOTTONI (Aggiunto Bottone Crypto) ---
btn_container = tk.Frame(scroll_frame, bg=BG)
btn_container.pack(pady=15, anchor="center")

btn_crypto = RoundedButton(btn_container, text="🌐 CRYPTO: ON", bg_color=BTN_BLUE, command=toggle_crypto_mode, width=140)
btn_crypto.pack(side="left", padx=8)

buttons_layout = [
    ("🚀 START", BTN_START, start_bot, 120),
    ("🛑 STOP", BTN_STOP, stop_bot, 120),
    ("❌ CHIUDI TUTTE", BTN_ORANGE, bot_core.close_all_now, 150), 
    ("🛡️BREAKEVEN ", BTN_BLUE, bot_core.force_be_on_all_profitable, 140),
    ("💰 CHIUDI PROFIT", BTN_GOLD, bot_core.close_all_profitable, 150)
]

for txt, clr, cmd, w in buttons_layout:
    btn = RoundedButton(btn_container, text=txt, bg_color=clr, command=cmd, width=w)
    btn.pack(side="left", padx=8)

# ==============================================================================
# GRIGLIA SIMMETRICA
# ==============================================================================

row1_frame = tk.Frame(scroll_frame, bg=BG)
row1_frame.pack(fill="x", padx=30, pady=(15, 5))

ai_engine_frame = tk.LabelFrame(row1_frame, text=" 🎯 ANALIZER ", bg=BG, fg=FG, font=("Segoe UI", 11, "bold"), bd=0, highlightthickness=1, highlightbackground=CARD_BG)
ai_engine_frame.pack(side="left", fill="both", expand=True, padx=(0, 15))

radar_status_var = tk.StringVar(value="📡 Inizializzazione...")
tk.Label(ai_engine_frame, textvariable=radar_status_var, bg=BG, fg="#94a3b8", font=("Segoe UI", 9)).pack(anchor="w", padx=15, pady=(5,0))

tree_frame = tk.Frame(ai_engine_frame, bg=BG)
tree_frame.pack(fill="both", expand=True, padx=15, pady=10)
tree_scroll = ttk.Scrollbar(tree_frame, style="Vertical.TScrollbar")
tree_scroll.pack(side="right", fill="y")

tree = ttk.Treeview(tree_frame, columns=("Asset", "Segnale", "Timing", "Confidenza", "Status"), show="headings", height=8, yscrollcommand=tree_scroll.set)
tree.pack(side="left", fill="both", expand=True)
tree_scroll.config(command=tree.yview)

cols_width = {"Asset": 80, "Segnale": 80, "Timing": 80, "Confidenza": 80, "Status": 140}
for col, width in cols_width.items(): 
    tree.heading(col, text=col)
    tree.column(col, anchor="center", width=width)
tree.bind("<MouseWheel>", _on_inner_mousewheel)

tree.tag_configure("entry", foreground="#22c55e")
tree.tag_configure("monitor", foreground="#eab308")
tree.tag_configure("neutral", foreground="#38bdf8") 
tree.tag_configure("low_vol", foreground="#64748b")
tree.tag_configure("closed_mkt", foreground="#ef4444") 

live_container, live_analysis_box = create_scrollable_text_box(row1_frame, "📜 SNIPING SCANNER ", LOG_BG, "#fca5a5", height=10)
live_analysis_box.config(font=("Consolas", 8)) 
live_container.pack(side="left", fill="both", expand=True, padx=(15, 0))

row2_frame = tk.Frame(scroll_frame, bg=BG)
row2_frame.pack(fill="x", padx=30, pady=10)

monitor_frame = tk.LabelFrame(row2_frame, text=" 🔬 OPERATIONS IN PROGRESS ", bg=BG, fg=BTN_BLUE, font=("Segoe UI", 11, "bold"), bd=0, highlightthickness=1, highlightbackground=CARD_BG)
monitor_frame.pack(side="left", fill="both", expand=True, padx=(0, 15))

monitor_inner = tk.Frame(monitor_frame, bg=BG)
monitor_inner.pack(fill="both", expand=True, padx=10, pady=10)
monitor_scroll = ttk.Scrollbar(monitor_inner, style="Vertical.TScrollbar")
monitor_scroll.pack(side="right", fill="y")

monitor_tree = ttk.Treeview(monitor_inner, columns=("Asset", "Dir", "Time", "Profit", "Live Conf", "Action"), show="headings", height=8, yscrollcommand=monitor_scroll.set)
monitor_tree.pack(side="left", fill="both", expand=True)
monitor_scroll.config(command=monitor_tree.yview)

m_cols_width = {"Asset": 80, "Dir": 70, "Time": 80, "Profit": 80, "Live Conf": 90, "Action": 150}
for col, width in m_cols_width.items(): 
    monitor_tree.heading(col, text=col)
    monitor_tree.column(col, anchor="center", width=width)
monitor_tree.bind("<MouseWheel>", _on_inner_mousewheel)

monitor_tree.tag_configure("profit", foreground="#22c55e")
monitor_tree.tag_configure("loss", foreground="#ef4444")

log_container, log_box = create_scrollable_text_box(row2_frame, "📜 REGISTRO ATTIVITÀ", LOG_BG, "white", height=10)
log_container.pack(side="left", fill="both", expand=True, padx=(15, 0))

row3_frame = tk.Frame(scroll_frame, bg=BG)
row3_frame.pack(fill="x", padx=30, pady=(5, 20))

err_container, error_box = create_scrollable_text_box(row3_frame, "⚠️ ERROR TRACKER", ERR_BG, "#fca5a5", height=6)
err_container.config(fg="#ef4444")
err_container.pack(fill="both", expand=True)

def main_loop():
    while True:
        if running: 
            try: bot_core.run_cycle()
            except Exception as e: log_error(f"Errore critico script: {e}")
        time.sleep(10)

def on_closing():
    bot_core.set_log("Spegnimento forzato del sistema...")
    root.destroy()
    os._exit(0)

root.protocol("WM_DELETE_WINDOW", on_closing) 

threading.Thread(target=main_loop, daemon=True).start()
update_dashboard(); update_analysis(); update_logs(); root.mainloop()