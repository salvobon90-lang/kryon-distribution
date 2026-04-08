import tkinter as tk
from tkinter import ttk
import threading
import MetaTrader5 as mt5
import time
from datetime import datetime
import os
import sys
import traceback
from PIL import Image, ImageTk
import bot_core

VERSION = " 31.0.0 (SMART LOCK & HYBRID)"
AUTHOR_BRAND = "POWERED BY BONS ⚡ FOR MY DAUGHTER EMMA ❤️ AND MY TRUE LOVE LAUPHI ❤️  "
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

# ==============================================================================
# LOGICA UI & BOT
# ==============================================================================
def start_bot():
    global running, session_start_time, initial_balance
    running = True
    session_start_time = datetime.now()
    initial_balance = None 
    status_var.set("STATO: ON 🟢")
    status_label.config(fg="#22c55e")
    log_event("SISTEMA ONLINE: ENGINE OPERATIVO.")

def stop_bot():
    global running
    running = False
    status_var.set("STATO: OFF 🔴")
    status_label.config(fg="#ef4444")
    log_event("SISTEMA OFFLINE.")

def toggle_crypto_mode():
    global crypto_active
    crypto_active = not crypto_active
    bot_core.toggle_crypto(crypto_active)
    if crypto_active:
        crypto_status_var.set("CRYPTO: ON 🟢")
        crypto_status_label.config(fg="#22c55e")
        btn_crypto.change_style("🌐 CRYPTO: ON", BTN_BLUE)
    else:
        crypto_status_var.set("CRYPTO: OFF 🔴")
        crypto_status_label.config(fg="#ef4444")
        btn_crypto.change_style("🌐 CRYPTO: OFF", "#64748b")

# 🔴 FIX 1: Robust Text Index Parsing
def log_event(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_box.insert(tk.END, f"[{timestamp}] {msg}\n")
    log_box.see(tk.END)
    if int(log_box.index('end-1c').split('.')[0]) > 300: log_box.delete("1.0", "2.0")

def log_error(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    error_box.insert(tk.END, f"[{timestamp}] ⚠️ {msg}\n")
    error_box.see(tk.END)
    if int(error_box.index('end-1c').split('.')[0]) > 100: error_box.delete("1.0", "2.0")

def log_live_analysis(msg):
    live_analysis_box.insert(tk.END, f"{msg}\n")
    live_analysis_box.see(tk.END)
    if int(live_analysis_box.index('end-1c').split('.')[0]) > 300: live_analysis_box.delete("1.0", "2.0")

def update_logs():
    main_msg = bot_core.get_latest_log()
    if main_msg:
        if any(kw in main_msg.upper() for kw in ["❌", "ERRORE", "FALLITO", "⚠️", "INVALID", "⛔", "BLOCK", "FAIL", "FATAL"]): 
            log_error(main_msg)
        else: 
            log_event(main_msg)
            
    live_msgs = bot_core.get_latest_live_logs()
    for msg in live_msgs: log_live_analysis(msg)
    root.after(400, update_logs)

def format_duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

class RoundedButton(tk.Canvas):
    def __init__(self, parent, text, bg_color, command, width, height=40):
        super().__init__(parent, bg=CARD_BG, highlightthickness=0, width=width, height=height)
        self.command = command
        self.bg_color = bg_color
        self.hover_color = self._adjust_color(bg_color, 1.2) 
        self.rect = self.create_rounded_rect(2, 2, width-2, height-2, r=15, fill=bg_color, outline=bg_color)
        self.text = self.create_text(width/2, height/2, text=text, fill="white", font=("Segoe UI", 11, "bold"))
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
        if self.command: threading.Thread(target=self.command, daemon=True).start() # 🔴 Thread isolation per bottoni manuali
    def _on_hover(self, event): self.itemconfig(self.rect, fill=self.hover_color)
    def _on_leave(self, event): self.itemconfig(self.rect, fill=self.bg_color)
    def _adjust_color(self, hex_color, factor):
        hex_color = hex_color.lstrip('#')
        r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        return f'#{min(int(r*factor), 255):02x}{min(int(g*factor), 255):02x}{min(int(b*factor), 255):02x}'

# ==============================================================================
# ROOT & SIDEBAR
# ==============================================================================
root = tk.Tk()
root.title(f"KRYON PRO v{VERSION}")
root.minsize(1100, 700) 
try: root.state("zoomed") 
except: root.geometry("1400x900") 
root.configure(bg=BG)
try: root.tk.call('tk', 'scaling', 1.2) 
except: pass

style = ttk.Style()
style.theme_use("clam")
style.configure("Vertical.TScrollbar", background=CARD_BG, bordercolor=BG, arrowcolor=FG, troughcolor=BG, relief="flat")
style.configure("Treeview", background=LOG_BG, foreground="white", fieldbackground=LOG_BG, rowheight=24, font=("Segoe UI", 9), borderwidth=0)
style.configure("Treeview.Heading", background=CARD_BG, foreground=FG, font=("Segoe UI", 9, "bold"), borderwidth=0)
style.map("Treeview", background=[("selected", "#1e293b")])

root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(1, weight=1)

sidebar = tk.Frame(root, bg=CARD_BG, width=220)
sidebar.grid(row=0, column=0, sticky="ns")
sidebar.grid_propagate(False)

path_logo = os.path.join(IMG_DIR, "logo2.png")
try:
    img_pil = Image.open(path_logo)
    img_res = img_pil.resize((180, 100), Image.Resampling.LANCZOS)
    img_tk = ImageTk.PhotoImage(img_res)
    logo_lbl = tk.Label(sidebar, image=img_tk, bg=CARD_BG)
    logo_lbl.image = img_tk
    logo_lbl.pack(pady=20)
except:
    tk.Label(sidebar, text="KRYON", bg=CARD_BG, fg=FG, font=("Impact", 24, "bold")).pack(pady=20)

RoundedButton(sidebar, "🚀 START", BTN_START, start_bot, 180).pack(pady=5)
RoundedButton(sidebar, "🛑 STOP", BTN_STOP, stop_bot, 180).pack(pady=5)
RoundedButton(sidebar, "❌ EMERGENZA", BTN_ORANGE, bot_core.close_all_now, 180).pack(pady=5)
RoundedButton(sidebar, "💰 CLOSE PROFIT", "#10b981", bot_core.close_only_profit, 180).pack(pady=5)
RoundedButton(sidebar, "🛡️ BE+", "#8b5cf6", bot_core.force_break_even_plus, 180).pack(pady=5)

btn_crypto = RoundedButton(sidebar, "🌐 CRYPTO: ON", BTN_BLUE, toggle_crypto_mode, 180)
btn_crypto.pack(pady=5)

status_var = tk.StringVar(value="IN ATTESA")
status_label = tk.Label(sidebar, textvariable=status_var, fg="#ef4444", bg=CARD_BG, font=("Segoe UI", 14, "bold"))
status_label.pack(pady=10)
crypto_status_var = tk.StringVar(value="CRYPTO: ON 🟢")
crypto_status_label = tk.Label(sidebar, textvariable=crypto_status_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG, fg="#22c55e")
crypto_status_label.pack(pady=5)

tk.Label(sidebar, text=AUTHOR_BRAND, bg=CARD_BG, fg=BTN_GOLD, font=("Segoe UI", 8, "italic"), wraplength=180).pack(side="bottom", pady=20)

# ==============================================================================
# WORKSPACE: GRIGLIA FISSA (STRICT PROP LAYOUT)
# ==============================================================================
workspace = tk.Frame(root, bg=BG)
workspace.grid(row=0, column=1, sticky="nsew")

workspace.grid_rowconfigure(0, weight=1)
workspace.grid_rowconfigure(1, weight=1)
workspace.grid_rowconfigure(2, weight=1)
workspace.grid_columnconfigure(0, weight=1)
workspace.grid_columnconfigure(1, weight=1)

def add_title(parent, text):
    tk.Label(parent, text=f" {text} ", bg="#020617", fg=FG, font=("Segoe UI", 10, "bold"), anchor="w").pack(fill="x")

mod_analyzer = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_analyzer.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_analyzer, "🎯 ANALYZER")

radar_status_var = tk.StringVar(value="📡 Inizializzazione...")
tk.Label(mod_analyzer, textvariable=radar_status_var, bg=CARD_BG, fg="#94a3b8", font=("Segoe UI", 9)).pack(anchor="w", padx=5)
tree = ttk.Treeview(mod_analyzer, columns=("Asset", "Segnale", "Confidenza", "Status"), show="headings")
tree_scroll = ttk.Scrollbar(mod_analyzer, orient="vertical", command=tree.yview)
tree.configure(yscrollcommand=tree_scroll.set)
tree_scroll.pack(side="right", fill="y")
tree.pack(fill="both", expand=True, padx=5, pady=5)
for col in ("Asset", "Segnale", "Confidenza", "Status"):
    tree.heading(col, text=col)
    tree.column(col, stretch=True)
tree.tag_configure("entry", foreground="#22c55e")
tree.tag_configure("monitor", foreground="#eab308")
tree.tag_configure("watch", foreground="#c084fc")
tree.tag_configure("closed_mkt", foreground="#ef4444")
tree.tag_configure("neutral", foreground="#38bdf8")

mod_control = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_control.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_control, "🧠 PROP CONTROL PANEL")

prop_grid = tk.Frame(mod_control, bg=CARD_BG)
prop_grid.pack(fill="both", expand=True, padx=10, pady=10)

ai_mode_var, eq_mode_var, dd_var = tk.StringVar(value="---"), tk.StringVar(value="---"), tk.StringVar(value="0.0%")
wr_var, sig_var, exec_var = tk.StringVar(value="0.0%"), tk.StringVar(value="0 / 0"), tk.StringVar(value="0")

tk.Label(prop_grid, text="AI MODE:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w", pady=2)
lbl_ai = tk.Label(prop_grid, textvariable=ai_mode_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_ai.grid(row=0, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="EQUITY MODE:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=1, column=0, sticky="w", pady=2)
lbl_eq = tk.Label(prop_grid, textvariable=eq_mode_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_eq.grid(row=1, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="DRAWDOWN:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=2, column=0, sticky="w", pady=2)
lbl_dd = tk.Label(prop_grid, textvariable=dd_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_dd.grid(row=2, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="WINRATE:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=3, column=0, sticky="w", pady=2)
tk.Label(prop_grid, textvariable=wr_var, font=("Segoe UI", 11, "bold"), fg="white", bg=CARD_BG).grid(row=3, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="SIGNALS/FLT:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=4, column=0, sticky="w", pady=2)
tk.Label(prop_grid, textvariable=sig_var, font=("Segoe UI", 11, "bold"), fg="white", bg=CARD_BG).grid(row=4, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="EXECUTED:", fg="#94a3b8", bg=CARD_BG, font=("Segoe UI", 10, "bold")).grid(row=5, column=0, sticky="w", pady=2)
tk.Label(prop_grid, textvariable=exec_var, font=("Segoe UI", 11, "bold"), fg="#3b82f6", bg=CARD_BG).grid(row=5, column=1, sticky="w", pady=2, padx=10)

mod_positions = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_positions.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_positions, "🔬 OPEN POSITIONS")

monitor_tree = ttk.Treeview(mod_positions, columns=("Asset", "Dir", "Time", "Profit", "Info"), show="headings")
pos_scroll = ttk.Scrollbar(mod_positions, orient="vertical", command=monitor_tree.yview)
monitor_tree.configure(yscrollcommand=pos_scroll.set)
pos_scroll.pack(side="right", fill="y")
monitor_tree.pack(fill="both", expand=True, padx=5, pady=5)
for col in ("Asset", "Dir", "Time", "Profit", "Info"):
    monitor_tree.heading(col, text=col)
    monitor_tree.column(col, stretch=True)
monitor_tree.tag_configure("profit", foreground="#22c55e")
monitor_tree.tag_configure("loss", foreground="#ef4444")

mod_telemetry = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_telemetry.grid(row=1, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_telemetry, "📊 TELEMETRY DASHBOARD")

dash_top = tk.Frame(mod_telemetry, bg=CARD_BG)
dash_top.pack(fill="x", padx=5, pady=5)

balance_var, pnl_var = tk.StringVar(), tk.StringVar()
tk.Label(dash_top, text="BALANCE: ", fg="gray", bg=CARD_BG, font=("Segoe UI", 10, "bold")).pack(side="left")
tk.Label(dash_top, textvariable=balance_var, fg="white", bg=CARD_BG, font=("Segoe UI", 12, "bold")).pack(side="left", padx=(0,15))
tk.Label(dash_top, text="PNL LIVE: ", fg="gray", bg=CARD_BG, font=("Segoe UI", 10, "bold")).pack(side="left")
lbl_pnl = tk.Label(dash_top, textvariable=pnl_var, bg=CARD_BG, font=("Segoe UI", 12, "bold"))
lbl_pnl.pack(side="left")

session_frame = tk.Frame(dash_top, bg=CARD_BG)
session_frame.pack(side="right")
session_pnl_var = tk.StringVar(value="Sess PNL: 0.0€")
lbl_sess_pnl = tk.Label(session_frame, textvariable=session_pnl_var, bg=CARD_BG, font=("Segoe UI", 10, "bold"), fg="#38bdf8")
lbl_sess_pnl.pack(side="right", padx=5)

heat_frame = tk.LabelFrame(mod_telemetry, text="SIGNAL HEATMAP", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground="#1e293b")
heat_frame.pack(fill="x", padx=5, pady=5)

dash_bot = tk.Frame(mod_telemetry, bg=CARD_BG)
dash_bot.pack(fill="both", expand=True, padx=5, pady=5)
dash_bot.columnconfigure((0, 1), weight=1)

exp_frame = tk.LabelFrame(dash_bot, text="ACTIVE EXPOSURE", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground="#1e293b")
exp_frame.grid(row=0, column=0, sticky="nsew", padx=2)
risk_frame = tk.LabelFrame(dash_bot, text="RISK STATUS", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground="#1e293b")
risk_frame.grid(row=0, column=1, sticky="nsew", padx=2)
risk_label = tk.Label(risk_frame, font=("Segoe UI", 14, "bold"), bg=CARD_BG)
risk_label.pack(expand=True)

mod_log = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_log.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_log, "📜 SNIPING SCANNER & ACTIVITY LOG")

scan_cont = tk.Frame(mod_log, bg=LOG_BG)
scan_cont.pack(fill="both", expand=True, padx=5, pady=(5, 2))
tk.Label(scan_cont, text="SNIPING SCANNER", bg=LOG_BG, fg=FG, font=("Segoe UI", 8, "bold")).pack(anchor="w")
scroll_scan = ttk.Scrollbar(scan_cont)
scroll_scan.pack(side="right", fill="y")
live_analysis_box = tk.Text(scan_cont, bg=LOG_BG, fg="#fca5a5", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_scan.set)
live_analysis_box.pack(fill="both", expand=True)
scroll_scan.config(command=live_analysis_box.yview)

log_cont = tk.Frame(mod_log, bg=LOG_BG)
log_cont.pack(fill="both", expand=True, padx=5, pady=(2, 5))
tk.Label(log_cont, text="REGISTRO ATTIVITÀ", bg=LOG_BG, fg=FG, font=("Segoe UI", 8, "bold")).pack(anchor="w")
scroll_log = ttk.Scrollbar(log_cont)
scroll_log.pack(side="right", fill="y")
log_box = tk.Text(log_cont, bg=LOG_BG, fg="white", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_log.set)
log_box.pack(fill="both", expand=True)
scroll_log.config(command=log_box.yview)

mod_debug = tk.Frame(workspace, bg=CARD_BG, highlightthickness=1, highlightbackground="#1e293b")
mod_debug.grid(row=2, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_debug, "⚠️ DEBUG STATE & ERRORS")

debug_cont = tk.Frame(mod_debug, bg=LOG_BG)
debug_cont.pack(fill="both", expand=True, padx=5, pady=(5, 2))
tk.Label(debug_cont, text="DEBUG STATE", bg=LOG_BG, fg=FG, font=("Segoe UI", 8, "bold")).pack(anchor="w")
debug_tree = ttk.Treeview(debug_cont, columns=("Asset", "Data", "Signal", "Blocked", "Final"), show="headings")
debug_scroll = ttk.Scrollbar(debug_cont, orient="vertical", command=debug_tree.yview)
debug_tree.configure(yscrollcommand=debug_scroll.set)
debug_scroll.pack(side="right", fill="y")
debug_tree.pack(fill="both", expand=True)
for col in ("Asset", "Data", "Signal", "Blocked", "Final"):
    debug_tree.heading(col, text=col)
    debug_tree.column(col, stretch=True)
debug_tree.tag_configure("ok", foreground="#22c55e")
debug_tree.tag_configure("warn", foreground="#eab308")
debug_tree.tag_configure("bad", foreground="#ef4444")

err_cont = tk.Frame(mod_debug, bg=ERR_BG)
err_cont.pack(fill="both", expand=True, padx=5, pady=(2, 5))
tk.Label(err_cont, text="ERROR TRACKER", bg=ERR_BG, fg="#fca5a5", font=("Segoe UI", 8, "bold")).pack(anchor="w")
scroll_err = ttk.Scrollbar(err_cont)
scroll_err.pack(side="right", fill="y")
error_box = tk.Text(err_cont, bg=ERR_BG, fg="#fca5a5", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_err.set)
error_box.pack(fill="both", expand=True)
scroll_err.config(command=error_box.yview)

# ==============================================================================
# UPDATERS UI
# ==============================================================================
def update_prop_panel():
    if running:
        data = bot_core.get_decision_data()
        if data:
            ai_mode_var.set(data.get("ai_mode", "---"))
            eq_mode_var.set(data.get("equity_mode", "---"))
            dd_var.set(f"{data.get('drawdown', 0.0)}%")
            wr_var.set(f"{data.get('winrate', 0.0)}%")
            sig_var.set(f"{data.get('signals', 0)} / {data.get('filtered', 0)}")
            exec_var.set(str(data.get('executed', 0)))

            lbl_ai.config(fg="#22c55e" if data.get("ai_mode") == "AGGRESSIVE" else "#eab308")
            lbl_eq.config(fg="#22c55e" if data.get("equity_mode") == "AGGRESSIVE" else "#ef4444")
            lbl_dd.config(fg="#22c55e" if data.get("drawdown", 0) < 3 else "#ef4444")
            
            sess_prof = data.get('session_profit', 0)
            closed_num = data.get('closed_trades', 0)
            session_pnl_var.set(f"Sess PNL: {sess_prof}€ ({closed_num} Trd)")
            lbl_sess_pnl.config(fg="#22c55e" if sess_prof >= 0 else "#ef4444")

            if mt5.terminal_info():
                acc = mt5.account_info()
                if acc:
                    balance_var.set(f"{round(acc.balance, 2)} €")
                    pnl = acc.equity - acc.balance
                    pnl_var.set(f"{round(pnl, 2)} €")
                    lbl_pnl.config(fg="#22c55e" if pnl >= 0 else "#ef4444")
    root.after(1000, update_prop_panel)

def update_debug_panel():
    if running:
        data = bot_core.get_debug_state()
        debug_tree.delete(*debug_tree.get_children())
        for s, d in data.items():
            status = "ok"
            if not d.get("data", False): status = "bad"
            elif d.get("blocked_by") and d.get("blocked_by") != "CLEARED": status = "warn"
            debug_tree.insert("", "end", values=(s, "YES" if d.get("data") else "NO", d.get("signal", ""), d.get("blocked_by", "-"), d.get("final", "")), tags=(status,))
    root.after(1000, update_debug_panel)

def update_telemetry():
    if running:
        for widget in heat_frame.winfo_children(): widget.destroy()
        for s, state in bot_core.radar_state.items():
            sig = state.get("sig", "NEUTRAL")
            conf = state.get("conf", 0)
            color = "#22c55e" if sig == "BUY" else ("#ef4444" if sig == "SELL" else "#64748b")
            tk.Label(heat_frame, text=f"{s}\n{conf}%", bg=color, fg="white", font=("Segoe UI", 8, "bold")).pack(side="left", padx=2, pady=5, expand=True, fill="x")
        
        acc = mt5.account_info() if mt5.terminal_info() else None
        if not acc: risk_label.config(text="RISK: NO DATA", fg="gray")
        else:
            dd = (acc.balance - acc.equity) / acc.balance if acc.balance > 0 else 0
            if dd < 0.02: risk_label.config(text="RISK: SAFE", fg="#22c55e")
            elif dd < 0.05: risk_label.config(text="RISK: MEDIUM", fg="#eab308")
            else: risk_label.config(text="RISK: DANGER", fg="#ef4444")

        for w in exp_frame.winfo_children(): w.destroy()
        pos = mt5.positions_get() or [] if mt5.terminal_info() else []
        exposure = {}
        for p in pos:
            if p.magic == bot_core.MAGIC_ID: exposure[p.symbol] = exposure.get(p.symbol, 0) + p.volume
        if not exposure:
            tk.Label(exp_frame, text="FLAT", fg="gray", bg=CARD_BG, font=("Consolas", 10)).pack(anchor="w", padx=5, pady=5)
        else:
            for s, vol in exposure.items():
                tk.Label(exp_frame, text=f"▪ {s}: {round(vol,2)} Lts", fg="white", bg="#1e293b", font=("Consolas", 10, "bold")).pack(anchor="w", padx=5, pady=2, fill="x")

    root.after(1500, update_telemetry)

def update_analysis():
    items = []
    for s in bot_core.symbols:
        state = bot_core.radar_state.get(s, {})
        st = state.get("status", "INIT")
        sig = state.get("sig", "---")
        conf = state.get("conf", 0)
        
        st_u = st.upper()
        if "LANCIO" in st_u: prio, tag = 1, "entry"
        elif "CORSO" in st_u: prio, tag = 2, "monitor"
        elif "WATCH" in st_u or "PRE-MARKET" in st_u: prio, tag = 3, "watch"
        elif "SCANSIONE" in st_u: prio, tag = 4, "neutral"
        elif "FILTRATO" in st_u or "WAIT" in st_u: prio, tag = 5, "low_vol"
        elif "CHIUSO" in st_u or "NON DISP" in st_u or "FEED" in st_u: prio, tag = 6, "closed_mkt"
        else: prio, tag = 99, "neutral"

        items.append((s, sig, f"{conf}%", st, tag, prio))
        
    items.sort(key=lambda x: x[5])
    existing = {tree.item(i)["values"][0]: i for i in tree.get_children()}

    for index, data in enumerate(items):
        s, sig, conf, status, tag, prio = data
        if s in existing:
            item_id = existing[s]
            tree.item(item_id, values=(s, sig, conf, status), tags=(tag,))
            tree.move(item_id, "", index) 
        else:
            tree.insert("", "end", values=(s, sig, conf, status), tags=(tag,))
            
    root.after(1000, update_analysis)

def update_positions():
    if running and mt5.terminal_info():
        pos = mt5.positions_get()
        monitor_tree.delete(*monitor_tree.get_children())
        if pos:
            now_ts = time.time()
            for p in pos:
                if p.magic != bot_core.MAGIC_ID: continue 
                direction = "BUY 🟢" if p.type == 0 else "SELL 🔴"
                duration = format_duration(now_ts - p.time)
                profit_str = f"{round(p.profit, 2)} €"
                p_tag = "profit" if p.profit >= 0 else "loss"
                info_text = p.comment.split("_")[-1] if "_" in p.comment else "Cluster" 
                monitor_tree.insert("", "end", values=(p.symbol, direction, duration, profit_str, info_text), tags=(p_tag,))
    root.after(2000, update_positions)

def main_loop():
    while True:
        if running:
            try: bot_core.run_cycle()
            except Exception: log_error(traceback.format_exc())
        time.sleep(0.3)

def on_closing():
    bot_core.set_log("Spegnimento forzato del sistema...")
    mt5.shutdown()
    root.destroy()
    os._exit(0)

root.protocol("WM_DELETE_WINDOW", on_closing)
threading.Thread(target=main_loop, daemon=True).start()

update_analysis()
update_positions()
update_logs()
update_prop_panel()
update_debug_panel()
update_telemetry()

root.mainloop()