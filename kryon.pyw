import tkinter as tk
from tkinter import ttk
import threading
import MetaTrader5 as mt5
import time
from datetime import datetime
import os
import sys
import json
import traceback
import ctypes
from tkinter import messagebox
from PIL import Image, ImageTk
import bot_core
from kryon_license import LicenseManager
from kryon_update import UpdateManager
from kryon_runtime import get_runtime_dir

VERSION = "KRYON ULTIMATE PRO V 15.4.2"
AUTHOR_BRAND = "POWERED BY BONS ⚡ FOR MY DAUGHTER EMMA ❤️ AND MY TRUE LOVE LAUPHI ❤️  "
running = False

session_start_time = datetime.now()
initial_balance = None
scalping_active = False
crypto_active = False


def get_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = get_base_dir()
IMG_DIR = os.path.join(BASE_DIR, "immagini kryon")
_APP_MUTEX = None
license_manager = LicenseManager(VERSION)
update_manager = UpdateManager(VERSION)


def acquire_single_instance():
    global _APP_MUTEX
    mutex_name = "Global\\KRYON_ULTIMATE_PRO_SINGLE_INSTANCE"
    _APP_MUTEX = ctypes.windll.kernel32.CreateMutexW(None, False, mutex_name)
    if not _APP_MUTEX:
        return True
    return ctypes.windll.kernel32.GetLastError() != 183


if not acquire_single_instance():
    ctypes.windll.user32.MessageBoxW(
        0,
        "KRYON e' gia' in esecuzione. Chiudi l'altra finestra del bot prima di avviarne una nuova.",
        "KRYON ULTIMATE PRO",
        0x00000040,
    )
    sys.exit(0)

BG = "#07141b"
BG_ALT = "#0b1c25"
CARD_BG = "#10232d"
CARD_BG_2 = "#15303d"
CARD_BG_3 = "#1a3a48"
BORDER = "#204656"
BORDER_SOFT = "#173644"
FG = "#97fff2"
TEXT_MAIN = "#ebfffb"
TEXT_SOFT = "#a9d9d2"
TEXT_DIM = "#7398a3"
BTN_START = "#1fbf75"
BTN_STOP = "#d84b63"
BTN_ORANGE = "#f08b3e"
BTN_BLUE = "#2998ff"
BTN_GOLD = "#d6a64a"
BTN_VIOLET = "#7c6cf2"
LOG_BG = "#09161f"
LOG_BG_2 = "#0d202a"
ERR_BG = "#2a1116"
ERR_BORDER = "#6f2634"
ERR_FG = "#ffb4bf"
SUCCESS = "#4de2a8"
WARNING = "#f1be63"
DANGER = "#ff6b82"
INFO = "#66cfff"

# ==============================================================================
# LOGICA UI & BOT
# ==============================================================================
def start_bot():
    global running, session_start_time, initial_balance
    license_status = license_manager.get_runtime_status()
    if not license_status.get("run_allowed", False):
        log_error(f"{license_status.get('headline', 'LICENZA BLOCCATA')} | {license_status.get('detail', '')}")
        status_var.set("ENGINE: LICENSE BLOCK")
        status_label.config(fg=DANGER)
        return
    bot_core.reset_session_tracking()
    running = True
    session_start_time = datetime.now()
    initial_balance = None
    status_var.set("ENGINE: LIVE")
    status_label.config(fg=SUCCESS)
    log_event("SISTEMA ONLINE: ENGINE OPERATIVO.")


def stop_bot():
    global running
    running = False
    status_var.set("ENGINE: OFFLINE")
    status_label.config(fg=DANGER)
    log_event("SISTEMA OFFLINE.")


def toggle_scalping_mode():
    global scalping_active
    scalping_active = not scalping_active
    bot_core.toggle_scalping(scalping_active)
    if scalping_active:
        scalping_status_var.set("SCALPING: ON")
        scalping_status_label.config(fg=SUCCESS)
        btn_scalping.change_style("⚡ SCALPING: ON", BTN_GOLD)
    else:
        scalping_status_var.set("SCALPING: OFF")
        scalping_status_label.config(fg=DANGER)
        btn_scalping.change_style("⚡ SCALPING: OFF", TEXT_DIM)


def toggle_crypto_mode():
    global crypto_active
    crypto_active = not crypto_active
    bot_core.toggle_crypto(crypto_active)
    if crypto_active:
        crypto_mode_var.set("CRYPTO: ON")
        crypto_mode_label.config(fg=SUCCESS)
        btn_crypto.change_style("🌐 CRYPTO: ON", BTN_BLUE)
    else:
        crypto_mode_var.set("CRYPTO: OFF")
        crypto_mode_label.config(fg=DANGER)
        btn_crypto.change_style("🌐 CRYPTO: OFF", TEXT_DIM)


def apply_optimizer_profile(profile_name):
    if bot_core.set_optimizer_profile(profile_name):
        refresh_optimizer_buttons()


def refresh_optimizer_buttons():
    active = bot_core.get_optimizer_profile().get("name", "BALANCED")
    button_styles = {
        "SAFE": ("SAFE", "#1a7f64"),
        "BALANCED": ("BALANCED", BTN_BLUE),
        "ATTACK": ("ATTACK", BTN_ORANGE),
    }
    for name, button in optimizer_buttons.items():
        label, color = button_styles[name]
        if name == active:
            button.change_style(f"● {label}", color)
        else:
            button.change_style(label, TEXT_DIM)


def log_event(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_box.insert(tk.END, f"[{timestamp}] {msg}\n")
    log_box.see(tk.END)
    if int(log_box.index("end-1c").split(".")[0]) > 300:
        log_box.delete("1.0", "2.0")


def log_error(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    error_box.insert(tk.END, f"[{timestamp}] ⚠️ {msg}\n")
    error_box.see(tk.END)
    if int(error_box.index("end-1c").split(".")[0]) > 100:
        error_box.delete("1.0", "2.0")


def log_live_analysis(msg):
    live_analysis_box.insert(tk.END, f"{msg}\n")
    live_analysis_box.see(tk.END)
    if int(live_analysis_box.index("end-1c").split(".")[0]) > 300:
        live_analysis_box.delete("1.0", "2.0")


def open_runtime_folder():
    runtime_dir = get_runtime_dir()
    try:
        os.startfile(runtime_dir)
    except Exception as exc:
        log_error(f"Impossibile aprire runtime dir: {exc}")


def open_activation_dialog():
    dialog = tk.Toplevel(root)
    dialog.title("KRYON Licensing")
    dialog.configure(bg=BG_ALT)
    dialog.transient(root)
    dialog.grab_set()
    dialog.geometry("620x560")

    card = tk.Frame(dialog, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER_SOFT)
    card.pack(fill="both", expand=True, padx=16, pady=16)

    tk.Label(card, text="Licensing Base", bg=CARD_BG, fg=TEXT_MAIN, font=("Segoe UI Semibold", 14)).pack(anchor="w", padx=16, pady=(16, 4))
    tk.Label(
        card,
        text="Questo pannello genera la richiesta di attivazione e applica una risposta JSON nello stesso formato che usera' il server licenze.",
        bg=CARD_BG,
        fg=TEXT_DIM,
        font=("Segoe UI", 9),
        wraplength=560,
        justify="left",
    ).pack(anchor="w", padx=16, pady=(0, 12))

    form = tk.Frame(card, bg=CARD_BG)
    form.pack(fill="x", padx=16, pady=(0, 10))
    tk.Label(form, text="Email", bg=CARD_BG, fg=TEXT_SOFT, font=("Segoe UI Semibold", 9)).grid(row=0, column=0, sticky="w", pady=4)
    tk.Label(form, text="License Key", bg=CARD_BG, fg=TEXT_SOFT, font=("Segoe UI Semibold", 9)).grid(row=1, column=0, sticky="w", pady=4)
    email_entry = tk.Entry(form, bg=LOG_BG, fg=TEXT_MAIN, insertbackground=FG, relief="flat", font=("Consolas", 10))
    key_entry = tk.Entry(form, bg=LOG_BG, fg=TEXT_MAIN, insertbackground=FG, relief="flat", font=("Consolas", 10))
    email_entry.grid(row=0, column=1, sticky="ew", padx=(12, 0), pady=4)
    key_entry.grid(row=1, column=1, sticky="ew", padx=(12, 0), pady=4)
    form.grid_columnconfigure(1, weight=1)

    request_box = tk.Text(card, height=10, bg=LOG_BG, fg=TEXT_MAIN, relief="flat", font=("Consolas", 9))
    request_box.pack(fill="both", expand=False, padx=16, pady=(0, 10))
    request_box.insert("1.0", "{\n  \"activation_request\": \"genera payload\"\n}")

    response_box = tk.Text(card, height=10, bg=LOG_BG, fg=TEXT_MAIN, relief="flat", font=("Consolas", 9))
    response_box.pack(fill="both", expand=True, padx=16, pady=(0, 10))
    response_box.insert(
        "1.0",
        json.dumps(
            {
                "status": "ACTIVE",
                "email": "cliente@example.com",
                "license_key": "KRYON-PRO-XXXX",
                "plan": "PRO",
                "token_id": "token-001",
                "issued_at": "2026-04-08T08:00:00Z",
                "expires_at": "2026-05-08T08:00:00Z",
                "refresh_after": "2026-04-15T08:00:00Z",
                "offline_grace_until": "2026-05-13T08:00:00Z",
                "machine_fingerprint": license_manager.machine_fingerprint,
                "update_channel": "stable",
                "entitlements": {"updates": True, "strategy_config": True},
            },
            indent=2,
        ),
    )

    footer = tk.Frame(card, bg=CARD_BG)
    footer.pack(fill="x", padx=16, pady=(0, 16))

    def build_request():
        payload = license_manager.build_activation_payload(email_entry.get(), key_entry.get())
        request_box.delete("1.0", tk.END)
        request_box.insert("1.0", json.dumps(payload, indent=2))
        root.clipboard_clear()
        root.clipboard_append(json.dumps(payload, indent=2))
        log_event("Richiesta attivazione copiata negli appunti.")

    def apply_response():
        try:
            payload = json.loads(response_box.get("1.0", tk.END).strip())
        except Exception as exc:
            messagebox.showerror("Licensing", f"JSON risposta non valido:\n{exc}")
            return
        license_manager.apply_activation_response(payload)
        messagebox.showinfo("Licensing", "Risposta licenza applicata e salvata.")
        update_distribution_panel()

    def activate_online():
        try:
            state = license_manager.activate_online(email_entry.get(), key_entry.get())
        except Exception as exc:
            messagebox.showerror(
                "Licensing",
                f"Attivazione online fallita:\n{exc}\n\nConfig letta da:\n{get_runtime_dir()}",
            )
            return
        response_box.delete("1.0", tk.END)
        response_box.insert("1.0", json.dumps(state, indent=2))
        messagebox.showinfo("Licensing", "Licenza attivata online con successo.")
        update_distribution_panel()

    def refresh_token():
        try:
            state = license_manager.refresh_online()
        except Exception as exc:
            messagebox.showerror("Licensing", f"Refresh token fallito:\n{exc}")
            return
        response_box.delete("1.0", tk.END)
        response_box.insert("1.0", json.dumps(state, indent=2))
        messagebox.showinfo("Licensing", "Token aggiornato con successo.")
        update_distribution_panel()

    RoundedButton(footer, "🔐 CREA RICHIESTA", BTN_BLUE, build_request, 180, 36).pack(side="left", padx=(0, 8))
    RoundedButton(footer, "🌐 ATTIVA ONLINE", BTN_ORANGE, activate_online, 180, 36).pack(side="left", padx=8)
    RoundedButton(footer, "✅ APPLICA RISPOSTA", BTN_START, apply_response, 180, 36).pack(side="left", padx=8)
    RoundedButton(footer, "🔄 REFRESH TOKEN", BTN_VIOLET, refresh_token, 180, 36).pack(side="left", padx=8)
    RoundedButton(footer, "📂 RUNTIME", TEXT_DIM, open_runtime_folder, 120, 36).pack(side="right")


def update_logs():
    main_msg = bot_core.get_latest_log()
    if main_msg:
        if any(kw in main_msg.upper() for kw in ["❌", "ERRORE", "FALLITO", "⚠️", "INVALID", "⛔", "BLOCK", "FAIL", "FATAL"]):
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
    if h > 0:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"


class RoundedButton(tk.Canvas):
    def __init__(self, parent, text, bg_color, command, width, height=40):
        super().__init__(parent, bg=CARD_BG, highlightthickness=0, width=width, height=height)
        self.command = command
        self.bg_color = bg_color
        self.hover_color = self._adjust_color(bg_color, 1.2)
        self.rect = self.create_rounded_rect(2, 2, width - 2, height - 2, r=15, fill=bg_color, outline=bg_color)
        self.text = self.create_text(width / 2, height / 2, text=text, fill=TEXT_MAIN, font=("Segoe UI", 11, "bold"))
        self.bind("<Button-1>", self._on_click)
        self.bind("<Enter>", self._on_hover)
        self.bind("<Leave>", self._on_leave)

    def create_rounded_rect(self, x1, y1, x2, y2, r, **kwargs):
        points = [
            x1 + r,
            y1,
            x1 + r,
            y1,
            x2 - r,
            y1,
            x2 - r,
            y1,
            x2,
            y1,
            x2,
            y1 + r,
            x2,
            y1 + r,
            x2,
            y2 - r,
            x2,
            y2 - r,
            x2,
            y2,
            x2 - r,
            y2,
            x2 - r,
            y2,
            x1 + r,
            y2,
            x1 + r,
            y2,
            x1,
            y2,
            x1,
            y2 - r,
            x1,
            y2 - r,
            x1,
            y1 + r,
            x1,
            y1 + r,
            x1,
            y1,
        ]
        return self.create_polygon(points, smooth=True, **kwargs)

    def change_style(self, text, bg_color):
        self.bg_color = bg_color
        self.hover_color = self._adjust_color(bg_color, 1.2)
        self.itemconfig(self.rect, fill=bg_color, outline=bg_color)
        self.itemconfig(self.text, text=text)

    def _on_click(self, event):
        self.itemconfig(self.rect, fill=self._adjust_color(self.bg_color, 0.8))
        self.after(100, lambda: self.itemconfig(self.rect, fill=self.hover_color))
        if self.command:
            threading.Thread(target=self.command, daemon=True).start()

    def _on_hover(self, event):
        self.itemconfig(self.rect, fill=self.hover_color)

    def _on_leave(self, event):
        self.itemconfig(self.rect, fill=self.bg_color)

    def _adjust_color(self, hex_color, factor):
        hex_color = hex_color.lstrip("#")
        r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
        return f"#{min(int(r * factor), 255):02x}{min(int(g * factor), 255):02x}{min(int(b * factor), 255):02x}"


def make_card(parent):
    return tk.Frame(parent, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER, highlightcolor=BORDER)


def style_text_widget(widget, bg_color, fg_color):
    widget.configure(
        bg=bg_color,
        fg=fg_color,
        insertbackground=fg_color,
        selectbackground=CARD_BG_3,
        selectforeground=TEXT_MAIN,
        highlightthickness=1,
        highlightbackground=BORDER_SOFT,
        highlightcolor=BORDER,
        padx=8,
        pady=6,
    )


def status_color(status):
    if status in ("ACTIVE", "LIVE", "EXECUTED"):
        return SUCCESS
    if status in ("STANDBY", "MONITORING"):
        return WARNING
    if status in ("LOCKED", "OFF", "DISABLED"):
        return DANGER
    return INFO


def strategy_chip_style(strategy):
    status = strategy.get("status", "OFF")
    live_block = str(strategy.get("live_block", "INIT")).upper()
    if status == "LOCKED":
        return "LOCKED", DANGER, BG
    if status == "STANDBY":
        return "STANDBY", WARNING, BG
    if live_block in ("LIVE", "READY", "WIN"):
        return "LIVE", SUCCESS, BG
    if status == "ACTIVE":
        return "ARMED", INFO, BG
    return status, TEXT_DIM, CARD_BG


def block_badge_style(block_reason):
    block = str(block_reason).upper()
    if block in ("LIVE", "READY", "WIN"):
        return SUCCESS, BG
    if block in ("LOSS", "FAIL", "LOCKED", "OFF"):
        return DANGER, BG
    if block in ("NO SETUP", "INIT", "---"):
        return TEXT_DIM, CARD_BG
    if block in ("SCALP OFF", "COOLDOWN", "FAST", "SPREAD", "LOW SCORE", "REGIME", "BE", "OUT SESSION"):
        return WARNING, BG
    return INFO, BG


# ==============================================================================
# ROOT & SIDEBAR
# ==============================================================================
root = tk.Tk()
path_icon_ico = os.path.join(IMG_DIR, "icona.ico")
path_icon_png = os.path.join(IMG_DIR, "icona.png")
try:
    if os.path.exists(path_icon_ico):
        root.iconbitmap(path_icon_ico)
except:
    pass
try:
    if os.path.exists(path_icon_png):
        icon_pil = Image.open(path_icon_png)
        root._icon_image = ImageTk.PhotoImage(icon_pil)
        root.iconphoto(True, root._icon_image)
except:
    pass
root.title(VERSION)
root.minsize(1100, 700)
try:
    root.state("zoomed")
except:
    root.geometry("1400x900")
root.configure(bg=BG)
try:
    root.tk.call("tk", "scaling", 1.2)
except:
    pass

style = ttk.Style()
style.theme_use("clam")
style.configure("Vertical.TScrollbar", background=CARD_BG_2, bordercolor=BG_ALT, arrowcolor=FG, troughcolor=BG, relief="flat")
style.configure(
    "Treeview",
    background=LOG_BG,
    foreground=TEXT_MAIN,
    fieldbackground=LOG_BG,
    rowheight=26,
    font=("Segoe UI", 9),
    borderwidth=0,
)
style.configure(
    "Treeview.Heading",
    background=CARD_BG_2,
    foreground=FG,
    font=("Segoe UI Semibold", 9),
    borderwidth=0,
)
style.map("Treeview", background=[("selected", CARD_BG_3)])

root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(1, weight=1)

sidebar = tk.Frame(root, bg=CARD_BG_2, width=240, highlightthickness=1, highlightbackground=BORDER)
sidebar.grid(row=0, column=0, sticky="ns")
sidebar.grid_propagate(False)

path_logo = os.path.join(IMG_DIR, "logo2.png")
try:
    img_pil = Image.open(path_logo)
    img_res = img_pil.resize((180, 100), Image.Resampling.LANCZOS)
    img_tk = ImageTk.PhotoImage(img_res)
    logo_lbl = tk.Label(sidebar, image=img_tk, bg=CARD_BG_2)
    logo_lbl.image = img_tk
    logo_lbl.pack(pady=(22, 10))
except:
    tk.Label(sidebar, text="KRYON", bg=CARD_BG_2, fg=FG, font=("Impact", 24, "bold")).pack(pady=(22, 10))

tk.Label(sidebar, text=VERSION, bg=CARD_BG_2, fg=TEXT_SOFT, font=("Segoe UI Semibold", 10)).pack()
tk.Label(sidebar, text="Trade Engine Console", bg=CARD_BG_2, fg=TEXT_DIM, font=("Segoe UI", 9)).pack(pady=(2, 2))
tk.Label(sidebar, text="Mercati accesi, regole lucide", bg=CARD_BG_2, fg=TEXT_DIM, font=("Segoe UI", 9)).pack()
tk.Label(sidebar, text="Il denaro qua non dorme mai", bg=CARD_BG_2, fg=TEXT_DIM, font=("Segoe UI", 9)).pack(pady=(0, 18))

RoundedButton(sidebar, "🚀 START", BTN_START, start_bot, 190, 42).pack(pady=5)
RoundedButton(sidebar, "🛑 STOP", "#b34a5d", stop_bot, 190, 42).pack(pady=5)
RoundedButton(sidebar, "❌ PANIC", "#cf6a45", bot_core.close_all_now, 190, 42).pack(pady=5)
RoundedButton(sidebar, "💰 TAKE PROFIT", "#1ca8a0", bot_core.close_only_profit, 190, 42).pack(pady=5)
RoundedButton(sidebar, "🛡️ BE+", BTN_VIOLET, bot_core.force_break_even_plus, 190, 42).pack(pady=5)

btn_scalping = RoundedButton(sidebar, "⚡ SCALPING: OFF", TEXT_DIM, toggle_scalping_mode, 190, 42)
btn_scalping.pack(pady=5)
btn_crypto = RoundedButton(sidebar, "🌐 CRYPTO: OFF", TEXT_DIM, toggle_crypto_mode, 190, 42)
btn_crypto.pack(pady=5)

status_box = tk.Frame(sidebar, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER_SOFT)
status_box.pack(fill="x", padx=18, pady=(14, 8))
tk.Label(status_box, text="ENGINE", bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=10, pady=(8, 0))
status_var = tk.StringVar(value="ENGINE: OFFLINE")
status_label = tk.Label(status_box, textvariable=status_var, fg=DANGER, bg=CARD_BG, font=("Segoe UI Semibold", 13))
status_label.pack(anchor="w", padx=10, pady=(2, 8))

scalp_box = tk.Frame(sidebar, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER_SOFT)
scalp_box.pack(fill="x", padx=18, pady=(0, 10))
tk.Label(scalp_box, text="MODES", bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=10, pady=(8, 0))
tk.Label(scalp_box, text="SCALPING", bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=10, pady=(4, 0))
scalping_status_var = tk.StringVar(value="SCALPING: OFF")
scalping_status_label = tk.Label(scalp_box, textvariable=scalping_status_var, font=("Segoe UI Semibold", 11), bg=CARD_BG, fg=DANGER)
scalping_status_label.pack(anchor="w", padx=10, pady=(2, 4))
tk.Label(scalp_box, text="CRYPTO", bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=10, pady=(2, 0))
crypto_mode_var = tk.StringVar(value="CRYPTO: OFF")
crypto_mode_label = tk.Label(scalp_box, textvariable=crypto_mode_var, font=("Segoe UI Semibold", 11), bg=CARD_BG, fg=DANGER)
crypto_mode_label.pack(anchor="w", padx=10, pady=(2, 8))

dist_box = tk.Frame(sidebar, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER_SOFT)
dist_box.pack(fill="x", padx=18, pady=(0, 10))
tk.Label(dist_box, text="DISTRIBUTION", bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=10, pady=(8, 0))
license_status_var = tk.StringVar(value="LICENZA: INIT")
license_detail_var = tk.StringVar(value="Controllo stato...")
update_status_var = tk.StringVar(value="UPDATE: INIT")
update_detail_var = tk.StringVar(value="Manifest non letto")
license_status_label = tk.Label(dist_box, textvariable=license_status_var, font=("Segoe UI Semibold", 10), bg=CARD_BG, fg=INFO)
license_status_label.pack(anchor="w", padx=10, pady=(4, 0))
tk.Label(dist_box, textvariable=license_detail_var, font=("Segoe UI", 8), bg=CARD_BG, fg=TEXT_DIM, wraplength=180, justify="left").pack(anchor="w", padx=10, pady=(0, 4))
update_status_label = tk.Label(dist_box, textvariable=update_status_var, font=("Segoe UI Semibold", 10), bg=CARD_BG, fg=INFO)
update_status_label.pack(anchor="w", padx=10, pady=(2, 0))
tk.Label(dist_box, textvariable=update_detail_var, font=("Segoe UI", 8), bg=CARD_BG, fg=TEXT_DIM, wraplength=180, justify="left").pack(anchor="w", padx=10, pady=(0, 8))
dist_actions = tk.Frame(dist_box, bg=CARD_BG)
dist_actions.pack(fill="x", padx=10, pady=(0, 8))
RoundedButton(dist_actions, "🔐 LICENZA", BTN_BLUE, open_activation_dialog, 84, 32).pack(side="left")
RoundedButton(dist_actions, "📂 FILES", TEXT_DIM, open_runtime_folder, 84, 32).pack(side="right")

tk.Label(sidebar, text=AUTHOR_BRAND, bg=CARD_BG_2, fg=BTN_GOLD, font=("Segoe UI", 8, "italic"), wraplength=190, justify="left").pack(side="bottom", pady=18)

# ==============================================================================
# WORKSPACE
# ==============================================================================
workspace = tk.Frame(root, bg=BG_ALT)
workspace.grid(row=0, column=1, sticky="nsew")

workspace.grid_rowconfigure(0, weight=3, minsize=220)
workspace.grid_rowconfigure(1, weight=5, minsize=280)
workspace.grid_rowconfigure(2, weight=5, minsize=320)
workspace.grid_columnconfigure(0, weight=1)
workspace.grid_columnconfigure(1, weight=1)


def add_title(parent, text, subtitle=""):
    header = tk.Frame(parent, bg=CARD_BG_2, height=38)
    header.pack(fill="x", padx=0, pady=0)
    header.pack_propagate(False)
    title_row = tk.Frame(header, bg=CARD_BG_2)
    title_row.pack(fill="both", expand=True, padx=10, pady=0)
    tk.Label(title_row, text=text, bg=CARD_BG_2, fg=TEXT_MAIN, font=("Segoe UI Semibold", 11), anchor="w").pack(side="left", pady=8)
    if subtitle:
        tk.Label(
            title_row,
            text=subtitle,
            bg=CARD_BG_2,
            fg=TEXT_DIM,
            font=("Segoe UI", 8),
            anchor="w",
        ).pack(side="left", padx=(10, 0), pady=9)


mod_analyzer = make_card(workspace)
mod_analyzer.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_analyzer, "Radar", "Segnali live e stato asset")

radar_status_var = tk.StringVar(value="📡 Inizializzazione...")
tk.Label(mod_analyzer, textvariable=radar_status_var, bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI", 9), anchor="center", justify="center").pack(fill="x", padx=8, pady=(6, 0))
tree = ttk.Treeview(mod_analyzer, columns=("Asset", "Segnale", "Confidenza", "Status"), show="headings")
tree_scroll = ttk.Scrollbar(mod_analyzer, orient="vertical", command=tree.yview)
tree.configure(yscrollcommand=tree_scroll.set)
tree_scroll.pack(side="right", fill="y")
tree.pack(fill="both", expand=True, padx=5, pady=5)
for col in ("Asset", "Segnale", "Confidenza", "Status"):
    tree.heading(col, text=col, anchor="center")
tree.column("Asset", width=78, minwidth=70, stretch=False, anchor="center")
tree.column("Segnale", width=86, minwidth=80, stretch=False, anchor="center")
tree.column("Confidenza", width=96, minwidth=90, stretch=False, anchor="center")
tree.column("Status", width=180, minwidth=150, stretch=True, anchor="w")
tree.tag_configure("entry", foreground=SUCCESS)
tree.tag_configure("monitor", foreground=WARNING)
tree.tag_configure("watch", foreground="#b79bff")
tree.tag_configure("closed_mkt", foreground=DANGER)
tree.tag_configure("neutral", foreground=INFO)
tree.tag_configure("low_vol", foreground=BTN_GOLD)

mod_control = make_card(workspace)
mod_control.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_control, "Control", "Modalita, drawdown e motore decisionale")

control_split = tk.Frame(mod_control, bg=CARD_BG)
control_split.pack(fill="both", expand=True, padx=10, pady=(10, 10))
control_split.grid_columnconfigure(0, weight=3)
control_split.grid_columnconfigure(1, weight=4)
control_split.grid_rowconfigure(0, weight=1)

control_left = tk.Frame(control_split, bg=CARD_BG)
control_left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

control_right = tk.Frame(control_split, bg=CARD_BG)
control_right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

prop_grid = tk.Frame(control_left, bg=CARD_BG)
prop_grid.pack(fill="x", padx=10, pady=(10, 6))

ai_mode_var, eq_mode_var, opt_var, dd_var = tk.StringVar(value="---"), tk.StringVar(value="---"), tk.StringVar(value="BALANCED"), tk.StringVar(value="0.0%")
wr_var, sig_var, exec_var = tk.StringVar(value="0.0% / 0.0%"), tk.StringVar(value="0 / 0"), tk.StringVar(value="0")

tk.Label(prop_grid, text="AI MODE:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=0, column=0, sticky="w", pady=4)
lbl_ai = tk.Label(prop_grid, textvariable=ai_mode_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_ai.grid(row=0, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="EQUITY:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=1, column=0, sticky="w", pady=4)
lbl_eq = tk.Label(prop_grid, textvariable=eq_mode_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_eq.grid(row=1, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="PROFILE:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=2, column=0, sticky="w", pady=4)
lbl_opt = tk.Label(prop_grid, textvariable=opt_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG, fg=INFO)
lbl_opt.grid(row=2, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="DRAWDOWN:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=3, column=0, sticky="w", pady=4)
lbl_dd = tk.Label(prop_grid, textvariable=dd_var, font=("Segoe UI", 11, "bold"), bg=CARD_BG)
lbl_dd.grid(row=3, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="WINRATE S/A:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=4, column=0, sticky="w", pady=4)
tk.Label(prop_grid, textvariable=wr_var, font=("Segoe UI", 11, "bold"), fg=TEXT_MAIN, bg=CARD_BG).grid(row=4, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="SIGNALS/FILT:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=5, column=0, sticky="w", pady=4)
tk.Label(prop_grid, textvariable=sig_var, font=("Segoe UI", 11, "bold"), fg=TEXT_MAIN, bg=CARD_BG).grid(row=5, column=1, sticky="w", pady=2, padx=10)

tk.Label(prop_grid, text="EXECUTED:", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).grid(row=6, column=0, sticky="w", pady=4)
tk.Label(prop_grid, textvariable=exec_var, font=("Segoe UI", 11, "bold"), fg=INFO, bg=CARD_BG).grid(row=6, column=1, sticky="w", pady=2, padx=10)

optimizer_bar = tk.Frame(control_left, bg=CARD_BG)
optimizer_bar.pack(fill="x", padx=10, pady=(0, 8))
tk.Label(optimizer_bar, text="OPTIMIZER", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 8)).pack(anchor="w", pady=(0, 4))
optimizer_buttons_wrap = tk.Frame(optimizer_bar, bg=CARD_BG)
optimizer_buttons_wrap.pack(fill="x")
optimizer_buttons = {
    "SAFE": RoundedButton(optimizer_buttons_wrap, "SAFE", TEXT_DIM, lambda: apply_optimizer_profile("SAFE"), 95, 34),
    "BALANCED": RoundedButton(optimizer_buttons_wrap, "BALANCED", BTN_BLUE, lambda: apply_optimizer_profile("BALANCED"), 115, 34),
    "ATTACK": RoundedButton(optimizer_buttons_wrap, "ATTACK", TEXT_DIM, lambda: apply_optimizer_profile("ATTACK"), 95, 34),
}
optimizer_buttons["SAFE"].pack(side="left", padx=(0, 6))
optimizer_buttons["BALANCED"].pack(side="left", padx=6)
optimizer_buttons["ATTACK"].pack(side="left", padx=(6, 0))
refresh_optimizer_buttons()

strategy_frame = tk.LabelFrame(
    control_right,
    text="STRATEGIES",
    bg=CARD_BG,
    fg=FG,
    bd=0,
    highlightthickness=1,
    highlightbackground=BORDER_SOFT,
)
strategy_frame.pack(fill="both", expand=True, padx=0, pady=0)
strategy_overview_var = tk.StringVar(value="Active 0 | Positive 0 | Net 0.0€")
strategy_overview = tk.Frame(strategy_frame, bg=CARD_BG)
strategy_overview.pack(fill="x", padx=6, pady=(6, 0))
tk.Label(strategy_overview, text="LIVE MATRIX", bg=CARD_BG, fg=FG, font=("Segoe UI Semibold", 8)).pack(side="left")
tk.Label(strategy_overview, textvariable=strategy_overview_var, bg=CARD_BG, fg=TEXT_DIM, font=("Segoe UI", 8)).pack(side="right")

strategy_summary = ttk.Treeview(strategy_frame, columns=("Branch", "State", "Block", "Net", "WR", "Trd"), show="headings", height=8)
for col in ("Branch", "State", "Block", "Net", "WR", "Trd"):
    strategy_summary.heading(col, text=col, anchor="center")
strategy_summary.column("Branch", width=116, minwidth=110, stretch=True, anchor="w")
strategy_summary.column("State", width=68, minwidth=64, stretch=False, anchor="center")
strategy_summary.column("Block", width=108, minwidth=98, stretch=False, anchor="center")
strategy_summary.column("Net", width=74, minwidth=68, stretch=False, anchor="e")
strategy_summary.column("WR", width=56, minwidth=52, stretch=False, anchor="e")
strategy_summary.column("Trd", width=46, minwidth=42, stretch=False, anchor="center")
strategy_summary.tag_configure("active", foreground=SUCCESS)
strategy_summary.tag_configure("standby", foreground=INFO)
strategy_summary.tag_configure("locked", foreground=DANGER)
strategy_summary.tag_configure("weak", foreground=WARNING)
strategy_summary.pack(fill="x", padx=6, pady=(4, 4))

strategy_divider = tk.Frame(strategy_frame, bg=BORDER_SOFT, height=1)
strategy_divider.pack(fill="x", padx=6, pady=(0, 2))

strategy_canvas = tk.Canvas(strategy_frame, bg=CARD_BG, highlightthickness=0, bd=0)
strategy_scroll = ttk.Scrollbar(strategy_frame, orient="vertical", command=strategy_canvas.yview)
strategy_list = tk.Frame(strategy_canvas, bg=CARD_BG)
strategy_list.bind(
    "<Configure>",
    lambda e: strategy_canvas.configure(scrollregion=strategy_canvas.bbox("all"))
)
strategy_canvas_window = strategy_canvas.create_window((0, 0), window=strategy_list, anchor="nw")
strategy_canvas.configure(yscrollcommand=strategy_scroll.set)
strategy_scroll.pack(side="right", fill="y", padx=(0, 4), pady=6)
strategy_canvas.pack(fill="both", expand=True, padx=(6, 0), pady=6)
strategy_canvas.bind(
    "<Configure>",
    lambda e: strategy_canvas.itemconfigure(strategy_canvas_window, width=e.width - 2)
)

mod_positions = make_card(workspace)
mod_positions.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_positions, "Positions", "Trade aperti, direzione e cluster")

monitor_tree = ttk.Treeview(mod_positions, columns=("Asset", "Dir", "Time", "Profit", "Info"), show="headings")
pos_scroll = ttk.Scrollbar(mod_positions, orient="vertical", command=monitor_tree.yview)
monitor_tree.configure(yscrollcommand=pos_scroll.set)
pos_scroll.pack(side="right", fill="y")
monitor_tree.pack(fill="both", expand=True, padx=5, pady=5)
for col in ("Asset", "Dir", "Time", "Profit", "Info"):
    monitor_tree.heading(col, text=col)
monitor_tree.column("Asset", width=78, minwidth=70, stretch=False, anchor="center")
monitor_tree.column("Dir", width=86, minwidth=80, stretch=False, anchor="center")
monitor_tree.column("Time", width=88, minwidth=80, stretch=False, anchor="center")
monitor_tree.column("Profit", width=90, minwidth=84, stretch=False, anchor="e")
monitor_tree.column("Info", width=150, minwidth=120, stretch=True, anchor="w")
monitor_tree.tag_configure("profit", foreground=SUCCESS)
monitor_tree.tag_configure("loss", foreground=DANGER)

mod_telemetry = make_card(workspace)
mod_telemetry.grid(row=1, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_telemetry, "Portfolio", "Saldo, heatmap, esposizione e rischio")

dash_top = tk.Frame(mod_telemetry, bg=CARD_BG)
dash_top.pack(fill="x", padx=5, pady=5)

balance_var, pnl_var = tk.StringVar(), tk.StringVar()
tk.Label(dash_top, text="BAL ", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).pack(side="left")
tk.Label(dash_top, textvariable=balance_var, fg=TEXT_MAIN, bg=CARD_BG, font=("Segoe UI Semibold", 12)).pack(side="left", padx=(0, 15))
tk.Label(dash_top, text="LIVE ", fg=TEXT_DIM, bg=CARD_BG, font=("Segoe UI Semibold", 10)).pack(side="left")
lbl_pnl = tk.Label(dash_top, textvariable=pnl_var, bg=CARD_BG, font=("Segoe UI", 12, "bold"))
lbl_pnl.pack(side="left")

session_frame = tk.Frame(dash_top, bg=CARD_BG)
session_frame.pack(side="right")
session_pnl_var = tk.StringVar(value="Session: 0.0€")
lbl_sess_pnl = tk.Label(session_frame, textvariable=session_pnl_var, bg=CARD_BG, font=("Segoe UI", 10, "bold"), fg=INFO)
lbl_sess_pnl.pack(side="right", padx=5)

heat_frame = tk.LabelFrame(mod_telemetry, text="LIVE MAP", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground=BORDER_SOFT)
heat_frame.pack(fill="x", padx=5, pady=5)
heat_frame.configure(height=72)
heat_frame.pack_propagate(False)

dash_bot = tk.Frame(mod_telemetry, bg=CARD_BG)
dash_bot.pack(fill="both", expand=True, padx=5, pady=5)
dash_bot.columnconfigure((0, 1), weight=1)

exp_frame = tk.LabelFrame(dash_bot, text="EXPOSURE", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground=BORDER_SOFT)
exp_frame.grid(row=0, column=0, sticky="nsew", padx=2)
risk_frame = tk.LabelFrame(dash_bot, text="RISK", bg=CARD_BG, fg=FG, bd=0, highlightthickness=1, highlightbackground=BORDER_SOFT)
risk_frame.grid(row=0, column=1, sticky="nsew", padx=2)
risk_label = tk.Label(risk_frame, font=("Segoe UI", 14, "bold"), bg=CARD_BG)
risk_label.pack(expand=True)

mod_log = make_card(workspace)
mod_log.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
add_title(mod_log, "Flow", "Scanner live e registro operativo")

scan_cont = tk.Frame(mod_log, bg=LOG_BG_2, highlightthickness=1, highlightbackground=BORDER_SOFT)
scan_cont.pack(fill="x", padx=5, pady=(5, 2))
scan_cont.configure(height=120)
scan_cont.pack_propagate(False)
tk.Label(scan_cont, text="SCANNER", bg=LOG_BG_2, fg=FG, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=8, pady=(6, 2))
scroll_scan = ttk.Scrollbar(scan_cont)
scroll_scan.pack(side="right", fill="y")
live_analysis_box = tk.Text(scan_cont, bg=LOG_BG, fg="#fca5a5", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_scan.set)
live_analysis_box.pack(fill="both", expand=True)
scroll_scan.config(command=live_analysis_box.yview)
style_text_widget(live_analysis_box, LOG_BG, TEXT_SOFT)

log_cont = tk.Frame(mod_log, bg=LOG_BG_2, highlightthickness=1, highlightbackground=BORDER_SOFT)
log_cont.pack(fill="both", expand=True, padx=5, pady=(2, 5))
tk.Label(log_cont, text="EVENTS", bg=LOG_BG_2, fg=FG, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=8, pady=(6, 2))
scroll_log = ttk.Scrollbar(log_cont)
scroll_log.pack(side="right", fill="y")
log_box = tk.Text(log_cont, bg=LOG_BG, fg="white", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_log.set)
log_box.pack(fill="both", expand=True)
scroll_log.config(command=log_box.yview)
style_text_widget(log_box, LOG_BG, TEXT_MAIN)

mod_debug = make_card(workspace)
mod_debug.grid(row=2, column=1, sticky="nsew", padx=5, pady=5)
add_title(mod_debug, "Debug", "Stato engine ed errori critici")

debug_cont = tk.Frame(mod_debug, bg=LOG_BG_2, highlightthickness=1, highlightbackground=BORDER_SOFT)
debug_cont.pack(fill="x", padx=5, pady=(5, 2))
debug_cont.configure(height=170)
debug_cont.pack_propagate(False)
tk.Label(debug_cont, text="STATE", bg=LOG_BG_2, fg=FG, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=8, pady=(6, 2))
debug_tree = ttk.Treeview(debug_cont, columns=("Asset", "Feed", "Strategy", "Block", "Final"), show="headings")
debug_scroll = ttk.Scrollbar(debug_cont, orient="vertical", command=debug_tree.yview)
debug_tree.configure(yscrollcommand=debug_scroll.set)
debug_scroll.pack(side="right", fill="y")
debug_tree.pack(fill="both", expand=True)
for col in ("Asset", "Feed", "Strategy", "Block", "Final"):
    debug_tree.heading(col, text=col, anchor="center")
debug_tree.column("Asset", width=64, minwidth=60, stretch=False, anchor="center")
debug_tree.column("Feed", width=56, minwidth=50, stretch=False, anchor="center")
debug_tree.column("Strategy", width=156, minwidth=140, stretch=False, anchor="w")
debug_tree.column("Block", width=150, minwidth=136, stretch=False, anchor="w")
debug_tree.column("Final", width=110, minwidth=100, stretch=True, anchor="w")
debug_tree.tag_configure("ok", foreground=SUCCESS)
debug_tree.tag_configure("warn", foreground=WARNING)
debug_tree.tag_configure("bad", foreground=DANGER)

err_cont = tk.Frame(mod_debug, bg=ERR_BG, highlightthickness=1, highlightbackground=ERR_BORDER)
err_cont.pack(fill="both", expand=True, padx=5, pady=(2, 5))
tk.Label(err_cont, text="ERRORS", bg=ERR_BG, fg=ERR_FG, font=("Segoe UI Semibold", 8)).pack(anchor="w", padx=8, pady=(6, 2))
scroll_err = ttk.Scrollbar(err_cont)
scroll_err.pack(side="right", fill="y")
error_box = tk.Text(err_cont, bg=ERR_BG, fg="#fca5a5", font=("Consolas", 8), relief="flat", wrap="none", yscrollcommand=scroll_err.set)
error_box.pack(fill="both", expand=True)
scroll_err.config(command=error_box.yview)
style_text_widget(error_box, ERR_BG, ERR_FG)

# ==============================================================================
# UPDATERS UI
# ==============================================================================
def update_prop_panel():
    if running:
        data = bot_core.get_decision_data()
        if data:
            ai_mode_var.set(data.get("ai_mode", "---"))
            eq_mode_var.set(data.get("equity_mode", "---"))
            opt_var.set(data.get("optimizer_profile", "BALANCED"))
            dd_var.set(f"{data.get('drawdown', 0.0)}%")
            wr_var.set(f"{data.get('winrate', 0.0)}% / {data.get('total_winrate', 0.0)}%")
            sig_var.set(f"{data.get('signals', 0)} / {data.get('filtered', 0)}")
            exec_var.set(str(data.get('executed', 0)))

            lbl_ai.config(fg=SUCCESS if data.get("ai_mode") == "AGGRESSIVE" else WARNING)
            lbl_eq.config(fg=SUCCESS if data.get("equity_mode") == "AGGRESSIVE" else DANGER)
            lbl_dd.config(fg=SUCCESS if data.get("drawdown", 0) < 3 else DANGER)
            lbl_opt.config(fg=INFO if data.get("optimizer_profile") == "BALANCED" else (SUCCESS if data.get("optimizer_profile") == "SAFE" else BTN_ORANGE))
            refresh_optimizer_buttons()

            sess_prof = data.get("session_profit", 0)
            sess_closed = data.get("session_closed_profit", 0)
            sess_open = data.get("session_open_pnl", 0)
            closed_num = data.get("closed_trades", 0)
            session_start_lbl = data.get("session_started_at_label", "--:--")
            flat_num = data.get("flat_trades", 0)
            flat_chunk = f" | Flat {flat_num}" if flat_num else ""
            session_pnl_var.set(
                f"Session {session_start_lbl}: {sess_prof}€ | Closed {sess_closed}€ | Open {sess_open}€ | {closed_num} Trd{flat_chunk} | WR {data.get('winrate', 0.0)}%"
            )
            lbl_sess_pnl.config(fg=SUCCESS if sess_prof >= 0 else DANGER)

            if mt5.terminal_info():
                acc = mt5.account_info()
                if acc:
                    balance_var.set(f"{round(acc.balance, 2)} €")
                    pnl = acc.equity - acc.balance
                    pnl_var.set(f"{round(pnl, 2)} €")
                    lbl_pnl.config(fg=SUCCESS if pnl >= 0 else DANGER)
    root.after(1000, update_prop_panel)


def update_strategy_panel():
    for widget in strategy_list.winfo_children():
        widget.destroy()

    strategies = bot_core.get_strategy_dashboard()
    strategy_summary.delete(*strategy_summary.get_children())
    strategies = sorted(
        strategies,
        key=lambda s: (
            0 if s["status"] == "ACTIVE" else 1 if s["status"] == "STANDBY" else 2,
            -float(s.get("net_profit", 0.0)),
            s["name"],
        ),
    )
    strategy_summary.configure(height=min(max(len(strategies), 6), 12))

    active_count = len([s for s in strategies if s["status"] == "ACTIVE"])
    live_count = len([s for s in strategies if s.get("live_block") in ("LIVE", "READY")])
    positive_count = len([s for s in strategies if s.get("net_profit", 0.0) > 0])
    net_total = round(sum(s.get("net_profit", 0.0) for s in strategies), 2)
    best_branch = max(strategies, key=lambda s: s.get("net_profit", 0.0))["name"] if strategies else "---"
    strategy_overview_var.set(f"Active {active_count} | Live {live_count} | Positive {positive_count} | Net {net_total}€ | Best {best_branch}")

    for strategy in strategies:
        if strategy["status"] == "LOCKED":
            row_tag = "locked"
        elif strategy["status"] == "STANDBY":
            row_tag = "standby"
        elif strategy.get("net_profit", 0.0) < 0:
            row_tag = "weak"
        else:
            row_tag = "active"
        strategy_summary.insert(
            "",
            "end",
            values=(
                strategy["name"],
                strategy["status"],
                str(strategy.get("live_block", "INIT"))[:12],
                f"{strategy['net_profit']}€",
                f"{strategy['winrate']}%",
                strategy["closed"],
            ),
            tags=(row_tag,),
        )

    for strategy in strategies:
        card = tk.Frame(strategy_list, bg=CARD_BG_2, highlightthickness=1, highlightbackground=BORDER_SOFT)
        card.pack(fill="x", padx=8, pady=6)

        header = tk.Frame(card, bg=CARD_BG_2)
        header.pack(fill="x", padx=10, pady=(8, 2))
        tk.Label(header, text=strategy["name"], bg=CARD_BG_2, fg=TEXT_MAIN, font=("Segoe UI Semibold", 10)).pack(side="left")
        chip_text, chip_fg, chip_bg = strategy_chip_style(strategy)
        tk.Label(header, text="●", bg=CARD_BG_2, fg=chip_fg, font=("Segoe UI Semibold", 12)).pack(side="right", padx=(0, 8))
        tk.Label(
            header,
            text=chip_text,
            bg=chip_bg,
            fg=chip_fg,
            font=("Segoe UI Semibold", 8),
            padx=8,
            pady=2,
            highlightthickness=1,
            highlightbackground=chip_fg,
            highlightcolor=chip_fg,
        ).pack(side="right")
        tk.Label(header, text=strategy["status"], bg=CARD_BG_2, fg=status_color(strategy["status"]), font=("Segoe UI Semibold", 9)).pack(side="right", padx=(0, 8))

        meta = tk.Frame(card, bg=CARD_BG_2)
        meta.pack(fill="x", padx=10, pady=(0, 4))
        tk.Label(
            meta,
            text=f"{strategy['family']} | {strategy['tag']} | {strategy['mode']} | risk x{strategy['risk_multiplier']}",
            bg=CARD_BG_2,
            fg=TEXT_DIM,
            font=("Segoe UI", 8),
        ).pack(anchor="w")

        matrix = tk.Frame(card, bg=CARD_BG_2)
        matrix.pack(fill="x", padx=10, pady=(0, 6))
        tk.Label(
            matrix,
            text=f"{strategy['session']} | TP {strategy['tp_mode']} {strategy.get('split_range', '1')} | rr {strategy['tp_rr']} | SL atr {strategy['sl_atr']}",
            bg=CARD_BG_2,
            fg=TEXT_SOFT,
            font=("Segoe UI", 8),
        ).pack(anchor="w")
        tk.Label(
            matrix,
            text=f"BE {strategy['be_trigger']}% | Trail {strategy['trail']}",
            bg=CARD_BG_2,
            fg=TEXT_DIM,
            font=("Segoe UI", 8),
        ).pack(anchor="w")

        badge_row = tk.Frame(card, bg=CARD_BG_2)
        badge_row.pack(fill="x", padx=10, pady=(0, 6))
        badge_fg, badge_bg = block_badge_style(strategy.get("live_block", "INIT"))
        tk.Label(
            badge_row,
            text="LIVE BLOCK",
            bg=CARD_BG_2,
            fg=TEXT_DIM,
            font=("Segoe UI Semibold", 8),
        ).pack(side="left")
        tk.Label(
            badge_row,
            text=strategy.get("live_block", "INIT"),
            bg=badge_bg,
            fg=badge_fg,
            font=("Segoe UI Semibold", 8),
            padx=8,
            pady=2,
            highlightthickness=1,
            highlightbackground=badge_fg,
            highlightcolor=badge_fg,
        ).pack(side="left", padx=(8, 0))

        stats = tk.Frame(card, bg=CARD_BG_2)
        stats.pack(fill="x", padx=10, pady=(0, 8))
        tk.Label(stats, text=f"WR {strategy['winrate']}%", bg=CARD_BG_2, fg=SUCCESS if strategy["winrate"] >= 50 else WARNING, font=("Segoe UI Semibold", 9)).pack(side="left")
        tk.Label(stats, text=f"PF {strategy['profit_factor']}", bg=CARD_BG_2, fg=INFO, font=("Segoe UI Semibold", 9)).pack(side="left", padx=(12, 0))
        tk.Label(stats, text=f"Net {strategy['net_profit']}€", bg=CARD_BG_2, fg=SUCCESS if strategy["net_profit"] >= 0 else DANGER, font=("Segoe UI Semibold", 9)).pack(side="left", padx=(12, 0))
        tk.Label(stats, text=f"{strategy['closed']} trd", bg=CARD_BG_2, fg=TEXT_SOFT, font=("Segoe UI", 9)).pack(side="right")

        live_box = tk.Frame(card, bg=CARD_BG, highlightthickness=1, highlightbackground=BORDER_SOFT)
        live_box.pack(fill="x", padx=10, pady=(0, 8))
        tk.Label(
            live_box,
            text=f"Last signal: {strategy['last_signal']} {strategy['last_signal_symbol']} {strategy['last_signal_conf']}% @ {strategy['last_signal_time']}",
            bg=CARD_BG,
            fg=TEXT_SOFT,
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=8, pady=(6, 2))
        pre_color = SUCCESS if strategy.get("pretrigger") == "TRIGGER READY" else INFO
        if strategy.get("pretrigger") in ("---", "", None):
            pre_color = TEXT_DIM
        tk.Label(
            live_box,
            text=f"Pre-trigger: {strategy.get('pretrigger', '---')}",
            bg=CARD_BG,
            fg=pre_color,
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=8, pady=(0, 2))
        tk.Label(
            live_box,
            text=f"TP plan: {strategy.get('tp_plan', '---')}",
            bg=CARD_BG,
            fg=INFO if strategy.get("tp_plan") not in ("---", "", None) else TEXT_DIM,
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=8, pady=(0, 2))
        result_color = SUCCESS if strategy["last_result"] == "WIN" else (DANGER if strategy["last_result"] == "LOSS" else WARNING)
        tk.Label(
            live_box,
            text=f"Last result: {strategy['last_result']} {strategy['last_result_profit']}€ @ {strategy['last_result_time']} | Event: {strategy['last_event']}",
            bg=CARD_BG,
            fg=result_color if strategy["last_result"] != "---" else TEXT_DIM,
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=8, pady=(0, 6))

        if strategy["disabled_reason"]:
            tk.Label(
                card,
                text=f"Locked: {strategy['disabled_reason']}",
                bg=CARD_BG_2,
                fg=DANGER,
                font=("Segoe UI", 8),
                anchor="w",
                justify="left",
            ).pack(fill="x", padx=10, pady=(0, 8))

    root.after(1500, update_strategy_panel)


def update_debug_panel():
    if running:
        data = bot_core.get_debug_state()
        debug_tree.delete(*debug_tree.get_children())
        for s, d in data.items():
            status = "ok"
            blocked_by = d.get("blocked_by")
            if not d.get("data", False):
                status = "bad"
            elif blocked_by not in (None, "", "NONE", "OK", "CLEARED"):
                status = "warn"
            debug_tree.insert(
                "",
                "end",
                values=(s, "YES" if d.get("data") else "NO", d.get("signal", ""), d.get("blocked_by", "-"), d.get("final", "")),
                tags=(status,),
            )
    root.after(1000, update_debug_panel)


def update_telemetry():
    if running:
        for widget in heat_frame.winfo_children():
            widget.destroy()
        for s, state in bot_core.radar_state.items():
            sig = state.get("sig", "NEUTRAL")
            conf = state.get("conf", 0)
            status = str(state.get("status", "INIT"))
            if sig == "BUY":
                color = SUCCESS
            elif sig == "SELL":
                color = DANGER
            elif "OUT" in status or "NO DATA" in status or "CHAOS" in status:
                color = "#6b3340"
            elif "WAIT" in sig or "REGIME" in status or "M15" in status:
                color = WARNING
            else:
                color = TEXT_DIM
            short_status = status[:12]
            heat_text = f"{s}\n{sig} {conf}%\n{short_status}"
            tk.Label(heat_frame, text=heat_text, bg=color, fg=TEXT_MAIN, font=("Segoe UI", 8, "bold"), justify="center").pack(
                side="left", padx=2, pady=5, expand=True, fill="x"
            )

        acc = mt5.account_info() if mt5.terminal_info() else None
        if not acc:
            risk_label.config(text="RISK: NO DATA", fg=TEXT_DIM)
        else:
            dd = (acc.balance - acc.equity) / acc.balance if acc.balance > 0 else 0
            if dd < 0.02:
                risk_label.config(text="RISK: SAFE", fg=SUCCESS)
            elif dd < 0.05:
                risk_label.config(text="RISK: MEDIUM", fg=WARNING)
            else:
                risk_label.config(text="RISK: DANGER", fg=DANGER)

        for w in exp_frame.winfo_children():
            w.destroy()
        pos = mt5.positions_get() or [] if mt5.terminal_info() else []
        exposure = {}
        for p in pos:
            if p.magic == bot_core.MAGIC_ID:
                exposure[p.symbol] = exposure.get(p.symbol, 0) + p.volume
        if not exposure:
            tk.Label(exp_frame, text="FLAT", fg=TEXT_DIM, bg=CARD_BG, font=("Consolas", 10)).pack(anchor="w", padx=5, pady=5)
        else:
            for s, vol in exposure.items():
                tk.Label(
                    exp_frame,
                    text=f"▪ {s}: {round(vol,2)} Lts",
                    fg=TEXT_MAIN,
                    bg=CARD_BG_2,
                    font=("Consolas", 10, "bold"),
                ).pack(anchor="w", padx=5, pady=2, fill="x")

    root.after(1500, update_telemetry)


def update_analysis():
    items = []
    live_count = 0
    wait_count = 0
    for s in bot_core.symbols:
        state = bot_core.radar_state.get(s, {})
        st = state.get("status", "INIT")
        sig = state.get("sig", "---")
        conf = state.get("conf", 0)
        if sig in ("BUY", "SELL"):
            live_count += 1
        else:
            wait_count += 1

        st_u = st.upper()
        if "LIVE" in st_u or "ENTRY" in st_u or "SIGNAL" in st_u:
            prio, tag = 1, "entry"
        elif "MONITOR" in st_u or "READY" in st_u:
            prio, tag = 2, "monitor"
        elif "WATCH" in st_u or "PRE-MARKET" in st_u or "COOLDOWN" in st_u:
            prio, tag = 3, "watch"
        elif "SCAN" in st_u or "INIT" in st_u:
            prio, tag = 4, "neutral"
        elif "WAIT" in sig or "REGIME" in st_u or "BREAK" in st_u or "VWAP" in st_u or "EMA" in st_u:
            prio, tag = 5, "low_vol"
        elif "OUT" in st_u or "NO DATA" in st_u or "CHAOS" in st_u or "FEED" in st_u:
            prio, tag = 6, "closed_mkt"
        else:
            prio, tag = 99, "neutral"

        items.append((s, sig, f"{conf}%", st, tag, prio))

    items.sort(key=lambda x: x[5])
    radar_status_var.set(f"Asset {len(items)} | Live {live_count} | Wait {wait_count}")
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
                if p.magic != bot_core.MAGIC_ID:
                    continue
                direction = "BUY 🟢" if p.type == 0 else "SELL 🔴"
                duration = format_duration(now_ts - p.time)
                profit_str = f"{round(p.profit, 2)} €"
                p_tag = "profit" if p.profit >= 0 else "loss"
                info_text = p.comment.split("_")[-1] if "_" in p.comment else "Cluster"
                monitor_tree.insert("", "end", values=(p.symbol, direction, duration, profit_str, info_text), tags=(p_tag,))
    root.after(2000, update_positions)


def _status_color_from_text(text):
    text = str(text or "").upper()
    if any(flag in text for flag in ["ACTIVE", "DEV", "OK"]):
        return SUCCESS
    if any(flag in text for flag in ["CONFIG", "INIT", "CHECK"]):
        return WARNING
    if any(flag in text for flag in ["BLOCK", "EXPIRED", "UNLICENSED", "MISMATCH"]):
        return DANGER
    return INFO


def update_distribution_panel():
    license_manager.auto_refresh(force=False)
    license_status = license_manager.get_runtime_status()
    license_status_var.set(license_status.get("headline", "LICENZA: --"))
    license_detail_var.set(license_status.get("detail", ""))
    license_status_label.config(fg=_status_color_from_text(license_status.get("headline")))

    if update_manager.config.get("auto_check"):
        update_status = update_manager.check_for_updates(force=False)
        if update_manager.config.get("auto_download") and update_status.get("update_available"):
            update_manager.download_update(force=False)
            update_status = update_manager.get_status()
    else:
        update_status = update_manager.get_status()
    update_status_var.set(update_status.get("headline", "UPDATE: --"))
    update_detail_var.set(update_status.get("detail", ""))
    update_status_label.config(fg=_status_color_from_text(update_status.get("headline")))
    root.after(5000, update_distribution_panel)


def main_loop():
    while True:
        if running:
            try:
                bot_core.run_cycle()
            except Exception:
                log_error(traceback.format_exc())
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
update_strategy_panel()
update_debug_panel()
update_telemetry()
update_distribution_panel()

root.mainloop()
