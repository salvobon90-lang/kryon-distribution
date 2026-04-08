import json
import os
import sys
from datetime import datetime


APP_NAME = "KRYON"
APP_VENDOR = "KRYON_AUTOMATION"


def get_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def get_runtime_dir():
    fallback_dir = os.path.join(get_base_dir(), ".kryon_runtime")
    if not getattr(sys, "frozen", False):
        os.makedirs(fallback_dir, exist_ok=True)
        return fallback_dir

    appdata = os.getenv("APPDATA")
    if appdata:
        runtime_dir = os.path.join(appdata, APP_VENDOR, APP_NAME)
        try:
            os.makedirs(runtime_dir, exist_ok=True)
            probe_path = os.path.join(runtime_dir, ".write_test")
            with open(probe_path, "w", encoding="utf-8") as probe:
                probe.write("ok")
            os.remove(probe_path)
            return runtime_dir
        except Exception:
            pass
    os.makedirs(fallback_dir, exist_ok=True)
    return fallback_dir


def get_runtime_file(filename):
    return os.path.join(get_runtime_dir(), filename)


def ensure_runtime_subdir(name):
    path = os.path.join(get_runtime_dir(), name)
    os.makedirs(path, exist_ok=True)
    return path


def load_json(path, default=None):
    default = {} if default is None else default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, payload):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)


def now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
