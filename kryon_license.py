import hashlib
import json
import os
import platform
import sys
import uuid
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from kryon_runtime import get_runtime_file, load_json, now_iso, save_json


LICENSE_STATE_FILE = get_runtime_file("license_state.json")
LICENSE_CONFIG_FILE = get_runtime_file("license_config.json")


def _http_headers(user_agent_suffix=""):
    base_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    )
    if user_agent_suffix:
        base_agent = f"{base_agent} {user_agent_suffix}"
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": base_agent,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def get_machine_fingerprint():
    raw = "|".join(
        [
            platform.node() or "unknown-node",
            platform.system() or "unknown-os",
            platform.machine() or "unknown-arch",
            hex(uuid.getnode()),
            os.getenv("PROCESSOR_IDENTIFIER", "unknown-cpu"),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def mask_license_key(license_key):
    key = str(license_key or "").strip()
    if len(key) <= 8:
        return key
    return f"{key[:4]}-****-{key[-4:]}"


class LicenseManager:
    def __init__(self, current_version):
        self.current_version = current_version
        self.machine_fingerprint = get_machine_fingerprint()
        self.config = {}
        self.reload_config()
        if not os.path.exists(LICENSE_CONFIG_FILE):
            save_json(LICENSE_CONFIG_FILE, self.config)

    def reload_config(self):
        self.config = load_json(
            LICENSE_CONFIG_FILE,
            {
                "api_base_url": "",
                "activation_endpoint": "/api/license/activate",
                "refresh_endpoint": "/api/license/refresh",
                "grace_days": 5,
                "force_packaged_mode": False,
                "enforce_packaged_only": True,
            },
        )
        return self.config

    def load_state(self):
        return load_json(LICENSE_STATE_FILE, {})

    def save_state(self, state):
        payload = dict(state or {})
        payload["updated_at"] = now_iso()
        save_json(LICENSE_STATE_FILE, payload)

    def _set_state_error(self, message):
        state = self.load_state()
        state["last_error"] = str(message or "")
        state["last_error_at"] = now_iso()
        self.save_state(state)

    def _build_url(self, endpoint):
        self.reload_config()
        base = str(self.config.get("api_base_url", "") or "").strip().rstrip("/")
        endpoint = "/" + str(endpoint or "").strip().lstrip("/")
        return f"{base}{endpoint}" if base else ""

    def _post_json(self, endpoint, payload):
        url = self._build_url(endpoint)
        if not url:
            raise RuntimeError("api_base_url non configurato")
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=_http_headers(f"KRYON/{self.current_version.replace(' ', '_')}"),
            method="POST",
        )
        try:
            with urlopen(request, timeout=6) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"{exc.code} {body or exc.reason}") from exc
        except URLError as exc:
            raise RuntimeError(str(exc.reason or exc)) from exc

    def is_dev_mode(self):
        self.reload_config()
        return (not getattr(sys, "frozen", False)) and not bool(self.config.get("force_packaged_mode", False))

    def build_activation_payload(self, email, license_key):
        return {
            "email": str(email or "").strip(),
            "license_key": str(license_key or "").strip(),
            "machine_fingerprint": self.machine_fingerprint,
            "app_id": "KRYON",
            "app_version": self.current_version,
            "platform": platform.platform(),
        }

    def apply_activation_response(self, response):
        response = dict(response or {})
        state = {
            "status": response.get("status", "ACTIVE"),
            "email": response.get("email", ""),
            "license_key": response.get("license_key", ""),
            "license_key_masked": mask_license_key(response.get("license_key", "")),
            "plan": response.get("plan", "PRO"),
            "token_id": response.get("token_id", ""),
            "issued_at": response.get("issued_at", now_iso()),
            "expires_at": response.get("expires_at", ""),
            "refresh_after": response.get("refresh_after", ""),
            "offline_grace_until": response.get("offline_grace_until", ""),
            "machine_fingerprint": response.get("machine_fingerprint", self.machine_fingerprint),
            "update_channel": response.get("update_channel", "stable"),
            "entitlements": response.get("entitlements", {"updates": True, "strategy_config": True}),
            "last_validated_at": now_iso(),
            "last_error": "",
        }
        self.save_state(state)
        return state

    def activate_online(self, email, license_key):
        self.reload_config()
        payload = self.build_activation_payload(email, license_key)
        response = self._post_json(self.config.get("activation_endpoint", "/api/license/activate"), payload)
        return self.apply_activation_response(response)

    def refresh_online(self):
        self.reload_config()
        state = self.load_state()
        token_id = state.get("token_id", "")
        if not token_id:
            raise RuntimeError("token_id mancante")
        payload = {
            "token_id": token_id,
            "machine_fingerprint": self.machine_fingerprint,
            "app_id": "KRYON",
            "app_version": self.current_version,
        }
        response = self._post_json(self.config.get("refresh_endpoint", "/api/license/refresh"), payload)
        return self.apply_activation_response(response)

    def should_refresh(self):
        state = self.load_state()
        token_id = state.get("token_id", "")
        if not token_id:
            return False
        refresh_after = _parse_iso(state.get("refresh_after"))
        if refresh_after and datetime.utcnow() >= (refresh_after - timedelta(minutes=5)):
            return True
        last_validated = _parse_iso(state.get("last_validated_at"))
        if last_validated and datetime.utcnow() >= (last_validated + timedelta(hours=12)):
            return True
        return False

    def auto_refresh(self, force=False):
        state = self.load_state()
        if not state.get("token_id"):
            return None
        if not force and not self.should_refresh():
            return None
        try:
            return self.refresh_online()
        except Exception as exc:
            self._set_state_error(str(exc))
            return None

    def get_runtime_status(self):
        if self.is_dev_mode():
            return {
                "mode": "DEVELOPMENT",
                "status": "ACTIVE",
                "run_allowed": True,
                "headline": "LICENZA: DEV MODE",
                "detail": "Sorgente locale sbloccato",
                "plan": "DEV",
                "update_channel": "dev",
                "entitlements": {"updates": True, "strategy_config": True},
            }

        state = self.load_state()
        if not state:
            return {
                "mode": "PACKAGED",
                "status": "UNLICENSED",
                "run_allowed": False,
                "headline": "LICENZA: NON ATTIVA",
                "detail": "Serve email + license key",
                "plan": "--",
                "update_channel": "stable",
                "entitlements": {},
            }

        if state.get("machine_fingerprint") and state.get("machine_fingerprint") != self.machine_fingerprint:
            return {
                "mode": "PACKAGED",
                "status": "DEVICE_MISMATCH",
                "run_allowed": False,
                "headline": "LICENZA: DEVICE MISMATCH",
                "detail": "Token legato a un altro dispositivo",
                "plan": state.get("plan", "--"),
                "update_channel": state.get("update_channel", "stable"),
                "entitlements": state.get("entitlements", {}),
            }

        status = str(state.get("status", "ACTIVE")).upper()
        expires_at = _parse_iso(state.get("expires_at"))
        grace_until = _parse_iso(state.get("offline_grace_until"))
        now = datetime.utcnow()
        run_allowed = status == "ACTIVE"
        detail = f"{state.get('plan', 'PRO')} | {state.get('license_key_masked', '--')}"

        if expires_at and now > expires_at:
            if grace_until and now <= grace_until:
                status = "GRACE"
                run_allowed = True
                detail = f"Grace fino a {grace_until.strftime('%d/%m %H:%M')}"
            else:
                status = "EXPIRED"
                run_allowed = False
                detail = "Licenza scaduta"

        headline = f"LICENZA: {status}"
        if state.get("last_error") and run_allowed:
            detail = f"{detail} | last err: {state.get('last_error')}"
        return {
            "mode": "PACKAGED",
            "status": status,
            "run_allowed": run_allowed,
            "headline": headline,
            "detail": detail,
            "plan": state.get("plan", "--"),
            "update_channel": state.get("update_channel", "stable"),
            "entitlements": state.get("entitlements", {}),
        }

    def seed_demo_license(self, email="demo@kryon.local", license_key="KRYON-DEMO-0001"):
        now = datetime.utcnow()
        return self.apply_activation_response(
            {
                "status": "ACTIVE",
                "email": email,
                "license_key": license_key,
                "plan": "DEMO",
                "token_id": "demo-token",
                "issued_at": now_iso(),
                "expires_at": (now + timedelta(days=30)).replace(microsecond=0).isoformat() + "Z",
                "refresh_after": (now + timedelta(days=7)).replace(microsecond=0).isoformat() + "Z",
                "offline_grace_until": (now + timedelta(days=35)).replace(microsecond=0).isoformat() + "Z",
                "machine_fingerprint": self.machine_fingerprint,
                "update_channel": "stable",
                "entitlements": {"updates": True, "strategy_config": True},
            }
        )
