import json
import os
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from db import (
    BASE_DIR,
    count_active_devices,
    get_activation,
    get_activation_by_token,
    get_license,
    init_db,
    touch_activation,
    upsert_activation,
)


HOST = "127.0.0.1"
PORT = 8787
MANIFEST_PATH = os.path.join(BASE_DIR, "latest.json")


def parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def build_license_response(license_row, machine_fingerprint, token_id):
    now = datetime.utcnow()
    expires_at = parse_iso(license_row["expires_at"]) or (now + timedelta(days=30))
    refresh_after = min(expires_at, now + timedelta(days=3))
    offline_grace_until = expires_at + timedelta(days=5)
    return {
        "status": license_row["status"],
        "email": license_row["email"],
        "license_key": license_row["license_key"],
        "plan": license_row["plan"],
        "token_id": token_id,
        "issued_at": now.replace(microsecond=0).isoformat() + "Z",
        "expires_at": expires_at.replace(microsecond=0).isoformat() + "Z",
        "refresh_after": refresh_after.replace(microsecond=0).isoformat() + "Z",
        "offline_grace_until": offline_grace_until.replace(microsecond=0).isoformat() + "Z",
        "machine_fingerprint": machine_fingerprint,
        "update_channel": license_row["update_channel"],
        "entitlements": {
            "updates": bool(license_row["allow_updates"]),
            "strategy_config": bool(license_row["allow_strategy_config"]),
        },
    }


class LicensingHandler(BaseHTTPRequestHandler):
    server_version = "KryonLicensing/0.1"

    def _send_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self._send_json(200, {"ok": True, "service": "kryon-licensing"})
        if path == "/api/releases/latest":
            if os.path.exists(MANIFEST_PATH):
                with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                    return self._send_json(200, json.load(f))
            return self._send_json(
                200,
                {
                    "channel": "stable",
                    "version": "0.0.0",
                    "download_url": "",
                    "sha256": "",
                },
            )
        return self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
        except Exception as exc:
            return self._send_json(400, {"error": f"invalid_json: {exc}"})

        if path == "/api/license/activate":
            return self._handle_activate(payload)
        if path == "/api/license/refresh":
            return self._handle_refresh(payload)
        return self._send_json(404, {"error": "not_found"})

    def _handle_activate(self, payload):
        email = str(payload.get("email", "")).strip().lower()
        license_key = str(payload.get("license_key", "")).strip()
        machine_fingerprint = str(payload.get("machine_fingerprint", "")).strip()
        if not email or not license_key or not machine_fingerprint:
            return self._send_json(400, {"error": "missing_fields"})

        license_row = get_license(license_key)
        if not license_row:
            return self._send_json(404, {"error": "license_not_found"})
        if str(license_row["email"]).strip().lower() != email:
            return self._send_json(403, {"error": "email_mismatch"})
        if str(license_row["status"]).upper() != "ACTIVE":
            return self._send_json(403, {"error": "license_not_active"})

        expires_at = parse_iso(license_row["expires_at"])
        if expires_at and datetime.utcnow() > expires_at:
            return self._send_json(403, {"error": "license_expired"})

        existing_activation = get_activation(license_key, machine_fingerprint)
        active_devices = count_active_devices(license_key)
        if not existing_activation and active_devices >= int(license_row["max_devices"] or 1):
            return self._send_json(403, {"error": "device_limit_reached"})

        token_id = upsert_activation(license_key, machine_fingerprint)
        return self._send_json(200, build_license_response(license_row, machine_fingerprint, token_id))

    def _handle_refresh(self, payload):
        token_id = str(payload.get("token_id", "")).strip()
        machine_fingerprint = str(payload.get("machine_fingerprint", "")).strip()
        if not token_id or not machine_fingerprint:
            return self._send_json(400, {"error": "missing_fields"})

        activation = get_activation_by_token(token_id)
        if not activation:
            return self._send_json(404, {"error": "activation_not_found"})
        if activation["machine_fingerprint"] != machine_fingerprint:
            return self._send_json(403, {"error": "machine_mismatch"})
        if str(activation["status"]).upper() != "ACTIVE":
            return self._send_json(403, {"error": "activation_not_active"})

        license_row = get_license(activation["license_key"])
        if not license_row:
            return self._send_json(404, {"error": "license_not_found"})
        if str(license_row["status"]).upper() != "ACTIVE":
            return self._send_json(403, {"error": "license_not_active"})

        touch_activation(token_id)
        return self._send_json(200, build_license_response(license_row, machine_fingerprint, token_id))


def run():
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), LicensingHandler)
    print(f"Kryon licensing server listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    run()
