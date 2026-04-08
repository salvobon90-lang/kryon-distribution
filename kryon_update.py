import hashlib
import json
import os
from datetime import datetime
from urllib.error import URLError
from urllib.request import Request, urlopen

from kryon_runtime import ensure_runtime_subdir, get_runtime_file, load_json, now_iso, save_json


UPDATE_STATE_FILE = get_runtime_file("update_state.json")
UPDATE_CONFIG_FILE = get_runtime_file("update_config.json")


def _http_request(url, current_version):
    return Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                f"Chrome/135.0.0.0 Safari/537.36 KRYON/{str(current_version).replace(' ', '_')}"
            ),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        method="GET",
    )


def _version_tuple(version):
    digits = []
    for chunk in str(version or "").replace("KRYON ULTIMATE PRO V", "").replace("-", ".").split("."):
        chunk = chunk.strip()
        if chunk.isdigit():
            digits.append(int(chunk))
    return tuple(digits or [0])


class UpdateManager:
    def __init__(self, current_version):
        self.current_version = current_version
        self.config = {}
        self.reload_config()
        if not os.path.exists(UPDATE_CONFIG_FILE):
            save_json(UPDATE_CONFIG_FILE, self.config)

    def reload_config(self):
        self.config = load_json(
            UPDATE_CONFIG_FILE,
            {
                "manifest_url": "",
                "channel": "stable",
                "auto_check": False,
                "auto_download": False,
                "check_interval_hours": 6,
                "require_latest_for_run": False,
                "strict_manifest_required": False,
            },
        )
        return self.config

    def load_state(self):
        return load_json(UPDATE_STATE_FILE, {})

    def save_state(self, state):
        payload = dict(state or {})
        payload["updated_at"] = now_iso()
        save_json(UPDATE_STATE_FILE, payload)

    def get_status(self):
        self.reload_config()
        state = self.load_state()
        latest = state.get("latest_version", self.current_version)
        if not self.config.get("manifest_url"):
            return {
                "headline": "UPDATE: CONFIG NEEDED",
                "detail": "Manifest URL non impostato",
                "update_available": False,
                "latest_version": latest,
            }
        if _version_tuple(latest) > _version_tuple(self.current_version):
            return {
                "headline": "UPDATE: DISPONIBILE",
                "detail": f"{self.current_version} -> {latest}",
                "update_available": True,
                "latest_version": latest,
            }
        last_check = state.get("last_check_at", "--")
        return {
            "headline": "UPDATE: OK",
            "detail": f"Ultimo check {last_check}",
            "update_available": False,
            "latest_version": latest,
        }

    def get_runtime_gate(self, force=False):
        self.reload_config()
        require_latest = bool(self.config.get("require_latest_for_run", False))
        strict_manifest = bool(self.config.get("strict_manifest_required", False))
        status = self.check_for_updates(force=force) if force else self.get_status()
        state = self.load_state()
        last_error = str(state.get("last_error", "") or "").strip()

        if not require_latest:
            return {
                "run_allowed": True,
                "headline": status.get("headline", "UPDATE: OK"),
                "detail": status.get("detail", ""),
                "update_available": bool(status.get("update_available", False)),
                "latest_version": status.get("latest_version", self.current_version),
            }

        if strict_manifest and last_error:
            return {
                "run_allowed": False,
                "headline": "UPDATE: CHECK FAILED",
                "detail": f"Impossibile verificare la versione: {last_error}",
                "update_available": False,
                "latest_version": state.get("latest_version", self.current_version),
            }

        if status.get("update_available", False):
            return {
                "run_allowed": False,
                "headline": "UPDATE: REQUIRED",
                "detail": f"Serve la versione {status.get('latest_version', '--')}",
                "update_available": True,
                "latest_version": status.get("latest_version", self.current_version),
            }

        return {
            "run_allowed": True,
            "headline": status.get("headline", "UPDATE: OK"),
            "detail": status.get("detail", ""),
            "update_available": False,
            "latest_version": status.get("latest_version", self.current_version),
        }

    def check_for_updates(self, force=False):
        self.reload_config()
        manifest_url = str(self.config.get("manifest_url", "") or "").strip()
        state = self.load_state()
        if not manifest_url:
            state.update({"last_check_at": now_iso(), "last_error": "manifest_url missing"})
            self.save_state(state)
            return self.get_status()

        if not force:
            last_check = state.get("last_check_at")
            if last_check:
                try:
                    last_dt = datetime.fromisoformat(last_check.replace("Z", "+00:00"))
                    elapsed = datetime.utcnow() - last_dt.replace(tzinfo=None)
                    if elapsed.total_seconds() < max(1, int(self.config.get("check_interval_hours", 6))) * 3600:
                        return self.get_status()
                except Exception:
                    pass

        try:
            with urlopen(_http_request(manifest_url, self.current_version), timeout=4) as response:
                manifest = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, ValueError) as exc:
            state.update({"last_check_at": now_iso(), "last_error": str(exc)})
            self.save_state(state)
            return self.get_status()

        state.update(
            {
                "last_check_at": now_iso(),
                "last_error": "",
                "latest_version": manifest.get("version", self.current_version),
                "download_url": manifest.get("download_url", ""),
                "sha256": manifest.get("sha256", ""),
                "channel": manifest.get("channel", self.config.get("channel", "stable")),
                "notes": manifest.get("notes", []),
            }
        )
        self.save_state(state)
        return self.get_status()

    def download_update(self, force=False):
        state = self.load_state()
        latest_version = state.get("latest_version", self.current_version)
        download_url = str(state.get("download_url", "") or "").strip()
        expected_sha256 = str(state.get("sha256", "") or "").strip().lower()
        if not force and _version_tuple(latest_version) <= _version_tuple(self.current_version):
            return {"ok": False, "reason": "no_update"}
        if not download_url:
            return {"ok": False, "reason": "missing_download_url"}

        updates_dir = ensure_runtime_subdir("updates")
        filename = f"kryon-{latest_version}.zip"
        file_path = os.path.join(updates_dir, filename)
        try:
            with urlopen(_http_request(download_url, self.current_version), timeout=10) as response:
                payload = response.read()
        except (URLError, TimeoutError) as exc:
            state["last_error"] = str(exc)
            self.save_state(state)
            return {"ok": False, "reason": str(exc)}

        digest = hashlib.sha256(payload).hexdigest().lower()
        if expected_sha256 and digest != expected_sha256:
            state["last_error"] = f"sha256 mismatch {digest}"
            self.save_state(state)
            return {"ok": False, "reason": "sha256_mismatch", "sha256": digest}

        with open(file_path, "wb") as f:
            f.write(payload)

        state.update(
            {
                "downloaded_version": latest_version,
                "downloaded_file": file_path,
                "downloaded_sha256": digest,
                "downloaded_at": now_iso(),
                "last_error": "",
            }
        )
        self.save_state(state)
        return {"ok": True, "file": file_path, "version": latest_version, "sha256": digest}
