import os
import sqlite3
import uuid
from datetime import datetime, timedelta


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "licenses.db")


def now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                license_key TEXT NOT NULL UNIQUE,
                plan TEXT NOT NULL DEFAULT 'PRO',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                expires_at TEXT,
                max_devices INTEGER NOT NULL DEFAULT 1,
                update_channel TEXT NOT NULL DEFAULT 'stable',
                allow_updates INTEGER NOT NULL DEFAULT 1,
                allow_strategy_config INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key TEXT NOT NULL,
                machine_fingerprint TEXT NOT NULL,
                token_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(license_key, machine_fingerprint)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_license(email, license_key, plan="PRO", days=30, max_devices=1, update_channel="stable"):
    now = datetime.utcnow()
    expires_at = (now + timedelta(days=int(days))).replace(microsecond=0).isoformat() + "Z"
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO licenses (
                email, license_key, plan, status, expires_at, max_devices,
                update_channel, allow_updates, allow_strategy_config, created_at, updated_at
            )
            VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, 1, 1, ?, ?)
            """,
            (email, license_key, plan, expires_at, int(max_devices), update_channel, now_iso(), now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def list_licenses():
    conn = get_connection()
    try:
        return conn.execute(
            """
            SELECT l.*, COUNT(a.id) AS active_devices
            FROM licenses l
            LEFT JOIN activations a ON a.license_key = l.license_key AND a.status = 'ACTIVE'
            GROUP BY l.id
            ORDER BY l.created_at DESC
            """
        ).fetchall()
    finally:
        conn.close()


def list_activations(license_key=None):
    conn = get_connection()
    try:
        if license_key:
            return conn.execute(
                """
                SELECT * FROM activations
                WHERE license_key = ?
                ORDER BY created_at DESC
                """,
                (license_key,),
            ).fetchall()
        return conn.execute(
            """
            SELECT * FROM activations
            ORDER BY created_at DESC
            """
        ).fetchall()
    finally:
        conn.close()


def get_license(license_key):
    conn = get_connection()
    try:
        return conn.execute("SELECT * FROM licenses WHERE license_key = ?", (license_key,)).fetchone()
    finally:
        conn.close()


def get_activation_by_token(token_id):
    conn = get_connection()
    try:
        return conn.execute("SELECT * FROM activations WHERE token_id = ?", (token_id,)).fetchone()
    finally:
        conn.close()


def get_activation(license_key, machine_fingerprint):
    conn = get_connection()
    try:
        return conn.execute(
            "SELECT * FROM activations WHERE license_key = ? AND machine_fingerprint = ?",
            (license_key, machine_fingerprint),
        ).fetchone()
    finally:
        conn.close()


def count_active_devices(license_key):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM activations WHERE license_key = ? AND status = 'ACTIVE'",
            (license_key,),
        ).fetchone()
        return int(row["c"] or 0)
    finally:
        conn.close()


def upsert_activation(license_key, machine_fingerprint):
    now = now_iso()
    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT * FROM activations WHERE license_key = ? AND machine_fingerprint = ?",
            (license_key, machine_fingerprint),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE activations SET status = 'ACTIVE', last_seen_at = ? WHERE id = ?",
                (now, existing["id"]),
            )
            token_id = existing["token_id"]
        else:
            token_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO activations (license_key, machine_fingerprint, token_id, status, created_at, last_seen_at)
                VALUES (?, ?, ?, 'ACTIVE', ?, ?)
                """,
                (license_key, machine_fingerprint, token_id, now, now),
            )
        conn.commit()
        return token_id
    finally:
        conn.close()


def touch_activation(token_id):
    conn = get_connection()
    try:
        conn.execute("UPDATE activations SET last_seen_at = ? WHERE token_id = ?", (now_iso(), token_id))
        conn.commit()
    finally:
        conn.close()


def revoke_license(license_key):
    conn = get_connection()
    try:
        conn.execute("UPDATE licenses SET status = 'REVOKED', updated_at = ? WHERE license_key = ?", (now_iso(), license_key))
        conn.execute("UPDATE activations SET status = 'REVOKED', last_seen_at = ? WHERE license_key = ?", (now_iso(), license_key))
        conn.commit()
    finally:
        conn.close()


def clear_activations(license_key):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE activations SET status = 'REVOKED', last_seen_at = ? WHERE license_key = ?",
            (now_iso(), license_key),
        )
        conn.commit()
    finally:
        conn.close()


def delete_activation(token_id):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM activations WHERE token_id = ?", (token_id,))
        conn.commit()
    finally:
        conn.close()


def set_max_devices(license_key, max_devices):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE licenses SET max_devices = ?, updated_at = ? WHERE license_key = ?",
            (int(max_devices), now_iso(), license_key),
        )
        conn.commit()
    finally:
        conn.close()
