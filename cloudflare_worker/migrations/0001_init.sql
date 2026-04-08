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

CREATE TABLE IF NOT EXISTS release_channels (
    channel TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    download_url TEXT NOT NULL,
    sha256 TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    updated_at TEXT NOT NULL
);

INSERT INTO release_channels (channel, version, download_url, sha256, notes, updated_at)
VALUES ('stable', '15.4.0', '', '', 'Initial online channel', datetime('now'))
ON CONFLICT(channel) DO NOTHING;
