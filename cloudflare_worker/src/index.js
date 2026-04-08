function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function nowIso() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function addDaysIso(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function parseIso(value) {
  if (!value) return null;
  const d = new Date(String(value));
  return Number.isNaN(d.getTime()) ? null : d;
}

function isoNoMs(date) {
  return new Date(date).toISOString().replace(/\.\d{3}Z$/, "Z");
}

function buildLicenseResponse(licenseRow, machineFingerprint, tokenId) {
  const now = new Date();
  const expiresAt = parseIso(licenseRow.expires_at) || new Date(Date.now() + 30 * 86400000);
  const refreshAfter = new Date(Math.min(expiresAt.getTime(), now.getTime() + 3 * 86400000));
  const offlineGraceUntil = new Date(expiresAt.getTime() + 5 * 86400000);
  return {
    status: licenseRow.status,
    email: licenseRow.email,
    license_key: licenseRow.license_key,
    plan: licenseRow.plan,
    token_id: tokenId,
    issued_at: isoNoMs(now),
    expires_at: isoNoMs(expiresAt),
    refresh_after: isoNoMs(refreshAfter),
    offline_grace_until: isoNoMs(offlineGraceUntil),
    machine_fingerprint: machineFingerprint,
    update_channel: licenseRow.update_channel,
    entitlements: {
      updates: Boolean(Number(licenseRow.allow_updates || 0)),
      strategy_config: Boolean(Number(licenseRow.allow_strategy_config || 0)),
    },
  };
}

function requireAdmin(request, env) {
  const incoming = request.headers.get("x-admin-key") || "";
  if (!env.ADMIN_API_KEY || incoming !== env.ADMIN_API_KEY) {
    return json({ error: "admin_unauthorized" }, 401);
  }
  return null;
}

async function getLicense(env, licenseKey) {
  return env.DB.prepare("SELECT * FROM licenses WHERE license_key = ?")
    .bind(licenseKey)
    .first();
}

async function getActivation(env, licenseKey, machineFingerprint) {
  return env.DB.prepare(
    "SELECT * FROM activations WHERE license_key = ? AND machine_fingerprint = ?"
  )
    .bind(licenseKey, machineFingerprint)
    .first();
}

async function getActivationByToken(env, tokenId) {
  return env.DB.prepare("SELECT * FROM activations WHERE token_id = ?")
    .bind(tokenId)
    .first();
}

async function countActiveDevices(env, licenseKey) {
  const row = await env.DB.prepare(
    "SELECT COUNT(*) AS c FROM activations WHERE license_key = ? AND status = 'ACTIVE'"
  )
    .bind(licenseKey)
    .first();
  return Number(row?.c || 0);
}

async function upsertActivation(env, licenseKey, machineFingerprint) {
  const now = nowIso();
  const existing = await getActivation(env, licenseKey, machineFingerprint);
  if (existing) {
    await env.DB.prepare(
      "UPDATE activations SET status = 'ACTIVE', last_seen_at = ? WHERE id = ?"
    )
      .bind(now, existing.id)
      .run();
    return existing.token_id;
  }
  const tokenId = crypto.randomUUID();
  await env.DB.prepare(
    `
      INSERT INTO activations (
        license_key, machine_fingerprint, token_id, status, created_at, last_seen_at
      )
      VALUES (?, ?, ?, 'ACTIVE', ?, ?)
    `
  )
    .bind(licenseKey, machineFingerprint, tokenId, now, now)
    .run();
  return tokenId;
}

async function touchActivation(env, tokenId) {
  await env.DB.prepare("UPDATE activations SET last_seen_at = ? WHERE token_id = ?")
    .bind(nowIso(), tokenId)
    .run();
}

async function listLicenses(env) {
  const res = await env.DB.prepare(
    `
      SELECT l.*, COUNT(a.id) AS active_devices
      FROM licenses l
      LEFT JOIN activations a ON a.license_key = l.license_key AND a.status = 'ACTIVE'
      GROUP BY l.id
      ORDER BY l.created_at DESC
    `
  ).all();
  return res.results || [];
}

async function handleActivate(env, payload) {
  const email = String(payload.email || "").trim().toLowerCase();
  const licenseKey = String(payload.license_key || "").trim();
  const machineFingerprint = String(payload.machine_fingerprint || "").trim();
  if (!email || !licenseKey || !machineFingerprint) {
    return json({ error: "missing_fields" }, 400);
  }

  const licenseRow = await getLicense(env, licenseKey);
  if (!licenseRow) return json({ error: "license_not_found" }, 404);
  if (String(licenseRow.email || "").trim().toLowerCase() !== email) {
    return json({ error: "email_mismatch" }, 403);
  }
  if (String(licenseRow.status || "").toUpperCase() !== "ACTIVE") {
    return json({ error: "license_not_active" }, 403);
  }
  const expiresAt = parseIso(licenseRow.expires_at);
  if (expiresAt && Date.now() > expiresAt.getTime()) {
    return json({ error: "license_expired" }, 403);
  }

  const existingActivation = await getActivation(env, licenseKey, machineFingerprint);
  const activeDevices = await countActiveDevices(env, licenseKey);
  if (!existingActivation && activeDevices >= Number(licenseRow.max_devices || 1)) {
    return json({ error: "device_limit_reached" }, 403);
  }

  const tokenId = await upsertActivation(env, licenseKey, machineFingerprint);
  return json(buildLicenseResponse(licenseRow, machineFingerprint, tokenId), 200);
}

async function handleRefresh(env, payload) {
  const tokenId = String(payload.token_id || "").trim();
  const machineFingerprint = String(payload.machine_fingerprint || "").trim();
  if (!tokenId || !machineFingerprint) {
    return json({ error: "missing_fields" }, 400);
  }
  const activation = await getActivationByToken(env, tokenId);
  if (!activation) return json({ error: "activation_not_found" }, 404);
  if (activation.machine_fingerprint !== machineFingerprint) {
    return json({ error: "machine_mismatch" }, 403);
  }
  if (String(activation.status || "").toUpperCase() !== "ACTIVE") {
    return json({ error: "activation_not_active" }, 403);
  }
  const licenseRow = await getLicense(env, activation.license_key);
  if (!licenseRow) return json({ error: "license_not_found" }, 404);
  if (String(licenseRow.status || "").toUpperCase() !== "ACTIVE") {
    return json({ error: "license_not_active" }, 403);
  }

  await touchActivation(env, tokenId);
  return json(buildLicenseResponse(licenseRow, machineFingerprint, tokenId), 200);
}

async function handleAdminCreateLicense(request, env, payload) {
  const authError = requireAdmin(request, env);
  if (authError) return authError;

  const email = String(payload.email || "").trim().toLowerCase();
  const licenseKey = String(payload.license_key || "").trim();
  const plan = String(payload.plan || "PRO").trim().toUpperCase();
  const status = String(payload.status || "ACTIVE").trim().toUpperCase();
  const days = Number(payload.days || 30);
  const maxDevices = Math.max(1, Number(payload.max_devices || 1));
  const updateChannel = String(payload.update_channel || "stable").trim().toLowerCase();
  const allowUpdates = payload.allow_updates === false ? 0 : 1;
  const allowStrategyConfig = payload.allow_strategy_config === false ? 0 : 1;

  if (!email || !licenseKey) {
    return json({ error: "missing_fields" }, 400);
  }

  const now = nowIso();
  const expiresAt = payload.expires_at ? String(payload.expires_at) : addDaysIso(days);

  await env.DB.prepare(
    `
      INSERT INTO licenses (
        email, license_key, plan, status, expires_at, max_devices, update_channel,
        allow_updates, allow_strategy_config, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(license_key) DO UPDATE SET
        email = excluded.email,
        plan = excluded.plan,
        status = excluded.status,
        expires_at = excluded.expires_at,
        max_devices = excluded.max_devices,
        update_channel = excluded.update_channel,
        allow_updates = excluded.allow_updates,
        allow_strategy_config = excluded.allow_strategy_config,
        updated_at = excluded.updated_at
    `
  )
    .bind(
      email,
      licenseKey,
      plan,
      status,
      expiresAt,
      maxDevices,
      updateChannel,
      allowUpdates,
      allowStrategyConfig,
      now,
      now
    )
    .run();

  const licenseRow = await getLicense(env, licenseKey);
  return json({ ok: true, license: licenseRow }, 200);
}

async function handleAdminRevokeLicense(request, env, payload) {
  const authError = requireAdmin(request, env);
  if (authError) return authError;
  const licenseKey = String(payload.license_key || "").trim();
  if (!licenseKey) return json({ error: "missing_fields" }, 400);

  await env.DB.batch([
    env.DB.prepare("UPDATE licenses SET status = 'REVOKED', updated_at = ? WHERE license_key = ?").bind(nowIso(), licenseKey),
    env.DB.prepare("UPDATE activations SET status = 'REVOKED', last_seen_at = ? WHERE license_key = ?").bind(nowIso(), licenseKey),
  ]);
  return json({ ok: true }, 200);
}

async function handleAdminClearActivations(request, env, payload) {
  const authError = requireAdmin(request, env);
  if (authError) return authError;
  const licenseKey = String(payload.license_key || "").trim();
  if (!licenseKey) return json({ error: "missing_fields" }, 400);

  await env.DB.prepare(
    "UPDATE activations SET status = 'REVOKED', last_seen_at = ? WHERE license_key = ?"
  )
    .bind(nowIso(), licenseKey)
    .run();
  return json({ ok: true }, 200);
}

async function handleAdminListLicenses(request, env) {
  const authError = requireAdmin(request, env);
  if (authError) return authError;
  return json({ ok: true, licenses: await listLicenses(env) }, 200);
}

async function handleAdminSetRelease(request, env, payload) {
  const authError = requireAdmin(request, env);
  if (authError) return authError;
  const channel = String(payload.channel || "stable").trim().toLowerCase();
  const version = String(payload.version || "").trim();
  const downloadUrl = String(payload.download_url || "").trim();
  const sha256 = String(payload.sha256 || "").trim();
  const notes = String(payload.notes || "").trim();
  if (!channel || !version || !downloadUrl) {
    return json({ error: "missing_fields" }, 400);
  }

  await env.DB.prepare(
    `
      INSERT INTO release_channels (channel, version, download_url, sha256, notes, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(channel) DO UPDATE SET
        version = excluded.version,
        download_url = excluded.download_url,
        sha256 = excluded.sha256,
        notes = excluded.notes,
        updated_at = excluded.updated_at
    `
  )
    .bind(channel, version, downloadUrl, sha256, notes, nowIso())
    .run();

  return json({ ok: true }, 200);
}

async function handleLatestRelease(env, requestUrl) {
  const channel = (requestUrl.searchParams.get("channel") || "stable").trim().toLowerCase();
  const sessionDb = env.DB.withSession("first-primary");
  const row = await sessionDb.prepare("SELECT * FROM release_channels WHERE channel = ?")
    .bind(channel)
    .first();
  if (!row) {
    return json({
      channel,
      version: "0.0.0",
      download_url: "",
      sha256: "",
      notes: "",
    });
  }
  return json(row, 200);
}

async function readJson(request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, service: env.APP_NAME || "kryon-licensing" });
    }
    if (request.method === "GET" && url.pathname === "/api/releases/latest") {
      return handleLatestRelease(env, url);
    }
    if (request.method === "GET" && url.pathname === "/api/admin/licenses") {
      return handleAdminListLicenses(request, env);
    }

    const payload = await readJson(request);

    if (request.method === "POST" && url.pathname === "/api/license/activate") {
      return handleActivate(env, payload);
    }
    if (request.method === "POST" && url.pathname === "/api/license/refresh") {
      return handleRefresh(env, payload);
    }
    if (request.method === "POST" && url.pathname === "/api/admin/license/create") {
      return handleAdminCreateLicense(request, env, payload);
    }
    if (request.method === "POST" && url.pathname === "/api/admin/license/revoke") {
      return handleAdminRevokeLicense(request, env, payload);
    }
    if (request.method === "POST" && url.pathname === "/api/admin/license/clear-activations") {
      return handleAdminClearActivations(request, env, payload);
    }
    if (request.method === "POST" && url.pathname === "/api/admin/release/set") {
      return handleAdminSetRelease(request, env, payload);
    }

    return json({ error: "not_found" }, 404);
  },
};
