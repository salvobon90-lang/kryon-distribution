# KRYON Cloudflare Licensing

Questo worker sostituisce il server locale `licensing_server` con:

- attivazione licenza online
- refresh token online
- manifest update online
- endpoint admin protetti da `x-admin-key`

## Step rapidi

1. Crea il database D1
2. Inserisci il `database_id` in `wrangler.toml`
3. Applica la migration
4. Imposta il secret `ADMIN_API_KEY`
5. Pubblica il worker

## Endpoint pubblici

- `GET /health`
- `POST /api/license/activate`
- `POST /api/license/refresh`
- `GET /api/releases/latest?channel=stable`

## Endpoint admin

Richiedono header:

`x-admin-key: <ADMIN_API_KEY>`

- `GET /api/admin/licenses`
- `POST /api/admin/license/create`
- `POST /api/admin/license/revoke`
- `POST /api/admin/license/clear-activations`
- `POST /api/admin/release/set`

## Payload admin create license

```json
{
  "email": "cliente@example.com",
  "license_key": "KRYON-PRO-0001",
  "plan": "PRO",
  "days": 30,
  "max_devices": 1,
  "update_channel": "stable"
}
```

## Payload admin set release

```json
{
  "channel": "stable",
  "version": "15.4.0",
  "download_url": "https://github.com/<USER>/<REPO>/releases/download/v15.4.0/kryon-v15.4.0.zip",
  "sha256": "",
  "notes": "Initial public online release"
}
```
