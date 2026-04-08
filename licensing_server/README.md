# KRYON Licensing Server

Server minimale locale per:
- attivazione licenze
- refresh token
- manifest aggiornamenti

## Avvio rapido

1. Inizializza il database:

```powershell
python licensing_server/admin_cli.py init-db
```

2. Crea una licenza:

```powershell
python licensing_server/admin_cli.py create-license --email cliente@example.com --key KRYON-PRO-0001 --plan PRO --days 30 --max-devices 1
```

3. Avvia il server:

```powershell
python licensing_server/server.py
```

Il server ascolta su:

`http://127.0.0.1:8787`

## Endpoint

- `POST /api/license/activate`
- `POST /api/license/refresh`
- `GET /api/releases/latest`
- `GET /health`

## Config client KRYON

Nel runtime del bot, imposta:

`license_config.json`

```json
{
  "api_base_url": "http://127.0.0.1:8787",
  "activation_endpoint": "/api/license/activate",
  "refresh_endpoint": "/api/license/refresh",
  "grace_days": 5,
  "enforce_packaged_only": true
}
```

`update_config.json`

```json
{
  "manifest_url": "http://127.0.0.1:8787/api/releases/latest",
  "channel": "stable",
  "auto_check": true,
  "check_interval_hours": 6
}
```

## Nota

Questa e' una base tecnica locale, utile per iniziare e testare il flusso completo.
Per la vendita reale il passo successivo e':
- deploy su VPS o server pubblico
- HTTPS
- firma risposte/token
- pannello admin
