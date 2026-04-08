# KRYON Selling Setup

## Obiettivo
Preparare KRYON come prodotto vendibile con:
- licenza cliente
- aggiornamenti automatici
- separazione tra core e configurazione strategie

## Componenti

### 1. App desktop
- `kryon.pyw`
- `bot_core.py`
- moduli runtime/licenza/update

### 2. License server
API minime:
- `POST /api/license/activate`
- `POST /api/license/refresh`
- `GET /api/releases/latest`

### 3. Database
Campi minimi:
- customer_email
- license_key
- plan
- status
- expires_at
- max_devices
- active_devices
- update_channel

### 4. Updater
Manifest JSON con:
- version
- channel
- download_url
- sha256

## Flusso consigliato
1. Cliente inserisce email + key al primo avvio.
2. L'app invia il payload di attivazione.
3. Il server restituisce token/permessi.
4. L'app salva il token nel runtime dir.
5. All'avvio il bot valida lo stato locale.
6. In background controlla gli update.

## Nota importante
In sviluppo il bot resta sbloccato in `DEV MODE`.
In build vendibile, la licenza deve essere valida per avviare il motore.

## Prossimo passo tecnico
- costruire l'API licenze
- firmare le risposte server
- trasformare l'attivazione manuale JSON in chiamata HTTP reale
- preparare packaging `.exe` e installer
