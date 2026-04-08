import argparse
import json
import os

from db import (
    BASE_DIR,
    clear_activations,
    create_license,
    delete_activation,
    init_db,
    list_activations,
    list_licenses,
    now_iso,
    revoke_license,
    set_max_devices,
)


MANIFEST_PATH = os.path.join(BASE_DIR, "latest.json")


def cmd_init(_args):
    init_db()
    print("Database licensing inizializzato.")


def cmd_create_license(args):
    init_db()
    create_license(
        email=args.email,
        license_key=args.key,
        plan=args.plan,
        days=args.days,
        max_devices=args.max_devices,
        update_channel=args.channel,
    )
    print(f"Licenza creata: {args.key}")


def cmd_list(_args):
    init_db()
    rows = list_licenses()
    if not rows:
        print("Nessuna licenza.")
        return
    for row in rows:
        print(
            f"{row['license_key']} | {row['email']} | {row['plan']} | {row['status']} | "
            f"dev {row['active_devices']}/{row['max_devices']} | exp {row['expires_at']}"
        )


def cmd_revoke(args):
    init_db()
    revoke_license(args.key)
    print(f"Licenza revocata: {args.key}")


def cmd_list_activations(args):
    init_db()
    rows = list_activations(args.key)
    if not rows:
        print("Nessuna attivazione.")
        return
    for row in rows:
        print(
            f"{row['token_id']} | {row['license_key']} | {row['machine_fingerprint']} | "
            f"{row['status']} | last {row['last_seen_at']}"
        )


def cmd_clear_activations(args):
    init_db()
    clear_activations(args.key)
    print(f"Attivazioni rilasciate per: {args.key}")


def cmd_delete_activation(args):
    init_db()
    delete_activation(args.token)
    print(f"Attivazione eliminata: {args.token}")


def cmd_set_max_devices(args):
    init_db()
    set_max_devices(args.key, args.max_devices)
    print(f"max_devices aggiornato a {args.max_devices} per {args.key}")


def cmd_set_manifest(args):
    payload = {
        "channel": args.channel,
        "version": args.version,
        "download_url": args.download_url,
        "sha256": args.sha256,
        "updated_at": now_iso(),
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Manifest aggiornato: {MANIFEST_PATH}")


def build_parser():
    parser = argparse.ArgumentParser(description="KRYON licensing admin CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-db", help="Inizializza SQLite")
    p_init.set_defaults(func=cmd_init)

    p_create = sub.add_parser("create-license", help="Crea una licenza")
    p_create.add_argument("--email", required=True)
    p_create.add_argument("--key", required=True)
    p_create.add_argument("--plan", default="PRO")
    p_create.add_argument("--days", type=int, default=30)
    p_create.add_argument("--max-devices", type=int, default=1)
    p_create.add_argument("--channel", default="stable")
    p_create.set_defaults(func=cmd_create_license)

    p_list = sub.add_parser("list-licenses", help="Elenca licenze")
    p_list.set_defaults(func=cmd_list)

    p_revoke = sub.add_parser("revoke-license", help="Revoca una licenza")
    p_revoke.add_argument("--key", required=True)
    p_revoke.set_defaults(func=cmd_revoke)

    p_list_act = sub.add_parser("list-activations", help="Elenca attivazioni")
    p_list_act.add_argument("--key", required=False)
    p_list_act.set_defaults(func=cmd_list_activations)

    p_clear_act = sub.add_parser("clear-activations", help="Rilascia tutte le attivazioni di una licenza")
    p_clear_act.add_argument("--key", required=True)
    p_clear_act.set_defaults(func=cmd_clear_activations)

    p_delete_act = sub.add_parser("delete-activation", help="Elimina una singola attivazione")
    p_delete_act.add_argument("--token", required=True)
    p_delete_act.set_defaults(func=cmd_delete_activation)

    p_set_max = sub.add_parser("set-max-devices", help="Aggiorna il numero massimo di dispositivi")
    p_set_max.add_argument("--key", required=True)
    p_set_max.add_argument("--max-devices", type=int, required=True)
    p_set_max.set_defaults(func=cmd_set_max_devices)

    p_manifest = sub.add_parser("set-manifest", help="Aggiorna manifest release")
    p_manifest.add_argument("--version", required=True)
    p_manifest.add_argument("--download-url", required=True)
    p_manifest.add_argument("--sha256", default="")
    p_manifest.add_argument("--channel", default="stable")
    p_manifest.set_defaults(func=cmd_set_manifest)
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
