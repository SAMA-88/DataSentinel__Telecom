"""Manage review-app users. Passwords are asked interactively and stored only as salted PBKDF2 hashes.

    python review_app/manage_users.py add admin --role admin
    python review_app/manage_users.py sources                  one user per data source (crm, billing, network, app, partner)
    python review_app/manage_users.py sources --only billing   just that source
    python review_app/manage_users.py add billing --role owner --datasets billing_payments,billing_invoices
    python review_app/manage_users.py passwd billing
    python review_app/manage_users.py remove billing
    python review_app/manage_users.py list
"""
import argparse
import getpass
import sys
from pathlib import Path

from logic import SOURCE_GROUPS, hash_password, load_users, save_users

USERS_FILE = Path(__file__).resolve().parent / "users.json"
MIN_LEN = 10


def ask_password() -> str:
    p1 = getpass.getpass("Password: ")
    if len(p1) < MIN_LEN:
        sys.exit(f"Password must be at least {MIN_LEN} characters.")
    if p1 != getpass.getpass("Repeat password: "):
        sys.exit("Passwords do not match.")
    return p1


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add")
    a.add_argument("username")
    a.add_argument("--role", choices=["admin", "owner"], default="owner")
    a.add_argument("--datasets", default="", help="comma separated (owners only); admins see all")
    sub.add_parser("passwd").add_argument("username")
    sub.add_parser("remove").add_argument("username")
    sub.add_parser("list")
    so = sub.add_parser("sources", help="create one owner user per data source, each with its own password")
    so.add_argument("--only", default="", help="comma separated source names, e.g. billing,crm")
    args = ap.parse_args()

    users = load_users(USERS_FILE)
    if args.cmd == "add":
        if args.username in users:
            sys.exit("User exists; use passwd to change the password.")
        ds = ["*"] if args.role == "admin" else [d.strip() for d in args.datasets.split(",") if d.strip()]
        users[args.username] = {**hash_password(ask_password()), "role": args.role, "datasets": ds}
    elif args.cmd == "passwd":
        if args.username not in users:
            sys.exit("No such user.")
        users[args.username].update(hash_password(ask_password()))
    elif args.cmd == "sources":
        only = [x.strip() for x in args.only.split(",") if x.strip()]
        unknown = set(only) - set(SOURCE_GROUPS)
        if unknown:
            sys.exit(f"Unknown source(s): {', '.join(sorted(unknown))}. Choose from: {', '.join(SOURCE_GROUPS)}")
        for name, datasets in SOURCE_GROUPS.items():
            if only and name not in only:
                continue
            if name in users:
                print(f"{name}: already exists (change its password with: passwd {name})")
                continue
            print(f"\nUser '{name}' can see and edit: {', '.join(datasets)}")
            users[name] = {**hash_password(ask_password()), "role": "owner", "datasets": datasets}
            save_users(USERS_FILE, users)      # save after each user so a typo later does not lose the earlier ones
            print(f"{name}: created")
        return
    elif args.cmd == "remove":
        users.pop(args.username, None)
    else:
        for name, u in users.items():
            print(f"{name:20} {u.get('role'):6} {','.join(u.get('datasets', []))}")
        return
    save_users(USERS_FILE, users)
    print("saved", USERS_FILE)


if __name__ == "__main__":
    main()
