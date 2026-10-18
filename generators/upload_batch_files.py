"""Upload the local batch CSV files to the Databricks Volume (Files API).

  python upload_batch_files.py --src data/landing
  python upload_batch_files.py --src data/landing --only network

Expected local layout:  <src>/network/*.csv, <src>/billing_invoices/*.csv,
                        <src>/billing_payments/*.csv, <src>/partner/*.csv
Target:                 /Volumes/data_sentinel/bronze/raw/batch/<source>/<file>
Re-running is safe: files with the same name are overwritten.
"""
import argparse
import os
import sys
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
TARGET = "/Volumes/data_sentinel/bronze/raw/batch"
SOURCES = ["network", "billing_invoices", "billing_payments", "partner"]
H = {"Authorization": f"Bearer {TOKEN}"}


def ensure_dir(path):
    requests.put(f"{HOST}/api/2.0/fs/directories{quote(path, safe='/')}/", headers=H, timeout=30).raise_for_status()


def upload(local: Path, remote: str):
    with open(local, "rb") as f:
        r = requests.put(f"{HOST}/api/2.0/fs/files{quote(remote, safe='/')}", params={"overwrite": "true"},
                         headers={**H, "Content-Type": "application/octet-stream"}, data=f, timeout=120)
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/landing")
    ap.add_argument("--only", choices=SOURCES)
    a = ap.parse_args()
    if not HOST or not TOKEN:
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")

    total = 0
    for source in ([a.only] if a.only else SOURCES):
        folder = Path(a.src) / source
        files = sorted(p for p in folder.glob("*.csv") if not p.name.startswith("_"))
        if not files:
            print(f"[skip] no CSV files in {folder}")
            continue
        ensure_dir(f"{TARGET}/{source}")
        for p in files:
            upload(p, f"{TARGET}/{source}/{p.name}")
            total += 1
            print(f"uploaded {source}/{p.name} ({p.stat().st_size:,} bytes)")
    print(f"done: {total} files")


if __name__ == "__main__":
    main()
