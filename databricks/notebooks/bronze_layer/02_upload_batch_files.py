"""Upload the local batch CSV files to the Databricks Volume (Files API).

  python upload_batch_files.py --src data/landing
  python upload_batch_files.py --src data/landing --only network
  python upload_batch_files.py --src data/landing --overwrite      (replace files already in the Volume)

Expected local layout:  <src>/network/*.csv, <src>/billing_invoices/*.csv,
                        <src>/billing_payments/*.csv, <src>/partner/*.csv
Target:                 /Volumes/data_sentinel/bronze/raw/batch/<source>/<file>
Re-running is safe: a file already in the Volume is skipped, not uploaded again. Its modification time is the
moment it arrived, which the late file detector (33_late_file_detector) uses, so it must not change.
--overwrite replaces existing files anyway; the detector still keeps the first arrival time it recorded, but
02.1_bronze_batch_files does not load a file again under a name it already loaded.
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


def upload(local: Path, remote: str, overwrite=False):
    """Upload one file. Returns False when it already exists and overwrite is off (nothing is changed)."""
    with open(local, "rb") as f:
        r = requests.put(f"{HOST}/api/2.0/fs/files{quote(remote, safe='/')}",
                         params={"overwrite": "true" if overwrite else "false"},
                         headers={**H, "Content-Type": "application/octet-stream"}, data=f, timeout=120)
    if r.status_code == 409 and not overwrite:          # ALREADY_EXISTS
        return False
    r.raise_for_status()
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/landing")
    ap.add_argument("--only", choices=SOURCES)
    ap.add_argument("--overwrite", action="store_true", help="replace files that are already in the Volume")
    a = ap.parse_args()
    if not HOST or not TOKEN:
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")

    total = skipped = 0
    for source in ([a.only] if a.only else SOURCES):
        folder = Path(a.src) / source
        files = sorted(p for p in folder.glob("*.csv") if not p.name.startswith("_"))
        if not files:
            print(f"[skip] no CSV files in {folder}")
            continue
        ensure_dir(f"{TARGET}/{source}")
        for p in files:
            if upload(p, f"{TARGET}/{source}/{p.name}", a.overwrite):
                total += 1
                print(f"uploaded {source}/{p.name} ({p.stat().st_size:,} bytes)")
            else:
                skipped += 1
                print(f"[skip] {source}/{p.name} is already in the Volume")
    print(f"done: {total} uploaded, {skipped} already there")


if __name__ == "__main__":
    main()
