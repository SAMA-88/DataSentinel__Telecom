"""Upload the contract YAML files to the Databricks Volume used by the quality notebooks.

  python tools/upload_contracts.py            (run from the quality_phase folder, needs .env with DATABRICKS_HOST and DATABRICKS_TOKEN)

Run notebook 10_quality_setup first, so the volume data_sentinel.monitoring.contracts exists.
"""

import dns_fix  # noqa: F401
import os
import sys
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
TARGET = "/Volumes/data_sentinel/monitoring/contracts"
SRC = Path(__file__).resolve().parent.parent / "contract"

if not HOST or not TOKEN:
    sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")

files = sorted(p for p in SRC.glob("*.yaml") if not p.name.startswith("_"))
for p in files:
    with open(p, "rb") as f:
        r = requests.put(f"{HOST}/api/2.0/fs/files{quote(TARGET + '/' + p.name, safe='/')}", params={"overwrite": "true"},
                         headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/octet-stream"}, data=f, timeout=60)
    if r.status_code == 404:
        sys.exit("Volume not found: run notebook 10_quality_setup first")
    r.raise_for_status()
    print("uploaded", p.name)
print(f"done: {len(files)} contracts")
