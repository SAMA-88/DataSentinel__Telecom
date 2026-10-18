"""Databricks SQL access for the quarantine review app (SQL warehouse via databricks-sql-connector)."""
import os

from databricks import sql

from logic import ALL_STATUSES, EDITABLE_STATUSES, FIXED_STATUS, safe_name

CATALOG = "data_sentinel"
T_EDITS = f"{CATALOG}.monitoring.quarantine_edits"
DEFAULT_HOST = "dbc-21b8c56c-c07d.cloud.databricks.com"  # same workspace as airflow/docker-compose.yml and dbt


def connect():
    host = os.getenv("DATABRICKS_HOST", DEFAULT_HOST).replace("https://", "").rstrip("/")
    return sql.connect(server_hostname=host, http_path=os.environ["DATABRICKS_HTTP_PATH"],
                       access_token=os.environ["DATABRICKS_TOKEN"])


def quarantine_table(dataset: str) -> str:
    return f"{CATALOG}.quarantine.{safe_name(dataset)}"


def table_exists(conn, dataset: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SHOW TABLES IN {CATALOG}.quarantine LIKE '{safe_name(dataset)}'")
        return bool(cur.fetchall())


def fetch_rows(conn, dataset: str, columns: list[str], statuses: list[str], limit: int = 500) -> list[dict]:
    """Rows of one quarantine table. For rows already fixed, the fixed values are shown instead of the originals."""
    statuses = [s for s in statuses if s in ALL_STATUSES] or list(EDITABLE_STATUSES)
    cols = ", ".join(f"CASE WHEN _fixed IS NOT NULL THEN _fixed['{safe_name(c)}'] ELSE CAST({c} AS STRING) END AS {c}"
                     for c in columns)
    status_list = ", ".join(f"'{s}'" for s in statuses)
    q = (f"SELECT _row_id, {cols}, _failure_reasons, _quarantine_status, _attempts, "
         f"CAST(_quarantined_at AS STRING) AS _quarantined_at FROM {quarantine_table(dataset)} "
         f"WHERE _quarantine_status IN ({status_list}) ORDER BY _quarantined_at DESC LIMIT {int(limit)}")
    with conn.cursor() as cur:
        cur.execute(q)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]


def status_counts(conn, dataset: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"SELECT _quarantine_status, count(*) FROM {quarantine_table(dataset)} GROUP BY 1")
        return {s: n for s, n in cur.fetchall()}


def save_fixes(conn, dataset: str, edits: list[dict], editor: str) -> int:
    """Write each corrected row to _fixed and mark it for revalidation; log every changed cell. Returns rows updated."""
    table = quarantine_table(dataset)
    editable = ", ".join(f"'{s}'" for s in EDITABLE_STATUSES)
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {T_EDITS} (edited_at TIMESTAMP, editor STRING, dataset STRING,
                        row_id STRING, column_name STRING, old_value STRING, new_value STRING)""")
        updated = 0
        for e in edits:
            cols = list(e["values"].keys())
            params = {"rid": e["row_id"]}
            pairs = []
            for i, c in enumerate(cols):
                params[f"v{i}"] = e["values"][c]
                pairs.append(f"'{safe_name(c)}', CAST(:v{i} AS STRING)")
            cur.execute(f"""UPDATE {table} SET _fixed = map({', '.join(pairs)}),
                            _quarantine_status = '{FIXED_STATUS}', _attempts = _attempts + 1
                            WHERE _row_id = :rid AND _quarantine_status IN ({editable})""", params)
            updated += 1
            for c, old, new in e["changes"]:
                cur.execute(f"INSERT INTO {T_EDITS} VALUES (current_timestamp(), :editor, :ds, :rid, :col, :old, :new)",
                            {"editor": editor, "ds": dataset, "rid": e["row_id"], "col": c, "old": old, "new": new})
        return updated


def recent_edits(conn, dataset: str, limit: int = 50) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW TABLES IN {CATALOG}.monitoring LIKE 'quarantine_edits'")
        if not cur.fetchall():
            return []
        cur.execute(f"SELECT CAST(edited_at AS STRING) AS edited_at, editor, row_id, column_name, old_value, new_value "
                    f"FROM {T_EDITS} WHERE dataset = :ds ORDER BY edited_at DESC LIMIT {int(limit)}", {"ds": dataset})
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
