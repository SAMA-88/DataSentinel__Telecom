"""Sentinel pipeline: Bronze -> quality gate -> Silver -> reconciliation -> incidents -> Gold (dbt).

Airflow only orchestrates. The work stays in the Databricks notebooks and in dbt.
Order of the tasks and why:
  bronze_*          load new CDC events (01_bronze) and batch files into Bronze, in parallel
                    (app events are not loaded by this pipeline)
  gate              contract + rules: pass -> validated table, fail -> quarantine.
                    Circuit breaker: if 50% or more of any table's rows fail, the gate raises after saving its
                    results, so everything after it stops (the incident is still alerted, Gold is not rebuilt)
  autofix           deterministic repairs on quarantined rows
  revalidate        same rules again on the fixed rows
  monitors          the five observability pillars
  silver_*          SCD2 history, clean tables, customer_xref
  recon             cross-system checks (only reads Silver)
  incidents         assign owner/priority, alert, escalate, record resolved (after gate, monitors and recon)
  dbt_build         Gold dims, facts, marts and tests (after incidents, because mart_incidents reads them)
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

NB_DIR = os.environ.get("SENTINEL_NOTEBOOK_DIR", "/Workspace/Users/REPLACE/sentinel").rstrip("/")
CLUSTER_ID = os.environ.get("SENTINEL_CLUSTER_ID", "").strip()
SCHEDULE = os.environ.get("SENTINEL_SCHEDULE", "*/30 * * * *")
SLA_SCALE = os.environ.get("SENTINEL_SLA_SCALE", "1")


def notebook(task_id, name, params=None, **operator_args):
    # Multi-task format (a "tasks" array): required by Databricks for serverless runs, also valid with a cluster.
    task = {"task_key": task_id,
            "notebook_task": {"notebook_path": f"{NB_DIR}/{name}", "source": "WORKSPACE",
                              "base_parameters": params or {}}}
    if CLUSTER_ID:
        task["existing_cluster_id"] = CLUSTER_ID      # no cluster given = serverless compute
    spec = {"run_name": f"sentinel-{task_id}", "tasks": [task]}
    return DatabricksSubmitRunOperator(task_id=task_id, databricks_conn_id="databricks_default", json=spec,
                                       **operator_args)


default_args = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="sentinel_pipeline",
    description="Bronze to Gold with the quality gate, reconciliation and incident management",
    start_date=datetime(2026, 10, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,          # a run must finish before the next one starts (the gate is not safe to overlap)
    default_args=default_args,
    tags=["sentinel", "data-quality"],
) as dag:

    # Notebook names match databricks/notebooks/ in the repo (all notebooks sit in one Databricks folder).
    bronze_cdc = notebook("bronze_cdc", "01_bronze")
    bronze_files = notebook("bronze_batch_files", "02.1_bronze_batch_files")

    # a tripped circuit breaker is a deliberate stop, so no retry
    gate = notebook("gate", "20_quality_gate", {"max_reject_rate": "0.5", "stop_on_breaker": "true"}, retries=0)
    autofix = notebook("autofix", "21_quarantine_autofix")
    revalidate = notebook("revalidate", "22_revalidate")
    monitors = notebook("monitors", "30_observability_monitors")

    silver_history = notebook("silver_history", "50_silver_history")
    silver_clean = notebook("silver_clean", "51_silver_clean")
    silver_xref = notebook("silver_xref", "52_silver_customer_xref")

    recon = notebook("recon", "60_recon")
    incidents = notebook("incidents", "71_incident_manage", {"sla_scale": SLA_SCALE},
                         trigger_rule="all_done")   # runs even after the breaker stopped the pipeline

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt_simple && /home/airflow/dbt_venv/bin/dbt build --profiles-dir /opt/airflow/dbt_profiles",
    )

    [bronze_cdc, bronze_files] >> gate >> autofix >> revalidate >> monitors
    monitors >> silver_history >> silver_clean >> silver_xref >> recon >> incidents >> dbt_build
    gate >> dbt_build             # Gold is not rebuilt when the breaker tripped
