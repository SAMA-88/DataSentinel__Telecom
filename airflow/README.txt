AIRFLOW FOR THE SENTINEL PIPELINE
=================================
Airflow runs the pipeline; Databricks and dbt do the work. Airflow replaces the Databricks Job from the plan.

Setup (PowerShell, Docker Desktop running)
  0. Put the folders airflow_sentinel and dbt_simple SIDE BY SIDE (same parent folder). Docker mounts ..\dbt_simple.
  1. cd airflow_sentinel
  2. copy .env.example .env      and edit it: token, SQL warehouse path, notebook folder
  3. In Databricks, put ALL notebooks in ONE workspace folder (01_bronze, 02_bronze_app_events, 03_bronze_batch_files, 11_dq_lib, 20, 21, 22, 30,
     49_silver_lib, 50, 51, 52, 60_recon, 70-73 incident notebooks),
     because they load each other with  %run ./11_dq_lib  and  %run ./49_silver_lib
  4. docker compose up -d --build        (first build takes a few minutes)
  5. open http://localhost:8080 ; user admin ; password: docker compose logs airflow | findstr -i password
  6. In the page, unpause "sentinel_pipeline" and press Trigger. Open the Graph view to watch each task.

Token: Databricks > top right > Settings > Developer > Access tokens. Keep it only in .env.

What stays outside Airflow
  - Postgres, Debezium, Kafka and bridge.py (docker compose in cdc_fixed) run all the time.
  - Source files are copied into the Databricks Volume by upload_batch_files.py (later: a task or a file sensor).

Limits you should know
  - Each notebook is a separate Databricks run, so one cycle takes minutes, not seconds. Use a 15 to 30 minute schedule.
    True "every 1-2 minutes" would need Structured Streaming for Bronze and the gate, which is a bigger change.
  - If a task fails, the tasks after it do not run. Look at the red task log, fix, press Clear to retry from there.
  - The verify notebooks (53, 61, 79) print PASS/FAIL but do not fail the task; keep running them by hand for now.
  - This is one container with a SQLite database. Good for the demo, not for production.
  - Not tested here: I could not run Airflow or Databricks from my workspace.
