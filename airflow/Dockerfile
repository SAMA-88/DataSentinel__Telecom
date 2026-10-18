FROM apache/airflow:2.10.5
# Databricks provider goes into Airflow itself; dbt gets its own virtualenv so the two never fight over versions.
RUN pip install --no-cache-dir apache-airflow-providers-databricks
RUN python -m venv /home/airflow/dbt_venv && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-databricks
