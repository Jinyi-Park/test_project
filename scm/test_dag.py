from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor


def default_args():
    return {
        "owner": "airflow",
        "start_date": datetime(2025, 1, 1),
        "retries": 0,
        "retry_delay": timedelta(minutes=5)
    }

with DAG(
    dag_id='test_databricks_operator',
    default_args=default_args(),
    schedule_interval='0 0 * * *',
    description="Test databricks operator",
    catchup=False,
) as dag:
    
    sensor_data = DatabricksSqlSensor(
        task_id="sensor_contribution",
        databricks_conn_id="musinsa-analysis-ws",
        sql_warehouse_name="data-analysis-shared-sql-warehouse",
        sql="""
            SELECT * FROM team.tech.f_mss_contribution_daily WHERE dt = '{{ macros.ds_add(ds, -1) }}'
        """
    )

    extract_and_load_data = DatabricksSqlOperator(
        task_id="extract_and_load_to_gold",
        databricks_conn_id="musinsa-analysis-ws",
        sql_endpoint_name="data-analysis-shared-sql-warehouse",
        sql=[
            "CREATE TABLE team.tech.test_airflow AS SELECT * FROM team.tech.f_mss_contribution_daily WHERE dt = '2025-02-01' LIMIT 10",
            "INSERT INTO team.tech.test_airflow SELECT * FROM team.tech.f_mss_contribution_daily WHERE dt = '{{ macros.ds_add(ds, -1) }}' LIMIT 10",
        ]
    )

    sensor_data >> extract_and_load_data