import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

test_dag = DAG( 
    dag_id='test_new_dag',
    description='testing dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2)
    }
)

dummy_step_1 = DummyOperator(
        task_id="step_1",
        trigger_rule="all_success",
    )