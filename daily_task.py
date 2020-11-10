import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_arguments = {
    'owner': 'pavithra',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 9),
    'email': ['pavithra.kariyawasam@datamtx.com','saminda.jayasinghe@datamtx.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(dag_id='dailytask',
    default_args=default_arguments,
    description='DAG to push and pull both airflow and dbt and to run dbt file',
    schedule_interval=timedelta(minutes=5)
)

# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = BashOperator(
    dag=dag,
	email_on_failure=True,
    emai='pavithra.kariyawasam@datamtx.com',
    task_id='pull_airflow_dag',
    bash_command="""
        cd /home/pavithra/airflow/test-dags/ \
		&& \
		git pull \
		&& \
		git push
		"""
)

t2 = BashOperator(
    dag=dag,
    task_id='pull_dbt',
    bash_command="""
        cd /home/pavithra/dbt_/dbt_toscsa/ \
		&& \
		git pull \
		&& \
		git push
		"""
)

t3 = BashOperator(
         dag=dag,
         task_id='daily_task_dbt',
         bash_command="""
         cd /home/pavithra/dbt_/dbt_toscsa/ \
         && \
         dbt run --models dailytask --target dev
         """
 )

t1 >> t2 >> t3
