import airflow
from airflow.operators.python_operator import python_operator
from datetime import datetime, timedelta
from Loading import *

# define ETL functions
def etl():
    movies_df = extract_movies_to_df()
    user_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movies_df, user_df)
    load_df_to_db(transformed_df)

# define the arguments for the DAG
kwargs = {
    'owner': 'Henry',
    'start_date': airflow.utils.dates.days_age(1),
    'retries': 3,
}

# instantiate the DAG
dag = airflow.DAG(
    dag_id = 'etl_pipeline',
    default_args = kwargs,
    schedule_interval = '0 0 * * *' # @daily	Run once a day at midnight	0 0 * * *
)

etl_task = python_operator(
    task_id = 'etl_task',
    python_callable = etl,
    dag = dag
)

etl()