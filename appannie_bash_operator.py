from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta



#schedule_interval = timedelta(minutes=10)

args = {
	
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2016, 12, 22)
	#'start_date': datetime(2016, 12, 22, 0, 37)
	#'start_date': datetime.now() - schedule_interval
	#'start_date' : datetime(2016, 12, 19, 21, 30)
	#'start_date' : datetime.now()
}

dag = DAG(
	dag_id = 'appannie_bash_operator', 
	default_args = args,
	#schedule_interval="30 20 * * *"
	#schedule_interval = @hourly
	#schedule_interval = @daily
	#schedule_interval = None
)

t1 = BashOperator(
	task_id = 'get_json',
	bash_command = SCRIPT TO RUN'python ~/PATH/appannie_appannie_api_data.py',
	dag=dag
)

t2 = BashOperator(
	task_id = 'export_s3',
	bash_command = 'python ~/PATH/export_s3.py',
	dag = dag
	)

t2.set_upstream(t1)