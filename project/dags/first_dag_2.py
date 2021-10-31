try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are ok .......")
except Exception as e:
    print("Error {}".format(e))

def first_function_execute(**context):
    print("first_function_execute")
    context['ti'].xcom_push(key='mykey', value="Came from first_execute")

def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    # kwargsVar = kwargs.get("name","Did not get the key")
    print("I am in second_function_execute got vaalue :{} from Function 1" .format(instance))
    # print("I can still get kwargs arg: {}".format(kwargsVar))

with DAG(
        dag_id="first_dag_2", #dag title
        schedule_interval="@daily",
        default_args ={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2020,11,1),
        },
        #used to catch up from start date to current date
        catchup=False) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name" : "Mark Wan",
                   "name2" : "Riyen Tan"}
    )
    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True
    )
first_function_execute>>second_function_execute