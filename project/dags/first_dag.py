try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are ok .......")
except Exception as e:
    print("Error {}".format(e))

def first_function_execute(**context):
    print("First Fucntion Execute")
    variable = kwargs.get("name","Did not get the key")
    return "Hello World " + variable

def second_function_execute(*args, **kwargs):
    variable = kwargs.get("name2","Did not get the key")
    print("Hello World :{}".format(variable))
    return "Hello World " + variable

with DAG(
        dag_id="first_dag", #dag title
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
        op_kwargs={"name" : "Mark Wan",
                   "name2" : "Riyen Tan"}
    )