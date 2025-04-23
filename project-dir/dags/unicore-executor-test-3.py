import pendulum, os

from airflow.decorators import dag, task
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def unicore_executor_test_3():

    @task()
    def check_version():
        return os.system('curl --unix-socket /var/run/docker.sock http:/v1.35/containers/json')

    @task.docker(image="python:3.9-slim-bookworm", multiple_outputs=True)
    def produce_value():
        print("Hello World! I produced some data to be returned!")
        return {"1" : "A", "2" : "B"}
    
    @task(multiple_outputs=True,executor="UnicoreExecutor")
    def transform_values(data_dict: dict):
        values_as_tuple = (data_dict["1"], data_dict["2"])

        return {"data_tuple": values_as_tuple}

    @task()
    def print_data_tuple(data_tuple: tuple):

        print(f"Data tuple is: {data_tuple}")

    check_version()
    value = produce_value()
    transformed_data = transform_values(value)
    print_data_tuple(transformed_data)

unicore_executor_test_3_dag = unicore_executor_test_3()
