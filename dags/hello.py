from airflow.decorators import dag, task 
from datetime import datetime 

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False) 
def hello(): 

    @task 
    def test(): 
        print("Hola Gonorreas!!!") 

    test() 

hello()