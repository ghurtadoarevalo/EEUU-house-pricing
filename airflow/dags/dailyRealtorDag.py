from datetime import timedelta,datetime
import os
from dotenv import load_dotenv, find_dotenv
import sys
sys.path.append("/opt/airflow/dags/app")
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_path = os.getcwd()

# Se utiliza librería para .env
load_dotenv(find_dotenv())
from app.helpers.emails import sendErrorEmail

# Argumentos por defecto para el DAG
# Se utiliza timezone de Santiago/Chile
default_args = {
    'owner': 'GustavoH',
    'start_date': datetime(2023,10,15, tzinfo= pytz.timezone('America/Santiago')),
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}

# El dag Realtor_ETL se ejecuta a diario
REALTOR_dag = DAG(
    dag_id='Realtor_ETL',
    default_args=default_args,
    description='Agrega data de propiedades vendidas en EEUU de forma diaria a redshift',
    schedule_interval="@daily",
    catchup=True,
    on_failure_callback=sendErrorEmail
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def getData(exec_date):
    from app.functions.properties import getPropertiesData

    # Listado de ciudades a consultar
    #cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    cities = ["New York", "Los Angeles"]
    # Rango de fechas para las propiedades  
    todayStr = exec_date
    datesRange = [todayStr, todayStr]
    # Rango de precios para las propiedades  
    pricesRange = [1000, 40000000]
    # Cantidad máxima de propiedades que se obtendrían por ciudad
    maxPropertiesNumberByCity = 200
    response = getPropertiesData(cities, datesRange, pricesRange, maxPropertiesNumberByCity, dag_path)
    if response:
        print(f""" Fin del task 1 exitoso: {response}\n""")
    else:
        raise Exception(f""" Fin del task 1 fallido: {response}\n""")

# funcion de transformación de datos
def transformData(exec_date):
    from app.functions.properties import transformPropertiesData

    response = transformPropertiesData(exec_date, dag_path)

    if response:
        print(f""" Fin del task 2 exitoso\n""")
    else:
        raise Exception(f""" Fin del task 2 fallido\n""")
    
# funcion para validación de rangos
def tresholdDataValitador(exec_date):
    from app.functions.tresholdValidator import mainValidator

    response = mainValidator(exec_date, dag_path)

    if response:
        print(f""" Fin del task 3 exitoso\n""")
    else:
        raise Exception(f""" Fin del task 3 fallido\n""")
    
# funcion de persistencia de datos
def persistData(exec_date):
    from app.functions.properties import insertNewData
    import pandas as pd

    response = insertNewData(exec_date, dag_path)

    if response:
        print(f""" Fin del task 4 exitoso\n""")
    else:
        raise Exception(f""" Fin del task 4 fallido\n""")

# funcion para envío de alertas de treshold
def sendAlertEmail(exec_date):
    from app.helpers.emails import sendAlertEmail

    response = sendAlertEmail(exec_date, dag_path)

    if response:
        print(f""" Fin del task 5 exitoso\n""")
    else:
        raise Exception(f""" Fin del task 5 fallido\n""")


# Tareas
task_1 = PythonOperator(
    task_id='get_data',
    python_callable=getData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d')}}"],
    dag=REALTOR_dag,
)

task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transformData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=REALTOR_dag,
)

task_3 = PythonOperator(
    task_id='treshold_validator',
    python_callable=tresholdDataValitador,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=REALTOR_dag,
)

task_4 = PythonOperator(
    task_id='persist_data',
    python_callable=persistData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=REALTOR_dag,
)

task_5 = PythonOperator(
    task_id='send_email',
    python_callable=sendAlertEmail,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=REALTOR_dag,
)

 
# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4 >> task_5