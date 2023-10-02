from datetime import timedelta,datetime
import json
import os
from dotenv import load_dotenv, find_dotenv
import sys
sys.path.append("/opt/airflow/dags/app")
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_path = os.getcwd()

# Nombre de tabla para propiedades
propertiesTableName = 'properties_hist'
datesValidationTableName = 'properties_dates_validation'

# Se utiliza librería para .env
load_dotenv(find_dotenv())

# Argumentos por defecto para el DAG
# Se utilia timezone de Santiago/Chile
default_args = {
    'owner': 'GustavoH',
    'start_date': datetime(2023,9,29, tzinfo= pytz.timezone('America/Santiago')),
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}

# El dag Realtor_ETL se ejecuta a diario
BC_dag = DAG(
    dag_id='Realtor_ETL',
    default_args=default_args,
    description='Agrega data de propiedades vendidas en EEUU de forma diaria a redshift',
    schedule_interval="@daily",
    catchup=True
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def getData(exec_date):
    from app.functions.properties import getPropertiesData

    # Listado de ciudades a consultar
    cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    #cities = ["New York"]
    # Rango de fechas para las propiedades  
    todayStr = exec_date
    datesRange = [todayStr, todayStr]
    # Rango de precios para las propiedades  
    pricesRange = [1000, 40000000]

    # Cantidad máxima de propiedades que se obtendrían por ciudad
    maxPropertiesNumberByCity = 200
    propertiesData = getPropertiesData(cities, datesRange, pricesRange, maxPropertiesNumberByCity)

    if propertiesData:
        with open(dag_path+'/raw_data/'+"data_"+exec_date+".json", "w") as json_file:
            json.dump(propertiesData, json_file)
            print(f""" Se guardan las propiedas obtenidas en la ruta {dag_path}+'/raw_data/'+"data_"+{exec_date}+".json \n""")
    print(f""" Fin del task 1 exitoso. \n""")

# funcion de transformación de datos
def transformData(exec_date):
    from app.functions.properties import transformPropertiesData

    with open(dag_path+'/raw_data/'+"data_"+exec_date+".json", "r") as json_file:
        loaded_data=json.load(json_file)
        print(f""" Se obtienen las propiedas desde la ruta {dag_path}+'/raw_data/'+"data_"+{exec_date}+".json \n""")

    propertiesTransformed = transformPropertiesData(loaded_data)
    print(f""" Se filtran los datos útiles y se les da formato adecuado. \n""")

    with open(dag_path+'/processed_data/'+"data_"+exec_date+".json", "w") as json_file:
        json.dump(propertiesTransformed, json_file)
        print(f""" Se guardan las propiedas formateadas en la ruta {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")
    print(f""" Fin del task 2 exitoso. \n""")

# funcion de persistencia de datos
def persistData(exec_date):
    from app.functions.properties import validatePropertiesDate, propertiesDataToBD, datesValidationDataToBD
    import pandas as pd

    dateValidationResult = validatePropertiesDate(datesValidationTableName, exec_date)

    if dateValidationResult == 1:
        print("\nNo se guarda ningún registro ya que la API fue consultada hoy")

    else: 
        with open(dag_path+'/processed_data/'+"data_"+exec_date+".json", "r") as json_file:
            loaded_data=json.load(json_file)
            print(f""" Se obtienen las propiedas formateadas desde la ruta {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")

        propertiesDf = pd.DataFrame(loaded_data)
        propertiesDataToBD(propertiesTableName, propertiesDf)
        datesValidationDataToBD(datesValidationTableName, exec_date)
    print(f""" Fin del task 3 exitoso. \n""")


# Tareas
task_1 = PythonOperator(
    task_id='get_data',
    python_callable=getData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d')}}"],
    dag=BC_dag,
)

task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transformData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=BC_dag,
)

task_3 = PythonOperator(
    task_id='persist_data',
    python_callable=persistData,
    op_args=["{{ execution_date.in_timezone('America/Santiago').strftime('%Y-%m-%d') }}"],
    dag=BC_dag,
)
 
# Definicion orden de tareas
task_1 >> task_2 >> task_3