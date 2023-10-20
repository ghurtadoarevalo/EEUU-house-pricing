from helpers.utils import formatPropertiesData
from services.realtorAPI import APIData
from db.sqlAlchemyDbAdapter import dbClass as sqlAlchemyDbClass
from db.psycoPgDbAdapter import dbClass as sqlPsycoPgDbClass
import json
import pandas as pd

# Función encargada de obtener y limpiar los datos de las propiedades.
# param | (array) city: listado de ciudades en EEUU  | ex: ["Nueva York","Los Angeles"]
# param | (array) datesRange: rango de fecha inicial y final para obtener el listado de propiedades | ex: ["2021-09-25", "2022-09-25"]
# param | (array) pricesRange: precio mínimo y máximo que pueden tener las propiedades | ex: [1000, 200000]
# param | (integer) limit: número máximo de propiedades que se traen por llamado a la API | ex: 10
# param | (string) dag_path: url base del DAG
# returns | (dataFrame) dataFrame con todas las propiedades
def getPropertiesData(cities, datesRange, pricesRange, maxPropertiesNumberByCity, dag_path):

    citiesData = []
    # Se recorre un listado de ciudades en los que se buscan propiedades dado un rango de fechas y precios
    # Además se filtra y da formato a lo descargado desde la API.
    for city in cities:
        print("Se comienza a obtener las propiedades de la ciudad:", city, "\n")
        propertiesData = APIData(
            datesRange[0], datesRange[1], pricesRange[0], pricesRange[1], city, maxPropertiesNumberByCity)
        print("Se obtuvieron", len(propertiesData), "propiedades de la ciudad:", city, "\n")
        citiesData.append(propertiesData)

    if citiesData:
        with open(dag_path+'/raw_data/'+"data_"+datesRange[0]+".json", "w") as json_file:
            json.dump(citiesData, json_file)
            print(f""" Se guardan las propiedas obtenidas en la ruta {dag_path}+'/raw_data/'+"data_"+{datesRange[0]}+".json \n""")
            return True
    else:
        return False
    

def transformPropertiesData(exec_date, dag_path):

    with open(dag_path+'/raw_data/'+"data_"+exec_date+".json", "r") as json_file:
        citiesData=json.load(json_file)
        print(f""" Se obtienen las propiedas desde la ruta {dag_path}+'/raw_data/'+"data_"+{exec_date}+".json \n""")

    citiesDataFormated = []
    for cityData in citiesData:
        propertyDataFormated = formatPropertiesData(cityData)
        citiesDataFormated.append(propertyDataFormated)
    # Se deja una lista de listas de diccionarios en una única lista 
    flattened_list_of_dicts = [item for sublist in citiesDataFormated for item in sublist]

    if flattened_list_of_dicts:
        with open(dag_path+'/processed_data/'+"data_"+exec_date+".json", "w") as json_file:
            json.dump(flattened_list_of_dicts, json_file)
            print(f""" Se guardan las propiedas formateadas en la ruta {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")
            print(f""" Se filtran los datos útiles y se les da formato adecuado. \n""")

            return True
    else:
        print(f""" No se obtuvieron propiedades, el task anterior. \n""")
        return False


def insertNewData(exec_date, dag_path):

    # Nombre de tabla para propiedades
    propertiesTableName = 'properties_hist'
    # Nombre de tabla para validaciones
    datesValidationTableName = 'properties_dates_validation'

    dateValidationResult = validatePropertiesDate(datesValidationTableName, exec_date)

    if dateValidationResult == 1:
        print("\nNo se guarda ningún registro ya que la API fue consultada hoy")
        return False

    else: 
        with open(dag_path+'/processed_data/'+"data_"+exec_date+".json", "r") as json_file:
            loaded_data=json.load(json_file)
            print(f""" Se obtienen las propiedas formateadas desde la ruta {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")

        propertiesDf = pd.DataFrame(loaded_data)
        propertiesDataToBD(propertiesTableName, propertiesDf)
        datesValidationDataToBD(datesValidationTableName, exec_date)
        return True


def propertiesDataToBD(propertiesTableName, propertiesDf):
    # Se crea el cliente de la BD
    dbAdapterClass = sqlPsycoPgDbClass()

    print('Generando conexión a la BD para cargar la data de las propiedades \n')
    # Se crea la tabla en la BD según el nombre de la tabla
    print('Se crea la tabla de propiedades si no existe \n')
    dbAdapterClass.createPropertiesTable(tableName=propertiesTableName)

    print('Se insertan los datos de propiedas en la BD \n')
    dbAdapterClass.insertToBd(propertiesTableName, propertiesDf)    
    print('Cierre conexión a BD\n')
    dbAdapterClass.endConnection()


def datesValidationDataToBD(tableName, datesRange):
    dbAdapterClass = sqlPsycoPgDbClass()

    print('Generando conexión a la BD para cargar la data de las fechas de validación \n')

     # Se crea la tabla en la BD según el nombre de la tabla
    print('Se crea la tabla de fechas de validación si no existe \n')
    dbAdapterClass.createDateValidationTable(tableName=tableName)

    dateDf = pd.DataFrame([datesRange], columns=['read_date'])

    print('Se inserta la fecha en la tabla de validación en la BD \n')
    dbAdapterClass.insertToBd(tableName, dateDf)    
    
    print('\nCierre conexión a BD\n')
    dbAdapterClass.endConnection()
    
def validatePropertiesDate(tableName, datesRange):
    # Se crea el cliente de la BD
    dbAdapterClass = sqlPsycoPgDbClass()
    tableExist = dbAdapterClass.verifyTableExist(tableName)
    if tableExist:
        return dbAdapterClass.dateValidation(tableName, datesRange)
    else:
        return 0
