from helpers.utils import formatPropertiesData
from services.realtorAPI import APIData
from db.sqlAlchemyDbAdapter import dbClass as sqlAlchemyDbClass
from db.psycoPgDbAdapter import dbClass as sqlPsycoPgDbClass

import pandas as pd

# Función encargada de obtener y limpiar los datos de las propiedades.
# param | (array) city: listado de ciudades en EEUU  | ex: ["Nueva York","Los Angeles"]
# param | (array) datesRange: rango de fecha inicial y final para obtener el listado de propiedades | ex: ["2021-09-25", "2022-09-25"]
# param | (array) pricesRange: precio mínimo y máximo que pueden tener las propiedades | ex: [1000, 200000]
# param | (integer) limit: número máximo de propiedades que se traen por llamado a la API | ex: 10
# returns | (dataFrame) dataFrame con todas las propiedades
def getPropertiesData(cities, datesRange, pricesRange, maxPropertiesNumberByCity):

    citiesData = []

    print('Comenzando la conexión a la API \n')

    # Se recorre un listado de ciudades en los que se buscan propiedades dado un rango de fechas y precios
    # Además se filtra y da formato a lo descargado desde la API.
    for city in cities:
        propertiesData = APIData(
            datesRange[0], datesRange[1], pricesRange[0], pricesRange[1], city, maxPropertiesNumberByCity)
        propertiesDataFormated = formatPropertiesData(propertiesData)
        citiesData.append(propertiesDataFormated)
    print('Terminando la conexión a la API y transformación de los datos\n')

    # Se deja una lista de listas de diccionarios en una única lista que luego es pasada a un DF
    flattened_list_of_dicts = [item for sublist in citiesData for item in sublist]
    propertiesDf = pd.DataFrame(flattened_list_of_dicts)
    print(propertiesDf)

    return propertiesDf

def propertiesDataToBD(tableName, propertiesData):
    # Se crea el cliente de la BD
    dbAdapterClass = sqlPsycoPgDbClass()

    print('Generando conexión a la BD \n')
    # Se crea la tabla en la BD según el nombre de la tabla
    dbAdapterClass.createPropertiesTable(tableName=tableName)
    print('Creando tabla de la BD \n')

    dbAdapterClass.insertToBd(tableName, propertiesData)    
    print('\nCierre conexión a BD\n')
    dbAdapterClass.endConnection()


def datesValidationDataToBD(tableName, datesRange):
    dbAdapterClass = sqlPsycoPgDbClass()

     # Se crea la tabla en la BD según el nombre de la tabla
    dbAdapterClass.createDateValidationTable(tableName=tableName)

    dateDf = pd.DataFrame([datesRange], columns=['init_date','end_date'])

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
        return {"initDate": False, "endDate": False}
