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
    # Se recorre un listado de ciudades en los que se buscan propiedades dado un rango de fechas y precios
    # Además se filtra y da formato a lo descargado desde la API.
    for city in cities:
        print("Se comienza a obtener las propiedades de la ciudad:", city, "\n")
        propertiesData = APIData(
            datesRange[0], datesRange[1], pricesRange[0], pricesRange[1], city, maxPropertiesNumberByCity)
        print("Se obtuvieron", len(propertiesData), "propiedades de la ciudad:", city, "\n")
        citiesData.append(propertiesData)
    return citiesData

def transformPropertiesData(citiesData):
    citiesDataFormated = []
    for cityData in citiesData:
        propertyDataFormated = formatPropertiesData(cityData)
        citiesDataFormated.append(propertyDataFormated)
    # Se deja una lista de listas de diccionarios en una única lista 
    flattened_list_of_dicts = [item for sublist in citiesDataFormated for item in sublist]
    return flattened_list_of_dicts

def propertiesDataToBD(tableName, propertiesData):
    # Se crea el cliente de la BD
    dbAdapterClass = sqlPsycoPgDbClass()

    print('Generando conexión a la BD para cargar la data de las propiedades \n')
    # Se crea la tabla en la BD según el nombre de la tabla
    print('Se crea la tabla de propiedades si no existe \n')
    dbAdapterClass.createPropertiesTable(tableName=tableName)

    print('Se insertan los datos de propiedas en la BD \n')
    dbAdapterClass.insertToBd(tableName, propertiesData)    
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
