# Librería para llamadas
import requests
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from propertiesDict import propertiesDictionary
from dbAdapter import dbClass
import pandas as pd

# Función encargada de obtener un litado de propiedades por ciudad mediante la API de realtor.
# param | (string) initDate: fecha inicial desde que se desea obtener el listado de propiedades | ex: "2022-09-25"
# param | (string) endDate: fecha final hasta que se desea obtener el listado de propiedades | ex: "2021-09-25"
# param | (int) minPrice: precio mínimo que deben tener las propiedades | ex: 1000 
# param | (int) maxPrice: precio máximo que deben tener las propiedades | ex: 200000
# param | (string) city: ciudad de las propiedades | ex: "Nueva York"
# param | (integer) limit: número máximo de propiedades que se traen por llamado a la API | ex: 10 | default: 5
# returns | (array) Arreglo que contiene una serie de diccionarios con toda la información de las propiedades 
def getPropertiesData(initDate, endDate, minPrice, maxPrice, city, limit = 5):
    url = "https://realtor.p.rapidapi.com/properties/v3/list"
    payload = {
        "limit": limit,
        "offset": 0,
        "city": city,
        "sold_date": {
            "max": endDate,
            "min": initDate
        },
        "sold_price": {
            "max": maxPrice,
            "min": minPrice
        },
        "status": ["for_rent", "sold", "for_sale", "ready_to_build"],
        "sort": {
            "direction": "desc",
            "field": "list_date"
        }
    }
    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": os.getenv('RAPID_API_KEY'),
        "X-RapidAPI-Host": os.getenv('RAPID_API_HOST')
    }

    response = requests.post(url, json=payload, headers=headers)
    responseDict = response.json()
    return responseDict["data"]["home_search"]["results"]


""" Esto se le puede aplicar más adelante a formatPropertiesData para que se respeten ciertas reglas del diccionario     

def applyRules(rules, data):
    if (data == None or data == ''):
        return None
    else:
        return data
"""

# Función encargada de obtener sólo los campos que son de importancia para las propiedades y 
# darles un formato según un diccionario específico (propertiesDictionary).
# param | (array) propertiesData: Arreglo que contiene una serie de diccionarios con toda la información de las propiedades
# returns | (array) Arreglo que contiene una serie de diccionarios con toda la información de las propiedades pero filtradas y formateadas
def formatPropertiesData(propertiesData):
    propertiesDataFormated = []
    for propertyData in propertiesData:
        formatedDict = {}
        for key in propertiesDictionary:
            if key == "location":
                for subKey in propertiesDictionary[key]:
                    for subSubKey in propertiesDictionary[key][subKey]:
                        formatedDict[subSubKey] = {}
                        if (propertiesDictionary[key][subKey][subSubKey]["target"] in propertyData[key][subKey]):
                            formatedDict[subSubKey] = propertyData[key][subKey][subSubKey]
            elif key == "description":
                for subKey in propertiesDictionary[key]:
                    formatedDict[subKey] = {}
                    if (propertiesDictionary[key][subKey]["target"] in propertyData[key]):
                        formatedDict[subKey] = propertyData[key][subKey]
            else:
                if (propertiesDictionary[key]["target"] in propertyData):
                    formatedDict[key] = propertyData[key]
        propertiesDataFormated.append(formatedDict)
    return propertiesDataFormated

def main():
    print('\nIniciando programa... \n')

    # Se utiliza librería para .env
    load_dotenv()

    # Se crea el cliente de la BD
    dbAdapterClass = dbClass()

    print('Generando conexión a la BD \n')

    # Se crea la tabla en la BD según el nombre de la tabla
    dbAdapterClass.createPropertiesTable(tableName='properties_hist')
    print('Creando tabla de la BD \n')

    # Listado de ciudades a consultar
    cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami",
              "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    
    citiesData = []

    print('Comenzando la conexión a la API \n')

    # Se recorre un listado de ciudades en los que se buscan propiedades dado un rango de fechas y precios
    # Además se filtra y da formato a lo descargado desde la API.
    for city in cities:
        propertiesData = getPropertiesData(
            '2021-09-25', '2022-09-25', 1000, 40000000, city, 10)
        propertiesDataFormated = formatPropertiesData(propertiesData)
        citiesData.append(propertiesDataFormated)
    print('Terminando la conexión a la API y transformación de los datos\n')

    # Se deja una lista de listas de diccionarios en una única lista que luego es pasada a un DF
    flattened_list_of_dicts = [item for sublist in citiesData for item in sublist]
    propertiesDf = pd.DataFrame(flattened_list_of_dicts)
    print(propertiesDf)
    
    
    #propertiesDf.to_sql('properties_hist', schema='ghurtadoarevalo_94_coderhouse' ,con=dbAdapterClass.getEngine(), index=False, if_exists='replace')    
    print('\nCierre conexión a BD\n')
    dbAdapterClass.endConnection()

    print('Fin del programa\n')
    exit(1)

if __name__ == "__main__":
    main()
