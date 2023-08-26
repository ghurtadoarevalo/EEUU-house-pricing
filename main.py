# Librería para llamadas
import requests
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from propertiesDict import propertiesDictionary

# Función encargada de obtener un litado de propiedades por ciudad mediante la API de realtor.
# param | (string) initDate: fecha inicial desde que se desea obtener el listado de propiedades | ex: "2022-09-25"
# param | (string) endDate: fecha final hasta que se desea obtener el listado de propiedades | ex: "2021-09-25"
# param | (int) minPrice: precio mínimo que deben tener las propiedades | ex: 1000 
# param | (int) maxPrice: precio máximo que deben tener las propiedades | ex: 200000
# param | (string) city: ciudad de las propiedades | ex: "Nueva York"
# returns | (array) Arreglo que contiene una serie de diccionarios con toda la información de las propiedades 
def getPropertiesData(initDate, endDate, minPrice, maxPrice, city):
    url = "https://realtor.p.rapidapi.com/properties/v3/list"
    payload = {
        "limit": 100,
        "offset": 0,
        "city": city,
        "sold_date": {
            "max": initDate,
            "min": endDate
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
                formatedDict[key] = {}
                for subKey in propertiesDictionary[key]:
                    formatedDict[key][subKey] = {}
                    for subSubKey in propertiesDictionary[key][subKey]:
                        formatedDict[key][subKey][subSubKey] = {}
                        if (propertiesDictionary[key][subKey][subSubKey]["target"] in propertyData[key][subKey]):
                            formatedDict[key][subKey][subSubKey] = propertyData[key][subKey][subSubKey]
            elif key == "description":
                formatedDict[key] = {}
                for subKey in propertiesDictionary[key]:
                    formatedDict[key][subKey] = {}
                    if (propertiesDictionary[key][subKey]["target"] in propertyData[key]):
                        formatedDict[key][subKey] = propertyData[key][subKey]
            else:
                if (propertiesDictionary[key]["target"] in propertyData):
                    formatedDict[key] = propertyData[key]
        propertiesDataFormated.append(formatedDict)
    return propertiesDataFormated

"""
def plotPricesByCity(citiesName, citiesData):
    for i, current_city in enumerate(citiesName):
        plt.subplot(len(citiesName), 1, i+1)
        current_city.history(period='365d')['Close'].plot(figsize=(
            16, 60), title='Precio historico de 1 de un año para: ' + current_city)
"""

def main():
    # Se utiliza librería para .env
    load_dotenv()

    # Listado de ciudades a consultar
    cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami",
              "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    
    citiesData = []

    for city in cities:
        propertiesData = getPropertiesData(
            '2022-09-25', '2021-09-25', 1000, 40000000, city)
        propertiesDataFormated = formatPropertiesData(propertiesData)
        citiesData.append(propertiesDataFormated)



if __name__ == "__main__":
    main()
