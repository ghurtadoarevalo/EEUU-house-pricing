import os
import requests

# Función encargada de obtener un litado de propiedades por ciudad mediante la API de realtor.
# param | (string) initDate: fecha inicial desde que se desea obtener el listado de propiedades | ex: "2022-09-25"
# param | (string) endDate: fecha final hasta que se desea obtener el listado de propiedades | ex: "2021-09-25"
# param | (int) minPrice: precio mínimo que deben tener las propiedades | ex: 1000 
# param | (int) maxPrice: precio máximo que deben tener las propiedades | ex: 200000
# param | (string) city: ciudad de las propiedades | ex: "Nueva York"
# param | (integer) limit: número máximo de propiedades que se traen por llamado a la API | ex: 10 | default: 5
# returns | (array) Arreglo que contiene una serie de diccionarios con toda la información de las propiedades 
def APIData(initDate, endDate, minPrice, maxPrice, city, limit = 5):
    try:
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
        if(response.status_code == 200):
            responseDict = response.json()
            return responseDict["data"]["home_search"]["results"]
        else:
            raise Exception(response.text)
    except (Exception, requests.exceptions.ConnectionError) as error:
            print("Error: %s" % error)
            exit(1)