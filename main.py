# Librería para llamadas 
import requests
from dotenv import load_dotenv
import os
import json

# Función encargada de obtener los 
def getPropertiesData(initDate, endDate, minPrice, maxPrice, city):
    url = "https://realtor.p.rapidapi.com/properties/v3/list"
    payload = {
        "limit": 200,
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
        "status": ["sold"],
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
    return response.json()

#def filterPropertiesData()

def main():
    # Se utiliza librería para .env
    load_dotenv()
    f = open('response.json')
    cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    propertiesDataJson = getPropertiesData('2022-09-25','2020-09-25',1000,40000000, cities[1])

    print(len(propertiesDataJson['data']['home_search']['results']))

if __name__ == "__main__":
   main()