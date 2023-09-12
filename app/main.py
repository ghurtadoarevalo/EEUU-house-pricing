# Librería para llamadas
from dotenv import load_dotenv
from functions.properties import propertiesDataToBD, getPropertiesData, datesValidationDataToBD, validatePropertiesDate
from db.psycoPgDbAdapter import dbClass as sqlPsycoPgDbClass
from datetime import date


def main():
    print('\nIniciando programa... \n')

    # Se utiliza librería para .env
    load_dotenv()

    # Nombre de tabla para propiedades
    propertiesTableName = 'properties_hist'

    datesValidationTableName = 'properties_dates_validation'

    # Listado de ciudades a consultar
    #cities = ["Nueva York", "Los Angeles", "Chicago", "Houston", "Miami",
    #          "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
    
    cities = ["New York"]
    # Rango de fechas para las propiedades  
    todayStr = date.today().strftime('%Y-%m-%d')
    datesRange = ['2021-08-24', todayStr]
    
    # Rango de precios para las propiedades  
    pricesRange = [1000, 40000000]

    maxPropertiesNumberByCity = 1000

    dateValidationResult = validatePropertiesDate(datesValidationTableName, datesRange)

    print("\nDe las propiedades que se traigan, sólo se van a almacenar las que su last_sold_date, esté fuera del rango", dateValidationResult, "\n")

    # Se llama a la función que crea la BD si no existe, obtiene los datos de las propiedades y luego los inserta en la BD
    propertiesData = getPropertiesData(cities, datesRange, pricesRange, maxPropertiesNumberByCity)
    
    # En caso de que no haya nada guardado en la BD, entonces se guarda todo lo que venga por primera vez
    if dateValidationResult["initDate"] == False and dateValidationResult["endDate"] == False:
        propertiesDataToBD(propertiesTableName, propertiesData)
        datesValidationDataToBD(datesValidationTableName, datesRange)
        print("\nSe almacenan los datos de las propiedades y los fechas de validación")

    # En caso de que ya existan registros, estos se filtran por la fecha
    # dejando sólo aquellos que queden fuera del rango de solicitudes anteriores
    else:
         # Se filtran las propiedades que no estén en el rango de validación
        dataFilter = None
        if dateValidationResult["initDate"] != None and dateValidationResult["endDate"] != None:
            dataFilter = (propertiesData['last_sold_date'] < dateValidationResult["initDate"]) | (propertiesData['last_sold_date'] > dateValidationResult["endDate"])
        elif dateValidationResult["initDate"] != None and dateValidationResult["endDate"] == None:
            dataFilter = (propertiesData['last_sold_date'] < dateValidationResult["initDate"])
        elif dateValidationResult["initDate"] == None and dateValidationResult["endDate"] != None:
            dataFilter = (propertiesData['last_sold_date'] > dateValidationResult["endDate"])
        
        if dataFilter is not None:
            filteredPropertiesData = propertiesData[dataFilter]
            print("\nEl dataframe filtrado es:")
            print(filteredPropertiesData)
            propertiesDataToBD(propertiesTableName, filteredPropertiesData)
            datesValidationDataToBD(datesValidationTableName, datesRange)
            print("\nSe almacenan los datos de las propiedades y los fechas de validación")

        else:
            print("\nNo se guarda ningún registro ya que se consultaron los que están en un rango de fechas ya obtenido", datesRange )

if __name__ == "__main__":
    main()
