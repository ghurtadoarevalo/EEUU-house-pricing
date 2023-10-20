
from config.alerts import alertsConfig
import json

def propertyValueValidator(propertyValue):
    if propertyValue > alertsConfig['propertyValue']['maxValue']:
        return True
    elif propertyValue < alertsConfig['propertyValue']['minValue']:
        return True
    else:
        return False

def propertyTypeValidator(propertyType):
    if propertyType not in alertsConfig['aceptablePropertyType']:
        return True
    else:
        return False
    
def cityMonitorValidator(city):
    if city not in alertsConfig['aceptableCities']:
        return True
    else:
        return False
    
def mainValidator(exec_date, dag_path):

    with open(dag_path+'/processed_data/'+"data_"+exec_date+".json", "r") as json_file:
        propertiesData=json.load(json_file)
        print(f""" Se obtienen las propiedas desde la ruta {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")

    alerts = []
    
    for propertyData in propertiesData:
        if cityMonitorValidator(propertyData['city']):
            alertCityBody = {
                "variableName": 'city',
                "variableTreshold": ', '.join([str(city) for city in alertsConfig['aceptableCities']]),
                "variableValue": propertyData['city'],
                "propertyId": propertyData['property_id'],
                "propertyDate": propertyData['last_sold_date']
            }
            alerts.append(alertCityBody)


        if propertyTypeValidator(propertyData['type']):
            alertTypeBody = {
                "variableName": 'type',
                "variableTreshold": ', '.join([str(city) for city in alertsConfig['aceptablePropertyType']]),
                "variableValue": propertyData['type'],
                "propertyId": propertyData['property_id'],
                "propertyDate": propertyData['last_sold_date']

            }
            alerts.append(alertTypeBody)

        if propertyValueValidator(propertyData['list_price']):
            alertPriceBody = {
                "variableName": 'list_price',
                "variableTreshold": f"""Max: {alertsConfig['propertyValue']['maxValue']} - Min: {alertsConfig['propertyValue']['minValue']} """,
                "variableValue": propertyData['list_price'],
                "propertyId": propertyData['property_id'],
                "propertyDate": propertyData['last_sold_date']

            }
            alerts.append(alertPriceBody)

    with open(dag_path+'/error_data/'+"data_"+exec_date+".json", "w") as json_file:
            json.dump(alerts, json_file)
            print(f""" Se guardan las propiedas con error en la ruta {dag_path}+'/raw_data/'+"data_"+{exec_date}+".json \n""")
            return True