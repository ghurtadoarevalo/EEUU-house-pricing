from dictionaries.propertiesDict import propertiesDictionary
import pandas as pd
import numpy as np

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

# Create a list of tupples from the dataframe values
def dfToTuples(dataFrame):
    dataFrameClean = dataFrame.replace({np.nan: None})
    tuples = [tuple(x) for x in dataFrameClean.values]
    return tuples

