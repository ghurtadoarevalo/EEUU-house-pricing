# proyectoCH

# Realtor API Property Viewer

![GitHub License](https://img.shields.io/github/license/yghurtadoarevalo/realtor-api-property-viewer)
![Python Version](https://img.shields.io/badge/python-%3E%3D3.6-blue)
![Last Commit](https://img.shields.io/github/last-commit/ghurtadoarevalo/realtor-api-property-viewer)

Este proyecto es una herramienta para obtener y en un futuro visualizar información detallada de propiedades inmobiliarias a través de la API de Realtor. De momento sólo hace la conexión a la API, pero en un futuro podría proporcionar una forma conveniente de acceder a los datos de diferentes propiedades y mostrarlos en un formato legible, pero más importante aún, obtener información relevante de los datos que serán almacenados en un datawarehouse. 

## Características

- Recupera datos de propiedades de la API de Realtor.
- Muestra información relevante en un formato estructurado (futuro).
- Permite visualizar detalles como estado, ubicación, descripción, precio de lista y más (futuro).
- Permite obtener información relevante acerca de de las propiedades según precios, ciudad, tamaño de la propiedad, etc.


## Instalación

1. Clona este repositorio en tu máquina local:

   ```bash
   git clone https://github.com/ghurtadoarevalo/proyectoCH.git
   ```

2. Accede al directorio del proyecto:

   ```bash
   cd proyectoCH
   ```

3. Instala las dependencias utilizando pip (asegúrate de tener Python 3.6 o superior):

   ```bash
   pip install -r requirements.txt
   ```

## Uso

1. Obtén una clave de API de Realtor https://rapidapi.com/apidojo/api/realty-in-us/ y dejarlas en un .env con el formato:
    
    ```plaintext
    RAPID_API_KEY = INSERTAR API KEY
    RAPID_API_HOST = INSERTAR HOST 
    ```

2. Ejecuta el script principal para obtener información de propiedades:

   ```bash
   python main.py
   ```

## Estructura del Código

- `main.py`: El punto de entrada del programa. Genera la llamada a la API y procesa los datos obtenidos.
- `propertiesDictionary.py`: Tiene el diccionario con el que se formatean los datos obtenidos mediante la API de Realtor. 
- `.env`: Archivo de configuración para almacenar la clave y host de API de Realtor.

## Contribuciones

Las contribuciones son bienvenidas y agradecidas. Si encuentras un error, tienes una idea para una mejora o quieres agregar una nueva característica, siéntete libre de abrir un issue o enviar un pull request.

## Licencia

Este proyecto está bajo la Licencia [MIT](LICENSE).

---