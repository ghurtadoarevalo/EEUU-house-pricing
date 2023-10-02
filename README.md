# EEUU House pricing

![GitHub License](https://img.shields.io/github/license/ghurtadoarevalo/proyectoCH)
![Python Version](https://img.shields.io/badge/python-%3E%3D3.6-blue)
![Last Commit](https://img.shields.io/github/last-commit/ghurtadoarevalo/proyectoCH)

La base de este proyecto nace al ver que en EE.UU los precios de las casas y departamentos al año 2023 se han disparado. Altas tasas de interés y una economía convulsionada dan pie a precios poco accesibles para los norte americanos. Considerando lo anterior, este proyecto busca identificar si estas alzas son significativas en todo el país, si se ven reflejados en los arriendos, cuánto ha sido el aumento en base a los años, entre otros índices que podrían ser interesantes de analizar.

Este proyecto es una herramienta para obtener y en un futuro visualizar información detallada de propiedades inmobiliarias a través de la API de Realtor. De momento sólo hace la conexión a la API, pero en un futuro podría proporcionar una forma conveniente de acceder a los datos de diferentes propiedades y mostrarlos en un formato legible, pero más importante aún, obtener información relevante de los datos que serán almacenados en un datawarehouse. 

## Características

- Recupera datos de propiedades de la API de Realtor.
- Se filtran y transforman sólo los datos que son relevantes.
- Se persisten los datos sólo si no se ha consultado anteriormente durante el día.

## Requisitos

1. Docker desktop

2. Docker compose

## Instalación

1. Clona este repositorio en tu máquina local:

   ```bash
   git clone https://github.com/ghurtadoarevalo/proyectoCH.git
   ```

2. Accede al directorio del proyecto:

   ```bash
   cd proyectoCH
   ```

## Uso

1. Generar un archivo .env dentro de proyectoCH/airflow/dags

2. Obtén una clave de API de Realtor https://rapidapi.com/apidojo/api/realty-in-us/ y dejarlas en el .env con el formato:
    
    ```plaintext
    RAPID_API_KEY = INSERTAR API KEY
    RAPID_API_HOST = INSERTAR HOST 
    ```

3. Ingresar credenciales de la BD en el archivo .env con los siguientes secretos:

   ```plaintext
   DB_USERNAME= NOMBRE DE USUARIO
   DB_PASSWORD= PASSWORD DE LA BD
   DB_CONNECTION= URL DE CONEXIÓN A LA BD
   DB_NAME= NOMBRE DE LA BD
   ```

4. Se ejecuta el docker-compose que levantará las imágenes necesarias para airflow e instalará las dependencias del programa desde requirements.txt

   ```bash
   docker-compose up
   ```

5. Ingresar a localhost:8080 e ingresar con usuario airflow y contraseña airflow

6. Hacer click en el switch que se encuentra al lado izquierdo de Realtor_ETL. Con eso quedará encolada la ejecución del DAG.

7. Si no se quisiera esperar al tiempo de ejecución automático, entonces debajo de Actions presionar el botón de play y luego "Trigger DAG".

8. Los Task pueden ser monitoreados presionando debajo de Links y luego haciendo click en Graphs.

9. Los secretos utilizados para este código fueron entregados en los comentarios de la entrega dentro de la plataforma de Coder House

## Estructura del Código

- `docker-compose.yml`: Archivo de docker compose que levantará los contenedores de Docker para Airflow y las dependencias del DAG.
- `Dockerfile`: Archivo Dockerfile utilizado en docker-compose que permite traer la imagen de airflow e instalar las dependencias del DAG que se encuentra en requirements.txt
- `requirements.txt`: Dependencias necesarias para el correcto funcionamiento del DAG.
- `airflow/dags/app/functions/properties.py`: Documento que contiene una serie de funciones para obtener las propiedades y darle formato al DF, así como otras para validar fechas en las que se han obtenido registros.
- `airflow/dags/app/service/realtorAPI.py`: Servicio que genera la conexión a la API de Realtor desde la cual se obtienen los registros de propiedades.
- `airflow/dags/app/dictionaries/propertiesDictionary.py`: Tiene el diccionario con el que se formatean los datos obtenidos mediante la API de Realtor. 
- `airflow/dags/app/db/psycoPgDbAdapter.py`: Contiene la clase que permite generar la conexión a la BD redshift con psycoPgDbAdapter. 
- `airflow/dags/app/db/sqlAlchemyDbAdapter.py`: Contiene la clase que permite generar la conexión a la BD redshift con sqlAlchemyDbAdapter [DEPRECATED]. 
- `airflow/dags/app/helpers/utils.py`: Contiene funciones que son de utilidad para diferentes partes del programa.
- `airflow/dags/app/logs`: Contiene los logs de airflow.
- `airflow/dags/app/plugins`: Contiene los plugins que podrían agregarse a airflow.
- `airflow/dags/app/raw_data`: Contiene los json con la data raw de las propiedades obtenidas.
- `airflow/dags/app/processed_data`: Contiene los json con la data procesada de las propiedades obtenidas.
- `airflow/dags/.env`: Archivo de configuración para almacenar la clave y host de API de Realtor, así como las configuraciones de la BD.

## Contribuciones

Las contribuciones son bienvenidas y agradecidas. Si encuentras un error, tienes una idea para una mejora o quieres agregar una nueva característica, siéntete libre de abrir un issue o enviar un pull request.

## Licencia

Este proyecto está bajo la Licencia [MIT](LICENSE).

---