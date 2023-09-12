import os
import psycopg2
import psycopg2.extras as extras
from datetime import date

from helpers.utils import dfToTuples
# Clase de la base de datos mediante psycopg2. Se encarga de generar la conexión a la BD y 
# se irán agregando funciones a medida que vaya creciendo el proyecto
class dbClass:
    def __init__(self):
        self.database = os.getenv("DB_NAME")
        self.username = os.getenv("DB_USERNAME")
        self.schema = os.getenv("DB_SCHEMA") 
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_CONNECTION")
        self.port = os.getenv("DB_PORT")
        self.conn = self.createDBConnection()
        self.cur = self.conn.cursor()

    def createDBConnection(self):
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(f"Error creating database connection: {e}")

    def verifyTableExist(self, tableName):
        try:
            self.cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);", (self.schema, tableName))
            return self.cur.fetchone()[0]
        except Exception as e:
            print(f"Error verifying table existence: {e}")

    def getCursor(self):
        return self.cur

    def endConnection(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")

    def createTable(self, tableName, createTableQuery):
        try:
            if not self.verifyTableExist(tableName):
                self.cur.execute(createTableQuery)
                self.conn.commit()
                print(f'The table with the name {tableName} has been created successfully\n')
            else:
                print(f'The table with the name {tableName} already exists\n')
        except Exception as e:
            print(f"Error creating table: {e}")
            exit(1)

    def createDatesTable(self, tableName, createTableQuery):
        try:
            if not self.verifyTableExist(tableName):
                self.cur.execute(createTableQuery)
                self.conn.commit()
                print(f'The table with the name {tableName} has been created successfully\n')
            else:
                print(f'The table with the name {tableName} already exists\n')
        except Exception as e:
            print(f"Error creating table: {e}")
            exit(1)

    def executeReadQuery(self, query):
        cursor = self.cur
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error '{e}' ha ocurrido")

    def insertToBd(self, tableName, dataFrame):
        tuples = dfToTuples(dataFrame)
        # Comma-separated dataframe columns
        cols = ','.join(list(dataFrame.columns))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES %%s" % (tableName, cols)
        cursor = self.cur
        try:
            extras.execute_values(cursor, query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        print("execute_values() done")
        cursor.close()

    def createPropertiesTable(self, tableName):
        createTableQuery = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{tableName} (
                property_id VARCHAR,
                last_sold_date DATE,
                status VARCHAR,
                country VARCHAR,
                state VARCHAR,
                type VARCHAR,
                beds INT,
                lot_sqft INT,
                sqft INT,
                city VARCHAR,
                list_price INT,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (property_id, last_sold_date)
            );
            """
        self.createTable(tableName, createTableQuery)

    def createDateValidationTable(self, tableName):
        createTableQuery = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{tableName} (
            date_id INT IDENTITY(0,1),
            init_date DATE,
            end_date DATE,
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_id)
        );
        """
        self.createTable(tableName, createTableQuery)

    def dateValidation(self, tableName, datesRange):

        initDateStr = datesRange[0]
        endDateStr = datesRange[1]

        selectQuery = f"""
            	SELECT init_date , end_date, initDaysDiff, null as endDaysDiff FROM
                (SELECT top 1 init_date, end_date, datediff(days, '{initDateStr}', init_date) as initDaysDiff
                FROM {self.schema}.{tableName} where initDaysDiff > 0 ORDER BY initDaysDiff asc) 
                UNION
                SELECT init_date , end_date, null as initDaysDiff, endDaysDiff  FROM
                (SELECT top 1 init_date, end_date, datediff(days, end_date, '{endDateStr}') as endDaysDiff
                FROM {self.schema}.{tableName} where endDaysDiff > 0 ORDER BY endDaysDiff asc );
        """

        # Con esta Query se obtienen 2 registros, 1 para el filtro del init date y otro para el de end date
        # La lógica es que se busca en la tabla properties_dates_validation aquellos registros
        # Que tenga una menor diferencia de días con initDate y endDate entregado por el usuario
        # De esta manera, luego se filtran aquellas fechas fuera de la validación
        queryResult = self.executeReadQuery(selectQuery)

        validationDatesRange = {"initDate": None, "endDate": None}
        for dates in queryResult:
            if dates[3] == None:
                initDateStr = dates[0].strftime('%Y-%m-%d')
                validationDatesRange["initDate"] = initDateStr
            elif dates[2] == None:
                endDateStr = dates[1].strftime('%Y-%m-%d')
                validationDatesRange["endDate"] = endDateStr
        return validationDatesRange
