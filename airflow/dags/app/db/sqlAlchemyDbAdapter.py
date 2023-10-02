import sqlalchemy as db
import os
from sqlalchemy.engine.url import URL
from datetime import datetime

# Clase de la base de datos mediante sqlalchemy. Se encarga de generar la conexión a la BD y 
# se irán agregando funciones a medida que vaya creciendo el proyecto
class dbClass:
    def __init__(self):
        self.database=os.getenv("DB_NAME")
        self.username=os.getenv("DB_USERNAME")
        self.schema=os.getenv("DB_USERNAME")
        self.password=os.getenv("DB_PASSWORD")
        self.host=os.getenv("DB_CONNECTION")
        self.dbEngine = self.createDBEngine()
        self.dbMetadata = self.createMetadata()
        

    def createDBEngine(self):
        try:
            url = URL.create(
                drivername='redshift+redshift_connector', # redshift_connector driver y dialect 
                host= self.host, # Amazon Redshift host
                port=5439, # Amazon Redshift port
                database= self.database, # Amazon Redshift database
                username=self.username, # Amazon Redshift username
                password= self.password # Amazon Redshift password
            )
            dbEngine = db.create_engine(url)
            return dbEngine 
        except Exception as e:
            print(f"Error creating database connection: {e}")

    def createMetadata(self):
        try:
            dbMetadata = db.MetaData()
            return dbMetadata
        except Exception as e:
            print(f"Error creating metadata: {e}")

    def verifyTableExist(self, tableName):
        inspector = db.inspect(self.dbEngine)
        if inspector.has_table(tableName):
            return True
        else:
            return False

    def getEngine(self):
        return self.dbEngine
    
    def endConnection(self):
        self.dbEngine.dispose()

    def createPropertiesTable(self, tableName):
        try:
            if not self.verifyTableExist(tableName):
                db.Table(
                    tableName,                                        
                    self.dbMetadata,
                    db.Column('property_id', db.String, primary_key=True),
                    db.Column('status', db.String),
                    db.Column('country', db.String),
                    db.Column('state', db.String),
                    db.Column('type', db.String),
                    db.Column('beds', db.Integer),
                    db.Column('lot_sqft', db.Integer),
                    db.Column('sqft', db.Integer),
                    db.Column('city', db.String),
                    db.Column('list_price', db.Integer),
                    db.Column('last_sold_date', db.Date, primary_key=True),
                    db.Column('created_on', db.DateTime(), default=datetime.now),
                    db.Column('updated_on', db.DateTime(), default=datetime.now, onupdate=datetime.now)    
                )
                self.dbMetadata.create_all(self.dbEngine)
                print('La tabla de nombre ' + tableName + ' se ha creado correctamente\n')
            else:
                print('La tabla de nombre ' + tableName + ' ya existe\n')

        except Exception as e:
            print(f"Error creating table: {e}")

    def insertPropertiesToBd(self, tableName, dataFrame):
        try:
            dataFrame.to_sql(tableName, schema=self.schema ,con=self.dbEngine, index = False, if_exists='append')    
        except Exception as e:
            print(f"Error inserting in table: {e}")


