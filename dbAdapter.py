import sqlalchemy as db
import os
from sqlalchemy.engine.url import URL

# Clase de la base de datos mediante sqlalchemy. Se encarga de generar la conexión a la BD y 
# se irán agregando funciones a medida que vaya creciendo el proyecto
class dbClass:
    def __init__(self):
        self.dbEngine = self.createDBEngine()
        self.dbMetadata = self.createMetadata()

    def createDBEngine(self):
        try:
            url = URL.create(
                drivername='redshift+redshift_connector', # redshift_connector driver y dialect 
                host=os.getenv("DB_CONNECTION"), # Amazon Redshift host
                port=5439, # Amazon Redshift port
                database=os.getenv("DB_NAME"), # Amazon Redshift database
                username=os.getenv("DB_USERNAME"), # Amazon Redshift username
                password=os.getenv("DB_PASSWORD") # Amazon Redshift password
            )
            dbEngine = db.create_engine(url)
            return dbEngine 
        except db.exc.OperationalError as e:
            print(f"Error creating database connection: {e}")

    def createMetadata(self):
        try:
            dbMetadata = db.MetaData()
            return dbMetadata
        except db.exc.OperationalError as e:
            print(f"Error creating metadata: {e}")

    def verifyTableExist(self, tableName):
        inspector = db.inspect(self.dbEngine)
        if inspector.has_table(tableName):
            return True
        else:
            return False

    def createPropertiesTable(self, tableName):
        try:
            if not self.verifyTableExist(tableName):
                db.Table(
                    tableName,                                        
                    self.dbMetadata,
                    db.Column('property_id', db.String, primary_key=True),
                    db.Column('ppt_type', db.String),
                    db.Column('ppt_beds', db.Integer),
                    db.Column('ppt_lot_sqft', db.Integer),
                    db.Column('ppt_sqft', db.Integer),
                    db.Column('status', db.String),
                    db.Column('country', db.String),
                    db.Column('state', db.String),
                    db.Column('city', db.String),
                    db.Column('list_price', db.Integer),
                    db.Column('last_sold_date', db.String)       
                )
                self.dbMetadata.create_all(self.dbEngine)
                print('La tabla de nombre ' + tableName + ' se ha creado correctamente\n')
            else:
                print('La tabla de nombre ' + tableName + ' ya existe\n')

        except db.exc.OperationalError as e:
            print(f"Error creating table: {e}")

    def getEngine(self):
        return self.dbEngine
    
    def endConnection(self):
        self.dbEngine.dispose()