import mysql.connector
from mysql.connector import errorcode
import logging

class SQLHandler:
    def __init__(self, host, user, password, database):
        """
        Initialize the SQL handler.
        :param host: Database host (e.g., 'localhost' or a docker container IP).
        :param user: Database user (e.g., 'root').
        :param password: Database password.
        :param database: Database name.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

        self.connect()

    def connect(self):
        """Connect to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            logging.info("Connected to the database successfully.")
            self.setup_schema()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Invalid username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error(f"Database '{self.database}' does not exist.")
            else:
                logging.error(err)

    def setup_schema(self):
        """Create the necessary tables if they do not exist."""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Field (
                FieldId INT AUTO_INCREMENT PRIMARY KEY,
                CropType VARCHAR(255) NOT NULL,
                MarkNr VARCHAR(255) NOT NULL,
                CVR VARCHAR(255),
                Journalnr VARCHAR(255) NOT NULL,
                Markblok VARCHAR(255),
                Polygon POLYGON NOT NULL,
                AverageRed VARCHAR(255),
                AverageNIR VARCHAR(255),
                AverageNDVI VARCHAR(255),
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS Tile (
                TileID INT AUTO_INCREMENT PRIMARY KEY,
                NirRed VARCHAR(255) NOT NULL,
                Red VARCHAR(255) NOT NULL,
                NDVI VARCHAR(255) NOT NULL,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FieldId INT NOT NULL,
                FOREIGN KEY (FieldId) REFERENCES Field(FieldId)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def InsertField(self, CropType, MarkNr, Journalnr, Markblok, CVR, Polygon):
        """Insert a new fiel record into the database."""
        try:
            
            
            insert_query = """
            INSERT INTO Field (CropType, MarkNr, Journalnr, Markblok, CVR, Polygon) 
            VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s))
            """
            self.cursor.execute(insert_query, (CropType, MarkNr, Journalnr, Markblok, CVR, Polygon))
            self.connection.commit()
            logging.info(f"Inserted new field into the database with MarkNr '{MarkNr}' and Markblok '{Markblok}'.")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def field_exists(self, marknr, markblok):
        """Check if a field already exists based on MarkNr or Journalnr"""
        # If MarkNr & Markblok is not already in the database, special operator that allows comparison for null values.
        query = "SELECT EXISTS(SELECT 1 FROM Field WHERE MarkNr = %s AND (Markblok <=> %s))"
        self.cursor.execute(query, (marknr, markblok))
        
        return self.cursor.fetchone()[0]

    def getAllFieldPolygons(self):
        "Method that queries all of the different fields and their corresponding poylgons and adds them to a dictionary."
        query = "SELECT FieldID, ST_AsText(Polygon) FROM Field;"
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        fieldDict = {field_id: polygon_wkt for field_id, polygon_wkt in results}
        logging.info(f"Queryed all Field Polygons found: {len(fieldDict)} Fields")
        return fieldDict


if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()