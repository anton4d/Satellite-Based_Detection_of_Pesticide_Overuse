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
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS BoundingboxMetaData(
                MetaDataID INT AUTO_INCREMENT PRIMARY KEY,
                BoundingBoxId VARCHAR(255) NOT NULL,
                FullDate VARCHAR(255) NOT NULL,
                DateOnly VARCHAR(255) NOT NULL,
                Platform VARCHAR(255) NOT NULL,
                CloudCover Float NOT NULL
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS ApiUsers(
                ApiID INT AUTO_INCREMENT PRIMARY KEY,
                ClientID VARCHAR(255) NOT NULL,
                ClientSecret VARCHAR(255) NOT NULL,
                Tokens VARCHAR(255) NOT NULL
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def ApiUser(self, OldClientId):
        """Deactivate old API user and return a new valid one"""
        try:

            update_query = """
                UPDATE ApiUsers 
                SET Tokens = "FALSE"
                WHERE ClientID = %s
            """
            self.cursor.execute(update_query, (OldClientId,))
            self.connection.commit()


            select_query = """
                SELECT ClientID, ClientSecret FROM ApiUsers
                WHERE Tokens = "TRUE"
                LIMIT 1
            """
            self.cursor.execute(select_query)
            result = self.cursor.fetchone()

            if result:
                logging.info(f"ClientId {result[0]}")
                return result[0], result[1]
            else:
                logging.error("No valid API user with Tokens = TRUE")
                return None,None

        except mysql.connector.Error as err:
            logging.error(f"MySQL Error: {err}")
            raise


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

    def getAllFieldPolygonsBasedOnRegion(self,Region):
        "Method that queries all of the different fields and their corresponding poylgons and adds them to a dictionary."
        query = "SELECT FieldID, ST_AsText(Polygon) FROM Field WHERE Region = %s;"
        values = (Region,)
        self.cursor.execute(query,values)
        results = self.cursor.fetchall()

        fieldDict = {field_id: polygon_wkt for field_id, polygon_wkt in results}
        logging.info(f"Queryed all Field Polygons found: {len(fieldDict)} Fields")
        return fieldDict

    def getAllFieldPolygonsBasedOnId(self,FieldId):
        "Method that queries all of the different fields and their corresponding poylgons and adds them to a dictionary."
        query = "SELECT FieldID, ST_AsText(Polygon) FROM Field WHERE FieldID = %s;"
        values = (FieldId,)
        self.cursor.execute(query,values)
        results = self.cursor.fetchall()

        fieldDict = {field_id: polygon_wkt for field_id, polygon_wkt in results}
        logging.info(f"Queryed all Field Polygons found: {len(fieldDict)} Fields")
        return fieldDict
    
    def GetAllFieldIdAndPolygonsBasedOnYearAndCropId(self,cropid,year):
        "Method that queries all of the different fields and their corresponding poylgons and adds them to a dictionary. based on cropid and year"
        Getquery = """SELECT FieldCropYear.FieldId, ST_AsText(Field.Polygon) FROM `FieldCropYear` 
            INNER JOIN Field ON Field.FieldId = FieldCropYear.FieldId 
            WHERE CropId = %s AND Year = %s;"""
        values=(cropid,year)
        self.cursor.execute(Getquery,values)
        results = self.cursor.fetchall()

        fieldDict = {field_id: polygon_wkt for field_id, polygon_wkt in results}
        logging.info(f"Queryed all Field Polygons found: {len(fieldDict)} Fields")
        return fieldDict

    def GetAllFieldIdAndPolygonsBasedOnYearAndCropIdAndRegion(self,cropid,year,region):
        "Method that queries all of the different fields and their corresponding poylgons and adds them to a dictionary. based on cropid and year"
        Getquery = """SELECT FieldCropYear.FieldId, ST_AsText(Field.Polygon) FROM `FieldCropYear` 
            INNER JOIN Field ON Field.FieldId = FieldCropYear.FieldId 
            WHERE CropId = %s AND Year = %s AND Field.Region = %s;"""
        values=(cropid,year,region)
        self.cursor.execute(Getquery,values)
        results = self.cursor.fetchall()

        fieldDict = {field_id: polygon_wkt for field_id, polygon_wkt in results}
        logging.info(f"Queryed all Field Polygons found: {len(fieldDict)} Fields")
        return fieldDict
    
    def insertBoundingboxMetaData(self, metadata):
        """Method that inserts all metadata from the features"""
        try:
            self.cursor.execute("SELECT * FROM BoundingboxMetaData WHERE BoundingBoxId=%s AND FullDate=%s", (metadata[0], metadata[1]))
            foundMetaData = self.cursor.fetchone()

            if foundMetaData is not None:
                query = """
                UPDATE BoundingboxMetaData 
                SET DateOnly=%s, Platform=%s, CloudCover=%s 
                WHERE BoundingBoxId=%s AND FullDate=%s
                """
                values = (metadata[2], metadata[3], metadata[4], metadata[0], metadata[1])
                logging.info(f"MetaData for {metadata[0]} on the {metadata[1]} has been updated")
            else:
                query = """
                INSERT INTO BoundingboxMetaData (BoundingBoxId, FullDate, DateOnly, Platform, CloudCover)
                VALUES (%s, %s, %s, %s, %s)
                """
                values = metadata
                logging.info(f"MetaData for {metadata[0]} on the {metadata[1]} has been inserted")

            self.cursor.execute(query, values)
            self.connection.commit()

        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")



if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()