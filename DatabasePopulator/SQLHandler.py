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
            CREATE TABLE IF NOT EXISTS CropType (
                CropId INT AUTO_INCREMENT PRIMARY KEY,
                CropType VARCHAR(255) UNIQUE NOT NULL
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS FieldCropYear (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                FieldId INT NOT NULL,
                CropId INT NOT NULL,
                Year VARCHAR(255) NOT NULL,
                FOREIGN KEY (FieldId) REFERENCES Field(FieldId),
                FOREIGN KEY (CropId) REFERENCES CropType(CropId)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()


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

            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def InsertCropType(self, croptypesList):
        """Insert a list of croptypes into the Database, ignoring duplicates"""
        try:
            for crop in croptypesList:
                insert_query = """
                INSERT IGNORE INTO CropType (Croptype)
                VALUES (%s)
                """
                values = (crop,)
                self.cursor.execute(insert_query, values)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def InsertField(self, MarkNr, Journalnr, Markblok, CVR, Polygon):
        """Insert a new fiel record into the database."""
        try:
            
            insert_query = """
            INSERT INTO Field (MarkNr, Journalnr, Markblok, CVR, Polygon) 
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s))
            """
            self.cursor.execute(insert_query, (MarkNr, Journalnr, Markblok, CVR, Polygon))
            self.connection.commit()
            logging.debug(f"Inserted new field into the database with MarkNr '{MarkNr}' and Markblok '{Markblok}'.")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def field_exists(self, marknr, markblok):
        """Check if a field already exists based on MarkNr or Journalnr"""
        # If MarkNr & Markblok is not already in the database, special operator that allows comparison for null values.
        query = "SELECT EXISTS(SELECT 1 FROM Field WHERE MarkNr = %s AND (Markblok <=> %s))"
        self.cursor.execute(query, (marknr, markblok))
        
        return self.cursor.fetchone()[0]
    
    def GetAllCropTypes(self):
        try:
            get_query="""
            SELECT Croptype FROM CropType
            """
            self.cursor.execute(get_query)
            data = self.cursor.fetchall()
            return [item[0] for item in data]
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")

    def InsertCropYear(self, CropType, year, MarkNr, Markblok):
        try:
            self.cursor.execute("SELECT FieldId FROM Field WHERE MarkNr = %s AND (Markblok <=> %s)", (MarkNr, Markblok))
            field_row = self.cursor.fetchone()

            self.cursor.execute("SELECT CropId FROM CropType WHERE CropType=%s", (CropType,))
            crop_row = self.cursor.fetchone()

            if field_row is None:
                logging.error("Field not found.")
                return

            if crop_row is None:
                logging.error("Crop type not found.")
                return

            FieldId = field_row[0]
            CropId = crop_row[0]

            # Check for duplicates before inserting
            self.cursor.execute("""
                SELECT * FROM FieldCropYear
                WHERE FieldId = %s AND CropId = %s AND Year = %s
            """, (FieldId, CropId, year))
            exists = self.cursor.fetchone()

            if exists:
                logging.debug("Duplicate entry skipped.")
                return

            query = """
                INSERT INTO FieldCropYear (FieldId, CropId, Year)
                VALUES (%s, %s, %s)
            """
            self.cursor.execute(query, (FieldId, CropId, year))
            self.connection.commit()
            logging.debug("Crop year has been inserted.")

        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")


if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()