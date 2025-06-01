import mysql.connector
from mysql.connector import errorcode
import logging
import json

class SQL_Handler:
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

        self.Connect()

    def Connect(self):
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
            self.Setup_Cchema()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Invalid username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error(f"Database '{self.database}' does not exist.")
            else:
                logging.error(f"{err}")

    def Setup_Cchema(self):
        """Create the necessary tables if they do not exist."""
        try:
            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS Field (
                FieldId INT AUTO_INCREMENT PRIMARY KEY,
                MarkNr VARCHAR(255) NOT NULL,
                CVR VARCHAR(255),
                Journalnr VARCHAR(255) NOT NULL,
                Markblok VARCHAR(255),
                Region VARCHAR(255),
                Polygon POLYGON NOT NULL,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_Marknr_Markblok (MarkNr, Markblok)
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS CropType (
                CropId INT AUTO_INCREMENT PRIMARY KEY,
                CropType VARCHAR(255) UNIQUE NOT NULL,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()
            with open ("Crop_Type.json","r") as file:
                data = json.load(file)
                self.Insert_Crop_Type(data)

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS FieldCropYear (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                FieldId INT NOT NULL,
                CropId INT NOT NULL,
                Year VARCHAR(255) NOT NULL,
                FOREIGN KEY (FieldId) REFERENCES Field(FieldId),
                FOREIGN KEY (CropId) REFERENCES CropType(CropId),
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()


            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS BoundingboxMetaData(
                MetaDataID INT AUTO_INCREMENT PRIMARY KEY,
                BoundingBoxId VARCHAR(255) NOT NULL,
                FullDate VARCHAR(255) NOT NULL,
                DateOnly VARCHAR(255) NOT NULL,
                Platform VARCHAR(255) NOT NULL,
                CloudCover Float NOT NULL,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS FieldMetaDataConnection (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                FieldCropYearId INT NOT NULL,
                BBID VARCHAR(255) NOT NULL,
                UNIQUE INDEX IDX_NdviId_BBID (FieldCropYearId,BBID),
                FOREIGN KEY (FieldCropYearId) REFERENCES FieldCropYear(Id),
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS dataType (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255) UNIQUE NOT NULL
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()

            with open ("Data_Type.json","r") as file:
                data = json.load(file)
                self.Insert_Data_Type(data)

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS data (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                FieldCropYearId INT NOT NULL,
                collection_date DATE NOT NULL,
                datatypeId INT NOT NULL,
                tiffFormat VARCHAR(255) NOT NULL,
                Average FLOAT, Median FLOAT, STD FLOAT, Min FLOAT, Max FLOAT,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE INDEX idx_FCY_CD_DT_TF (FieldCropYearId, collection_date,datatypeId,tiffFormat),
                FOREIGN KEY (FieldCropYearId) REFERENCES FieldCropYear(Id),
                FOREIGN KEY (datatypeId) REFERENCES dataType(Id)
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()

            Create_Table_Query = """
            CREATE TABLE IF NOT EXISTS ApiUsers (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                ClientID VARCHAR(255) NOT NULL,
                ClientSecret VARCHAR(255) NOT NULL,
                UploadedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(Create_Table_Query)
            self.connection.commit()


            
            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def Insert_Crop_Type(self, Crop_Types_List):
        """Insert a list of croptypes into the Database, ignoring duplicates"""
        try:
            for crop in Crop_Types_List:
                Insert_Query = """
                INSERT IGNORE INTO CropType (Croptype)
                VALUES (%s)
                """
                Values = (crop,)
                self.cursor.execute(Insert_Query, Values)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def Insert_Data_Type(self,Data_Type_List):
        """Insert a list of Data_Types into the Database, ignoring duplicates"""
        try:
            for crop in Data_Type_List:
                Insert_Query = """
                INSERT IGNORE INTO dataType (Name)
                VALUES (%s)
                """
                Values = (crop,)
                self.cursor.execute(Insert_Query, Values)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def Insert_Or_Update_Field(self, MarkNr, Journalnr, Markblok, CVR, Polygon):
        """Insert a new field or update it if it already exists."""
        try:
            Query = "SELECT FieldId FROM Field WHERE MarkNr = %s AND (Markblok <=> %s)"
            self.cursor.execute(Query, (MarkNr, Markblok))
            Result = self.cursor.fetchone()

            if Result:
                Update_Query = """
                UPDATE Field 
                SET Journalnr = %s,
                    CVR = %s,
                    Polygon = ST_GeomFromText(%s)
                WHERE MarkNr = %s AND (Markblok <=> %s)
                """
                self.cursor.execute(Update_Query, (Journalnr, CVR, Polygon, MarkNr, Markblok))
                logging.debug(f"Updated field with MarkNr '{MarkNr}' and Markblok '{Markblok}'.")
            else:
                insert_query = """
                INSERT INTO Field (MarkNr, Journalnr, Markblok, CVR, Polygon)
                VALUES (%s, %s, %s, %s, ST_GeomFromText(%s))
                """
                self.cursor.execute(insert_query, (MarkNr, Journalnr, Markblok, CVR, Polygon))
                logging.debug(f"Inserted new field with MarkNr '{MarkNr}' and Markblok '{Markblok}'.")

            self.connection.commit()

        except mysql.connector.Error as err:
            logging.error(f"Error inserting/updating field: {err}")
            raise

        
    
    def Get_All_Crop_Types(self):
        """Get all Croptypes Currently in the database"""
        try:
            get_query="""
            SELECT Croptype FROM CropType
            """
            self.cursor.execute(get_query)
            data = self.cursor.fetchall()
            return [item[0] for item in data]
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")

    def Insert_Crop_Year(self, CropType, year, MarkNr, Markblok):
        try:
            self.cursor.execute("SELECT FieldId FROM Field WHERE MarkNr = %s AND (Markblok <=> %s)", (MarkNr, Markblok))
            Field_Row = self.cursor.fetchone()

            self.cursor.execute("SELECT CropId FROM CropType WHERE CropType=%s", (CropType,))
            Crop_Row = self.cursor.fetchone()

            if Field_Row is None:
                logging.error("Field not found.")
                return

            if Crop_Row is None:
                logging.error("Crop type not found.")
                return

            Field_ID = Field_Row[0]
            Crop_ID = Crop_Row[0]


            self.cursor.execute("""
                SELECT * FROM FieldCropYear
                WHERE FieldId = %s AND CropId = %s AND Year = %s
            """, (Field_ID, Crop_ID, year))
            exists = self.cursor.fetchone()

            if exists:
                logging.debug("Duplicate entry skipped.")
                return

            Query = """
                INSERT INTO FieldCropYear (FieldId, CropId, Year)
                VALUES (%s, %s, %s)
            """
            self.cursor.execute(Query, (Field_ID, Crop_ID, year))
            self.connection.commit()
            logging.debug("Crop year has been inserted.")

        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")


if __name__ == "__main__":
    
    DB_Host = 'localhost'
    DB_User = 'root'
    DB_Password = 'root_password'  
    DB_Name = 'test_db'  

    
    Sql_Handler = SQL_Handler(host=DB_Host, user=DB_User, password=DB_Password, database=DB_Name)

    Sql_Handler.close()