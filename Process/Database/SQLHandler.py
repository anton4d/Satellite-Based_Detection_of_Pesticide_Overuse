import mysql.connector
from mysql.connector import errorcode
import logging
from shapely import wkt
import geopandas as gpd

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
            CREATE TABLE IF NOT EXISTS ndvi_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                FieldId INT NOT NULL,
                CropId INT NOT NULL,
                collection_date DATE,
                BBID Varchar(255) NOT NULL,
                AverageRed FLOAT, MedianRed FLOAT, STDRed FLOAT, MinRed FLOAT, MaxRed FLOAT, 
                AverageNir FLOAT, MedianNir FLOAT, STDNir FLOAT, MinNir FLOAT, MaxNir FLOAT, 
                AverageNdvi FLOAT, MedianNdvi FLOAT, STDNdvi FLOAT, MinNdvi FLOAT, MaxNdvi FLOAT, 
                FOREIGN KEY (FieldId) REFERENCES Field(FieldId),
                FOREIGN KEY (CropId) REFERENCES CropType(CropId)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")


    def insertSimpleDataPointsForAfield(self, ListOfData):
        try:
            insert_query = """
            INSERT INTO ndvi_data (FieldId,CropId,collection_date, BBID,
            AverageRed, MedianRed, STDRed, MinRed, MaxRed,
            AverageNir,MedianNir, STDNir, MinNir, MaxNir,
            AverageNdvi,MedianNdvi, STDNdvi, MinNdvi, MaxNdvi)
            VALUES (%s,%s, %s, %s,
            %s, %s, %s, %s,%s,
            %s, %s, %s, %s,%s,
            %s, %s, %s, %s,%s)
            """
            self.cursor.execute(insert_query,ListOfData)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def getAllPolygonsBasedOnYearAndCropType(self, year: int, cropid: int) -> gpd.GeoDataFrame:
        try:
            get_query = """
            SELECT Field.FieldId, ST_AsText(Field.Polygon)
            FROM Field 
            INNER JOIN FieldCropYear ON Field.FieldId = FieldCropYear.FieldId
            WHERE FieldCropYear.Year = %s AND FieldCropYear.CropId = %s
            ORDER BY Field.FieldId ASC
            """
            values = (year, cropid)
            self.cursor.execute(get_query, values)
            records = self.cursor.fetchall()

            if not records:
                logging.warning("No polygons found for the given year and crop type.")
                return gpd.GeoDataFrame(columns=["FieldId", "geometry"], crs="EPSG:4326")

            polygons = [{"FieldId": row[0], "geometry": wkt.loads(row[1])} for row in records]
            gdf = gpd.GeoDataFrame(polygons, crs="EPSG:4326")

            logging.info(f"Retrieved {len(gdf)} polygons from the database.")
            return gdf

        except mysql.connector.Error as err:
            logging.error(f"Error retrieving data: {err}")
            raise

    
    def insertAllDataPointsForAField(self, ListOfData):
        try:
            insert_query = """
            INSERT INTO ndvi_data (FieldId,collection_date,longitude, latitude, red, nir, ndvi)
            VALUES (%s,%s,%s, %s, %s, %s, %s)
            """
            batch_size = 10000
            total_inserted = 0
            for i in range(0, len(ListOfData), batch_size):
                batch = ListOfData[i:i + batch_size]
                try:
                    self.cursor.executemany(insert_query, batch)
                    self.connection.commit()
                    total_inserted += self.cursor.rowcount  # Track inserted rows
                except mysql.connector.Error as err:
                    logging.error(f"Error inserting batch {i//batch_size}: {err}")
            
            logging.info(f"Total inserted: {total_inserted}/{len(ListOfData)}")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise
    def GetAllFeildIdsFromNdviData(self):
        try:
            get_query = """
            SELECT DISTINCT FieldId
            FROM ndvi_data
            """
            self.cursor.execute(get_query)
            results = [row[0] for row in self.cursor.fetchall()]  # Extract only the field IDs
            #print(results)
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def GetallNdviDataBasedOnIdAndDateRange(self,ID,FromDate,toDate):
        try:
            get_query = """
            SELECT collection_date, AverageNdvi, MinNdvi, MaxNdvi, BBID
            FROM ndvi_data 
            WHERE (collection_date BETWEEN %s AND %s) 
            And FieldId = %s
            ORDER By collection_date ASC
            """
            values = (FromDate,toDate,ID)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = {date: {"minNdvi": minNdvi,
                               "MaxNdvi":maxNdvi,
                               "AverageNdvi": avgNdvi,
                               "CloudCover": self.GetCloudCoverBasedOnDateAndBBID(BBID,date)
                               } 
                               for date, avgNdvi,minNdvi,maxNdvi, BBID in results}
            return Datadict
        
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def GetAllNdviBasedOnDate(self,Date):
        try:
            get_query = """
            SELECT collection_date, FieldId, AverageNdvi, MinNdvi, MaxNdvi, BBID
            FROM ndvi_data
            WHERE collection_date = %s
            ORDER BY collection_date ASC
            """
            values = (Date,)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = {id: {"minNdvi": minNdvi,
                               "MaxNdvi":maxNdvi,
                               "AverageNdvi": avgNdvi,
                               "Date": date,
                               "CloudCover": self.GetCloudCoverBasedOnDateAndBBID(BBID,date)
                               } 
                               for date, id, avgNdvi,minNdvi,maxNdvi, BBID in results}
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def GetCloudCoverBasedOnDateAndBBID(self,ID,Date):
        try:
            get_query = """
            SELECT CloudCover 
            FROM BoundingboxMetaData
            WHERE DateOnly = %s and BoundingBoxId = %s
            """
            values = (str(Date), ID)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            CloudCoverStringComma = ",".join(str(CloudCover[0]) for CloudCover in results)
            return CloudCoverStringComma
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()