import mysql.connector
from mysql.connector import errorcode
import logging
from shapely import wkt
import geopandas as gpd
from collections import defaultdict

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
            CREATE TABLE IF NOT EXISTS dataType (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255) NOT NULL
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
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
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS FieldMetaDataConnection (
                Id INT AUTO_INCREMENT PRIMARY KEY,
                FieldCropYearId INT NOT NULL,
                BBID VARCHAR(255) NOT NULL,
                UNIQUE INDEX IDX_NdviId_BBID (FieldCropYearId,BBID),
                FOREIGN KEY (FieldCropYearId) REFERENCES FieldCropYear(Id)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def Get_Data_Type_Ids(self, name):
        try:
            if name.lower() == 'all':
                self.cursor.execute("SELECT Id FROM dataType")
            else:
                query = "SELECT Id FROM dataType WHERE LOWER(Name) = %s"
                values =(name.lower(),)
                self.cursor.execute(query, values)

            ids = [row[0] for row in self.cursor.fetchall()]
            return ids
        except mysql.connector.Error as err:
            logging.error(f"Error inserting or updating data: {err}")
            raise

    def Insert_Data_For_A_DataType(self, ListOfData):
        try:
            (
                FieldCropYearId, collection_date, DataType,TiffMode,
                Average, Median, STd, Min, Max
            ) = ListOfData

            
            select_query = "SELECT Id FROM data WHERE FieldCropYearId = %s AND collection_date = %s AND datatypeId = %s AND tiffFormat =%s"
            self.cursor.execute(select_query, (FieldCropYearId, collection_date, DataType,TiffMode))
            existing_row = self.cursor.fetchone()
            select_query = "SELECT Name FROM dataType WHERE Id =%s"
            self.cursor.execute(select_query, (DataType,))
            DataNamerow = self.cursor.fetchone()
            DataName = DataNamerow[0]
            if existing_row:
                Dataid = existing_row[0]
                update_query = """
                    UPDATE data
                    SET Average = %s, Median = %s, STD = %s, Min = %s, Max = %s
                    WHERE Id = %s
                """
                update_values = (
                    Average, Median, STd, Min, Max,
                    Dataid
                )
                self.cursor.execute(update_query, update_values)
                logging.info(f"Updated data into the database for datatype:{DataName}")
            else:
                insert_query = """
                    INSERT INTO data (
                        FieldCropYearId, collection_date, datatypeId,tiffFormat,
                        Average, Median, STD, Min, Max
                    )
                    VALUES (%s, %s,
                            %s,%s,
                            %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, ListOfData)
                logging.info(f"inserted data into the database for datatype:{DataName}")
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting or updating data: {err}")
            raise

    def Insert_Simple_Data_Points_For_A_field(self, ListOfData):
        try:
            (
                FieldCropYearId, collection_date,
                AverageRed, MedianRed, STDRed, MinRed, MaxRed,
                AverageNir, MedianNir, STDNir, MinNir, MaxNir,
                AverageNdvi, MedianNdvi, STDNdvi, MinNdvi, MaxNdvi
            ) = ListOfData

            
            select_query = "SELECT Id FROM ndvi_data WHERE FieldCropYearId = %s AND collection_date = %s"
            self.cursor.execute(select_query, (FieldCropYearId, collection_date))
            existing_row = self.cursor.fetchone()

            if existing_row:
                ndvi_id = existing_row[0]

                update_query = """
                    UPDATE ndvi_data
                    SET 
                        AverageRed = %s, MedianRed = %s, STDRed = %s, MinRed = %s, MaxRed = %s,
                        AverageNir = %s, MedianNir = %s, STDNir = %s, MinNir = %s, MaxNir = %s,
                        AverageNdvi = %s, MedianNdvi = %s, STDNdvi = %s, MinNdvi = %s, MaxNdvi = %s
                    WHERE Id = %s
                """
                update_values = (
                    AverageRed, MedianRed, STDRed, MinRed, MaxRed,
                    AverageNir, MedianNir, STDNir, MinNir, MaxNir,
                    AverageNdvi, MedianNdvi, STDNdvi, MinNdvi, MaxNdvi,
                    ndvi_id
                )
                self.cursor.execute(update_query, update_values)

            else:
                insert_query = """
                    INSERT INTO ndvi_data (
                        FieldCropYearId, collection_date,
                        AverageRed, MedianRed, STDRed, MinRed, MaxRed,
                        AverageNir, MedianNir, STDNir, MinNir, MaxNir,
                        AverageNdvi, MedianNdvi, STDNdvi, MinNdvi, MaxNdvi
                    )
                    VALUES (%s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, ListOfData)

            self.connection.commit()


        except mysql.connector.Error as err:
            logging.error(f"Error inserting or updating data: {err}")
            raise

    
    def Insert_MetaData_Connection(self,FieldCropYearId:int,BBID:str):
        try:
            insert_query="""
            INSERT IGNORE INTO FieldMetaDataConnection(FieldCropYearId, BBID)
            VALUES (%s,%s)
            """
            values=(FieldCropYearId,BBID)
            self.cursor.execute(insert_query,values)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def Get_All_Polygons_Based_On_Year_And_CropId(self, year: int, cropid: int) -> gpd.GeoDataFrame:
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

    def Get_All_Field_Based_On_YearAnd_CropId(self,cropid,year):
        "Method that queries all of the different fields. based on cropid and year"
        Getquery = """SELECT FieldCropYear.FieldId FROM `FieldCropYear` 
            WHERE CropId = %s AND Year = %s;"""
        values=(cropid,year)
        self.cursor.execute(Getquery,values)
        results = self.cursor.fetchall()

        fieldIdList = [field_id[0] for field_id in results]
        logging.info(f"Queryed all Fieldids found: {len(fieldIdList)} Fields")
        return fieldIdList
    
    def Get_Field_Polygon_Based_On_Year_CropId_And_FieldId(self,year:int,cropid:int,fieldid:int) -> gpd.GeoDataFrame:
        try:
            get_query = """
            SELECT Field.FieldId, ST_AsText(Field.Polygon)
            FROM Field 
            INNER JOIN FieldCropYear ON Field.FieldId = FieldCropYear.FieldId
            WHERE FieldCropYear.Year = %s AND FieldCropYear.CropId = %s AND Field.FieldId = %s
            ORDER BY Field.FieldId ASC
            """
            values = (year, cropid,fieldid)
            self.cursor.execute(get_query, values)
            records = self.cursor.fetchall()

            if not records:
                logging.warning("No polygons found for the given year and crop type and fieldid.")
                return gpd.GeoDataFrame(columns=["FieldId", "geometry"], crs="EPSG:4326")

            polygons = [{"FieldId": row[0], "geometry": wkt.loads(row[1])} for row in records]
            gdf = gpd.GeoDataFrame(polygons, crs="EPSG:4326")

            logging.info(f"Retrieved {len(gdf)} polygons from the database.")
            return gdf

        except mysql.connector.Error as err:
            logging.error(f"Error retrieving data: {err}")
            raise

    def Get_FieldCropYearId_Based_On_Year_CropId_And_FieldId(self,Year: int, cropid:int,fieldid: int):
        try:
            get_query = """
            SELECT Id FROM FieldCropYear
            WHERE Year =%s AND FieldId =%s AND CropId =%s
            """
            values = (Year,fieldid,cropid)
            self.cursor.execute(get_query,values)
            id = self.cursor.fetchone()
            logging.info(f"Found FieldCropYearId:{id} with values year: {Year}, cropid:{cropid} and Fieldid:{fieldid}")
            return id[0]
        except mysql.connector.Error as err:
            logging.error(f"Error Getting the FieldCropYearId: {err}")
            raise

    def Get_FieldId_Based_On_FieldCropYearId(self,id):
        try:
            get_query="""
            SELECT FieldId FROM FieldCropYear WHERE Id = %s
            """
            values = (id,)
            self.cursor.execute(get_query,values)
            result = self.cursor.fetchone()
            return result[0]
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_CropType_Based_On_CropId(self,cropid):
        try:
            get_query="""
            SELECT CropType FROM CropType WHERE CropId = %s
            """
            values = (cropid,)
            self.cursor.execute(get_query,values)
            result = self.cursor.fetchone()
            return result[0]
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_All_CropYearId_From_Ndvi_data_Based_On_CropId(self,cropid):
        try:
            get_query = """
            SELECT DISTINCT ndvi_data.FieldCropYearId
            FROM ndvi_data
            INNER JOIN FieldCropYear ON ndvi_data.FieldCropYearId = FieldCropYear.Id
            WHERE FieldCropYear.CropId =%s
            ORDER BY ndvi_data.FieldCropYearId ASC
            """
            values = (cropid,)
            self.cursor.execute(get_query,values)
            results = [row[0] for row in self.cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise
    
    def Get_All_CropYearId_From_Data_Based_On_CropId_And_TiffFormat(self,cropid,TiffFormat):
        try:
            get_query = """
            SELECT DISTINCT data.FieldCropYearId
            FROM data
            INNER JOIN FieldCropYear ON data.FieldCropYearId = FieldCropYear.Id
            WHERE FieldCropYear.CropId =%s AND data.tiffFormat = %s
            ORDER BY data.FieldCropYearId ASC
            """
            values = (cropid,TiffFormat)
            self.cursor.execute(get_query,values)
            results = [row[0] for row in self.cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise
    
    def Get_All_CropYearId_From_DB_Based_On_CropId_And_TiffFormat_And_Region(self,cropid,TiffFormat,region):
        try:
            get_query = """
            SELECT DISTINCT data.FieldCropYearId
            FROM data
            INNER JOIN FieldCropYear ON data.FieldCropYearId = FieldCropYear.Id
            INNER JOIN Field ON FieldCropYear.FieldId = Field.FieldId
            WHERE FieldCropYear.CropId =%s AND data.tiffFormat = %s AND Field.Region = %s
            ORDER BY data.FieldCropYearId ASC
            """
            values = (cropid,TiffFormat,region)
            self.cursor.execute(get_query,values)
            results = [row[0] for row in self.cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise
        
    def Get_All_CropYearId_Based_On_CropId_And_FieldId_From_Data(self,cropid,fieldid):
        try:
            get_query = """
            SELECT DISTINCT data.FieldCropYearId
            FROM data
            INNER JOIN FieldCropYear ON data.FieldCropYearId = FieldCropYear.Id
            WHERE FieldCropYear.CropId =%s AND FieldCropYear.FieldId = %s
            ORDER BY data.FieldCropYearId ASC
            """
            values = (cropid,fieldid)
            self.cursor.execute(get_query,values)
            results = [row[0] for row in self.cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_All_CropYearId_Based_On_CropId_And_FieldId_From_Ndvi_data(self,cropid,fieldid):
        try:
            get_query = """
            SELECT DISTINCT ndvi_data.FieldCropYearId
            FROM ndvi_data
            INNER JOIN FieldCropYear ON ndvi_data.FieldCropYearId = FieldCropYear.Id
            WHERE FieldCropYear.CropId =%s AND FieldCropYear.FieldId = %s
            ORDER BY ndvi_data.FieldCropYearId ASC
            """
            values = (cropid,fieldid)
            self.cursor.execute(get_query,values)
            results = [row[0] for row in self.cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_all_NdviData_Based_On_Id_And_DateRange(self,ID,FromDate,toDate):
        try:
            get_query = """
            SELECT collection_date, AverageNdvi, MinNdvi, MaxNdvi
            FROM ndvi_data
            WHERE (collection_date BETWEEN %s AND %s) 
            And FieldCropYearId = %s
            ORDER By collection_date ASC
            """
            values = (FromDate,toDate,ID)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = {date: {"minNdvi": minNdvi,
                               "MaxNdvi":maxNdvi,
                               "AverageNdvi": avgNdvi
                               } 
                               for date, avgNdvi,minNdvi,maxNdvi in results}
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_Datatype_Name(self,id):
        try:
            get_query = """
            SELECT Name
            FROM dataType
            WHERE Id = %s
            """
            values = (id,)
            self.cursor.execute(get_query,values)
            result = self.cursor.fetchone()
            return result[0] if result else None
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_All_DataPoints_Based_On_Id_And_DateRange_And_Datatype_And_TiffFormat(self,ID,FromDate,toDate,Datatype,TiffFormat):
        try:
            
            get_query = """
            SELECT collection_date, Average, Min, Max,Median
            FROM data
            WHERE (collection_date BETWEEN %s AND %s) 
            And FieldCropYearId = %s AND datatypeId = %s AND tiffFormat = %s
            ORDER By collection_date ASC
            """
            values = (FromDate,toDate,ID,Datatype,TiffFormat)
            print(values)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = {date: {"Min": min,
                               "Max":max,
                               "Average": avg,
                               "Median": med
                               } 
                               for date, avg,min,max,med in results}
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_All_DataPoints_Based_On_DateRange_And_Datatype(self,FromDate,toDate,Datatype):
        try:
            get_query = """
            SELECT FieldCropYearId,collection_date, Average, Min, Max,Median
            FROM data
            WHERE (collection_date BETWEEN %s AND %s) 
            AND datatypeId = %s
            ORDER By collection_date ASC
            """
            values = (FromDate,toDate,Datatype)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = defaultdict(dict)
            for id, date, avg, min, max, med in results:
                Datadict[id][date] = {
                    "Min": min,
                    "Max": max,
                    "Average": avg,
                    "Median": med
                }
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise

    def Get_All_DataPoints_Based_On_Id_And_Date_And_Datatype(self,date,Datatype):
        try:
            get_query = """
            SELECT collection_date,FieldCropYearId, Average, Min, Max, Median
            FROM data
            WHERE collection_date = %s
            AND datatypeId = %s
            ORDER By collection_date ASC
            """
            values = (date,Datatype)
            self.cursor.execute(get_query,values)
            results = self.cursor.fetchall()
            Datadict = {id: {"Min": min,
                               "Max":max,
                               "Average": avg,
                               "Median": med,
                               "Date": date
                               } 
                               for date,id, avg,min,max,med in results}
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise
    
    def Get_All_Ndvi_Based_On_Date(self,Date):
        try:
            get_query = """
            SELECT collection_date, FieldCropYearId, AverageNdvi, minNdvi, MaxNdvi
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
                               "Date": date
                               } 
                               for date, id, avgNdvi,minNdvi,maxNdvi in results}
            return Datadict
        except mysql.connector.Error as err:
            logging.error(f"Error Getting data: {err}")
            raise
    
    def Get_CloudCovers_Based_On_FieldCropYearId_And_Date(self, FieldCropYearId, Date):
        try:
            get_query = """
            SELECT BoundingboxMetaData.CloudCover
            FROM FieldMetaDataConnection
            INNER JOIN BoundingboxMetaData 
                ON FieldMetaDataConnection.BBID = BoundingboxMetaData.BoundingBoxId
            WHERE FieldMetaDataConnection.FieldCropYearId = %s
            AND BoundingboxMetaData.DateOnly = %s
            """
            values = (FieldCropYearId, str(Date))
            self.cursor.execute(get_query, values)
            results = self.cursor.fetchall()
            CloudCoverList = [row[0] for row in results]
            return CloudCoverList
        except mysql.connector.Error as err:
            logging.error(f"Error Getting cloud cover data: {err}")
            raise

    def Get_Field_And_Croptype_Based_On_FieldCropYearId(self,FieldCropYearId:int):
        """Get FieldId and Croptype based on FieldCropYearId"""
        try:
            get_query = """
            SELECT FieldCropYear.FieldId ,CropType.CropType
            FROM FieldCropYear
            INNER JOIN CropType 
                ON FieldCropYear.CropId = CropType.CropId
            WHERE FieldCropYear.Id = %s
            """
            values = (FieldCropYearId,)
            self.cursor.execute(get_query, values)
            results = self.cursor.fetchone()
            FieldId = results[0]
            cropType = results[1]
            return FieldId, cropType
        except mysql.connector.Error as err:
            logging.error(f"Error Getting cloud cover data: {err}")
            raise

if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()