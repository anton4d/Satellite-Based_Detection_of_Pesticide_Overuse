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
            CREATE TABLE IF NOT EXISTS ndvi_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                FieldId INT NOT NULL,
                collection_date DATE,
                AverageRed FLOAT, MedianRed FLOAT, STDRed FLOAT, MinRed FLOAT, MaxRed FLOAT, HistRed JSON,
                AverageNir FLOAT, MedianNir FLOAT, STDNir FLOAT, MinNir FLOAT, MaxNir FLOAT, HistNir JSON,
                AverageNdvi FLOAT, MedianNdvi FLOAT, STDNdvi FLOAT, MinNdvi FLOAT, MaxNdvi FLOAT, HistNdvi JSON,
                FOREIGN KEY (FieldId) REFERENCES Field(FieldId)
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
            INSERT INTO ndvi_data (FieldId,collection_date,
            AverageRed, MedianRed, STDRed, MinRed, MaxRed, HistRed,
            AverageNir,MedianNir, STDNir, MinNir, MaxNir, HistNir,
            AverageNdvi,MedianNdvi, STDNdvi, MinNdvi, MaxNdvi, HistNdvi)
            VALUES (%s,%s,
            %s, %s, %s, %s,%s,%s,
            %s, %s, %s, %s,%s,%s,
            %s, %s, %s, %s,%s,%s)
            """
            self.cursor.execute(insert_query,ListOfData)
            self.connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
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
        

if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()