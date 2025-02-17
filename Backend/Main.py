import logging
import dotenv
import os
from Gis.geojsonToDB import GeoJsonToDB
from Database.SQLHandler import SQLHandler

def setup_logging(log_file="app.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def main():
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    geojson_path = '../Shapefiles/Marker_2019.geojson'

    geojsonToDB = GeoJsonToDB(geojson_path, db_handler)
    geojsonToDB.process_geojson()



if __name__ == "__main__":
    setup_logging() 
    logging.info("Starting the application...")
    main()