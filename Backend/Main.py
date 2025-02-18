import logging
import dotenv
import os
from Gis.geojsonToDB import GeoJsonToDB
from Database.SQLHandler import SQLHandler
from Api.CatalogApiHandler import CatalogApiHandler

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

    #Muck up Polygon_Wkt data and Feild Id Create method that gets data from the database

    PolygonWkt = "POLYGON ((" \
        "9.955577 55.729285, 9.956843 55.729188, 9.958131 55.729418, 9.959075 55.729599, " \
        "9.96017 55.729805, 9.960942 55.729962, 9.961973 55.730119, 9.963046 55.730336, " \
        "9.964956 55.730566, 9.965814 55.730675, 9.966372 55.730469, 9.967102 55.73024, " \
        "9.967488 55.730179, 9.96781 55.73001, 9.968325 55.729611, 9.968067 55.729358, " \
        "9.966479 55.728886, 9.9659 55.7285, 9.963196 55.727352, 9.962702 55.726977, " \
        "9.961629 55.726566, 9.960985 55.726288, 9.959805 55.726325, 9.95841 55.726433, " \
        "9.957423 55.72647, 9.956672 55.726554, 9.954891 55.726603, 9.954504 55.726603, " \
        "9.954461 55.727521, 9.954504 55.72798, 9.95474 55.728427, 9.954976 55.728512, " \
        "9.95517 55.728717, 9.955384 55.728935, 9.955577 55.729285))"
    FeildId = 1
    
    Catalog_ApiHandler = CatalogApiHandler(
        ClientId=os.getenv("ApiClienId"),
        ClientSecret=os.getenv("ApiClienSecret"),
        FromDate="2024-10-01T00:00:00Z",
        ToDate="2025-02-01T23:59:59Z",
        ApiToken=os.getenv("APIToken")
        )

    CatalogData = Catalog_ApiHandler.GetPictureDates(PolygonWkt=PolygonWkt,FeildId=FeildId)





if __name__ == "__main__":
    setup_logging() 
    logging.info("Starting the application...")
    main()