import logging
import dotenv
import os
import time
from Gis.geojsonToDB import GeoJsonToDB
from Gis.WktHandler import ConvertWktToNestedCords
from Database.SQLHandler import SQLHandler
from Api.CatalogApiHandler import CatalogApiHandler
from Api.TokenApiHandler import TokenApiHandler
from Api.ProcessApiHandler import ProcessApiHandler

def setup_logging(log_file="app.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def main():
    logging.info("Starting the Download application...")
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    geojson_path = '../Shapefiles/Marker_2020.geojson'

    geojsonToDB = GeoJsonToDB(
        geojson_path, 
        db_handler)
    
    Token_ApiHandler = TokenApiHandler(
        ClientId=os.getenv("ApiClienId"),
        ClientSecret=os.getenv("ApiClienSecret"),
    )

    Catalog_ApiHandler = CatalogApiHandler(
        FromDate="2024-05-01T00:00:00Z",
        ToDate="2025-02-01T23:59:59Z",
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler
    )
    
    Process_ApiHandler = ProcessApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        SQLHandler=db_handler
    )



    geojsonToDB.process_geojson()
    fields = db_handler.getAllFieldPolygons()

    if fields:
        first_field_id, first_polygon_wkt = next(iter(fields.items()))

    #mock up data 
    #polygonWkt = "POLYGON ((8.943654 56.213578, 8.963054 56.221977, 9.002713 56.223409, 9.021598 56.2194, 9.026406 56.193335, 9.019367 56.181585, 8.986575 56.177572, 8.960651 56.176426, 8.946058 56.186935, 8.93998 56.204221, 8.946058 56.208327, 8.943654 56.213578))"
    logging.info(first_polygon_wkt)
    polygon = ConvertWktToNestedCords(first_polygon_wkt)
    CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=polygon,FeildId=first_field_id)
    logging.info(f"Catalog Api found: {len(CatalogData)} number of dates")
    dotenv.load_dotenv(dotenvFile)
    time.sleep(3) # Sleep timer to ensure that the CatalogApiHandler finishes before ProcessApiHandler starts
    for date in CatalogData:
        Process_ApiHandler.processDateIntoImages(date,polygon,first_field_id)   
    logging.info("Stopping the Download application...")


if __name__ == "__main__":
    setup_logging() 
    main()