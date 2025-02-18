import logging
import dotenv
import os
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
        FromDate="2024-10-01T00:00:00Z",
        ToDate="2025-02-01T23:59:59Z",
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler
        )
    
    Process_ApiHandler = ProcessApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        SQLHandler=db_handler
    )



    #geojsonToDB.process_geojson()
    fields = db_handler.getAllFieldPolygons()

    if fields:
        first_field_id, first_polygon_wkt = next(iter(fields.items()))
    logging.info(first_polygon_wkt)
    polygon = ConvertWktToNestedCords(first_polygon_wkt)
    CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=polygon,FeildId=first_field_id)
    date = next(iter(CatalogData))
    Process_ApiHandler.processDateIntoData(date,polygon)





if __name__ == "__main__":
    setup_logging() 
    logging.info("Starting the application...")
    main()