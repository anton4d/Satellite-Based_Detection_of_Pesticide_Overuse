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
    #geojsonToDB.process_geojson()

    polygon_wkt = ("POLYGON ((659478.154481784 6149596.98423771, "
               "659493.30449996 6149614.07274354, "
               "659493.436744312 6149613.98295175, "
               "659493.406906907 6149613.94859353, "
               "659543.552771315 6149579.954976, "
               "659529.096124877 6149562.96145264, "
               "659514.742468898 6149572.32873378, "
               "659506.51451136 6149577.96234791, "
               "659489.115440714 6149589.44857846, "
               "659478.154481784 6149596.98423771))")
    
    Catalog_ApiHandler = CatalogApiHandler(
        ClientId=os.getenv("ApiClienId"),
        ClientSecret=os.getenv("ApiClienSecret"),
        FromDate="2024-12-01T00:00:00Z",
        ToDate="2025-01-01T23:59:59Z",
        ApiToken=os.getenv("APIToken")
        )

    Catalog_ApiHandler.GetPictureDates(PolygonWkt=polygon_wkt)





if __name__ == "__main__":
    setup_logging() 
    logging.info("Starting the application...")
    main()