import pandas as pd
import logging
import dotenv
import os
import time
from shapely.geometry import box
from shapely.wkt import dumps
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
        FromDate="2024-08-01T00:00:00Z",
        ToDate="2024-08-10T23:59:59Z",
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
    
    # Load the CSV file
    csv_file = "../Shapefiles/CSV/denmarkBB.csv"
    df_regions = pd.read_csv(csv_file)

    # Iterate through each regionBB
    for x, row in df_regions.iterrows():
        logging.info(f"Processing regionBB {x + 1}")

        # Convert bounding box to polygon
        bbPoly = box(row["min_lon"], row["min_lat"], row["max_lon"], row["max_lat"])

        # Convert polygons to WKT
        bbPolyWKT = dumps(bbPoly)

        # Convert WKT Polygons to NestedCords
        nestedBB = ConvertWktToNestedCords(bbPolyWKT)

        # Set a unique FieldId for tracking
        FieldId = f"RegionBB_{x+1}"

        # Call the Catalog API to get image dates
        CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=nestedBB, FieldId=FieldId)

        if not CatalogData:
            logging.warning(f"No image dates found for regionBB {FieldId}, skipping...")
            continue

        logging.info(f"Catalog API found {len(CatalogData)} dates for Region {x + 1}")

        dotenv.load_dotenv(dotenvFile)

        # Sleep to ensure API stability
        time.sleep(3)

        # Process each date
        for date in CatalogData:
            try:
                Process_ApiHandler.processDateIntoImages(date, nestedBB, FieldId)
            except Exception as e:
                logging.error(f"Error processing date {date} for regionBB {FieldId}: {e}")

    #if fields:
        #first_field_id, first_polygon_wkt = next(iter(fields.items()))

    #mock up data 
    #polygonWkt = "POLYGON ((8.943654 56.213578, 8.963054 56.221977, 9.002713 56.223409, 9.021598 56.2194, 9.026406 56.193335, 9.019367 56.181585, 8.986575 56.177572, 8.960651 56.176426, 8.946058 56.186935, 8.93998 56.204221, 8.946058 56.208327, 8.943654 56.213578))"
    #logging.debug(first_polygon_wkt)
    #polygon = ConvertWktToNestedCords(first_polygon_wkt)
    #CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=polygon,FeildId=first_field_id)
    #logging.info(f"Catalog Api found: {len(CatalogData)} number of dates")
    #dotenv.load_dotenv(dotenvFile)
    #time.sleep(3) # Sleep timer to ensure that the CatalogApiHandler finishes before ProcessApiHandler starts
    #for date in CatalogData:
        #Process_ApiHandler.processDateIntoImages(date,polygon,first_field_id)   
    #logging.info("Stopping the Download application...")


if __name__ == "__main__":
    setup_logging() 
    main()