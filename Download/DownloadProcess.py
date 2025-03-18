import pandas as pd
import logging, dotenv, os, time , sys,csv,argparse,re

from shapely.geometry import box
from shapely.wkt import dumps
from Gis.geojsonToDB import GeoJsonToDB
from Gis.WktHandler import ConvertWktToNestedCords
from Database.SQLHandler import SQLHandler
from Api.CatalogApiHandler import CatalogApiHandler
from Api.TokenApiHandler import TokenApiHandler
from Api.ProcessApiHandler import ProcessApiHandler

def setup_logging(log_file="Download.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Logging initialized. Writing to {log_file}")

def DownloadProcess(FromDate,ToDate):
    logging.info(f"Starting the DownloadProcess using the dates {FromDate}, {ToDate}")
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
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        dbHandler=db_handler
    )
    
    Process_ApiHandler = ProcessApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        SQLHandler=db_handler
    )
    geojsonToDB.process_geojson()



    
    # Load the CSV file
    csv_file = "../Shapefiles/CSV/denmarkBB.csv"
    df_regions = pd.read_csv(csv_file)
    fields = ["ID","FullDate","DateOnly","Platform","ClouCover"]

    with open("DateMetaData.csv", 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)

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
        CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=nestedBB, FieldId=FieldId,FromDate=FromDate,ToDate=ToDate)

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
                
        logging.info(f"Completed the download proccess on the dates from: {FromDate} To: {ToDate}")
    
    
def validate_message_data(message_data):
    """Validates the MessagesData format."""
    DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    if not DATEPATTERN.match(message_data):
        logging.error(f"Invalid MessagesData format: {message_data}")
        sys.exit(1)
    return message_data.split("|")

def main():
    """Main function to handle argument parsing and processing."""
    parser = argparse.ArgumentParser(description="Process and download satellite data.")
    parser.add_argument("MessagesData", help="Date range in format YYYY-MM-DDTHH:MM:SSZ|YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("LogFile", nargs="?", default="download_process.log", help="Log file name (default: download_process.log)")

    args = parser.parse_args()

    setup_logging(args.LogFile)
    logging.info(f"Received MessagesData: {args.MessagesData}")

    FromDate, ToDate = validate_message_data(args.MessagesData)
    logging.info(f"Validated date range: From {FromDate} to {ToDate}")

    DownloadProcess(FromDate, ToDate)

if __name__ == "__main__":
    main()