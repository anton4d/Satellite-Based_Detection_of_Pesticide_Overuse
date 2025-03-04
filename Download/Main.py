import pandas as pd
import logging, dotenv, os, time , sys, pika

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

def DownloadProcess(Catalog_ApiHandler,Process_ApiHandler,FromDate,ToDate):
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)
    logging.info(f"Starting the DownloadProcess using the dates {FromDate}, {ToDate}")
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

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
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
        TokenApiHandler=Token_ApiHandler
    )
    
    Process_ApiHandler = ProcessApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        SQLHandler=db_handler
    )
    geojsonToDB.process_geojson()
    def callback(ch, method, properties, body):
        Message = body.decode()
        logging.info(f" [x] Received {Message}")
        try:
            FromDate , ToDate = Message.split("|")
            DownloadProcess(Catalog_ApiHandler,Process_ApiHandler,FromDate,ToDate)
            logging.info(" [x] Done")
        except:
            logging.error("Message format is wrong")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    channel.start_consuming()


    
    


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        setup_logging(f"{sys.argv[1]}.log") 
        logging.info(f"Starting the Download {sys.argv[1]}...")
    else:
        setup_logging()
        logging.info("Starting the Download process...")
    
    main()