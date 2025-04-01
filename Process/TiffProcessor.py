import logging, os, dotenv, sys, re, argparse
from datetime import datetime, timedelta
from ToDB.Todb import ToDb 
from ToDB.TiffIntersector import TiffIntersector
from Database.SQLHandler import SQLHandler

workername = ""

def setup_logging(log_file="Process.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Logging initialized. Writing to {log_file}")

def validate_message_data(message_data):
    DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}$")
    if not DATEPATTERN.match(message_data):
        logging.error(f"Invalid MessagesData format: {message_data}")
        sys.exit(1)
    return message_data.split("|")

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def ProcessTiff(start_date, end_date):
    logging.info(f"Starting TIFF Processing from {start_date} to {end_date}")
    
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    tiff_root = "../Download/Pictures"

    for date in daterange(start_date, end_date):
        DateStr = date.strftime("%Y-%m-%d")
        output_folder = f"Processed_TIFFs"
        os.makedirs(output_folder, exist_ok=True)

        try:
            intersector = TiffIntersector(db_handler, tiff_root)
            to_db = ToDb(db_handler, intersector)
            polygons = intersector.get_wkt_polygons()

            if polygons is not None and not polygons.empty:
                for _, row in polygons.iterrows():
                    polygon = row["geometry"]
                    field_id = row["FieldId"]
                    output_tiff = os.path.join(output_folder, f"processed_field_{field_id}_{DateStr}.tiff")

                    intersections = intersector.find_intersecting_tiffs(polygon, DateStr)

                    if intersections:
                        logging.info(f"Processing Field ID {field_id} from {tiff_root} for {DateStr}.")
                        to_db.InsertAveragePointsIntoDataBase(intersections, field_id, DateStr, polygon, output_tiff)
                    else:
                        logging.info(f"No intersecting TIFFs for Field ID {field_id} on {DateStr} in {tiff_root}.")
            else:
                logging.info(f"No polygons found in DB for {tiff_root}")
        except Exception as e:
            logging.error(f"Error processing {tiff_root} for {DateStr}: {e}")

    logging.info(f"Completed TIFF processing for {DateStr}")

def main():
    parser = argparse.ArgumentParser(description="Process TIFFs into NDVI data.")
    parser.add_argument("MessagesData", help="Data in format YYYY-MM-DD")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")

    args = parser.parse_args()
    logfile = args.LogFile
    setup_logging(logfile)
    match = re.search(r"ProcessFor(.*?)\.log", logfile)
    if match:
        global workername
        workername = match.group(1)

    logging.info(f"Received MessagesData: {args.MessagesData}")
    start_str, end_str = validate_message_data(args.MessagesData)
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    ProcessTiff(start_date, end_date)

if __name__ == "__main__":
    main()
