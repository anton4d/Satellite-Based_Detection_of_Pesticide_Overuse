import logging, os, dotenv, sys, re, argparse
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
    DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\|\d{4}$")
    if not DATEPATTERN.match(message_data):
        logging.error(f"Invalid MessagesData format: {message_data}")
        sys.exit(1)
    return message_data.split("|")

def ProcessTiff(Date, resolution):
    logging.info(f"Starting the TIFF Processing for date: {Date} and resolution: {resolution}")
    
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    TIFF_ROOT = f"../Download/Picturesres{resolution}date{Date.replace('-', '')}"
    output_folder = f"Processed_TIFFs{resolution}"
    os.makedirs(output_folder, exist_ok=True)

    intersector = TiffIntersector(db_handler, TIFF_ROOT)
    To_Db = ToDb(db_handler, intersector)
    polygons = intersector.get_wkt_polygons()

    if polygons is not None and not polygons.empty:
        for _, row in polygons.iterrows():
            polygon = row["geometry"]
            FieldID = row["FieldId"]
            output_tiff = os.path.join(output_folder, f"processed_field_{FieldID}.tiff")
            intersections = intersector.find_intersecting_tiffs(polygon, Date)

            if intersections:
                logging.info(f"Processing Field ID {FieldID} for {Date}.")
                To_Db.InsertAveragePointsIntoDataBase(intersections, FieldID, Date, polygon, output_tiff)
            else:
                logging.info(f"No intersecting TIFFs for Field ID {FieldID} on {Date}.")
    else:
        logging.info("No polygons found in the database.")

    logging.info("TIFF Processing complete.")

def main():
    parser = argparse.ArgumentParser(description="Process TIFFs into NDVI data.")
    parser.add_argument("MessagesData", help="Data in format YYYY-MM-DD|resolution")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")

    args = parser.parse_args()
    logfile = args.LogFile
    setup_logging(logfile)
    match = re.search(r"ProcessFor(.*?)\.log", logfile)
    if match:
        global workername
        workername = match.group(1)

    logging.info(f"Received MessagesData: {args.MessagesData}")
    Date, Resolution = validate_message_data(args.MessagesData)
    ProcessTiff(Date, Resolution)

if __name__ == "__main__":
    main()
