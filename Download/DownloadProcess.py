import pandas as pd
import logging, dotenv, os, time , sys,csv,argparse,re
from datetime import datetime
from shapely.geometry import box
from shapely.wkt import dumps
from Gis.WktHandler import ConvertWktToNestedCords
from Database.SQLHandler import SQLHandler
from Api.CatalogApiHandler import CatalogApiHandler
from Api.TokenApiHandler import TokenApiHandler
from Api.ProcessApiHandler import ProcessApiHandler
    
workername = ""

def setup_logging(log_file="Download.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Logging initialized. Writing to {log_file}")

def DownloadProcess(FromDate,ToDate,mode,region,resolution,cropid,FieldID):
    logging.info(f"Starting the DownloadProcess using the dates {FromDate}, {ToDate}, the mode: {mode}, the region: {region} resolution: {resolution} and cropid: {cropid}")
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    clienID,ClientSecret = db_handler.ApiUser("None")
    Token_ApiHandler = TokenApiHandler(
        ClientId=clienID,
        ClientSecret=ClientSecret,
        SQLHandler=db_handler
    )

    Catalog_ApiHandler = CatalogApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        dbHandler=db_handler,
        ClientID =clienID
    )
    
    Process_ApiHandler = ProcessApiHandler(
        ApiToken=os.getenv("APIToken"),
        TokenApiHandler=Token_ApiHandler,
        SQLHandler=db_handler,
        ClientID =clienID
    )
    
    polygondict = {}
    if mode == "Field":
        logging.info("DownloadProcess will use Fields")
        if FieldID == "all":
            if cropid == "all":
                polygondict = db_handler.getAllFieldPolygons()
            else:
                FromDate_parsed = datetime.strptime(FromDate, "%Y-%m-%dT%H:%M:%SZ")
                year = FromDate_parsed.strftime("%Y")
                polygondict = db_handler.GetAllFieldIdAndPolygonsBasedOnYearAndCropId(cropid,year)
        else:
            polygondict = db_handler.getAllFieldPolygonsBasedOnId(FieldID)
    elif mode == "BB":
        logging.info("DownloadProcess will use boundingboxes")
        if region == "Fyn":
            logging.info("The region of the boundingboxes is fyn")
            csv_file ="../Shapefiles/CSV/fynBB.csv"
        else:
            csv_file = "../Shapefiles/CSV/denmarkBB.csv"
            logging.info("The region of the boundingboxes is all of denmark")
        df_regions = pd.read_csv(csv_file)
        for x,row in df_regions.iterrows():

            
            ID = row["ID"]
            FieldId = f"RegionBB_{ID}"
            min_lon= row["min_lon"]
            min_lat= row["min_lat"]
            max_lon=row["max_lon"]
            max_lat=row["max_lat"]
            logging.debug(f"Processing regionBB {ID} with geo data: minlon {min_lon} minlat {min_lat} maxlon {max_lon} maxlat {max_lat}")
            
            bbPoly = box(min_lon, min_lat, max_lon, max_lat)
           
            bbPolyWKT = dumps(bbPoly)
            polygondict.update({FieldId:bbPolyWKT})
    else:
        logging.error("Messeage has no correct mode. supported mode is Field or BB")
        sys.exit(4)
            
    
    """
    fields = ["ID","FullDate","DateOnly","Platform","ClouCover"]
    with open("DateMetaData.csv", 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
    """

    
    for id,wkt in polygondict.items():
        FieldId = str(id)
        folderName = f"Pictures{mode}/FieldId{FieldId}"
        sleeptimer=1
        nestedBB = ConvertWktToNestedCords(wkt)
        CatalogData = Catalog_ApiHandler.GetPictureDates(Polygon=nestedBB, FieldId=FieldId,FromDate=FromDate,ToDate=ToDate)
        if not CatalogData:
            logging.warning(f"No image dates found for polygon with id {FieldId}, skipping...")
            continue

        logging.info(f"Catalog API found {len(CatalogData)} dates for polygon with id {FieldId}")
        dotenv.load_dotenv(dotenvFile)
        time.sleep(sleeptimer)

        for date in CatalogData:
            try:
                Process_ApiHandler.processDateIntoImages(date, nestedBB, FieldId,resolution,folderName)
            except Exception as e:
                logging.error(f"Error processing date {date} for polygon with id {FieldId}: {e}")

    logging.info(f"Completed the download proccess on the dates from: {FromDate} To: {ToDate}")
    
    
def validate_message_data(message_data):
    """Validates the MessagesData format."""
    DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|(BB|Field)\|(Fyn| )\|\d{3,4}\|(\d{1,}|all)\|(\d{1,}|all)$")
    if not DATEPATTERN.match(message_data):
        logging.error(f"Invalid MessagesData format: {message_data}")
        sys.exit(4)
    return message_data.split("|")

def main():
    """Main function to handle argument parsing and processing."""
    parser = argparse.ArgumentParser(description="Process and download satellite data.")
    parser.add_argument("MessagesData", help="Data in format YYYY-MM-DDTHH:MM:SSZ|YYYY-MM-DDTHH:MM:SSZ|Mode|region|resolution")
    parser.add_argument("LogFile", nargs="?", default="download_process.log", help="Log file name (default: download_process.log)")
    parser.add_argument("LogFolder", nargs="?",default="Logs/",help="The folder the log files should be saved to (default: Logs/")

    args = parser.parse_args()
    logfile = args.LogFolder+args.LogFile

    setup_logging(logfile)
    match = re.search(r"ProcessFor(.*?)\.log", logfile)
    if match:
        global workername
        workername = match.group(1)
        print(workername)
    logging.info(f"Received MessagesData: {args.MessagesData}")

    FromDate, ToDate, Mode, Region, Resolution, Cropid, FieldId = validate_message_data(args.MessagesData)
    logging.info(f"Validated date range: From {FromDate} to {ToDate}")
    starttime = time.time()
    DownloadProcess(FromDate, ToDate,Mode,Region,Resolution,Cropid,FieldId)
    endTime = time.time()
    logging.info(f"The process took {endTime-starttime} secounds")

if __name__ == "__main__":
    main()