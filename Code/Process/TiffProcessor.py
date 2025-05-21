import logging, os, dotenv, sys, re, argparse
import time
from datetime import datetime, timedelta
from TiffPolygonProcesing.ComputeTiffData import ComputeTiffData 
from TiffPolygonProcesing.TiffIntersector import TiffIntersector
from Database.SQLHandler import SQLHandler

workername = ""

def setup_logging(log_file="Process.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.getLogger('rasterio').setLevel(logging.ERROR)
    logging.info(f"Logging initialized. Writing to {log_file}")

def validate_message_data(message_data):
    DATEPATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}\|(True|False)\|(Plot|ToDb)\|(NDVI|EVI|GNDVI|CVI|NDRE|ALL)\|(Kdeplot|Histogram|HeatMap|None)\|\d{1,2}\|(\d{1,}|All)\|(BB|Field)$")
    if not DATEPATTERN.match(message_data):
        logging.error(f"Invalid MessagesData format: {message_data}")
        sys.exit(1)
    return message_data.split("|")

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def ProcessTiff(start_date, end_date, saveTiff, DataMode,DataType,PlotType,CropId,FieldId,TiffMode):
    logging.info(f"Starting TIFF Processing from {start_date} to {end_date} with crop id:{CropId}")
    
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    startTimeFull = time.time()
    tiff_root = "../Download/PicturesBB"
    idList = []
    last_polygon_year = None
    polygons = []

    for date in daterange(start_date, end_date):
        smallidList = []
        startTimeSmall = time.time()
        DateStr = date.strftime("%Y-%m-%d")
        year = date.strftime("%Y")
        output_folder = "Processed_TIFFs"
        os.makedirs(output_folder, exist_ok=True)
        logging.info(f"Processing the date: {DateStr}")

        try:
            intersector = TiffIntersector(db_handler, tiff_root)
            to_db = ComputeTiffData(db_handler)
            foundDataOnDate = False

            if TiffMode == "BB":
                if last_polygon_year != year:
                    logging.info(f"Getting polygons for year: {year} and CropId: {CropId}")
                    if FieldId == "All":
                        polygons = db_handler.getAllPolygonsBasedOnYearAndCropId(year=year, cropid=CropId)
                    else:
                        polygons = db_handler.getFieldPolygonBasedOnYearCropIdAndFieldId(year, CropId, FieldId)
                    last_polygon_year = year

                if polygons is not None and not polygons.empty:
                    for _, row in polygons.iterrows():
                        polygon = row["geometry"]
                        field_id = row["FieldId"]
                        output_tiff = (
                            os.path.join(output_folder, f"processed_field_{field_id}_{DateStr}_{TiffMode}.tiff")
                            if saveTiff == "True" else "None"
                        )

                        intersections = intersector.find_intersecting_tiffs(polygon, DateStr)
                        logging.debug(f"Looking for intersections for Field ID {field_id} on {DateStr}")
                        if intersections:
                            logging.debug(f"Processing Field ID {field_id} with intersections.")
                            idList.append(field_id)
                            smallidList.append(field_id)
                            to_db.ComputeDataFromTiff(
                                intersections, field_id, CropId, DateStr, year, polygon, output_tiff, DataType, DataMode, PlotType, TiffMode
                            )
                            foundDataOnDate = True
                        else:
                            logging.debug(f"No intersecting TIFFs for Field ID {field_id} on {DateStr}.")
                else:
                    logging.info(f"No polygons found in DB for year: {year} and CropId: {CropId}")
            elif TiffMode == "Field":
                if FieldId == "All":
                    idlist = db_handler.GetallFieldBasedOnYearAndCropId(CropId,year)
                    for id in idlist:
                        logging.debug(f"Processing Field ID: {id}")
                        idList.append(id)
                        smallidList.append(id)
                        output_tiff = (
                            os.path.join(output_folder, f"processed_field_{id}_{DateStr}_{TiffMode}.tiff")
                            if saveTiff == "True" else "None"
                        )
                        
                        tiffpath = f"../Download/PicturesField/FieldId{id}/{DateStr}.tiff"
                        foldername = os.path.basename(os.path.dirname(tiffpath))
                        BBid = foldername.replace("FieldId","")
                        BBLIST=[tiffpath,BBid]
                        logging.debug(f"Looking for TIFF file at: {tiffpath}")
                        if os.path.exists(tiffpath):
                            to_db.ComputeDataFromTiff(
                                BBLIST, id, CropId, DateStr, year, "null", output_tiff, DataType, DataMode, PlotType, TiffMode
                            )
                            foundDataOnDate = True
                        else:
                            logging.debug(f"TIFF file not found: {tiffpath}")
                else:
                    logging.debug(f"Processing specific Field ID: {FieldId}")
                    idList.append(FieldId)
                    smallidList.append(FieldId)
                    tiffpath = f"../Download/PicturesField/FieldId{FieldId}/{DateStr}.tiff"
                    foldername = os.path.basename(os.path.dirname(tiffpath))
                    BBid = foldername.replace("FieldId","")
                    BBLIST=[tiffpath,BBid]
                    output_tiff = (
                            os.path.join(output_folder, f"processed_field_{FieldId}_{DateStr}_{TiffMode}.tiff")
                            if saveTiff == "True" else "None"
                        )
                    logging.info(f"Looking for TIFF file at: {tiffpath}")
                    if os.path.exists(tiffpath):
                        to_db.ComputeDataFromTiff(
                            BBLIST, FieldId, CropId, DateStr, year, "null", output_tiff, DataType, DataMode, PlotType, TiffMode
                        )
                        foundDataOnDate = True
                    else:
                        logging.debug(f"TIFF file not found: {tiffpath}")

            if not foundDataOnDate:
                logging.info(f"No data found on date: {DateStr}")

        except Exception as e:
            logging.error(f"Error processing {tiff_root} for {DateStr}: {e}")


        endTimeSmall = time.time()
        fullTimeSmall = endTimeSmall - startTimeSmall
        logging.info(f"Completed TIFF processing for {DateStr}, took {fullTimeSmall:.2f} seconds over {len(smallidList)} fields")

    endTimeFull = time.time()
    fullTimeFull = endTimeFull - startTimeFull
    logging.info(f"Completed TIFF Processing from {start_date} to {end_date} with crop id: {CropId}")
    logging.info(f"It took {fullTimeFull:.2f} seconds over {len(idList)} fields")


def main():
    parser = argparse.ArgumentParser(description="Process TIFFs into NDVI data.")
    parser.add_argument("MessagesData", help="Data in format YYYY-MM-DD|yyyy-MM-DD|CropId")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    parser.add_argument("LogFolder", nargs="?",default="Logs/",help="The folder the log files should be saved to (default: Logs/")

    args = parser.parse_args()
    logfile = args.LogFolder+args.LogFile

    setup_logging(logfile)
    match = re.search(r"ProcessFor(.*?)\.log", logfile)
    if match:
        global workername
        workername = match.group(1)

    logging.info(f"Received MessagesData: {args.MessagesData}")
    start_str, end_str, saveTiff, DataMode,DataType,PlotType,CropID,FieldId,TiffMode = validate_message_data(args.MessagesData)
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    ProcessTiff(start_date, end_date, saveTiff, DataMode,DataType,PlotType,CropID,FieldId,TiffMode )

if __name__ == "__main__":
    main()
