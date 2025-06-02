import pandas as pd
import logging, dotenv, os, time, sys, csv, argparse, re
from datetime import datetime
from shapely.geometry import box
from shapely.wkt import dumps
from Gis.WktHandler import Convert_Wkt_To_NestedCords, get_pixel_dimensions
from Database.SQLHandler import SQLHandler
from Api.CatalogApiHandler import CatalogApiHandler
from Api.TokenApiHandler import TokenApiHandler
from Api.ProcessApiHandler import ProcessApiHandler

class TIFFDataDownloader:
    def __init__(self, log_file="download_process.log", log_folder="Logs/"):
        self.log_file = os.path.join(log_folder, log_file)
        self.db_handler = None
        self.catalog_api = None
        self.process_api = None
        self.token_api = None
        self.setup_logging()
        self.Load_Environment()

    def setup_logging(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        logging.info(f"Logging initialized. Writing to {self.log_file}")

    def Load_Environment(self):
        dotenv_path = dotenv.find_dotenv()
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path)
            logging.info(f"Loaded environment variables from {dotenv_path}")
        else:
            logging.warning("No .env file found.")
            sys.exit(2)

    def validate_message_data(self, message_data):
        pattern = re.compile(r"^\w{1,}\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\|(BB|Field)\|(Fyn|all|Test)\|(\d{3,4}|-1)\|(\d{1,}|all)\|(\d{1,}|all)$")
        if not pattern.match(message_data):
            logging.error(f"Invalid MessagesData format: {message_data}")
            sys.exit(4)
        return message_data.split("|")

    def initialize_handlers(self):
        self.db_handler = SQLHandler(
            host=os.getenv("SQLHOST"),
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWORD"),
            database=os.getenv("DBDB")
        )
        client_id, client_secret = self.db_handler.Api_User("None")
        self.token_api = TokenApiHandler(client_id, client_secret, self.db_handler)
        token,client_id = self.token_api.Set_A_New_User_If_Any(None)
        self.catalog_api = CatalogApiHandler(token, self.token_api, self.db_handler, client_id)
        self.process_api = ProcessApiHandler(token, self.token_api, self.db_handler, client_id)

    def get_polygons(self, mode, region, cropid, field_id, from_date):
        polygondict = {}
        logging.info(f"Region: {region}")
        if mode == "Field":
            logging.info("Mode: Field")
            
            if field_id == "all":
                if cropid == "all":
                    polygondict = self.db_handler.get_All_Field_Polygons() if region == "all" else self.db_handler.Get_All_Field_Polygons_Based_On_Region(region)
                else:
                    year = datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y")
                    if region == "all":
                        polygondict = self.db_handler.Get_All_FieldId_And_Polygons_Based_On_Year_And_CropId(cropid, year)
                    else:
                        polygondict = self.db_handler.Get_All_FieldId_And_Polygons_Based_On_Year_And_CropId_And_Region(cropid, year, region)
            else:
                polygondict = self.db_handler.Get_All_Field_Polygons_Based_On_Id(field_id)

        elif mode == "BB":
            csv_file = "../Shapefiles/CSV/fynBB.csv" if region == "Fyn" else "../Shapefiles/CSV/denmarkBB.csv"
            df = pd.read_csv(csv_file)
            for _, row in df.iterrows():
                field_id = f"RegionBB_{row['ID']}"
                bb_poly = box(row["min_lon"], row["min_lat"], row["max_lon"], row["max_lat"])
                polygondict[field_id] = dumps(bb_poly)
        else:
            logging.error("Invalid mode. Supported: Field, BB")
            sys.exit(4)

        return polygondict

    def run_download_process(self, from_date, to_date, mode, region, resolution, cropid, field_id):
        self.initialize_handlers()
        polygons = self.get_polygons(mode, region, cropid, field_id, from_date)

        for count, (fid, wkt) in enumerate(polygons.items(), start=1):
            nested_bb = Convert_Wkt_To_NestedCords(wkt)
            width, height = get_pixel_dimensions(nested_bb) if resolution == "-1" else (resolution, resolution)
            folder = f"Pictures{mode}/FieldId{fid}"
            logging.info(f"Processing {fid} ({count}/{len(polygons)})")

            dates = self.catalog_api.Get_Picture_Dates(nested_bb, fid, from_date, to_date)
            if not dates:
                logging.warning(f"No dates for {fid}")
                continue

            for date in dates:
                try:
                    self.process_api.Process_Date_Into_TIFFs(date, nested_bb, fid, width, height, folder)
                except Exception as e:
                    logging.error(f"Error processing {date} for {fid}: {e}")

    def process(self, message_data):
        Work_order_id,from_date, to_date, mode, region, resolution, cropid, field_id = self.validate_message_data(message_data)
        start_time = time.time()
        self.run_download_process(from_date, to_date, mode, region, resolution, cropid, field_id)
        elapsed = time.time() - start_time
        logging.info(f"Process completed in {elapsed:.2f} seconds.")


def main():
    parser = argparse.ArgumentParser(description="Download satellite data based on specified parameters.")
    parser.add_argument("MessagesData", help="Format: WorkOrderID|YYYY-MM-DDTHH:MM:SSZ|YYYY-MM-DDTHH:MM:SSZ|Mode|Region|Resolution|CropID|FieldID")
    parser.add_argument("LogFile", nargs="?", default="download_process.log", help="Name of the log file")
    parser.add_argument("LogFolder", nargs="?", default="Logs/", help="Log folder path")

    args = parser.parse_args()
    downloader = TIFFDataDownloader(log_file=args.LogFile, log_folder=args.LogFolder)

    downloader.process(args.MessagesData)
    

if __name__ == "__main__":
    main()
