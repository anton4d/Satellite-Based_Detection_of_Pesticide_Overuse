import logging, os, dotenv, sys, re, argparse
import time
from datetime import datetime, timedelta
from TiffPolygonProcesing.Compute_TIFF_Data import ComputeTiffData 
from TiffPolygonProcesing.TIFF_Intersector import TiffIntersector
from Database.SQL_Handler import SQLHandler


class TiffProcessor:
    DATEPATTERN = re.compile(
        r"^\w{1,}\|\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}\|(True|False)\|(Plot|ToDb)\|(NDVI|EVI|GNDVI|CVI|NDRE|ALL)\|(Kdeplot|Histogram|HeatMap|None)\|\d{1,2}\|(\d{1,}|All)\|(BB|Field)$"
    )

    def __init__(self, log_file="Process.log",log_folder="Logs/"):
        self.log_file = os.path.join(log_folder, f"{log_file}.log")
        self.db_handler = None
        self.to_db = None
        self.intersector = None
        self.BBTIFF_root = "../Download/PicturesBB"
        self.FieldTIFF_root = "../Download/PicturesField"
        self.output_folder = "Processed_TIFFs"
        self.setup_logging()
        self.load_environment()
        self.initialize_handlers()
        os.makedirs(self.output_folder, exist_ok=True)

    def setup_logging(self):
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        logging.getLogger('rasterio').setLevel(logging.ERROR)
        logging.info(f"Logging initialized. Writing to {self.log_file}")


    def load_environment(self):
        dotenv_path = dotenv.find_dotenv()
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path)
            logging.info(f"Loaded environment variables from {dotenv_path}")
        else:
            logging.warning("No .env file found.")

    def validate_message_data(self, message_data):
        if not self.DATEPATTERN.match(message_data):
            logging.error(f"Invalid MessagesData format: {message_data}")
            sys.exit(1)
        return message_data.split("|")

    def date_range(self, start_date, end_date):
        for n in range((end_date - start_date).days + 1):
            yield start_date + timedelta(n)

    def process(self, message_data):
        Work_order_id, start_date, end_date, save_tiff, data_mode, data_type, plot_type, crop_id, field_id, tiff_mode = self.validate_message_data(message_data)
        start_time = time.time()
        self.run_TIFF_Process(start_date, end_date, save_tiff, data_mode, data_type, plot_type, crop_id, field_id, tiff_mode)
        elapsed = time.time() - start_time
        logging.info(f"Process completed in {elapsed:.2f} seconds.")

    def initialize_handlers(self):
        self.db_handler = SQLHandler(
            host=os.getenv("SQLHOST"),
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWORD"),
            database=os.getenv("DBDB")
        )
        self.intersector = TiffIntersector(self.db_handler, self.BBTIFF_root)
        self.to_db = ComputeTiffData(self.db_handler)

    def run_TIFF_Process(self, start_date_str, end_date_str, save_tiff, data_mode, data_type, plot_type, crop_id, field_id, tiff_mode):
        logging.info(f"Starting TIFF Processing from {start_date_str} to {end_date_str} with crop id: {crop_id}")
        id_list = []
        last_polygon_year = None
        polygons = []
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        for date in self.date_range(start_date, end_date):
            date_str = date.strftime("%Y-%m-%d")
            year = date.strftime("%Y")
            small_id_list = []
            found_data = False
            start_time = time.time()
            logging.info(f"Processing the date: {date_str}")

            try:
                if tiff_mode == "BB":
                    if last_polygon_year != year:
                        logging.info(f"Getting polygons for year: {year} and CropId: {crop_id}")
                        polygons = self.db_handler.Get_Field_Polygon_Based_On_Year_CropId_And_FieldId(year, crop_id, field_id) \
                            if field_id != "All" else self.db_handler.Get_All_Polygons_Based_On_Year_And_CropId(year, crop_id)
                        last_polygon_year = year

                    if polygons is not None and not polygons.empty:
                        for _, row in polygons.iterrows():
                            polygon = row["geometry"]
                            field_id_val = row["FieldId"]
                            output_tiff = (
                                os.path.join(self.output_folder, f"processed_field_{field_id_val}_{date_str}_{tiff_mode}.tiff")
                                if save_tiff == "True" else "None"
                            )
                            intersections = self.intersector.find_intersecting_tiffs(polygon, date_str)
                            if intersections:
                                id_list.append(field_id_val)
                                small_id_list.append(field_id_val)
                                self.to_db.Compute_Data_From_Tiff(
                                    intersections, field_id_val, crop_id, date_str, year, polygon, output_tiff,
                                    data_type, data_mode, plot_type, tiff_mode
                                )
                                found_data = True
                    else:
                        logging.info(f"No polygons found for year {year} and crop ID {crop_id}")

                elif tiff_mode == "Field":
                    field_ids = [field_id] if field_id != "All" else self.db_handler.Get_All_Field_Based_On_YearAnd_CropId(crop_id, year)
                    for fid in field_ids:
                        tiff_path = f"{self.FieldTIFF_root}/FieldId{fid}/{date_str}.tiff"
                        folder_name = os.path.basename(os.path.dirname(tiff_path))
                        bb_id = folder_name.replace("FieldId", "")
                        bb_list = [tiff_path, bb_id]
                        output_tiff = (
                            os.path.join(self.output_folder, f"processed_field_{fid}_{date_str}_{tiff_mode}.tiff")
                            if save_tiff == "True" else "None"
                        )

                        if os.path.exists(tiff_path):
                            id_list.append(fid)
                            small_id_list.append(fid)
                            self.to_db.Compute_Data_From_Tiff(
                                bb_list, fid, crop_id, date_str, year, "null", output_tiff,
                                data_type, data_mode, plot_type, tiff_mode
                            )
                            found_data = True

                if not found_data:
                    logging.info(f"No data found for {date_str}")

            except Exception as e:
                logging.error(f"Error processing TIFF for {date_str}: {e}")

            duration = time.time() - start_time
            logging.info(f"Processed {date_str} in {duration:.2f}s for {len(small_id_list)} fields")

        total_duration = time.time() - time.mktime(start_date.timetuple())
        logging.info(f"Finished processing all TIFFs in {total_duration:.2f}s for {len(id_list)} fields")

   
def main():
    parser = argparse.ArgumentParser(description="Process TIFFs into NDVI data.")
    parser.add_argument("MessagesData", help="Format: start|end|saveTiff|DataMode|DataType|PlotType|CropId|FieldId|TiffMode")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    parser.add_argument("LogFolder", nargs="?", default="Logs/", help="Folder for logs (default: Logs/)")

    args = parser.parse_args()
    Tiffprocess = TiffProcessor(log_file=args.LogFile, log_folder=args.LogFolder)
    Tiffprocess.process(args.MessagesData)


if __name__ == "__main__":
    main()
