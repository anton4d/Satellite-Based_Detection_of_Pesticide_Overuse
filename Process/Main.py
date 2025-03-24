import logging, os, dotenv
from ToDB.Todb import ToDb 
from ToDB.TiffIntersector import TiffIntersector
from Database.SQLHandler import SQLHandler

def setup_logging(log_file="Process.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def main():
    logging.info("Starting the Process aplication...")
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    Date = "2024-08-01"
    TIFF_ROOT = "../Download/Picturesres2048date20240801"
    output_folder = "Processed_TIFFs2048"
    os.makedirs(output_folder, exist_ok=True)

    intersector = TiffIntersector(
        db_handler, 
        TIFF_ROOT
    )

    To_Db = ToDb(
        db_handler, intersector
    )


    #imagePath="../Download/Pictures/FieldId1/Test.tiff"
    #To_Db.InsertAveragePointsIntoDataBase(imagePath,1,"2024-05-01")

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

    logging.info("Stopping the Process application.")


if __name__ == "__main__":
    setup_logging() 
    main()