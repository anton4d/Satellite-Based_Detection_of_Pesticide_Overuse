import os, json,logging,dotenv,sys

from SQLHandler import SQLHandler
from geojsonToDB import GeoJsonToDB
from ShapefileDatasetToGeoJSON import shapefile_to_geojson

LOG_FILENAME = "Db_populator_process.log"


def setup_logging(filename):
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info("Logging initialized.")


def load_environment():
    dotenv_path = dotenv.find_dotenv()
    if dotenv_path:
        dotenv.load_dotenv(dotenv_path)
        logging.info(f"Loaded environment variables from {dotenv_path}")
    else:
        logging.warning("No .env file found.")


def prompt_year():
    year = input("Enter the year of the data: ").strip()
    logging.info(f"Year provided: {year}")
    return year


def prompt_filter_values(FILTER_OPTIONS):
    use_filter = input("Do you want to filter the shapefile? (y/n): ").strip().lower()
    selected_filters = []

    if use_filter == "y":
        print("\nAvailable filter options:")
        print('   '.join([f"[{i + 1}] {option}" for i, option in enumerate(FILTER_OPTIONS)]))
        print("\nSelect filters by number. Type 'done' when finished.\n")

        while True:
            choice = input("Choice: ").strip().lower()

            if choice == 'done':
                if selected_filters:
                    break
                else:
                    print("⚠️ You must select at least one filter.")
                    continue

            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(FILTER_OPTIONS):
                    selected = FILTER_OPTIONS[idx]
                    if selected not in selected_filters:
                        selected_filters.append(selected)
                        print(f"✔ Added: {selected}")
                    else:
                        print("❌ Already selected.")
                else:
                    print("❌ Invalid number.")
            else:
                print("❌ Invalid input. Please enter a number or 'done'.")
    
    return selected_filters
def prompt_GeojsonVsShapeFile():
    GeoVsShape = input("Do you want to use Geojson or shapeFile (g/s): ").strip().lower()
    if GeoVsShape == "s":
        return True
    elif GeoVsShape == "g":
        return False
def saveCropType(db_handler):
    with open('Croptype.json') as f:
        d = json.load(f)
        listoftCrop = d["Croptype"]
        db_handler.InsertCropType(listoftCrop)

def main():
    setup_logging(LOG_FILENAME)
    load_environment()

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    
    year = prompt_year()
    geojson_path = f"../Shapefiles/{year}.geojson"
    saveCropType(db_handler)
    value = prompt_GeojsonVsShapeFile()
    if value is None:
        sys.exit(1) 
        logging.error("wrong value given to the prompt")
    if value:
        FILTER_OPTIONS = db_handler.GetAllCropTypes()
        selected_filters = prompt_filter_values(FILTER_OPTIONS)
        shapefile_path = input("Enter path to the GIS shapefile from DEPA: ").strip()
        logging.info(f"Shapefile path provided: {shapefile_path}")
        shapefile_to_geojson(shapefile_path, geojson_path, selected_filters)
        
    geojson_to_db = GeoJsonToDB(geojson_path, db_handler, year)
    
    save_to_db = input("Do you want to save the GeoJSON to the database? (y/n): ").strip().lower()

    if save_to_db == "y":
        geojson_to_db.process_geojson()
        logging.info("GeoJSON successfully saved to the database.")
    else:
        logging.info("Skipped saving GeoJSON to the database.")


if __name__ == "__main__":
    main()
