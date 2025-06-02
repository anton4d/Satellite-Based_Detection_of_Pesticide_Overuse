import os, json,logging,dotenv,sys
import time

from SQL_Handler import SQL_Handler
from GeoJSON_To_DB import GeoJsonToDB
from GIS_Dataset_To_GeoJSON import Shapefile_To_Geojson

LOG_FILENAME = "Db_populator_process.log"


def Setup_Logging(filename):
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info("Logging initialized.")


def Load_Environment():
    dotenv_path = dotenv.find_dotenv()
    if dotenv_path:
        dotenv.load_dotenv(dotenv_path)
        logging.info(f"Loaded environment variables from {dotenv_path}")
    else:
        logging.warning("No .env file found.")


def Prompt_Year():
    year = input("Enter the year of the data: ").strip()
    logging.info(f"Year provided: {year}")
    return year


def Prompt_Filter_Values(Filter_Options):
    use_filter = input("Do you want to filter the shapefile? (y/n): ").strip().lower()
    Selected_Filters = []

    if use_filter == "y":
        print("\nAvailable filter options:")
        print('   '.join([f"[{i + 1}] {option}" for i, option in enumerate(Filter_Options)]))
        print("\nSelect filters by number. Type 'done' when finished.\n")

        while True:
            choice = input("Choice: ").strip().lower()

            if choice == 'done':
                if Selected_Filters:
                    break
                else:
                    print("You must select at least one filter.")
                    continue

            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(Filter_Options):
                    Selected = Filter_Options[idx]
                    if Selected not in Selected_Filters:
                        Selected_Filters.append(Selected)
                        print(f"Added: {Selected}")
                    else:
                        print("Already selected.")
                else:
                    print("Invalid number.")
            else:
                print("Invalid input. Please enter a number or 'done'.")
    
    return Selected_Filters
def Prompt_Geojson_Vs_ShapeFile():
    GeoVsShape = input("Do you want to use Geojson or shapeFile (g/s): ").strip().lower()
    if GeoVsShape == "s":
        return True
    elif GeoVsShape == "g":
        return False
    
def main():
    Setup_Logging(LOG_FILENAME)
    Load_Environment()

    db_handler = SQL_Handler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    
    year = Prompt_Year()
    geojson_path = f"../Shapefiles/{year}.geojson"
    value = Prompt_Geojson_Vs_ShapeFile()
    if value is None:
        sys.exit(1) 
        logging.error("wrong value given to the prompt")
    if value:
        Filter_Options = db_handler.Get_All_Crop_Types()
        Selected_Filters = Prompt_Filter_Values(Filter_Options)
        Shapefile_Path = input("Enter path to the GIS shapefile from DEPA: ").strip()
        logging.info(f"Shapefile path provided: {Shapefile_Path}")
        Shapefile_To_Geojson(Shapefile_Path, geojson_path, Selected_Filters)
        
    Geojson_To_DB = GeoJsonToDB(geojson_path, db_handler, year)
    
    save_to_db = input("Do you want to save the GeoJSON to the database? (y/n): ").strip().lower()

    if save_to_db == "y":
        Start_Time = time.time()
        Geojson_To_DB.Process_Geojson()
        End_Time = time.time()
        Full_Time = End_Time - Start_Time
        logging.info(f"GeoJSON successfully saved to the database. it took {Full_Time} seconds")
    else:
        logging.info("Skipped saving GeoJSON to the database.")


if __name__ == "__main__":
    main()
