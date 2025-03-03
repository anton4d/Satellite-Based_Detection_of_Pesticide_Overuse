import logging, os, dotenv
from ToDB.Todb import ToDb 
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
    To_Db = ToDb(
        db_handler
    )
    imagePath="../Download/Pictures/FieldId1/Test.tiff"
    To_Db.InsertAveragePointsIntoDataBase(imagePath,1,"2024-05-01")
    
    logging.info("Stopping the Process aplication")


if __name__ == "__main__":
    setup_logging() 
    main()