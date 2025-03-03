import numpy as np
import rasterio, os, logging

class ToDb:
    def __init__(self, SqlHandler):
        self.SqlHandler = SqlHandler

    def CalculateNdvi(self,NirRed,Red):
        """Take nir red and red and calculates the ndvi index"""
        pass

    def TiffToDatabase(self,path):
        """Take a tiff image and process it into data for the database"""
        logging.info("starting tiff to database process")
        try:
            with rasterio.open(path) as src:
                logging.info(f"{src.width}, {src.height}")
                logging.info(src.crs)
                logging.info(src.transform)
                logging.info(src.count)
                logging.info(src.indexes)
                logging.info(src.bounds)
                Red, NirRed, DataMask = src.read()
                logging.info(Red)
                logging.info(NirRed)
                logging.info(DataMask)
        except Exception as e:
                    logging.error(f"Failed to save tiff data to database: {e}")
