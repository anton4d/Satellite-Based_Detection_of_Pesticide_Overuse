import numpy as np
import rasterio, os, logging
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

class ToDb:
    def __init__(self, SqlHandler):
        self.SqlHandler = SqlHandler



    def InsertAveragePointsIntoDataBase(self, path, FieldID, Date, output_tiff="processed_field.tiff"):
        """Process a TIFF image: compute average NDVI & save a new TIFF with Red, NIR, Mask, and NDVI"""
        logging.info("Starting TIFF to database and new image process")

        try:
            with rasterio.open(path) as src:
  
                Red = src.read(1).astype(np.float32)
                NirRed = src.read(2).astype(np.float32) 
                DataMask = src.read(3)


                if Red.max() > 1.0 or NirRed.max() > 1.0:
                    Red /= 65535.0
                    NirRed /= 65535.0


                valid_pixels = DataMask == 1


                ndvi = np.full_like(Red, np.nan, dtype=np.float32)
                ndvi[valid_pixels] = (NirRed[valid_pixels] - Red[valid_pixels]) / (NirRed[valid_pixels] + Red[valid_pixels] + 1e-10)
                #plt.hist(ndvi)
                #plt.title(f"Histogram of ndvi of field {FieldID} on the date {Date}")
                #plt.savefig(f"Histogram of ndvi of field {FieldID} on the date {Date}")
                #print(ndvi.max())
                #print(ndvi.min())
                
                averageRed = np.nanmean(Red[valid_pixels]).item()
                MedianRed = np.nanmedian(Red[valid_pixels]).item()
                StdRed = np.nanstd(Red[valid_pixels]).item()
                MinRed = np.nanmin(Red[valid_pixels]).item()
                MaxRed = np.nanmax(Red[valid_pixels]).item()
                averageNirRed = np.nanmean(NirRed[valid_pixels]).item()
                MedianNirRed = np.nanmedian(NirRed[valid_pixels]).item()
                StdNirRed = np.nanstd(NirRed[valid_pixels]).item()
                MinNirRed = np.nanmin(NirRed[valid_pixels]).item()
                MaxNirRed = np.nanmax(NirRed[valid_pixels]).item()
                averageNdvi = np.nanmean(ndvi).item()
                MedianNdvi = np.nanmedian(ndvi[valid_pixels]).item()
                StdNdvi = np.nanstd(ndvi[valid_pixels]).item()
                MinNdvi = np.nanmin(ndvi[valid_pixels]).item()
                MaxNdvi = np.nanmax(ndvi[valid_pixels]).item()

                data_to_insert = [FieldID, Date, averageRed,MedianRed,StdRed,MinRed,MaxRed, 
                                  averageNirRed,MedianNirRed,StdNirRed,MinNirRed,MaxNirRed, 
                                  averageNdvi,MedianNdvi,StdNdvi,MinNdvi,MaxNdvi]
                logging.info(data_to_insert)
                self.SqlHandler.insertSimpleDataPointsForAfield(data_to_insert)
                logging.info(f"Inserted NDVI data for Field ID {FieldID} on {Date}.")

                out_meta = src.meta.copy()
                out_meta.update({
                    "count": 4, 
                    "dtype": "float32"
                })


                stacked_bands = np.stack([Red, NirRed, DataMask.astype(np.float32), ndvi])


                with rasterio.open(output_tiff, "w", **out_meta) as dest:
                    dest.write(stacked_bands)

                logging.info(f"New TIFF saved as {output_tiff} with Red, NIR, DataMask, and NDVI.")

        except Exception as e:
            logging.error(f" Error processing TIFF: {e}")




    def insertAllpointsIntoDataBase(self,path,FieldID,Date):
        """Take a tiff image and process every pixel into data for the database"""
        logging.info("starting tiff to database process")
        try:
            with rasterio.open(path) as src:
            # Read bands as UINT16
                Red = src.read(1).astype(np.float32)
                NirRed = src.read(2).astype(np.float32) 
                DataMask = src.read(3)

                if Red.max() > 1.0 or NirRed.max() > 1.0:
                    Red /= 65535.0
                    NirRed /= 65535.0

                # Calculate NDVI where DataMask == 1
                valid_pixels = DataMask == 1
                ndvi = np.where(valid_pixels, (NirRed - Red) / (NirRed + Red + 1e-10), np.nan)

                # Get geospatial transformation (pixel -> geographic coords)
                transform = src.transform

                # Prepare data for database
                data_to_insert = []
                rows, cols = np.where(valid_pixels)
                for row, col in zip(rows, cols):
                    x, y = rasterio.transform.xy(transform, row, col)
                    data_to_insert.append((
                        int(FieldID),
                        str(Date),
                        float(x),
                        float(y),
                        float(Red[row, col].item()),  
                        float(NirRed[row, col].item()),
                        float(ndvi[row, col].item()) if not np.isnan(ndvi[row, col]) else None  
                    ))

            self.SqlHandler.insertAllNdviForAField(data_to_insert)
            logging.info(f"Inserted {len(data_to_insert)} rows into the database for Field ID {FieldID} on {Date}.")


        except Exception as e:
                    logging.error(f"Failed to save tiff data to database: {e}") 
