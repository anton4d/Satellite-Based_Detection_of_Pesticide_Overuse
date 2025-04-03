import numpy as np
import mysql.connector
import rasterio, os, logging,json
import matplotlib
import geopandas as gpd
from shapely import wkt
from rasterio.mask import mask
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt



class ToDb:
    def __init__(self, SqlHandler, intersector):
        self.SqlHandler = SqlHandler

    def InsertAveragePointsIntoDataBase(self, path, FieldID, Date, polygon, output_tiff):
        """Process a TIFF image: compute average NDVI & save a new TIFF with out_image_red, NIR, Mask, and NDVI"""
        logging.info("Starting TIFF to database and new image process")

        try:
            with rasterio.open(path) as src:
                if src.crs != "EPSG:4326":
                    polygon = polygon.to_crs(src.crs)
                foldername = os.path.basename(os.path.dirname(path))
                BBid = foldername.replace("FieldId","")
                #out_image_red = src.read(1).astype(np.float32)
                #out_image_nir = src.read(2).astype(np.float32) 
                #DataMask = src.read(3)

                # Mask data to the polygon
                
                out_images, out_transform = mask(src, [polygon], crop=True, filled=True, nodata=0, indexes=[1, 2, 3])

                out_image_red = out_images[0].astype(np.float32)
                out_image_nir = out_images[1].astype(np.float32)
                out_image_dMask = out_images[2]
                try:
                    while out_images is not None and out_images.size > 0:
                        if out_image_red.max() > 1.0 or out_image_nir.max() > 1.0:
                            out_image_red /= 65535.0
                            out_image_nir /= 65535.0
                        logging.info(f"Masked Red shape: {out_image_red.shape}, NIR shape: {out_image_nir.shape}, Mask shape: {out_image_dMask.shape}")
                        valid_pixels = out_image_dMask == 1


                        ndvi = np.full_like(out_image_red, np.nan, dtype=np.float32)
                        ndvi[valid_pixels] = (out_image_nir[valid_pixels] - out_image_red[valid_pixels]) / (out_image_nir[valid_pixels] + out_image_red[valid_pixels] + 1e-10)
                        #plt.hist(ndvi)
                        #plt.title(f"Histogram of ndvi of field {FieldID} on the date {Date}")
                        #plt.savefig(f"Histogram of ndvi of field {FieldID} on the date {Date}")
                        #print(ndvi.max())
                        #print(ndvi.min())
                        
                        averageRed = np.nanmean(out_image_red[valid_pixels]).item()
                        MedianRed = np.nanmedian(out_image_red[valid_pixels]).item()
                        StdRed = np.nanstd(out_image_red[valid_pixels]).item()
                        MinRed = np.nanmin(out_image_red[valid_pixels]).item()
                        MaxRed = np.nanmax(out_image_red[valid_pixels]).item()
                        hist, bins = np.histogram(out_image_red[valid_pixels], density=True)
                        HistJsonRed = json.dumps({"hist": hist.tolist(), "bins": bins.tolist()}) 

                        averageNirRed = np.nanmean(out_image_nir[valid_pixels]).item()
                        MedianNirRed = np.nanmedian(out_image_nir[valid_pixels]).item()
                        StdNirRed = np.nanstd(out_image_nir[valid_pixels]).item()
                        MinNirRed = np.nanmin(out_image_nir[valid_pixels]).item()
                        MaxNirRed = np.nanmax(out_image_nir[valid_pixels]).item()
                        hist, bins = np.histogram(out_image_nir[valid_pixels], density=True)
                        HistJsonNir = json.dumps({"hist": hist.tolist(), "bins": bins.tolist()}) 

                        averageNdvi = np.nanmean(ndvi).item()
                        MedianNdvi = np.nanmedian(ndvi[valid_pixels]).item()
                        StdNdvi = np.nanstd(ndvi[valid_pixels]).item()
                        MinNdvi = np.nanmin(ndvi[valid_pixels]).item()
                        MaxNdvi = np.nanmax(ndvi[valid_pixels]).item()
                        hist, bins = np.histogram(ndvi[valid_pixels], density=True)
                        HistJsonNdvi = json.dumps({"hist": hist.tolist(), "bins": bins.tolist()}) 


                        data_to_insert = [FieldID, Date,BBid, averageRed,MedianRed,StdRed,MinRed,MaxRed, 
                                        averageNirRed,MedianNirRed,StdNirRed,MinNirRed,MaxNirRed, 
                                        averageNdvi,MedianNdvi,StdNdvi,MinNdvi,MaxNdvi]
                        logging.info(data_to_insert)
                        self.SqlHandler.insertSimpleDataPointsForAfield(data_to_insert)
                        logging.info(f"Inserted NDVI data for Field ID {FieldID} on {Date}.")

                        out_meta = src.meta.copy()
                        out_meta.update({
                        "driver": "GTiff",
                        "height": out_image_red.shape[0],
                        "width": out_image_red.shape[1],
                        "transform": out_transform,
                        "count": 4,
                        "dtype": "float32"
                        })

                        #stacked_bands = np.stack([
                        #out_image_red, 
                        #out_image_nir, 
                        #DataMask.astype(np.float32), 
                        #ndvi])
                        stacked_bands = np.stack([
                        out_image_red, 
                        out_image_nir, 
                        out_image_dMask.astype(np.float32),
                        ndvi
                        ])


                        with rasterio.open(output_tiff, "w", **out_meta) as dest:
                            dest.write(stacked_bands)

                        logging.info(f"New TIFF saved as {output_tiff} with out_image_red, NIR, DataMask, and NDVI.")
                        break
                except Exception as er:
                    logging.error(f" out_images is empty: {er}")

        except Exception as e:
            logging.error(f" Error processing TIFF: {e}")




    def insertAllpointsIntoDataBase(self,path,FieldID,Date):
        """Take a tiff image and process every pixel into data for the database"""
        logging.info("starting tiff to database process")
        try:
            with rasterio.open(path) as src:
            # Read bands as UINT16
                out_image_red = src.read(1).astype(np.float32)
                out_image_nir = src.read(2).astype(np.float32) 
                DataMask = src.read(3)

                if out_image_red.max() > 1.0 or out_image_nir.max() > 1.0:
                    out_image_red /= 65535.0
                    out_image_nir /= 65535.0

                # Calculate NDVI where DataMask == 1
                valid_pixels = DataMask == 1
                ndvi = np.where(valid_pixels, (out_image_nir - out_image_red) / (out_image_nir + out_image_red + 1e-10), np.nan)

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
                        float(out_image_red[row, col].item()),  
                        float(out_image_nir[row, col].item()),
                        float(ndvi[row, col].item()) if not np.isnan(ndvi[row, col]) else None  
                    ))

            self.SqlHandler.insertAllNdviForAField(data_to_insert)
            logging.info(f"Inserted {len(data_to_insert)} rows into the database for Field ID {FieldID} on {Date}.")


        except Exception as e:
                    logging.error(f"Failed to save tiff data to database: {e}") 
