import numpy as np
import mysql.connector
import rasterio, os, logging,json
import matplotlib
import geopandas as gpd
from shapely import wkt
from rasterio.mask import mask
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from datetime import datetime
from Database.SQLHandler import SQLHandler
import seaborn as sns

class ComputeTiffData:
    def __init__(self, SqlHandler: SQLHandler):
        self.SqlHandler = SqlHandler



    def ComputeDataFromTiff(self, BBLIST, FieldID, CropId, Date, year, polygon, outputTiff, DataType, DataMode, PlotType,TiffMode):
        """Process a TIFF image: compute average NDVI & save a new TIFF with out_image_red, NIR, Mask, and NDVI"""
        logging.info(f"Starting TIFF to database and new image process with tiffmode {TiffMode}")

        try:
            path = BBLIST[0]
            BBLIST.pop(0)
            with rasterio.open(path) as src:
                if src.crs != "EPSG:4326" and polygon is not None:
                    polygon = polygon.to_crs(src.crs)

                if TiffMode == "BB":
                    logging.info(f"Applying mask for FieldID {FieldID}")
                    outImages, out_transform = mask(src, [polygon], crop=True, filled=True, nodata=0, indexes=[1, 2, 3, 4, 5,6])
                    Blue = outImages[0].astype(np.float32)
                    Green = outImages[1].astype(np.float32)
                    Red = outImages[2].astype(np.float32)
                    RedEdge = outImages[3].astype(np.float32)
                    Nir = outImages[4].astype(np.float32)
                    DMask = outImages[5]
                    validPixels = DMask == 1
                elif TiffMode == "Field":
                    logging.info(f"Using full image for FieldID {FieldID} (no masking)")
                    image = src.read([1, 2, 3, 4, 5,6]) 
                    Blue = image[0].astype(np.float32)
                    Green = image[1].astype(np.float32)
                    Red = image[2].astype(np.float32)
                    RedEdge = image[3].astype(np.float32)
                    Nir = image[4].astype(np.float32)
                    DMask = image[5]
                    validPixels = DMask == 1
                    out_transform = src.transform

                logging.debug(f"Masked Red shape: {Red.shape}, NIR shape: {Nir.shape}, Mask shape: {DMask.shape}")
                validPixels = DMask == 1
                Blue = Blue[validPixels]
                Green = Green[validPixels]
                Red = Red[validPixels]
                RedEdge = RedEdge[validPixels]
                Nir = Nir[validPixels]
                DataTypeMap = {
                    1: lambda: Green,
                    2: lambda: Blue,
                    3: lambda: Red,
                    4: lambda: Nir,
                    5: lambda: self.CalculateNdvi(Red, Nir),
                    6: lambda: self.CalculateEvi(Nir, Red, Blue),
                    7: lambda: self.CalculateGndvi(Nir, Green),
                    8: lambda: self.CalculateCvi(Nir, Red, Green),
                    9: lambda: self.CalculateNdre(Nir,RedEdge),
                    10: lambda: RedEdge
                }
                FieldCropYearId = self.SqlHandler.GetFieldCropYearIdBasedOnYearCropIdAndFieldId(year, CropId, FieldID)
                dataTypes = self.SqlHandler.getDatatypeIds(DataType)
                AllValues = []
                for datatype in dataTypes:
                    value = DataTypeMap.get(datatype)
                    if value: 
                        result = value()
                        AllValues.append(result)
                        if (DataMode == "ToDb"):
                            self.saveToDatabase(BBLIST, Date, result, datatype, FieldCropYearId,TiffMode)
                        elif (DataMode == "Plot"):
                            if PlotType == "Histogram":
                                self.calculateHistogram(result,datatype,FieldCropYearId,Date,TiffMode)
                            elif PlotType == "Kdeplot":
                                self.calculateKde(result,datatype, FieldCropYearId,Date,TiffMode)
                            elif PlotType == "HeatMap":
                                self.CalculateHeatMap(validPixels,result,Date,datatype,FieldCropYearId,TiffMode)


                
                if outputTiff != "None":
                    logging.debug(f"About to reconstruct {len(AllValues)} bands")
                    assert len(AllValues) > 0, "No bands to write!"
                    for i, arr in enumerate(AllValues):
                        logging.debug(f"  flat band {i}: ndim={arr.ndim}, size={arr.size}")
                        if arr.ndim != 1:
                            raise ValueError(f"Band {i} is not 1D!")
                    self.SaveReconstructedTiff(
                            flat_results=AllValues,
                            mask_2d=validPixels,        
                            src_meta=src.meta,
                            out_transform=out_transform,
                            output_path=outputTiff
                        )

        except Exception as e:
            logging.error(f"Error processing TIFF: {e}")



    def Reconstruct2dNpArrey(self, flat_array: np.ndarray, mask_2d: np.ndarray) -> np.ndarray:
        """
        Given a 1D array of values for valid pixels and the 2D boolean mask,
        reconstruct a full 2D array (same shape as mask) with np.nan elsewhere.
        """
        full = np.full(mask_2d.shape, np.nan, dtype=np.float32)
        full[mask_2d] = flat_array
        return full

    def SaveReconstructedTiff(self,
                                 flat_results: list[np.ndarray],
                                 mask_2d: np.ndarray,
                                 src_meta: dict,
                                 out_transform,
                                 output_path: str):
        """
        Reconstruct all flat_results into 2D images, stack them, and write to output_path.
        """

        bands_2d = [self.Reconstruct2dNpArrey(arr, mask_2d) for arr in flat_results]


        out_meta = src_meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mask_2d.shape[0],
            "width":  mask_2d.shape[1],
            "transform": out_transform,
            "count": len(bands_2d),
            "dtype": "float32"
        })


        stacked = np.stack(bands_2d, axis=0) 
        with rasterio.open(output_path, "w", **out_meta) as dst:
            dst.write(stacked)
        logging.info(f"Reconstructed TIFF saved to {output_path} with {len(bands_2d)} bands.")

    
    def CalculateNdvi(self,red,nirred):
        numerator = nirred - red
        denominator = nirred + red
        with np.errstate(divide='ignore', invalid='ignore'):
            ndvi = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return ndvi
    
    def CalculateEvi(self,Nir,Red,Blue):
        numerator = Nir-Red
        denominator = Nir+6*Red-7.5*Blue+1
        with np.errstate(divide='ignore', invalid='ignore'):
            Evi = np.where(
                (denominator != 0) ,  
                2.5*numerator / denominator,
                np.nan  
            )
        return Evi
    
    def CalculateGndvi(self,Nir,Green):
        numerator = Nir-Green
        denominator = Nir+Green
        with np.errstate(divide='ignore', invalid='ignore'):
            Gndvi = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return Gndvi
    
    def CalculateCvi(self,Nir,Red,Green):
        numerator = Nir
        denominator = Green
        numerator1 = Red
        denominator1 = Green
        with np.errstate(divide='ignore', invalid='ignore'):
            Cvi = np.where(
                (denominator != 0),  
                numerator / denominator * numerator1 / denominator1,
                np.nan  
            )
        return Cvi
    
    def CalculateNdre(self,Nir,RedEdge):
        numerator = Nir - RedEdge
        denominator = Nir + RedEdge
        with np.errstate(divide='ignore', invalid='ignore'):
            Ndre = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return Ndre


    def saveToDatabase(self,BBLIST,Date,values,DataType,FieldCropYearId,TiffMode):
        """ Saves the values that have been calculated to the  database"""
        average = np.nanmean(values).item()
        Median = np.nanmedian(values).item()
        Std = np.nanstd(values).item()
        Min = np.nanmin(values).item()
        Max = np.nanmax(values).item()
        dataToInsert = [FieldCropYearId,Date,DataType,TiffMode,average,Median,Std,Min,Max]
        self.SqlHandler.insertDataForADataType(dataToInsert)
        for BBid in BBLIST:
            self.SqlHandler.InsertNdviMetaDataConnection(FieldCropYearId, BBid)
    
    def calculateHistogram(self, Values, DataType, FieldCropYearId,date,TiffMode):
        datatypename = self.SqlHandler.GetDatatypeName(DataType)
        Field,CropType = self.SqlHandler.GetFieldAndCroptypeBasedOnFieldCropYearId(FieldCropYearId)
        if Values is None or len(Values) == 0:
            print("Empty input array, skipping histogram.")
            return

        plt.figure(figsize=(10, 6))
        plt.hist(Values, color='skyblue', edgecolor='black')
        plt.title(f'Histogram of {datatypename} for Field: {Field} with croptType: {CropType} on the {date}\n TiffMode: {TiffMode}')
        plt.xlabel(f'{datatypename} Value')
        plt.ylabel('Frequency')
        plt.grid(True, linestyle='--', alpha=0.6)
        folder=f"plot/Histogram/{Field}"
        filename = f"{folder}/Histogram_{datatypename}_{Field}_{CropType}_{date}_{TiffMode}.png"
        plt.tight_layout()
        os.makedirs(folder, exist_ok=True)
        plt.savefig(filename)
        plt.close()

        logging.info(f"Histogram saved as {filename}")

    def calculateKde(self, Values, DataType, FieldCropYearId,date,TiffMode):
        logging.info("Starting the kde calculation")
        datatypename = self.SqlHandler.GetDatatypeName(DataType)
        Field, CropType = self.SqlHandler.GetFieldAndCroptypeBasedOnFieldCropYearId(FieldCropYearId)

        if Values is None or len(Values) == 0:
            print("Empty input array, skipping KDE.")
            return


        plt.figure(figsize=(10, 6))
        sns.kdeplot(Values, fill=True, color='skyblue')
        plt.title(f'KDE of {datatypename} for Field: {Field} with CropType: {CropType} on the {date}\n tiffmode:{TiffMode}')
        plt.xlabel(f'{datatypename} Value')
        plt.ylabel('Density')
        plt.grid(True, linestyle='--', alpha=0.6)

        folder=f"plot/KDE/{Field}"
        filename = f"{folder}/KDE_{datatypename}_{Field}_{CropType}_{date}_{TiffMode}.png"
        plt.tight_layout()
        os.makedirs(folder, exist_ok=True)
        plt.savefig(filename)
        plt.close()

        logging.info(f"KDE saved as {filename}")

    def CalculateHeatMap(self,Mask,value,Date,DataType,FieldCropYearId,TiffMode):
        datatypename = self.SqlHandler.GetDatatypeName(DataType)
        Field,CropType = self.SqlHandler.GetFieldAndCroptypeBasedOnFieldCropYearId(FieldCropYearId)
        data2d = self.Reconstruct2dNpArrey(value,Mask)
        if datatypename =="cvi":
            plt.imshow(data2d, cmap='RdYlGn',vmin=0.0,vmax=5.0)
        else:
            plt.imshow(data2d,
                cmap='RdYlGn',
                vmin=0.0,       
                vmax=1.0) 
        plt.colorbar(label=datatypename)
        title = (
            f"{datatypename} heatmap for Field {Field} on {Date}\n"
            f"CropType: {CropType}, TiffMode: {TiffMode}"
        )
        plt.title(title, fontsize=12, pad=12)
        plt.axis('off')
        folder=f"plot/HeatMap/{Field}"
        filename = f"{folder}/HeatMap{datatypename}_{Field}_{CropType}_{Date}_{TiffMode}.png"
        plt.tight_layout()
        os.makedirs(folder, exist_ok=True)
        plt.savefig(filename)
        plt.close()

        logging.info(f"HeatMap saved as {filename}")

