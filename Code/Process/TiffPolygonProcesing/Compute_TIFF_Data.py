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
from Database.SQL_Handler import SQLHandler
import seaborn as sns

class ComputeTiffData:
    def __init__(self, SqlHandler: SQLHandler):
        self.SqlHandler = SqlHandler



    def Compute_Data_From_Tiff(self, BBLIST, FieldID, CropId, Date, year, polygon, output_TIFF, Data_Type, Data_Mode, Plot_Type,Tiff_Mode):
        """Process a TIFF image: compute average NDVI & save a new TIFF with out_image_red, NIR, Mask, and NDVI"""
        logging.info(f"Starting TIFF to database and new image process with tiffmode {Tiff_Mode}")

        try:
            path = BBLIST[0]
            BBLIST.pop(0)
            with rasterio.open(path) as src:
                if src.crs != "EPSG:4326" and polygon is not None:
                    polygon = polygon.to_crs(src.crs)

                if Tiff_Mode == "BB":
                    logging.info(f"Applying mask for FieldID {FieldID}")
                    out_Images, out_transform = mask(src, [polygon], crop=True, filled=True, nodata=0, indexes=[1, 2, 3, 4, 5,6])
                    Blue = out_Images[0].astype(np.float32)
                    Green = out_Images[1].astype(np.float32)
                    Red = out_Images[2].astype(np.float32)
                    RedEdge = out_Images[3].astype(np.float32)
                    Nir = out_Images[4].astype(np.float32)
                    DMask = out_Images[5]
                    validPixels = DMask == 1
                elif Tiff_Mode == "Field":
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
                DataType_Map = {
                    1: lambda: Green,
                    2: lambda: Blue,
                    3: lambda: Red,
                    4: lambda: Nir,
                    5: lambda: RedEdge,
                    6: lambda: self.Calculate_NDVI(Red, Nir),
                    7: lambda: self.Calculate_EVI(Nir, Red, Blue),
                    8: lambda: self.Calculate_GNDVI(Nir, Green),
                    9: lambda: self.Calculate_CVI(Nir, Red, Green),
                    10: lambda: self.Calculate_NDRE(Nir,RedEdge),
                }
                FieldCropYearId = self.SqlHandler.Get_FieldCropYearId_Based_On_Year_CropId_And_FieldId(year, CropId, FieldID)
                Data_Types = self.SqlHandler.Get_Data_Type_Ids(Data_Type)
                AllValues = []
                for Data_Type in Data_Types:
                    value = DataType_Map.get(Data_Type)
                    if value: 
                        result = value()
                        AllValues.append(result)
                        if (Data_Mode == "ToDb"):
                            self.save_To_Database(BBLIST, Date, result, Data_Type, FieldCropYearId,Tiff_Mode)
                        elif (Data_Mode == "Plot"):
                            if Plot_Type == "Histogram":
                                self.calculate_Histogram(result,Data_Type,FieldCropYearId,Date,Tiff_Mode)
                            elif Plot_Type == "Kdeplot":
                                self.calculate_KDE(result,Data_Type, FieldCropYearId,Date,Tiff_Mode)
                            elif Plot_Type == "HeatMap":
                                self.Calculate_HeatMap(validPixels,result,Date,Data_Type,FieldCropYearId,Tiff_Mode)


                
                if output_TIFF != "None":
                    logging.debug(f"About to reconstruct {len(AllValues)} bands")
                    assert len(AllValues) > 0, "No bands to write!"
                    for i, arr in enumerate(AllValues):
                        logging.debug(f"  flat band {i}: ndim={arr.ndim}, size={arr.size}")
                        if arr.ndim != 1:
                            raise ValueError(f"Band {i} is not 1D!")
                    self.Save_Reconstructed_TIFF(
                            flat_results=AllValues,
                            mask_2d=validPixels,        
                            src_meta=src.meta,
                            out_transform=out_transform,
                            output_path=output_TIFF
                        )

        except Exception as e:
            logging.error(f"Error processing TIFF: {e}")



    def Reconstruct_2d_NpArrey(self, flat_array: np.ndarray, mask_2d: np.ndarray) -> np.ndarray:
        """
        Given a 1D array of values for valid pixels and the 2D boolean mask,
        reconstruct a full 2D array (same shape as mask) with np.nan elsewhere.
        """
        full = np.full(mask_2d.shape, np.nan, dtype=np.float32)
        full[mask_2d] = flat_array
        return full

    def Save_Reconstructed_TIFF(self,
                                 flat_results: list[np.ndarray],
                                 mask_2d: np.ndarray,
                                 src_meta: dict,
                                 out_transform,
                                 output_path: str):
        """
        Reconstruct all flat_results into 2D images, stack them, and write to output_path.
        """

        bands_2d = [self.Reconstruct_2d_NpArrey(arr, mask_2d) for arr in flat_results]


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

    
    def Calculate_NDVI(self,red,nirred):
        numerator = nirred - red
        denominator = nirred + red
        with np.errstate(divide='ignore', invalid='ignore'):
            ndvi = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return ndvi
    
    def Calculate_EVI(self,Nir,Red,Blue):
        numerator = Nir-Red
        denominator = Nir+6*Red-7.5*Blue+1
        with np.errstate(divide='ignore', invalid='ignore'):
            Evi = np.where(
                (denominator != 0) ,  
                2.5*numerator / denominator,
                np.nan  
            )
        return Evi
    
    def Calculate_GNDVI(self,Nir,Green):
        numerator = Nir-Green
        denominator = Nir+Green
        with np.errstate(divide='ignore', invalid='ignore'):
            Gndvi = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return Gndvi
    
    def Calculate_CVI(self,Nir,Red,Green):
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
    
    def Calculate_NDRE(self,Nir,RedEdge):
        numerator = Nir - RedEdge
        denominator = Nir + RedEdge
        with np.errstate(divide='ignore', invalid='ignore'):
            Ndre = np.where(
                (denominator != 0),  
                numerator / denominator,
                np.nan  
            )
        return Ndre


    def save_To_Database(self,BBLIST,Date,values,DataType,FieldCropYearId,TiffMode):
        """ Saves the values that have been calculated to the  database"""
        average = np.nanmean(values).item()
        Median = np.nanmedian(values).item()
        Std = np.nanstd(values).item()
        Min = np.nanmin(values).item()
        Max = np.nanmax(values).item()
        dataToInsert = [FieldCropYearId,Date,DataType,TiffMode,average,Median,Std,Min,Max]
        self.SqlHandler.Insert_Data_For_A_DataType(dataToInsert)
        for BBid in BBLIST:
            self.SqlHandler.Insert_MetaData_Connection(FieldCropYearId, BBid)
    
    def calculate_Histogram(self, Values, DataType, FieldCropYearId,date,TiffMode):
        datatypename = self.SqlHandler.Get_Datatype_Name(DataType)
        Field,CropType = self.SqlHandler.Get_Field_And_Croptype_Based_On_FieldCropYearId(FieldCropYearId)
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

    def calculate_KDE(self, Values, DataType, FieldCropYearId,date,TiffMode):
        logging.info("Starting the kde calculation")
        datatypename = self.SqlHandler.Get_Datatype_Name(DataType)
        Field, CropType = self.SqlHandler.Get_Field_And_Croptype_Based_On_FieldCropYearId(FieldCropYearId)

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

    def Calculate_HeatMap(self,Mask,value,Date,DataType,FieldCropYearId,TiffMode):
        datatypename = self.SqlHandler.Get_Datatype_Name(DataType)
        Field,CropType = self.SqlHandler.Get_Field_And_Croptype_Based_On_FieldCropYearId(FieldCropYearId)
        data2d = self.Reconstruct_2d_NpArrey(value,Mask)
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

