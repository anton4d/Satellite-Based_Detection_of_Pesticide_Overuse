import time
import geopandas as gpd
import logging

def Shapefile_To_Geojson(Shapefile_Path, Geojson_Output_Path, Filter_Values=None):
    """
    Convert a Shapefile to a GeoJSON file, with optional filtering by the 'Afgroede' column.
    
    Parameters:
    - shapefile_path (str): Path to the input .shp file.
    - geojson_output_path (str): Path to save the output .geojson file.
    - filter_values (list[str], optional): Values to filter by in the 'Afgroede' column.
    """
    try:
        logging.info(f"Reading shapefile from: {Shapefile_Path}")
        gdf = gpd.read_file(Shapefile_Path)
        logging.info(f"Loaded {len(gdf)} features")
        

        if 'Afgroede' not in gdf.columns:
            raise KeyError("'Afgroede' column not found in the shapefile.")
        

        gdf['Afgroede'] = gdf['Afgroede'].str.strip().str.lower()
        
        
        Unique_Values = gdf['Afgroede'].unique()
        Sorted_List = sorted(Unique_Values, key=lambda x: (x is None, x))
        logging.info(f"lenth of not sorted list:{len(Unique_Values)} lenth of sorted list:{len(Sorted_List)}")
        logging.info(f"Unique values in 'Afgroede' column: {Sorted_List}")
        Start_Time = time.time()
        if Filter_Values:
            Filter_Values = [val.strip().lower() for val in Filter_Values]
            Filtered_GDF = gdf[gdf['Afgroede'].isin(Filter_Values)]
            logging.info(f"Filtered features: {len(Filtered_GDF)}")
            Filtered_GDF.to_file(Geojson_Output_Path, driver="GeoJSON")
        else:
            gdf.to_file(Geojson_Output_Path, driver="GeoJSON")
        End_Time = time.time()
        Full_Time = End_Time - Start_Time
        logging.info(f"GeoJSON successfully written to: {Geojson_Output_Path}. it took {Full_Time} secounds")
    
    except Exception as e:
        logging.error(f"Failed to convert shapefile: {e}", exc_info=True)
