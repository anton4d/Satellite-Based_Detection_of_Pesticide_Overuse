import geopandas as gpd
import logging

def shapefile_to_geojson(shapefile_path, geojson_output_path, filter_values=None):
    """
    Convert a Shapefile to a GeoJSON file, with optional filtering by the 'Afgroede' column.
    
    Parameters:
    - shapefile_path (str): Path to the input .shp file.
    - geojson_output_path (str): Path to save the output .geojson file.
    - filter_values (list[str], optional): Values to filter by in the 'Afgroede' column.
    """
    try:
        logging.info(f"Reading shapefile from: {shapefile_path}")
        gdf = gpd.read_file(shapefile_path)
        logging.info(f"Loaded {len(gdf)} features")
        
        # Ensure 'Afgroede' column exists
        if 'Afgroede' not in gdf.columns:
            raise KeyError("'Afgroede' column not found in the shapefile.")
        
        # Clean up 'Afgroede' field
        gdf['Afgroede'] = gdf['Afgroede'].str.strip().str.lower()
        
        # Log unique values in 'Afgroede' column
        
        unique_values = gdf['Afgroede'].unique()
        sorted_list = sorted(unique_values, key=lambda x: (x is None, x))
        logging.info(f"lenth of not sorted list:{len(unique_values)} lenth of sorted list:{len(sorted_list)}")
        logging.info(f"Unique values in 'Afgroede' column: {sorted_list}")
        
        # Apply filter if provided
        if filter_values:
            filter_values = [val.strip().lower() for val in filter_values]
            filtered_gdf = gdf[gdf['Afgroede'].isin(filter_values)]
            logging.info(f"Filtered features: {len(filtered_gdf)}")
            filtered_gdf.to_file(geojson_output_path, driver="GeoJSON")
        else:
            gdf.to_file(geojson_output_path, driver="GeoJSON")
        
        logging.info(f"GeoJSON successfully written to: {geojson_output_path}")
    
    except Exception as e:
        logging.error(f"Failed to convert shapefile: {e}", exc_info=True)
