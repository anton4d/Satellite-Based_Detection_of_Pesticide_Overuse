import geopandas as gpd
import os

# Function to convert shapefile to GeoJSON
def shapefile_to_geojson(shapefile_path, geojson_output_path):
    try:
        logging.info(f"Reading shapefile: {shapefile_path}")
        gdf = gpd.read_file(shapefile_path)
        
        # Checking the number of features to understand the data size
        logging.info(f"Number of features in the shapefile: {len(gdf)}")
        
        filter_values = [
            "Kartofler, lægge- (certificerede)",
            "Kartofler, lægge- (egen opformering)",
            "Kartofler, spise-"
        ]

        # Ensure matching is case-insensitive and strip any extra spaces
        gdf['Afgroede'] = gdf['Afgroede'].str.strip().str.lower()
        filter_values = [value.lower() for value in filter_values]
        

        filtered_gdf = gdf[gdf['Afgroede'].isin(filter_values)]

        # Convert the GeoDataFrame to GeoJSON format
        logging.info(f"Writing GeoJSON to: {geojson_output_path}")
        filtered_gdf.to_file(geojson_output_path, driver="GeoJSON")
        
        logging.info(f"Conversion successful! The GeoJSON file has been saved at: {geojson_output_path}")
    except Exception as e:
        logging.info(f"Error during conversion: {e}")


if __name__ == "__main__":
    shapefile_path = 'localpath'
    geojson_output_path = '../../Shapefiles/Marker_2020.geojson'
    shapefile_to_geojson(shapefile_path, geojson_output_path)
