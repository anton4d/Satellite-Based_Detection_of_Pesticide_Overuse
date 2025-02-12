import json
import logging
from shapely.geometry import shape
from Database.SQLHandler import SQLHandler

def load_geojson(file_path):
    """Load the GeoJSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def geojson_to_wkt(geometry):
    """Convert GeoJSON geometry to WKT format."""
    geom = shape(geometry)  
    return geom.wkt  

def insert_geojson_to_db(geojson_data, db_handler):
    """Iterate through the GeoJSON features and insert them into the database."""
    for feature in geojson_data['features']:

        properties = feature.get('properties', {})
        marknr = properties.get('Marknr', None)
        cvr = properties.get('CVR', None)
        afgkode = properties.get('Afgkode', None)

        geometry = feature.get('geometry', {})
        polygon_wkt = geojson_to_wkt(geometry)

        db_handler.InsertModel(crop_type="Kartofler", marknr=marknr, cvr=cvr, polygon=polygon_wkt)

def main():
    geojson_path = ''  

    db_handler = SQLHandler(
        host='localhost',  
        user='user',  
        password='password', 
        database='db'  
    )

    geojson_data = load_geojson(geojson_path)

    insert_geojson_to_db(geojson_data, db_handler)

    logging.info("GeoJSON data has been successfully inserted into the database.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the geojsonToDB script...")
    main()
