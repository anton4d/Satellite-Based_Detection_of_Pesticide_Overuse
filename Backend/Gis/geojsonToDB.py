import json
import logging
from shapely.geometry import shape
from Database.SQLHandler import SQLHandler

class GeoJsonToDB:
    def __init__(self, geojson_path, sql_handler):
        self.geojson_path = geojson_path
        self.sql_handler = sql_handler

    def load_geojson(self):
        """Load the GeoJSON file."""
        with open(self.geojson_path, 'r') as f:
            return json.load(f)

    def geojson_to_wkt(self, geometry):
        """Convert GeoJSON geometry to WKT format."""
        geom = shape(geometry)  
        logging.info(geom.wkt)
        return geom.wkt  

    def insert_geojson_to_db(self, geojson_data):
        """Iterate through the GeoJSON features and insert them into the database."""
        for feature in geojson_data['features']:

            properties = feature.get('properties', {})
            marknr = properties.get('Marknr', None)
            cvr = properties.get('CVR', None)
            afgkode = properties.get('Afgkode', None)

            geometry = feature.get('geometry', {})
            polygon_wkt = self.geojson_to_wkt(geometry)
            polygon_wkt = polygon_wkt.strip()
            polygon_wkt = polygon_wkt.replace("  ", " ")

            self.sql_handler.InsertField(CropType="Kartofler", MarkNr=marknr, CVR=cvr, Polygon=polygon_wkt)

    def process_geojson(self):
        geojson_data = self.load_geojson()
        self.insert_geojson_to_db(geojson_data)
        logging.info("GeoJson data has been inserted into the database")