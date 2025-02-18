import json
import logging
from shapely.geometry import shape
from pyproj import CRS, Transformer
from Database.SQLHandler import SQLHandler

class GeoJsonToDB:
    def __init__(self, geojson_path, sql_handler):
        self.geojson_path = geojson_path
        self.sql_handler = sql_handler

        # Data from Miljøstyrelse is using a Coordinate Reference System (CRS) that is incompatible with Copernicus API
        # Using pyproj to transform the CRS to a compatible one
        self.crs_from = CRS("EPSG:25832")
        self.crs_to = CRS("EPSG:4326")
        self.transformer = Transformer.from_crs(self.crs_from, self.crs_to, always_xy=True)

    def load_geojson(self):
        """Load the GeoJSON file."""
        with open(self.geojson_path, 'r') as f:
            return json.load(f)

    def geojson_to_wkt(self, geometry):
        """Convert GeoJSON geometry to the correct CRS & then WKT format."""
        geom = shape(geometry)  

        # CRS Formatting
        new_coords = []
        for coord in geom.exterior.coords:
            lon, lat = self.transformer.transform(coord[0], coord[1])
            new_coords.append((lon, lat))
        geom = shape({"type": "Polygon", "coordinates": [new_coords]})

        wkt = geom.wkt
        logging.info(f"Generated WKT: {wkt}")
        return wkt

    def insert_geojson_to_db(self, geojson_data):
        """Iterate through the GeoJSON features and insert them into the database."""
        for feature in geojson_data['features']:


            properties = feature.get('properties', {})
            cropType = properties.get('Afgroede', None)
            marknr = properties.get('Marknr', None)
            Journalnr = properties.get('Journalnr', None)
            cvr = properties.get('CVR', None)
            afgkode = properties.get('Afgkode', None)

            geometry = feature.get('geometry', {})
            polygon_wkt = self.geojson_to_wkt(geometry)
            polygon_wkt = polygon_wkt.strip()
            polygon_wkt = polygon_wkt.replace("  ", " ")

            self.sql_handler.InsertField(CropType=cropType, MarkNr=marknr, Journalnr=Journalnr, CVR=cvr, Polygon=polygon_wkt)

    def process_geojson(self):
        geojson_data = self.load_geojson()
        self.insert_geojson_to_db(geojson_data)
        logging.info("GeoJson data has been inserted into the database")