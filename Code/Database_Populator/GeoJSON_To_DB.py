import json
import logging
from shapely.geometry import shape
from pyproj import CRS, Transformer
from SQL_Handler import SQL_Handler

class GeoJsonToDB:
    def __init__(self, Geojson_Path, Sql_Handler:SQL_Handler, year):
        self.Geojson_Path = Geojson_Path
        self.Sql_Handler = Sql_Handler
        self.year = year
        self.Crs_From = CRS("EPSG:25832")
        self.Crs_To = CRS("EPSG:4326")
        self.Transformer = Transformer.from_crs(self.Crs_From, self.Crs_To, always_xy=True)

    def Load_Geojson(self):
        """Load the GeoJSON file."""
        with open(self.Geojson_Path, 'r') as f:
            return json.load(f)

    def Geojson_To_WKT(self, geometry):
        """Convert GeoJSON geometry to the correct CRS & then WKT format."""
        Geom = shape(geometry)  

        
        new_Coords = []
        for coord in Geom.exterior.coords:
            lon, lat = self.Transformer.transform(coord[0], coord[1])
            new_Coords.append((lon, lat))
        Geom = shape({"type": "Polygon", "coordinates": [new_Coords]})

        wkt = Geom.wkt
        logging.debug(f"Generated WKT: {wkt}")
        return wkt

    def Insert_Geojson_To_DB(self, Geojson_Data):
        """Iterate through the GeoJSON features and insert them into the database."""
        for Feature in Geojson_Data['features']:

            year = self.year
            properties = Feature.get('properties', {})
            cropType = properties.get('Afgroede', None)
            marknr = properties.get('Marknr', None)
            journalnr = properties.get('Journalnr', None)
            markblok = properties.get('Markblok', None)
            cvr = properties.get('CVR', None)


            geometry = Feature.get('geometry', {})
            Polygon_WKT = self.Geojson_To_WKT(geometry)
            Polygon_WKT = Polygon_WKT.strip()
            Polygon_WKT = Polygon_WKT.replace("  ", " ")
            
          
            self.Sql_Handler.InsertOrUpdateField(MarkNr=marknr, Journalnr=journalnr, Markblok=markblok, CVR=cvr, Polygon=Polygon_WKT)

                
            self.Sql_Handler.InsertCropYear(cropType,year,marknr,markblok)
    def Process_Geojson(self):
        geojson_data = self.Load_Geojson()
        self.Insert_Geojson_To_DB(geojson_data)
        logging.info("GeoJson data has been inserted into the database")