import os
import mysql.connector
import geopandas as gpd
from rasterio.merge import merge
from shapely.geometry import box
from shapely import wkt
import rasterio
import logging

TIFF_ROOT = "../Download/Pictures"

class TiffIntersector:
    def __init__(self, SqlHandler, tiff_root):
        self.SqlHandler = SqlHandler
        self.TIFF_ROOT = tiff_root

    def get_wkt_polygons(self):
        """
        Fetch WKT polygons from the database and return as a GeoDataFrame, that allows for spatial data.
        """
        conn = self.SqlHandler.connection
        cur = conn.cursor()

        query = "SELECT FieldId, ST_AsText(Polygon) FROM Field;"
        cur.execute(query)
        records = cur.fetchall()

        # Convert to GeoDataFrame
        polygons = [{"FieldId": row[0], "geometry": wkt.loads(row[1])} for row in records]
        gdf = gpd.GeoDataFrame(polygons, crs="EPSG:4326")
        
        logging.info(f"Retrieved {len(gdf)} polygons from the database.")
        return gdf

    def find_intersecting_tiffs(self, polygon, date):
        """
        Finds TIFF files that intersect with a given polygon for a given date.
        If multiple TIFF files intersect with a polygon, merge them into one.
        """
        try:
            intersecting_tiffs = []

            for root, _, files in os.walk(self.TIFF_ROOT):
                for file in files:
                    if file.endswith(".tiff") and date in file:
                        tiff_path = os.path.join(root, file)
                        with rasterio.open(tiff_path) as src:
                            tiff_geom = box(*src.bounds)

                            if isinstance(polygon, gpd.GeoSeries):
                                poly = polygon.iloc[0]
                            else:
                                poly = polygon

                            poly_proj = gpd.GeoSeries([poly], crs="EPSG:4326").to_crs(src.crs).iloc[0]

                            if poly_proj.intersects(tiff_geom):
                                intersecting_tiffs.append(tiff_path)

            logging.info(f"Polygon intersects {len(intersecting_tiffs)} TIFFs on {date}. Array: {intersecting_tiffs}")
            logging.info(polygon)

            if len(intersecting_tiffs) == 1:
                return intersecting_tiffs[0]

            if len(intersecting_tiffs) > 1:
                merged_path = f"../tempMergedTiff/merged_{date}.tiff"
                os.makedirs(os.path.dirname(merged_path), exist_ok=True)

                src_files_to_merge = [rasterio.open(tiff) for tiff in intersecting_tiffs]

                merged_array, merged_transform = merge(src_files_to_merge)

                out_meta = src_files_to_merge[0].meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": merged_array.shape[1],
                    "width": merged_array.shape[2],
                    "transform": merged_transform
                })

                with rasterio.open(merged_path, "w", **out_meta) as dest:
                    dest.write(merged_array)

                for src in src_files_to_merge:
                    src.close()

                logging.info(f"Merged {len(intersecting_tiffs)} TIFFs into {merged_path}")
                return merged_path

            return None

        except Exception as e:
            logging.error(f"Failed to create intersections correctly maybe? : {e}")
            return None