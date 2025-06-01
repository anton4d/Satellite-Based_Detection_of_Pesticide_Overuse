import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

west, east = 7.9, 15.2   
south, north = 54.5, 57.8  


lat_step = 0.05
lon_step = 0.05  


lat_step_256 = (256 / 50) * lat_step
lon_step_256 = (256 / 50) * lon_step


lats_256 = np.arange(south, north, lat_step_256)
lons_256 = np.arange(west, east, lon_step_256)

bounding_boxes_256 = []
for lat in lats_256:
    for lon in lons_256:
        bounding_boxes_256.append((lon, lat, lon + lon_step_256, lat + lat_step_256))


df_boxes_256 = pd.DataFrame(bounding_boxes_256, columns=["min_lon", "min_lat", "max_lon", "max_lat"])


denmark_gdf = gpd.read_file("../../Shapefiles/Denmark_shapefile/dk_1km.shp")

denmark_gdf = denmark_gdf.to_crs("EPSG:4326")


df_boxes_256["geometry"] = df_boxes_256.apply(
    lambda row: box(row["min_lon"], row["min_lat"], row["max_lon"], row["max_lat"]), axis=1
)


gdf_boxes_256 = gpd.GeoDataFrame(df_boxes_256, geometry="geometry", crs="EPSG:4326")


gdf_filtered = gdf_boxes_256[gdf_boxes_256.intersects(denmark_gdf.geometry.union_all())]


df_filtered_boxes_256 = gdf_filtered.drop(columns=["geometry"])

print(gdf_filtered.head())
print(len(gdf_filtered), "bounding boxes intersect Denmark.")


csv_filename = "../../Shapefiles/CSV/denmarkBB.csv"
df_filtered_boxes_256.to_csv(csv_filename, index=False)

print(f"CSV file '{csv_filename}' has been created successfully.")