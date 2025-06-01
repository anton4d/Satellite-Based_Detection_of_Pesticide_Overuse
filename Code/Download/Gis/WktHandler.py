from shapely import wkt
from pyproj import Geod

def Convert_Wkt_To_NestedCords(PolygonWkt):
        """Converts a Wkt To the the Nested structure that the api takes"""
        polygon = wkt.loads(PolygonWkt)

        exteriorCoords = list(polygon.exterior.coords)

        NestedCoords = [[list(coord) for coord in exteriorCoords]]
        return NestedCoords
geod = Geod(ellps="WGS84")

def get_pixel_dimensions(nested_coords, meters_per_pixel=10, max_pixels=2500):
    """Calculate pixel dimensions based on fixed meter/pixel ratio, with a max limit"""
    coords = nested_coords[0]

    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]

    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    mid_lon = (min_lon + max_lon) / 2
    mid_lat = (min_lat + max_lat) / 2

    _, _, width_m = geod.inv(min_lon, mid_lat, max_lon, mid_lat)
    _, _, height_m = geod.inv(mid_lon, min_lat, mid_lon, max_lat)

    width_px = int(round(width_m / meters_per_pixel))
    height_px = int(round(height_m / meters_per_pixel))

    if width_px > max_pixels or height_px > max_pixels:
        print(f"ERROR: Image size too large ({width_px} x {height_px}), exceeds max {max_pixels} pixels.")
        return None, None

    return width_px, height_px