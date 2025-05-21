from shapely import wkt


def ConvertWktToNestedCords(PolygonWkt):
        """Converts a Wkt To the the Nested structure that the api takes"""
        polygon = wkt.loads(PolygonWkt)

        exteriorCoords = list(polygon.exterior.coords)

        NestedCoords = [[list(coord) for coord in exteriorCoords]]
        return NestedCoords