import requests
import dotenv
from shapely import wkt
import logging
import json
import sys


class CatalogApiHandler:
    def __init__(self,FromDate,ToDate,ApiToken,TokenApiHandler):
        self.FromDate = FromDate
        self.ToDate = ToDate
        self.TokenApiHandler = TokenApiHandler

        if ApiToken is None:
            self.ApiToken = self.TokenApiHandler.GetToken()
        else:
            self.ApiToken = ApiToken

    

    def ConvertWktToNestedCords(self, PolygonWkt):
        """Converts a Wkt To the the Nested structure that the api takes"""
        polygon = wkt.loads(PolygonWkt)

        exteriorCoords = list(polygon.exterior.coords)

        NestedCoords = [[list(coord) for coord in exteriorCoords]]
        return NestedCoords

    
    def GetPictureDates(self,PolygonWkt, FeildId):
        """Gets a list of dates between the from and To date Where the Satalite took a picture based on a polygon"""

        logging.info(f"Calling the Catalog api for catalog data on Feild with id: {FeildId}...")
        
        polygonWithNestedCoords = self.ConvertWktToNestedCords(PolygonWkt)

        url = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.ApiToken}"
        }
        data = {
            "collections": [
                "sentinel-2-l1c"
            ],
            "datetime": f"{self.FromDate}/{self.ToDate}",
            "intersects": {
                "type": "Polygon",
                "coordinates": polygonWithNestedCoords
            },
            "limit": 100
        }
        response = requests.post(url, headers=headers, json=data)
        responseJson = response.json()
        if response.status_code == 200:
            Features = responseJson.get("features", [])
            logging.info(f"Found {len(Features)} number of features")
            uniqueDates = set()
            for Feature in Features:
                properties = Feature.get("properties", {})
                datetime = properties.get("datetime", "No datetime provided")
                logging.debug(f"Found Feature with datetime: {datetime}")
                dateOnly = datetime.split("T")[0]

                if dateOnly not in uniqueDates:
                    uniqueDates.add(dateOnly)
                    logging.debug(f"Added new date: {dateOnly}")
            with open("ResponseCatalog.txt", "w") as f:
                json.dump(responseJson, f, indent=4)
            return uniqueDates
        elif response.status_code == 401:
            logging.error("Access code has expired or is incorrect.")
            self.ApiToken = self.TokenApiHandler.GetToken()
            Data = self.GetPictureDates(PolygonWkt=PolygonWkt, FeildId=FeildId)
            return Data
        else:
            description = responseJson.get("description", "No description provided")
            logging.error(f"Request failed (Status: {response.status_code}) - {description}")
            raise Exception(f"API request failed with status {response.status_code}: {description}")
