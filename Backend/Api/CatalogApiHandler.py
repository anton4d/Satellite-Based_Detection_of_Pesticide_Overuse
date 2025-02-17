import requests
import dotenv
from shapely import wkt
import logging
import json
import sys

class CatalogApiHandler:
    def __init__(self,ClientId,ClientSecret,FromDate,ToDate,ApiToken):
        self.ClientId = ClientId
        self.ClientSecret = ClientSecret
        self.FromDate = FromDate
        self.ToDate = ToDate

        if ApiToken is None:
            self.GetToken()
        else:
            self.ApiToken = ApiToken

    def GetToken(self):
        logging.info("Api Call to get An Api Token")
        dotenvFile = dotenv.find_dotenv()
        AuthServerUrl="https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        TokenRequestPaylod = {"grant_type": "client_credentials"}

        tokenResponse = requests.post(AuthServerUrl,
        data=TokenRequestPaylod, verify=False, allow_redirects=False,
        auth=(self.ClientId,self.ClientSecret))
        if tokenResponse.status_code != 200:
            logging.error(f"Failed to obtain token from the OAuth 2.0 server Response status code: {tokenResponse.status_code}")
        tokendump = json.dumps(tokenResponse.json(), indent=4)
        token = tokenResponse.json()
        logging.info(f"Successfuly obtained a new token with values: {tokendump}")
        self.ApiToken = token["access_token"]
        dotenv.set_key(dotenvFile, "APIToken", token["access_token"])

    
    def GetPictureDates(self,PolygonWkt):
        """Gets a list of dates between the from and To date Where the Satalite took a picture based on a polygon"""
        logging.info("Calling the Catalog for catalog data on Feild with id: 1")
        polygon = wkt.loads(PolygonWkt)

        exterior_coords = list(polygon.exterior.coords)

        nested_coords = [[list(coord) for coord in exterior_coords]]
        print(nested_coords)

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
                "coordinates": nested_coords
            },
            "limit": 100
        }
        response = requests.post(url, headers=headers, json=data)
        responseJson = response.json()
        if response.status_code == 200:
            json_object = json.dumps(responseJson, indent=4)
            with open("ResponseCatalog.txt", "w") as f:
                f.write(json_object)
        if response.status_code == 401:
            logging.error("AccesCode has expired or is wrong")
            self.GetToken()
        else:
            description = responseJson.get("description", "No description provided")
            logging.error(f"Request failed with status code: {response.status_code} and description: {description}")