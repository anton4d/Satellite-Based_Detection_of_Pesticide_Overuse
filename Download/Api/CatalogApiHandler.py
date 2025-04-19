import requests, csv, dotenv,logging,json,sys
from shapely import wkt


class CatalogApiHandler:
    def __init__(self,ApiToken,TokenApiHandler,dbHandler):
        self.TokenApiHandler = TokenApiHandler
        self.dbhandler = dbHandler

        if ApiToken is None:
            self.ApiToken = self.TokenApiHandler.GetToken()
        else:
            self.ApiToken = ApiToken

    
    def GetPictureDates(self,Polygon, FieldId,FromDate,ToDate, next = 0):
        """Gets a list of dates between the from and To date Where the Satalite took a picture based on a polygon"""

        logging.info(f"Calling the Catalog api for catalog data on Feild with id: {FieldId} ....")
        
        

        url = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.ApiToken}"
        }
        
        data = {
            "collections": [
                "sentinel-2-l1c"
            ],
            "datetime": f"{FromDate}/{ToDate}",
            "intersects": {
                "type": "Polygon",
                "coordinates": Polygon
            },
            "limit": 100
        }
        if next != 0:
            data["next"] = next
        with requests.post(url, headers=headers, json=data) as response:
            responseJson = response.json()
            statuscode = response.status_code
            logging.debug(f"statuscode:{statuscode}")
            if statuscode == 200:
                Features = responseJson.get("features", [])
                uniqueDates = set()
                if len(Features) >= 1:
                    logging.info(f"Found {len(Features)} number of features")
                    for Feature in Features:
                        properties = Feature.get("properties", {})
                        datetime = properties.get("datetime", "No datetime provided")
                        Platform = properties.get("platform", "No plateform provided")
                        CloudCover = properties.get("eo:cloud_cover", "No cloud_cover provided")
                        logging.debug(f"Found Feature with datetime: {datetime}, Platform: {Platform} and CloudCover: {CloudCover}")
                        dateOnly = datetime.split("T")[0]
                        DateMetaData = [FieldId,datetime,dateOnly,Platform,CloudCover]
                        if dateOnly not in uniqueDates:
                            uniqueDates.add(dateOnly)
                            logging.debug(f"Added new date: {dateOnly}")
                        self.dbhandler.insertBoundingboxMetaData(DateMetaData)
                        context = responseJson.get("context", [])
                        next = context.get("next", "No next")
                        if next != "No next":
                            logging.debug(f"list has:{len(uniqueDates)} dates in it before next")
                            uniqueDates.update(self.GetPictureDates(Polygon, FieldId,FromDate,ToDate ,next)) 
                            logging.debug(f"list has:{len(uniqueDates)} dates in it after next")
                            
                        """with open('DateMetaData.csv', 'a', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(DateMetaData)
                        """
                    return uniqueDates
                else:
                    logging.warning("No features found on the date")
                    with open("ResponseCatalog.txt", "a") as f:
                        f.write("Polygon: ")
                        json.dump(Polygon, f)
                        f.write("\n")
                        json.dump(responseJson, f, indent=4)

                    
            elif statuscode == 401:
                logging.error("Access code has expired or is incorrect.")
                self.ApiToken = self.TokenApiHandler.GetToken()
                Data = self.GetPictureDates(Polygon, FieldId,FromDate,ToDate)
                return Data
            elif statuscode == 403:
                logging.error("No more tokens")
                return "No Tokens"
            else:
                description = responseJson.get("description", "No description provided")
                logging.error(f"Request failed (Status: {response.status_code}) - {description}")
                raise Exception(f"API request failed with status {response.status_code}: {description}")

    def GetPictureBBoxes(self, Polygon, FieldID, next=0):
        """Gets a list of unique bounding boxes where the satellite took a picture based on a polygon."""

        logging.info(f"Calling the Catalog API for catalog data on Field with ID: {FieldID}...")

        url = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.ApiToken}"
        }

        data = {
            "collections": ["sentinel-2-l1c"],
            "datetime": f"{self.FromDate}/{self.ToDate}",
            "intersects": {
                "type": "Polygon",
                "coordinates": Polygon
            },
            "limit": 100
        }

        if next != 0:
            data["next"] = next

        response = requests.post(url, headers=headers, json=data)
        responseJson = response.json()
        statuscode = response.status_code
        logging.debug(f"statuscode:{statuscode}")
        if statuscode == 200:
            Features = responseJson.get("features", [])
            logging.info(f"Found {len(Features)} number of features")

            uniqueBBoxes = set()
            for Feature in Features:
                bbox = Feature.get("bbox")
                if bbox:
                    bbox_tuple = tuple(bbox)
                    if bbox_tuple not in uniqueBBoxes:
                        uniqueBBoxes.add(bbox_tuple)
                        logging.debug(f"Added new bbox: {bbox_tuple}")

            with open("ResponseCatalog.txt", "w") as f:
                json.dump(responseJson, f, indent=4)

            context = responseJson.get("context", {})
            next = context.get("next")
            if next:
                logging.debug(f"List has {len(uniqueBBoxes)} bboxes before fetching next page")
                uniqueBBoxes.update(self.GetPictureBBoxes(Polygon, FieldID, next)) 
                logging.debug(f"List has {len(uniqueBBoxes)} bboxes after fetching next page")

            return uniqueBBoxes

        elif statuscode == 401:
            logging.error("Access code has expired or is incorrect.")
            self.ApiToken = self.TokenApiHandler.GetToken()
            return self.GetPictureBBoxes(Polygon=Polygon, FieldID=FieldID)
        elif statuscode == 403:
            logging.error("No more tokens")
            return "No Tokens"
        else:
            description = responseJson.get("description", "No description provided")
            logging.error(f"Request failed (Status: {statuscode}) - {description}")
            raise Exception(f"API request failed with status {statuscode}: {description}")

