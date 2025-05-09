import requests, csv, dotenv,logging,json,sys
from shapely import wkt
from Api.TokenApiHandler import TokenApiHandler
from Database.SQLHandler import SQLHandler
import time

class CatalogApiHandler:
    def __init__(self,ApiToken,TokenApiHandler:TokenApiHandler,dbHandler:SQLHandler,ClientID):
        self.TokenApiHandler = TokenApiHandler
        self.dbhandler = dbHandler
        self.ClientId = ClientID

        if ApiToken is None:
            self.ApiToken = self.TokenApiHandler.GetToken()
        else:
            self.ApiToken = ApiToken

    def tryagain(self,Polygon, FieldId,FromDate,ToDate,next,count,MaxAttempts):
        if count <= MaxAttempts:
            timer = count*10
            time.sleep(timer)
            newcount = count + 1
            self.GetPictureDates(Polygon, FieldId,FromDate,ToDate,next, newcount,MaxAttempts)
        else:
            logging.error(f"After {MaxAttempts} attempts the dates was still not able to be downloaded")

    def GetPictureDates(self,Polygon, FieldId,FromDate,ToDate, next = 0,count =1, MaxAttempts = 12):
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
        try:
            response = requests.post(url, headers=headers, json=data,timeout=3)
            response.raise_for_status()
        except requests.exceptions.Timeout as errt:
            message = f"Timeout Error: {errt}"
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.exception(message)
            else:
                logging.error(message)
            self.tryagain(Polygon,FieldId,FromDate,ToDate,next,count,MaxAttempts)
        except requests.exceptions.ConnectionError as errc:
            message =f"Connection Error: {errc}"
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.exception(message)
            else:
                logging.error(message)
            self.tryagain(Polygon,FieldId,FromDate,ToDate,next,count,MaxAttempts)
        except requests.exceptions.HTTPError as errh:
            statuscode = errh.response.status_code
            message = f"HTTP Error: {errh}"
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.exception(message)
            else:
                logging.error(message)
            if statuscode == 401:
                self.ApiToken = self.TokenApiHandler.GetToken()
                Data = self.GetPictureDates(Polygon, FieldId,FromDate,ToDate)
                return Data
            elif statuscode == 403:
                logging.error("No more tokens – swapping user")
                self.ApiToken, self.ClientId = self.TokenApiHandler.SetANewUserIfAny(self.ClientId)
                Data = self.GetPictureDates(Polygon, FieldId,FromDate,ToDate)
                return Data
            else:
                description = response.json().get("description", "")
                logging.error(f"HTTP {statuscode}: {description}", exc_info=True)
                raise
        except requests.exceptions.RequestException as err:
            message =f"Unexpected request error: {err}"
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.exception(message)
            else:
                logging.error(message)
        else:
            data = response.json()
            Features = data.get("features", [])
            if not Features:
                logging.warning("No features found; dumping response to ResponseCatalog.txt")
                with open("ResponseCatalog.txt", "a") as f:
                    f.write(f"Polygon: {json.dumps(Polygon)}\n")
                    json.dump(data, f, indent=2)
                    f.write("\n\n")
                return set()
            
            logging.info(f"Received {len(Features)} features from {FromDate} to {ToDate}")
            uniqueDates = set()
            for feature in Features:
                props = feature.get("properties", {})
                timestamp = props.get("datetime", "")
                platform = props.get("platform", "")
                cloud_cover = props.get("eo:cloud_cover", 0.0)

                if not timestamp:
                    logging.warning("Feature missing datetime, skipping")
                    continue

                date_only = timestamp.split("T", 1)[0]
                if date_only not in uniqueDates:
                    uniqueDates.add(date_only)
                    logging.debug(f"Adding new date: {date_only}")

                metadata = [FieldId, timestamp, date_only, platform, cloud_cover]
                try:
                    self.dbhandler.insertBoundingboxMetaData(metadata)
                except Exception:
                    logging.exception(f"Failed to insert metadata for {timestamp}")


                next_tok = data.get("context", {}).get("next")
                if next_tok:
                    logging.info(f"Fetching next page with token {next_tok}")
                    uniqueDates.update(
                        self.GetPictureDates(Polygon, FieldId,FromDate,ToDate ,next_tok)
                    )

                if not uniqueDates:
                    logging.warning("No features found; dumping response to ResponseCatalog.txt")
                    with open("ResponseCatalog.txt", "a") as f:
                        f.write(f"Polygon: {json.dumps(Polygon)}\n")
                        json.dump(data, f, indent=2)
                        f.write("\n\n")

            return uniqueDates





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
            self.ApiToken = self.SetANewUserIfAny()
            return self.GetPictureBBoxes(Polygon=Polygon, FieldID=FieldID)
        else:
            description = responseJson.get("description", "No description provided")
            logging.error(f"Request failed (Status: {statuscode}) - {description}")
            raise Exception(f"API request failed with status {statuscode}: {description}")

