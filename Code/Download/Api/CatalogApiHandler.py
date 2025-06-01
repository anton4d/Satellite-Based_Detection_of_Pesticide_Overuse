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
            self.ApiToken = self.TokenApiHandler.Get_Token()
        else:
            self.ApiToken = ApiToken

    def Try_Again(self,Polygon, FieldId,FromDate,ToDate,next,count,MaxAttempts):
        if count <= MaxAttempts:
            timer = count*10
            time.sleep(timer)
            newcount = count + 1
            self.Get_Picture_Dates(Polygon, FieldId,FromDate,ToDate,next, newcount,MaxAttempts)
        else:
            logging.error(f"After {MaxAttempts} attempts the dates was still not able to be downloaded")

    def Get_Picture_Dates(self,Polygon, FieldId,From_Date,To_Date, next = 0,count =1, MaxAttempts = 12):
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
            "datetime": f"{From_Date}/{To_Date}",
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
            logging.error(f"Timeout Error: {errt}")
            self.Try_Again(Polygon,FieldId,From_Date,To_Date,next,count,MaxAttempts)
        except requests.exceptions.ConnectionError as errc:
            logging.error(f"Connection Error: {errc}")
            self.Try_Again(Polygon,FieldId,From_Date,To_Date,next,count,MaxAttempts)
        except requests.exceptions.HTTPError as errh:
            Status_Code = errh.response.status_code
            if Status_Code == 401:
                logging.error("Access code has expired or is incorrect.")
                self.ApiToken = self.TokenApiHandler.Get_Token()
                Data = self.Get_Picture_Dates(Polygon, FieldId,From_Date,To_Date)
                return Data
            elif Status_Code == 403:
                logging.error(f"Request failed (Status: {response.status_code}) - (Respone:{response.text})")
                logging.error("No more tokens")
                self.ApiToken, self.ClientId = self.TokenApiHandler.Set_A_New_User_If_Any(self.ClientId)
                Data = self.Get_Picture_Dates(Polygon, FieldId,From_Date,To_Date)
                return Data
            else:
                description = response.json().get("description", "")
                logging.error(f"HTTP {Status_Code}: {description}", exc_info=True)
                raise
        except requests.exceptions.RequestException as err:
            logging.error(f"Unexpected request error: {err}")
        else:
            data = response.json()
            Features = data.get("features", [])
            if not Features:
                logging.warning("No features found")
                return set()
            
            logging.info(f"Received {len(Features)} features from {From_Date} to {To_Date}")
            Unique_Dates = set()
            for feature in Features:
                props = feature.get("properties", {})
                timestamp = props.get("datetime", "")
                platform = props.get("platform", "")
                Cloud_Cover = props.get("eo:cloud_cover", 0.0)

                if not timestamp:
                    logging.warning("Feature missing datetime, skipping")
                    continue

                Date_Only = timestamp.split("T", 1)[0]
                if Date_Only not in Unique_Dates:
                    Unique_Dates.add(Date_Only)
                    logging.debug(f"Adding new date: {Date_Only}")

                metadata = [str(FieldId), timestamp, Date_Only, platform, Cloud_Cover]
                try:
                    self.dbhandler.Insert_Boundingbox_Meta_Data(metadata)
                except Exception:
                    logging.exception(f"Failed to insert metadata for {timestamp}")


                Next_tok = data.get("context", {}).get("next")
                if Next_tok:
                    logging.info(f"Fetching next page with token {Next_tok}")
                    Unique_Dates.update(
                        self.Get_Picture_Dates(Polygon, FieldId,From_Date,To_Date ,Next_tok)
                    )

                if not Unique_Dates:
                    logging.warning("No features found")

            return Unique_Dates

