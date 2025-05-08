import requests
import dotenv
import logging
import json
import sys
import os
import time
from datetime import datetime, timedelta
from Api.TokenApiHandler import TokenApiHandler
from Database.SQLHandler import SQLHandler

class ProcessApiHandler:
    def __init__(self, ApiToken,TokenApiHandler: TokenApiHandler, SQLHandler:SQLHandler,ClientID):
        self.TokenApiHandler = TokenApiHandler
        self.SQLHandler = SQLHandler
        self.ClientID = ClientID

        if ApiToken is None:
            self.ApiToken = self.TokenApiHandler.GetToken()
        else:
            self.ApiToken = ApiToken

    def getSurroundingDates(self, date):
        """
        Takes a date string in 'yyyy-mm-dd' format and returns two timestamps:
        - One day before at 00:00:00 UTC.
        - One day after at 23:59:59 UTC.
        """
        dateObj = datetime.strptime(date, "%Y-%m-%d")
        
        dateBefore = (dateObj - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        dateAfter = (dateObj + timedelta(days=1)).strftime("%Y-%m-%dT23:59:59Z")
        
        return dateBefore, dateAfter

    def tryagain(self,Date,polygon,FieldId,resolution,folderName,count,MaxAttempts=12):
        if count <= MaxAttempts:
            timer = count*10
            time.sleep(timer)
            newcount = count + 1
            self.processDateIntoImages(Date,polygon,FieldId,resolution,folderName,MaxAttempts,newcount)
        else:
            logging.error(f"After {MaxAttempts} attempts the image was still not able to be downloaded")

    def processDateIntoImages(self,Date,polygon,FieldId,resolution,folderName,MaxAttempts=12, count=1):
        """
        Takes a date and polygon and inserts the data from the picture of the date and polygon into the database
        """
        logging.info(f"Get picture from satalite on the date of: {Date} with resolution: {resolution}")
        dateBefore, dateAfter = self.getSurroundingDates(Date)
        logging.debug(f"converted the date into before date: {dateBefore} and After date: {dateAfter} ")

        url = "https://sh.dataspace.copernicus.eu/api/v1/process"
        headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {self.ApiToken}"
        }
        evalscript = """
                function setup() {
                return {
                    input: [
                    {
                        bands: ["B02","B03","B04","B05","B08","dataMask"],
                    },
                    ],
                    output: {
                    id: "default",
                    bands: 6,
                    sampleType: SampleType.FLOAT32,
                    },
                }
                }

                function evaluatePixel(sample) {
                return [
                    sample.B02,
                    sample.B03,
                    sample.B04,
                    sample.B05,
                    sample.B08,
                    sample.dataMask,
                ]
                }
        """
        data = {
        "input": {
            "bounds": {
            "geometry": {
                "type": "Polygon",
                "coordinates": polygon
            }
            },
            "data": [
            {
                "dataFilter": {
                "timeRange": {
                    "from": dateBefore,
                    "to": dateAfter
                    }
                },
                "type": "sentinel-2-l1c"
            }
            ]
        },
        "output": {
            "width": resolution,
            "height": resolution,
            "responses": [
            {
                "identifier": "default",
                "format": {
                "type": "image/tiff"
                }
            }
            ]
        },
        "evalscript": evalscript
        }
        try:
            response = requests.post(url, headers=headers, json=data, timeout=3)
            response.raise_for_status()
        except requests.exceptions.Timeout as errt:
            logging.error("Timeout Error:", errt)
            self.tryagain(Date, polygon, FieldId, resolution, folderName, count, MaxAttempts)
        except requests.exceptions.ConnectionError as errc:
            logging.error("Connection Error:", errc)
            self.tryagain(Date, polygon, FieldId, resolution, folderName, count, MaxAttempts)
        except requests.exceptions.HTTPError as errh:
            status = errh.response.status_code
            logging.error(f"HTTP Error {status}: {errh}")
            if status == 401:
                self.ApiToken = self.TokenApiHandler.GetToken()
                self.processDateIntoImages(Date, polygon, FieldId, resolution, folderName, MaxAttempts, count)
            elif status == 403:
                logging.error("No more tokens – swapping user")
                self.ApiToken, self.ClientID = self.TokenApiHandler.SetANewUserIfAny(self.ClientID)
                self.processDateIntoImages(Date, polygon, FieldId, resolution, folderName, MaxAttempts, count)
            else:
                self.tryagain(Date, polygon, FieldId, resolution, folderName, count, MaxAttempts)
        except requests.exceptions.RequestException as err:
            logging.error("Unexpected request error:", err)
        else:
            os.makedirs(folderName, exist_ok=True)
            image_path = os.path.join(folderName, f"{Date}.tiff")
            try:
                with open(image_path, "wb") as f:
                    f.write(response.content)
                logging.info(f"Image saved to {image_path} after {count} tries")
            except Exception:
                logging.exception(f"Failed to save image to {image_path}")

