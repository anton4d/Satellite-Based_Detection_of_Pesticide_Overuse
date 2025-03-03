import requests
import dotenv
import logging
import json
import sys
import os
from datetime import datetime, timedelta

class ProcessApiHandler:
    def __init__(self, ApiToken,TokenApiHandler, SQLHandler):
        self.TokenApiHandler = TokenApiHandler
        self.SQLHandler = SQLHandler

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

    def processDateIntoImages(self,Date,polygon,FieldId):
        """
        Takes a date and polygon and inserts the data from the picture of the date and polygon into the database
        """
        logging.info(f"Get picture from satalite on the date of: {Date}")
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
                        bands: ["B04", "B08","dataMask"],
                        units: "DN",
                    },
                    ],
                    output: {
                    id: "default",
                    bands: 3,
                    sampleType: SampleType.UINT16,
                    },
                }
                }

                function evaluatePixel(sample) {
                return [
                    sample.B04,
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
            "width": 1024,
            "height": 1024,
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

     
        with requests.post(url, headers=headers, json=data) as response:
            StatusCode = response.status_code
            if StatusCode == 200:
                folderName = f"Pictures/FieldId{FieldId}"  

                os.makedirs(folderName, exist_ok=True)
                image_path = os.path.join(folderName, f"{Date}.tiff")
                try:
                    with open(image_path, "wb") as f: 
                        f.write(response.content)
                    logging.info(f"Image successfully saved as {image_path}")
                except Exception as e:
                    logging.error(f"Failed to save image: {e}")
            

            else:
                logging.error(f"Request failed (Status: {response.status_code}) - (Respone:{response.text})")
                if StatusCode == 401:
                    self.ApiToken = self.TokenApiHandler.GetToken()
                    self.processDateIntoImages(Date,polygon, FieldId)
                else:
                    raise Exception(f"API request failed with status {response.status_code}: {description}")

