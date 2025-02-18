import requests
import dotenv
import logging
import json
import sys
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
        
        dateBefore = (dateObj - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")  # Start of previous day
        dateAfter = (dateObj + timedelta(days=1)).strftime("%Y-%m-%dT23:59:59Z")  # End of next day
        
        return dateBefore, dateAfter

    def processDateIntoData(self,Date,polygon):
        """
        Takes a date and polygon and inserts the data from the picture of the date and polygon into the database
        """
        logging.info(f"Get picture from satalite on the date of: {Date}")
        dateBefore, dateAfter = self.getSurroundingDates(Date)
        logging.info(f"converted the date into before date: {dateBefore} and After date: {dateAfter} ")

        url = "https://sh.dataspace.copernicus.eu/api/v1/process"
        headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {self.ApiToken}"
        }
        evalscript = """
            //VERSION=3

            let minVal = 0.0;
            let maxVal = 0.4;

            let viz = new HighlightCompressVisualizer(minVal, maxVal);

            function evaluatePixel(samples) {
                let val = [samples.B04, samples.B03, samples.B02];
                val = viz.processList(val);
                val.push(samples.dataMask);
                return val;
            }

            function setup() {
            return {
                input: [{
                bands: [
                    "B02",
                    "B03",
                    "B04",
                    "dataMask"
                ]
                }],
                output: {
                bands: 4
                }
            }
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
                },
                "maxCloudCoverage": 95
                },
                "type": "sentinel-2-l1c"
            }
            ]
        },
        "output": {
            "width": 512,
            "height": 512,
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

        response = requests.post(url, headers=headers, json=data)
        StatusCode = response.status_code
        if StatusCode == 200:
            image_path = "new_image.tiff"
            with open(image_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Image successfully saved as {image_path}")
        
        else:
            logging.error(f"Request failed (Status: {response.status_code}) - (Respone:{response.text})")
            if StatusCode == 401:
                self.ApiToken = self.TokenApiHandler.GetToken()
                self.processDateIntoData(Date,polygon)
            else:
                raise Exception(f"API request failed with status {response.status_code}: {description}")

