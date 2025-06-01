import requests
import dotenv
import logging
import json
import sys
import csv
import shutil
from tempfile import NamedTemporaryFile
from Database.SQLHandler import SQLHandler

class TokenApiHandler:
    def __init__(self, ClientId, ClientSecret,SQLHandler:SQLHandler):
        self.ClientId = ClientId
        self.ClientSecret = ClientSecret
        self.SQLHandler = SQLHandler

    def Set_A_New_User_If_Any(self,OldClientID):
        if OldClientID == self.ClientId:
            ClientID,ClientSecret = self.SQLHandler.Api_User(OldClientID)
            if ClientID:
                dotenvFile = dotenv.find_dotenv()
                logging.info(f"Using client_id: {ClientID}, client_secret: {ClientSecret}")
                self.ClientId = ClientID
                self.ClientSecret = ClientSecret
                dotenv.set_key(dotenvFile, "APIToken", ClientID)
                dotenv.set_key(dotenvFile, "APIToken", ClientSecret)
                Token = self.Get_Token()
                return Token, ClientID
            else:
                logging.error("No valid user found.")
                sys.exit(7)
        else:
            Token = self.Get_Token()
            return Token, self.ClientId

    def Get_Token(self):
            """Gets an Access Token from the API based on the client credentials"""

            logging.info("Attempting to get an API Token...")
            
            dotenvFile = dotenv.find_dotenv()
            AuthServerUrl = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            TokenRequestPayload = {"grant_type": "client_credentials"}
            try:
                tokenResponse = requests.post(
                    AuthServerUrl,
                    data=TokenRequestPayload,
                    verify=False,
                    allow_redirects=False,
                    auth=(self.ClientId, self.ClientSecret))
                tokenResponse.raise_for_status()
            except requests.exceptions.Timeout as errt:
                logging.error(f"Timeout Error: {errt}")
                
            except requests.exceptions.ConnectionError as errc:
                logging.error(f"Connection Error: {errc}")
                
            except requests.exceptions.HTTPError as errh:
                logging.error(f"Failed to obtain token. Status code: {tokenResponse.status_code}")
                logging.error(f"Response: {tokenResponse.text}")
                raise Exception("Authentication failed. Check your credentials.")
            except requests.exceptions.RequestException as err:
                logging.error(f"Unexpected request error: {err}")
            else:
                tokenjson = tokenResponse.json()
                token = tokenjson["access_token"]
                dotenv.set_key(dotenvFile, "APIToken", token)
                logging.info("Successfully obtained a new token.")
                return token    
