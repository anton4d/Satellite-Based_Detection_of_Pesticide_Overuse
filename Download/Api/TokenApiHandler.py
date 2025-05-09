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
    def __init__(self,SQLHandler:SQLHandler):
        self.SQLHandler = SQLHandler
        self.ClientId ="None"

    def SetANewUserIfAny(self,OldClientID):
        if OldClientID == self.ClientId:
            ClientID,ClientSecret = self.SQLHandler.ApiUser(OldClientID)
            if ClientID:
                dotenvFile = dotenv.find_dotenv()
                logging.info(f"Using client_id: {ClientID}, client_secret: {ClientSecret}")
                self.ClientId = ClientID
                self.ClientSecret = ClientSecret
                dotenv.set_key(dotenvFile, "APIToken", ClientID)
                dotenv.set_key(dotenvFile, "APIToken", ClientSecret)
                Token = self.GetToken()
                return Token, ClientID
            else:
                logging.fatal("No valid user found.")
                sys.exit(7)
        else:
            Token = self.GetToken()
            return Token, self.ClientId

    def GetToken(self):
            """Gets an Access Token from the API based on the client credentials"""

            logging.info("Attempting to get an API Token...")
            
            dotenvFile = dotenv.find_dotenv()
            AuthServerUrl = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            TokenRequestPayload = {"grant_type": "client_credentials"}
            try: 
                response = requests.post(AuthServerUrl,
                                         data=TokenRequestPayload,
                                         verify=False,
                                         allow_redirects=False,
                                         auth=(self.ClientId, self.ClientSecret),
                                         timeout=3
                                         )
            except requests.exceptions.Timeout as errt:
                messesage = f"Timeout Error: {errt}"
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.exception(messesage)
                else:
                    logging.error(messesage)
                return self.GetToken()
            except requests.exceptions.ConnectionError as errc:
                messesage = f"Connection Error: {errc}"
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.exception(messesage)
                else:
                    logging.error(messesage)
                return self.GetToken()
            except requests.exceptions.HTTPError as errh:
                status = errh.response.status_code
                if status == 401:
                    logging.error(f"The Found Client Id:{self.ClientId} \n is Invalid or has Invalid client credentials")
                    self.SetANewUserIfAny(self.ClientId)
            except requests.exceptions.RequestException as err:
                messesage =f"Unexpected request error: {err}"
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.exception(messesage)
                else:
                    logging.error(messesage)
            else:
                tokenjson = response.json()
                token = tokenjson["access_token"]
                dotenv.set_key(dotenvFile, "APIToken", token)
                logging.info("Successfully obtained a new token.")
                return token    