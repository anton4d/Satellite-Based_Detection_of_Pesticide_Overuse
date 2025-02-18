import requests
import dotenv
import logging
import json
import sys


class TokenApiHandler:
    def __init__(self, ClientId, ClientSecret):
        self.ClientId = ClientId
        self.ClientSecret = ClientSecret

    def GetToken(self):
            """Gets an Access Token from the API based on the client credentials"""

            logging.info("Attempting to get an API Token...")
            
            dotenvFile = dotenv.find_dotenv()
            AuthServerUrl = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            TokenRequestPayload = {"grant_type": "client_credentials"}

            tokenResponse = requests.post(
                AuthServerUrl,
                data=TokenRequestPayload,
                verify=False,
                allow_redirects=False,
                auth=(self.ClientId, self.ClientSecret)
            )

            if tokenResponse.status_code != 200:
                logging.error(f"Failed to obtain token. Status code: {tokenResponse.status_code}")
                logging.error(f"Response: {tokenResponse.text}")
                raise Exception("Authentication failed. Check your credentials.")

            tokenjson = tokenResponse.json()
            token = tokenjson["access_token"]
            dotenv.set_key(dotenvFile, "APIToken", token)
            logging.info("Successfully obtained a new token.")
            return token    
