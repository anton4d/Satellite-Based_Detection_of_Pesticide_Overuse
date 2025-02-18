import requests
import dotenv
import logging
import json
import sys

class ProcessApiHandler:
    def __init__(self, ApiToken,TokenApiHandler):
        self.TokenApiHandler = TokenApiHandler

        if ApiToken is None:
            self.ApiToken = self.TokenApiHandler.GetToken()
        else:
            self.ApiToken = ApiToken
    
    def processDateIntoData(self,Date,polygon):

