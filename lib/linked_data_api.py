"""This module provides a class to interact with the Linked Data API of UiTwisselingsplatform."""
from authlib.integrations.httpx_client import OAuth2Client
from pandas import DataFrame
import conf.config as config

class LinkedDataAPI:
    """
    LinkedDataAPI is a class that provides methods to interact 
    with the Linked Data API of UiTwisselingsplatform.
    """
    def __init__(self):
        self.client = OAuth2Client(
            client_id=config.client_id,
            client_secret=config.client_secret,
            scope='profile email openid')

        self.client.fetch_token(config.token_url)
        self.data_endpoint = None
        self.query = None

    def set_data_endpoint(self, data_endpoint):
        """Set the data endpoint of the Linked Data API."""
        self.data_endpoint = data_endpoint

    def set_query(self, query):
        """Set the query to be sent to the Linked Data API."""
        self.query = query

    def fetch_data(self):
        """Fetch data from the Linked Data API."""
        response = self.client.post(self.data_endpoint, data={'query': self.query}, timeout=None)
        if response.status_code == 200:
            data = response.json()
            df = DataFrame(data['results']['bindings'])
            return df.map(lambda x: x['value'])
        else:
            print(f'Error: {response.status_code} - {response.text}')
            return None
