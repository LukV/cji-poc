"""
Configuration file for the application.
"""
import os
from dotenv import load_dotenv

load_dotenv()

client_secret = os.getenv("CLIENT_SECRET")
client_id = os.getenv("CLIENT_ID")
token_url = os.getenv("TOKEN_URL")
