from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
