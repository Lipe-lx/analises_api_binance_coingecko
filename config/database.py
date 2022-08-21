import os
from dotenv import load_dotenv

load_dotenv()

database_infos = {

    'api_key': os.getenv('api_k'),
    'api_secret': os.getenv('api_s'),
}