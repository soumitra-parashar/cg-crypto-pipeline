from fileinput import filename
from urllib import response

from more_itertools import bucket
from pydantic import FilePath
from pydantic_core import Url
import requests
import json
import os
import boto3
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def upload_to_s3(filepath, filename):
    s3= boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket_name = os.getenv('S3_BUCKET_NAME')
    s3_key = f"raw/{filename}"
    print(f" Uploading to S3 bucket: {bucket_name}...")
    s3.upload_file(filepath, bucket_name, s3_key)
    print("Upload Complete!")

def fetch_crypto_data():
    url= "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.now().strftime("%Y&m%d_%H%M%S")
    filename = f"market_data_{timestamp}.json"
    filepath = os.path.join("data","raw", filename)

    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

    upload_to_s3(filepath, filename)

if __name__ == "__main__":
    fetch_crypto_data()