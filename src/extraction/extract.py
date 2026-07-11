import requests
import json
import os
from datetime import datetime

def fetch_crypto_data():

    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False
    }

    print("Pinging CoinGecko API...")

    try:
        response = requests.get(url, params=params)

        response.raise_for_status()

        data = response.json()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_raw_data_{timestamp}.json"
        filepath = os.path.join("data","raw", filename)

        os.makedirs(os.path.join("data", "raw"), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"Success! Extracted {len(data)} coins and saved to {filepath}")

    except requests.exceptions.RequestException as e:
        print(f" ERROR: No files found: {e}")

if __name__ == "__main__":
    fetch_crypto_data()