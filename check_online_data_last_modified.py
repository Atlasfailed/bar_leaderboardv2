#!/usr/bin/env python3
"""
Check last modified date for online datamart files via HTTP HEAD requests.
"""
import requests
from urllib.parse import urlparse

# List your online data file URLs here
DATA_URLS = [
    "https://data-marts.beyondallreason.dev/matches.parquet"
]

def main():
    if not DATA_URLS:
        print("Please add your data file URLs to DATA_URLS in this script.")
        return

    print("Last modified times for online data files:")
    for url in DATA_URLS:
        try:
            response = requests.head(url, timeout=10)
            last_modified = response.headers.get("Last-Modified", "N/A")
            filename = urlparse(url).path.split("/")[-1]
            print(f"{filename:35}  {last_modified}")
        except Exception as e:
            print(f"{url}  ERROR: {e}")

if __name__ == "__main__":
    main()
