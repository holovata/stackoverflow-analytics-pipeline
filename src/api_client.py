import os
import time
import requests
from dotenv import load_dotenv
from typing import Dict, Any

from logger import get_logger
logger = get_logger(__name__)

load_dotenv(encoding='utf-8')


class StackOverflowAPIClient:
    """
    Client for extracting data from the Stack Exchange API.
    Implements session management and rate limit handling (backoff).
    """

    BASE_URL = "https://api.stackexchange.com/2.3"

    def __init__(self):
        self.api_key = os.getenv("STACK_API_KEY")
        self.filter_id = os.getenv("STACK_FILTER")

        # reusing the same TCP connection
        self.session = requests.Session()

    def fetch_questions(self, tag: str = None, page: int = 1, pagesize: int = 100, from_date: int = None, to_date: int = None) -> Dict[str, Any]:
        endpoint = f"{self.BASE_URL}/questions"

        params = {
            "order": "desc",
            "sort": "creation",
            "site": "stackoverflow",
            "page": page,
            "pagesize": pagesize,
            "filter": self.filter_id,
            "key": self.api_key
        }

        if tag:
            params["tagged"] = tag
        if from_date:
            params["fromdate"] = from_date
        if to_date:
            params["todate"] = to_date

        try:
            log_msg = f"Fetching page {page} for tag '{tag}'..." if tag else f"Fetching global page {page} (all topics)..."
            logger.info(log_msg)

            response = self.session.get(endpoint, params=params)
            response.raise_for_status()

            data = response.json()

            if 'backoff' in data:
                backoff_seconds = data['backoff']
                logger.warning(f"API requested backoff. Sleeping for {backoff_seconds} seconds...")
                time.sleep(backoff_seconds + 1)

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            return {"items": [], "has_more": False}


# Test block
if __name__ == "__main__":
    client = StackOverflowAPIClient()

    result = client.fetch_questions(tag="python", page=1, pagesize=5)

    items = result.get("items", [])
    quota = result.get("quota_remaining", "Unknown")

    print(f"\n--- Test Results ---")
    print(f"Questions received: {len(items)}")
    print(f"Remaining API quota: {quota}")

    if items:
        print(f"First question title: {items[0].get('title')}")