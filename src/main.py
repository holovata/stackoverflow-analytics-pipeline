import datetime
import argparse
from api_client import StackOverflowAPIClient
from db_manager import DBManager

from logger import get_logger
logger = get_logger(__name__)


def run_pipeline(tag: str = None, total_pages: int = 1, from_date: int = None, to_date: int = None):
    """
    Orchestrates the ETL process with historical date support.
    """
    logger.info(f"--- Starting ETL Pipeline | Dates: {from_date} to {to_date} ---")

    api_client = StackOverflowAPIClient()
    db_manager = DBManager()
    current_page = 1

    while current_page <= total_pages:
        data = api_client.fetch_questions(
            tag=tag,
            page=current_page,
            from_date=from_date,
            to_date=to_date
        )

        items = data.get('items', [])
        if not items:
            logger.warning(f"No items found on page {current_page}. Stopping.")
            break

        # Load data into PostgreSQL
        db_manager.save_questions(items)
        logger.info(f"Page {current_page} processed. Items loaded: {len(items)}")

        # Stop if the API indicates there are no more pages
        if not data.get('has_more'):
            break

        current_page += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stack Overflow ETL Pipeline")

    # Default start date: December 1, 2025
    parser.add_argument("--start", type=str, default="2025-12-01", help="Start date in YYYY-MM-DD format")

    # Default end date: Today
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    parser.add_argument("--end", type=str, default=today_str, help="End date in YYYY-MM-DD format")

    # Default pages: 80 (up to 8000 questions)
    parser.add_argument("--pages", type=int, default=80, help="Number of pages to fetch (100 items per page)")

    args = parser.parse_args()
    start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d")

    # For the end date, set the time to the end of the day (23:59:59) to capture the full day
    end_date_parsed = datetime.datetime.strptime(args.end, "%Y-%m-%d")
    end_date = end_date_parsed.replace(hour=23, minute=59, second=59)

    # Convert standard datetime objects to Unix Timestamps for the API
    from_ts = int(start_date.timestamp())
    to_ts = int(end_date.timestamp())

    logger.info(f"=== ETL PIPELINE INITIATED ===")
    logger.info(f"Target Period: {start_date.date()} to {end_date.date()}")
    logger.info(f"Pages to fetch: {args.pages} (Up to {args.pages * 100} questions)")

    run_pipeline(
        tag=None,
        total_pages=args.pages,
        from_date=from_ts,
        to_date=to_ts
    )

    logger.info("=== Pipeline completed! Update Power BI report now. ===")