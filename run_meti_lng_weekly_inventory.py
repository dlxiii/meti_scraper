from datetime import datetime, timedelta
import argparse
import os
import sys

from meti_scraper import meti


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("METI_TIMEOUT", 10)),
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--proxy",
        type=str,
        default=os.getenv("METI_PROXY"),
        help="HTTPS proxy server URL",
    )
    args = parser.parse_args()

    today = datetime.today()
    weekday = today.weekday()  # Monday = 0, Sunday = 6

    if weekday >= 2:
        monday = today - timedelta(days=weekday)
        target_wed = monday + timedelta(days=2)
    else:
        monday = today - timedelta(days=weekday + 7)
        target_wed = monday + timedelta(days=2)

    scraper = meti()
    try:
        proxies = {"https": args.proxy} if args.proxy else None
        pdf_path = scraper.lng_weekly_inventory(
            date=target_wed.strftime("%Y%m%d"),
            timeout=args.timeout,
            proxies=proxies,
        )
        md_path = scraper.pdf_to_markdown(pdf_path)
        print(md_path)
        csv_paths = scraper.pdf_tables_to_csv(pdf_path)
        for path in csv_paths:
            print(path)
    except RuntimeError as err:
        print(err)
        print("Check network connection or if the METI site is reachable.")
        sys.exit(1)
    except Exception as err:
        print(f"Unexpected error: {err}")
        sys.exit(1)

