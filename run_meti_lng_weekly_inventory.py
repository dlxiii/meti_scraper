from datetime import datetime, timedelta
import sys

from meti_scraper import meti


if __name__ == "__main__":
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
        pdf_path = scraper.lng_weekly_inventory(date=target_wed.strftime("%Y%m%d"))
        scraper.pdf_to_markdown(pdf_path)
        scraper.pdf_tables_to_csv(pdf_path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)

