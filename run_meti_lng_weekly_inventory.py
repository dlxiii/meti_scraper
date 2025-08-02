from datetime import datetime, timedelta

from meti_scraper import meti


if __name__ == '__main__':
    today = datetime.today()
    offset = (today.weekday() - 2) % 7 or 7
    last_wednesday = today - timedelta(days=offset)

    scraper = meti()
    scraper.lng_weekly_inventory(date=last_wednesday.strftime('%Y%m%d'))
