from datetime import datetime
import sys

from meti_scraper import meti


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python run_meti_lng_weekly_inventory.py YYYY/MM/DD')
        sys.exit(1)

    date_str = sys.argv[1]
    dt = datetime.strptime(date_str, '%Y/%m/%d')

    scraper = meti()
    scraper.lng_weekly_inventory(date=dt.strftime('%Y%m%d'))
