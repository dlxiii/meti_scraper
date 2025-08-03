from datetime import datetime
import sys

from mlit_scraper import mlit


if __name__ == "__main__":
    today = datetime.today()

    scraper = mlit()
    try:
        paths = scraper.jutaku(date=today.strftime("%Y%m"))
        for path in paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
