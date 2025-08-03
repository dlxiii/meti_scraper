from datetime import datetime
import sys

from esri_scraper import esri


if __name__ == "__main__":
    today = datetime.today()

    scraper = esri()
    try:
        paths = scraper.kp23(date=today.strftime("%Y%m"))
        for path in paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
