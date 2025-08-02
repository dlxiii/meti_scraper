from datetime import datetime
import sys

from meti_scraper import meti


if __name__ == "__main__":
    today = datetime.today()

    scraper = meti()
    try:
        iip_paths = scraper.index_of_industrial_production(date=today.strftime("%Y%m"))
        for path in iip_paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
