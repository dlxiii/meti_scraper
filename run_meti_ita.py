from datetime import datetime
import sys

from meti_scraper import meti


if __name__ == "__main__":
    today = datetime.today()

    scraper = meti()
    try:
        ita_paths = scraper.index_of_tertiary_industry_activity(date=today.strftime("%Y%m"))
        for path in ita_paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
