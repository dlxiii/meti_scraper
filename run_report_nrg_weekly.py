from datetime import datetime, timedelta
import sys

from report_scraper import nrg


if __name__ == "__main__":
    today = datetime.today()
    weekday = today.weekday()  # Monday = 0, Sunday = 6
    if weekday == 0:
        monday = today - timedelta(days=7)
    else:
        monday = today - timedelta(days=weekday)

    scraper = nrg()

    if len(sys.argv) == 3:
        start = datetime.strptime(sys.argv[1], "%Y%m%d")
        end = datetime.strptime(sys.argv[2], "%Y%m%d")
        print(
            f"Downloading Japan NRG Weekly from {start:%Y-%m-%d} to {end:%Y-%m-%d}"
        )
        current = start
        while current <= end:
            try:
                path = scraper.nrg_japan_weekly(date=current)
                print(path)
            except RuntimeError as err:
                print(err)
            current += timedelta(days=7)
    else:
        try:
            path = scraper.nrg_japan_weekly(date=monday)
            print(path)
        except RuntimeError as err:
            print(err)
            sys.exit(1)
