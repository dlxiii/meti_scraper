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
        current = start + timedelta(days=(7 - start.weekday()) % 7)
        while current <= end:
            try:
                path = scraper.nrg_japan_weekly(date=current)
            except RuntimeError as err_mon:
                try:
                    path = scraper.nrg_japan_weekly(date=current + timedelta(days=1))
                except RuntimeError as err_tue:
                    print(err_mon)
                    print(err_tue)
                    current += timedelta(days=7)
                    continue
            print(path)
            current += timedelta(days=7)
    else:
        try:
            path = scraper.nrg_japan_weekly(date=monday)
        except RuntimeError as err_mon:
            try:
                path = scraper.nrg_japan_weekly(date=monday + timedelta(days=1))
            except RuntimeError as err_tue:
                print(err_mon)
                print(err_tue)
                sys.exit(1)
        print(path)
