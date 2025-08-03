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

    args = [arg for arg in sys.argv[1:] if arg != "--translate"]
    translate = len(args) != len(sys.argv[1:])

    if len(args) == 2:
        start = datetime.strptime(args[0], "%Y%m%d")
        end = datetime.strptime(args[1], "%Y%m%d")
        print(
            f"Downloading Japan NRG Weekly from {start:%Y-%m-%d} to {end:%Y-%m-%d}"
        )
        current = start
        while current <= end:
            try:
                paths = scraper.nrg_japan_weekly(date=current, translate=translate)
                for p in paths:
                    print(p)
            except RuntimeError as err:
                print(err)
            current += timedelta(days=7)
    else:
        try:
            paths = scraper.nrg_japan_weekly(date=monday, translate=translate)
            for p in paths:
                print(p)
        except RuntimeError as err:
            print(err)
            sys.exit(1)
