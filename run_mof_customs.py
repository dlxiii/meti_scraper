from mof_scraper import mof
import sys


if __name__ == "__main__":
    scraper = mof()
    try:
        scraper.customs()
    except RuntimeError as err:
        print(err)
        sys.exit(1)
