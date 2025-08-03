from soumu_scraper import soumu
import sys


if __name__ == "__main__":
    scraper = soumu()
    try:
        paths = scraper.it_survey()
        for path in paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
