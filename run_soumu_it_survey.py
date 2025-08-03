from soumu_scraper import soumu
import sys


if __name__ == "__main__":
    scraper = soumu()
    try:
        scraper.it_survey()
    except RuntimeError as err:
        print(err)
        sys.exit(1)
