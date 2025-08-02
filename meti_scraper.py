import sys
from datetime import datetime
from pathlib import Path

import requests


class meti:
    """Downloader for METI weekly LNG stock PDF."""

    _URL = (
        "https://www.enecho.meti.go.jp/category/electricity_and_gas/"
        "electricity_measures/pdf/denryoku_LNG_stock.pdf"
    )

    def lng_weekly_inventory(self, date: str) -> str:
        """Download weekly LNG inventory PDF for the given date.

        Parameters
        ----------
        date: str
            Date string in YYYYMMDD format used in the filename.

        Returns
        -------
        str
            Path to the downloaded PDF file.
        """
        directory = Path("pdf") / "lng"
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"denryoku_LNG_stock_{date}.pdf"

        response = requests.get(self._URL)
        response.raise_for_status()
        file_path.write_bytes(response.content)

        return str(file_path)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python meti_scraper.py YYYY/MM/DD")
        sys.exit(1)

    date_str = sys.argv[1]
    dt = datetime.strptime(date_str, "%Y/%m/%d")

    scraper = meti()
    scraper.lng_weekly_inventory(date=dt.strftime("%Y%m%d"))
