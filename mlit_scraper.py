from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime

import csv
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import xlrd


class mlit:
    """Downloader for MLIT housing and building statistics."""

    _URL = "https://www.mlit.go.jp/sogoseisaku/jouhouka/sosei_jouhouka_tk4_000002.html"

    def jutaku(self, date: str) -> list[str]:
        """Download housing and building time series and export sheet ``jyuu``.

        Parameters
        ----------
        date: str
            Date string in YYYYMM format appended to each filename.

        Returns
        -------
        list[str]
            Paths to the downloaded XLS file and generated CSV file.
        """

        directory = Path("xls") / "jutaku"
        directory.mkdir(parents=True, exist_ok=True)
        xls_path = directory / f"新設住宅着工_利用関係別戸数_床面積_{date}.xls"
        csv_path = directory / f"新設住宅着工_利用関係別戸数_床面積_{date}.csv"

        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        try:
            response = session.get(
                self._URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise RuntimeError("Failed to fetch MLIT landing page") from err

        soup = BeautifulSoup(response.content, "html.parser")
        link = soup.find("a", string="【住宅・建築物】　時系列")
        if not link or not link.get("href"):
            raise RuntimeError("Unable to locate housing time series link")

        xls_url = urljoin(self._URL, link["href"])

        try:
            file_response = session.get(
                xls_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            file_response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise RuntimeError("Failed to download housing time series XLS") from err

        xls_path.write_bytes(file_response.content)

        try:
            book = xlrd.open_workbook(xls_path)
        except xlrd.XLRDError as err:
            raise RuntimeError("Failed to open downloaded XLS file") from err

        if "jyuu" not in book.sheet_names():
            raise RuntimeError("Sheet 'jyuu' not found in workbook")

        sheet = book.sheet_by_name("jyuu")
        with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for row_idx in range(sheet.nrows):
                writer.writerow(sheet.row_values(row_idx))

        return [str(xls_path), str(csv_path)]


if __name__ == "__main__":
    today = datetime.today()
    scraper = mlit()
    try:
        file_paths = scraper.jutaku(date=today.strftime("%Y%m"))
        for path in file_paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
