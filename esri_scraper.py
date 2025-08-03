from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


class esri:
    """Downloader for ESRI GDP data."""

    _BASE_URL = "https://www.esri.cao.go.jp"
    _PAGE = _BASE_URL + "/jp/sna/menu.html"

    def gdp(self, date: str) -> list[str]:
        """Download GDP CSV files for the given date.

        Parameters
        ----------
        date: str
            Date string in YYYYMM format appended to each filename.

        Returns
        -------
        list[str]
            Paths to the downloaded CSV files.
        """

        directory = Path("csv") / "gdp"
        directory.mkdir(parents=True, exist_ok=True)

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
                self._PAGE,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            # Preserve the original exception message so callers can
            # understand why the request failed (e.g. proxy issues or
            # connection timeouts).
            raise RuntimeError(
                f"Failed to download ESRI GDP page: {err}"
            ) from err

        soup = BeautifulSoup(response.content, "html.parser")

        sections = [
            "四半期GDP成長率",
            "年次GDP成長率",
            "四半期GDP実額",
            "年次GDP実額",
        ]

        results: list[str] = []
        for section in sections:
            h3 = soup.find("h3", string=section)
            if not h3:
                continue
            block = h3.find_next("div", class_="sna_main_data_column_block")
            if not block:
                continue
            links = block.find_all("a", href=True)[:2]
            for label, link in zip(["実質", "名目"], links):
                csv_url = urljoin(self._BASE_URL, link["href"])
                try:
                    csv_response = session.get(
                        csv_url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=10,
                    )
                    csv_response.raise_for_status()
                except requests.exceptions.RequestException as err:
                    raise RuntimeError(
                        f"Failed to download CSV from {csv_url}: {err}"
                    ) from err

                file_path = directory / f"{section}_{label}_{date}.csv"

                # The CSV files provided by ESRI are encoded in Shift JIS.
                # Convert the content to UTF-8 so that downstream consumers
                # can read them without handling encoding details.
                try:
                    csv_text = csv_response.content.decode("shift_jis")
                except UnicodeDecodeError as err:
                    raise RuntimeError(
                        f"Failed to decode CSV from {csv_url}: {err}"
                    ) from err

                file_path.write_text(csv_text, encoding="utf-8")
                results.append(str(file_path))

        return results

if __name__ == "__main__":
    from datetime import datetime
    import sys

    today = datetime.today()

    scraper = esri()
    try:
        gdp_paths = scraper.gdp(date=today.strftime("%Y%m"))
        for path in gdp_paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
