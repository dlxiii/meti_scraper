from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


class esri:
    """Downloader for ESRI GDP data."""

    _BASE_URL = "https://www.esri.cao.go.jp"
    _PAGE = _BASE_URL + "/jp/sna/index.html"

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
            raise RuntimeError("Failed to download ESRI GDP page") from err

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
                        f"Failed to download CSV from {csv_url}"
                    ) from err

                file_path = directory / f"{section}_{label}_{date}.csv"
                file_path.write_bytes(csv_response.content)
                results.append(str(file_path))

        return results
