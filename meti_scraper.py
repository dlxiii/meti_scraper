import requests
from pathlib import Path


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
