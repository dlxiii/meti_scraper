from __future__ import annotations

from datetime import datetime
from pathlib import Path

import requests


class nrg:
    """Downloader for Japan NRG reports."""

    _BASE_URL = "https://japan-nrg.com/acms/wp-content/uploads"
    _MONTH_NAMES = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    def _download(self, url: str, directory: Path, filename: str) -> str:
        """Download a PDF from ``url`` into ``directory`` with ``filename``.

        The directory is created if it does not exist.  The downloaded file
        path is returned.
        """
        directory.mkdir(parents=True, exist_ok=True)
        response = requests.get(url)
        response.raise_for_status()
        file_path = directory / filename
        file_path.write_bytes(response.content)
        return str(file_path)

    def nrg_japan_data(self, date: datetime | None = None) -> str:
        """Download the monthly Japan NRG Data PDF.

        Parameters
        ----------
        date: datetime | None
            Date used to determine the report month.  Defaults to today.

        Returns
        -------
        str
            Path to the downloaded PDF file.
        """
        date = date or datetime.now()
        year = date.year
        month = date.month
        date_str = date.strftime("%Y%m%d")

        urls = [
            f"{self._BASE_URL}/{year}/{month:02d}/Japan-NRG-Data-{year}-{month}.pdf",
            f"{self._BASE_URL}/{year}/{month:02d}/Japan-NRG-Data-{year}-{self._MONTH_NAMES[month - 1]}.pdf",
            f"{self._BASE_URL}/{month:02d}/{year}/Japan-NRG-Data-{month}-{year}.pdf",
            f"{self._BASE_URL}/{month:02d}/{year}/Japan-NRG-Data-{self._MONTH_NAMES[month - 1]}-{year}.pdf",
        ]

        for url in urls:
            if requests.head(url).status_code == 200:
                return self._download(
                    url,
                    Path("pdf") / "nrg" / "data",
                    f"Japan_NRG_Data_{date_str}.pdf",
                )

        raise RuntimeError("Failed to locate Japan NRG Data PDF")

    def nrg_japan_weekly(self, date: datetime | None = None) -> str:
        """Download the weekly Japan NRG report PDF.

        Parameters
        ----------
        date: datetime | None
            Date used to determine the report week.  Defaults to today.

        Returns
        -------
        str
            Path to the downloaded PDF file.
        """
        date = date or datetime.now()
        year = date.year
        month = date.month
        day = date.day
        date_str = date.strftime("%Y%m%d")

        url = (
            f"{self._BASE_URL}/{year}/{month:02d}/"
            f"Japan-NRG-Weekly-{year}{month:02d}{day:02d}.pdf"
        )

        if requests.head(url).status_code != 200:
            raise RuntimeError("Failed to locate Japan NRG Weekly PDF")

        return self._download(
            url,
            Path("pdf") / "nrg" / "weekly",
            f"Japan_NRG_Weekly_{date_str}.pdf",
        )

