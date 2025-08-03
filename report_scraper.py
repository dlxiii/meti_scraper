from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sys

import requests
from googletrans import Translator

from meti_scraper import meti


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

    def nrg_japan_weekly(self, date: datetime | None = None) -> list[str]:
        """Download the weekly Japan NRG report and convert it to Markdown.

        In addition to downloading the PDF, this method creates two Markdown
        files: an English version with tables preserved and a Chinese version
        generated via machine translation.  Charts or other figures are not
        exported.

        Parameters
        ----------
        date: datetime | None
            Date used to determine the report week.  Defaults to today.

        Returns
        -------
        list[str]
            Paths to the downloaded PDF and the generated Markdown files in
            English and Chinese respectively.
        """
        date = date or datetime.now()

        def build_url(d: datetime) -> str:
            return (
                f"{self._BASE_URL}/{d.year}/{d.month:02d}/"
                f"Japan-NRG-Weekly-{d.year}{d.month:02d}{d.day:02d}.pdf"
            )

        url_date = date
        url = build_url(url_date)

        if requests.head(url).status_code != 200:
            url_date = date + timedelta(days=1)
            url = build_url(url_date)
            if requests.head(url).status_code != 200:
                raise RuntimeError("Failed to locate Japan NRG Weekly PDF")

        date_str = url_date.strftime("%Y%m%d")

        pdf_path = self._download(
            url,
            Path("pdf") / "nrg" / "weekly",
            f"Japan_NRG_Weekly_{date_str}.pdf",
        )

        helper = meti()
        md_en_path = helper.pdf_to_markdown(pdf_path, include_images=False)
        md_en_file = Path(md_en_path)
        md_en_en = md_en_file.with_name(md_en_file.stem + "_en.md")
        md_en_file.rename(md_en_en)
        md_en_file = md_en_en

        translator = Translator()
        lines = md_en_file.read_text(encoding="utf-8").splitlines()
        cn_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("|"):
                # Preserve table structure; translate each cell if not a separator
                if set(stripped) <= {"|", ":", "-", " "}:
                    cn_lines.append(line)
                    continue
                cells = line.split("|")
                new_cells = []
                for cell in cells:
                    cell_stripped = cell.strip()
                    if cell_stripped:
                        try:
                            new_cells.append(
                                translator.translate(cell_stripped, dest="zh-cn").text
                            )
                        except Exception:
                            new_cells.append(cell_stripped)
                    else:
                        new_cells.append("")
                cn_lines.append("|".join(new_cells))
            elif stripped:
                try:
                    cn_lines.append(translator.translate(line, dest="zh-cn").text)
                except Exception:
                    cn_lines.append(line)
            else:
                cn_lines.append(line)

        md_cn_file = md_en_file.with_name(md_en_file.stem.replace("_en", "_cn") + ".md")
        md_cn_file.write_text("\n".join(cn_lines), encoding="utf-8")

        return [pdf_path, str(md_en_file), str(md_cn_file)]


if __name__ == "__main__":
    today = datetime.today()
    weekday = today.weekday()  # Monday = 0, Sunday = 6
    if weekday == 0:
        monday = today - timedelta(days=7)
    else:
        monday = today - timedelta(days=weekday)

    scraper = nrg()

    start = datetime.strptime("20210101", "%Y%m%d")
    end = datetime.strptime("20251201", "%Y%m%d")

    offset = (7 - start.weekday()) % 7
    current = start + timedelta(days=offset)

    print(f"Downloading Japan NRG Weekly from {current:%Y-%m-%d} to {end:%Y-%m-%d} (Mondays only)")

    while current <= end:
        try:
            path = scraper.nrg_japan_weekly(date=current)
            print(path)
        except RuntimeError as err:
            print(err)
        current += timedelta(days=7)

    # try:
    #     path = scraper.nrg_japan_weekly(date=monday)
    #     print(path)
    # except RuntimeError as err:
    #     print(err)
    #     sys.exit(1)

