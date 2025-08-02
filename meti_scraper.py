from datetime import datetime, timedelta
from pathlib import Path

import csv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber
import pytesseract


class meti:
    """Downloader for METI data such as LNG stock and industrial production."""

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
        directory = Path("pdf") / "lng" / "stock"
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"denryoku_LNG_stock_{date}.pdf"

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
            raise RuntimeError("Failed to download METI LNG stock PDF") from err

        file_path.write_bytes(response.content)

        return str(file_path)

    def index_of_industrial_production(self, asof: str) -> list[str]:
        """Download industrial production index Excel files.

        This method retrieves three datasets related to the index of
        industrial production and saves them with Japanese filenames.

        Parameters
        ----------
        asof : str
            Date string in YYYYMM format appended to each filename.

        Returns
        -------
        list[str]
            Paths to the downloaded Excel files.
        """

        sources = {
            "過去の製造工業生産能力・稼働率指数（接続指数）":
                "https://www.meti.go.jp/statistics/tyo/iip/xls/b2020_nsgs1j.xlsx",
            "生産・出荷・在庫・在庫率指数":
                "https://www.meti.go.jp/statistics/tyo/iip/xls/b2020_gsm1j.xlsx",
            "過去の生産・出荷・在庫・在庫率指数（接続指数），鉱工業総合のみ（暦年・年度・四半期）（1953年～）":
                "https://www.meti.go.jp/statistics/tyo/iip/xls/b2020_sosq1j.xlsx",
        }

        directory = Path("xls") / "iip"
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

        file_paths: list[str] = []
        headers = {"User-Agent": "Mozilla/5.0"}
        for name, url in sources.items():
            file_path = directory / f"{name}_{asof}.xlsx"
            try:
                response = session.get(url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                raise RuntimeError(f"Failed to download {name}") from err
            file_path.write_bytes(response.content)
            file_paths.append(str(file_path))

        return file_paths

    def _extract_text(self, page) -> str:
        """Extract text from a PDF page, falling back to OCR if needed."""
        text = page.extract_text(layout=True)
        if text:
            lines = [line.strip() for line in text.splitlines()]
            while lines and not lines[0]:
                lines.pop(0)
            while lines and not lines[-1]:
                lines.pop()
            return "\n".join(lines)
        try:
            image = page.to_image(resolution=300).original
            ocr_text = pytesseract.image_to_string(image)
            lines = [line.strip() for line in ocr_text.splitlines()]
            while lines and not lines[0]:
                lines.pop(0)
            while lines and not lines[-1]:
                lines.pop()
            return "\n".join(lines)
        except Exception:
            return ""

    def _table_to_markdown(self, table) -> str:
        """Convert a table (list of lists) to a Markdown string."""
        if not table:
            return ""
        header = [cell or "" for cell in table[0]]
        md_lines = ["| " + " | ".join(header) + " |"]
        md_lines.append("| " + " | ".join("---" for _ in header) + " |")
        for row in table[1:]:
            md_lines.append("| " + " | ".join(cell or "" for cell in row) + " |")
        return "\n".join(md_lines)

    def _table_to_csv(self, table, csv_path: Path) -> None:
        """Write a table (list of lists) to a CSV file."""
        with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for row in table:
                writer.writerow([cell or "" for cell in row])

    def pdf_tables_to_csv(self, pdf_path: str) -> list[str]:
        """Extract tables from a PDF and save them as CSV files.

        If multiple tables are found, an index is appended to the base filename.

        Parameters
        ----------
        pdf_path : str
            Path to the source PDF file.

        Returns
        -------
        list[str]
            Paths to the generated CSV files.
        """

        pdf_file = Path(pdf_path)
        tables = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                tables.extend(page.extract_tables() or [])

        csv_paths: list[str] = []
        if not tables:
            return csv_paths

        if len(tables) == 1:
            csv_path = pdf_file.with_suffix(".csv")
            self._table_to_csv(tables[0], csv_path)
            csv_paths.append(str(csv_path))
        else:
            for idx, table in enumerate(tables, 1):
                csv_path = pdf_file.with_name(
                    f"{pdf_file.stem}_table_{idx:03d}.csv"
                )
                self._table_to_csv(table, csv_path)
                csv_paths.append(str(csv_path))

        return csv_paths

    def pdf_to_markdown(self, pdf_path: str) -> str:
        """Convert a PDF to Markdown, preserving tables when possible.

        This method extracts text and tables from each page. If a page
        lacks embedded text, OCR is used as a fallback.

        Parameters
        ----------
        pdf_path : str
            Path to the source PDF file.

        Returns
        -------
        str
            Path to the generated Markdown file.
        """

        pdf_file = Path(pdf_path)
        md_file = pdf_file.with_suffix(".md")

        parts = []
        image_counter = 1
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = self._extract_text(page)
                if text:
                    parts.append(text)
                for table in page.extract_tables() or []:
                    md_table = self._table_to_markdown(table)
                    if md_table:
                        parts.append(md_table)
                for img in page.images:
                    bbox = (
                        img["x0"],
                        page.height - img["y1"],
                        img["x1"],
                        page.height - img["y0"],
                    )
                    cropped = page.crop(bbox)
                    pil_img = cropped.to_image(resolution=300).original
                    image_path = pdf_file.with_name(
                        f"{pdf_file.stem}_{image_counter:03d}.png"
                    )
                    pil_img.save(image_path)
                    parts.append(
                        f"![Figure {image_counter:03d}]({image_path.name})"
                    )
                    image_counter += 1

        md_file.write_text("\n\n".join(parts), encoding="utf-8")
        return str(md_file)

    def pdf_to_markdown_plain(self, pdf_path: str) -> str:
        """Convert a PDF to Markdown with text extraction only."""

        pdf_file = Path(pdf_path)
        md_file = pdf_file.with_suffix(".md")

        text_parts = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = self._extract_text(page)
                if text:
                    text_parts.append(text)

        md_file.write_text("\n\n".join(text_parts), encoding="utf-8")
        return str(md_file)


if __name__ == "__main__":
    today = datetime.today()
    weekday = today.weekday()  # Monday = 0, Sunday = 6

    if weekday >= 2:
        monday = today - timedelta(days=weekday)
        target_wed = monday + timedelta(days=2)
    else:
        monday = today - timedelta(days=weekday + 7)
        target_wed = monday + timedelta(days=2)

    scraper = meti()
    pdf_path = scraper.lng_weekly_inventory(date=target_wed.strftime("%Y%m%d"))
    scraper.pdf_to_markdown(pdf_path)
    scraper.pdf_tables_to_csv(pdf_path)
