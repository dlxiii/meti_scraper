from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber
import pytesseract


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

    def _extract_text(self, page) -> str:
        """Extract text from a PDF page, falling back to OCR if needed."""
        text = page.extract_text()
        if text:
            return text
        try:
            image = page.to_image(resolution=300).original
            return pytesseract.image_to_string(image)
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
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = self._extract_text(page)
                if text:
                    parts.append(text)
                for table in page.extract_tables() or []:
                    md_table = self._table_to_markdown(table)
                    if md_table:
                        parts.append(md_table)

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
    offset = (today.weekday() - 2) % 7 or 7
    last_wednesday = today - timedelta(days=offset)

    scraper = meti()
    pdf_path = scraper.lng_weekly_inventory(date=last_wednesday.strftime("%Y%m%d"))
    scraper.pdf_to_markdown(pdf_path)
