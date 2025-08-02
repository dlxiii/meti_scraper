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

    def pdf_to_markdown(self, pdf_path: str) -> str:
        """Convert a PDF to a Markdown file with extracted text.

        The function first tries to extract embedded text from the PDF.
        If no text is found on a page, it falls back to OCR using
        ``pytesseract`` for better accuracy.

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

        text_parts = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)
                else:
                    try:
                        image = page.to_image(resolution=300).original
                        text_parts.append(pytesseract.image_to_string(image))
                    except Exception:
                        continue

        md_file.write_text("\n\n".join(text_parts), encoding="utf-8")
        return str(md_file)


if __name__ == "__main__":
    today = datetime.today()
    offset = (today.weekday() - 2) % 7 or 7
    last_wednesday = today - timedelta(days=offset)

    scraper = meti()
    pdf_path = scraper.lng_weekly_inventory(date=last_wednesday.strftime("%Y%m%d"))
    scraper.pdf_to_markdown(pdf_path)
