from pathlib import Path
from urllib.parse import urljoin

import hashlib
import re
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


class soumu:
    """Downloader for MIC Communications Usage Trend Survey PDFs."""

    _URL = "https://www.soumu.go.jp/johotsusintokei/statistics/statistics05a.html"

    def it_survey(self) -> list[str]:
        """Download Communications Usage Trend Survey PDFs.

        Returns
        -------
        list[str]
            Paths to the newly downloaded PDF files.
        """

        directory = Path("pdf") / "it"
        directory.mkdir(parents=True, exist_ok=True)

        # Compute hashes of existing PDFs to avoid duplicates.
        existing_hashes: set[str] = set()
        for pdf_file in directory.glob("*.pdf"):
            hasher = hashlib.sha256()
            hasher.update(pdf_file.read_bytes())
            existing_hashes.add(hasher.hexdigest())

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
            raise RuntimeError("Failed to fetch Soumu IT survey page") from err

        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", href=lambda h: h and h.endswith(".pdf"))

        downloaded: list[str] = []
        for link in links:
            pdf_url = urljoin(self._URL, link["href"])
            try:
                pdf_resp = session.get(
                    pdf_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                pdf_resp.raise_for_status()
            except requests.exceptions.RequestException as err:
                raise RuntimeError(
                    f"Failed to download PDF from {pdf_url}") from err

            file_hash = hashlib.sha256(pdf_resp.content).hexdigest()
            if file_hash in existing_hashes:
                continue

            # Extract YYYYMM from the first 6 digits in the href.
            match = re.search(r"(\d{6})", link["href"])
            if match:
                yyyymm = f"20{match.group(1)[:2]}{match.group(1)[2:4]}"
            else:
                yyyymm = "unknown"

            # Use the link text as the base filename.
            base_name = link.get_text(strip=True)
            safe_name = re.sub(r"[\\/:*?\"<>|\s]+", "_", base_name)

            file_path = directory / f"{safe_name}_{yyyymm}.pdf"
            file_path.write_bytes(pdf_resp.content)
            print(file_path)
            existing_hashes.add(file_hash)
            downloaded.append(str(file_path))

        return downloaded


if __name__ == "__main__":
    scraper = soumu()
    try:
        scraper.it_survey()
    except RuntimeError as err:
        print(err)
        sys.exit(1)
