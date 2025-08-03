from pathlib import Path
import sys

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class mof:
    """Downloader for MOF customs trade statistics."""

    _CUSTOMS_URLS = {
        "Import": [
            "https://www.customs.go.jp/toukei/suii/html/data/d61ma.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma001.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma002.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma003.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma004.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma005.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma006.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma007.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d62ma008.csv",
        ],
        "Export": [
            "https://www.customs.go.jp/toukei/suii/html/data/d51ma.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma001.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma002.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma003.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma004.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma005.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma006.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma007.csv",
            "https://www.customs.go.jp/toukei/suii/html/data/d52ma008.csv",
        ],
    }

    def customs(self) -> list[str]:
        """Download customs CSV files and save them under ``csv/customs/toukei``.

        Returns
        -------
        list[str]
            Paths to the downloaded CSV files.
        """

        directory = Path("csv") / "customs" / "toukei"
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

        downloaded: list[str] = []
        for urls in self._CUSTOMS_URLS.values():
            for url in urls:
                try:
                    response = session.get(
                        url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=10,
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as err:
                    raise RuntimeError(f"Failed to download CSV from {url}") from err

                file_name = Path(url).name
                file_path = directory / file_name
                file_path.write_bytes(response.content)
                print(file_path)
                downloaded.append(str(file_path))

        return downloaded


if __name__ == "__main__":
    scraper = mof()
    try:
        scraper.customs()
    except RuntimeError as err:
        print(err)
        sys.exit(1)
