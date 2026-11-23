import json
import os
import zipfile
from datetime import date

import pytest

from oceanwatch import landing
from oceanwatch.schema import EXPECTED_COLUMNS

DAY = date(2023, 6, 8)
CSV = ",".join(EXPECTED_COLUMNS) + "\n367000001,2023-06-08T00:00:01,30,-80,1,2,511,X,,,70,0,1,1,1,,A\n"


def fake_downloader(content=CSV, member="AIS_2023_06_08.csv"):
    calls = []

    def download(url, dest, expected_member):
        calls.append(url)
        with zipfile.ZipFile(dest, "w") as zf:
            zf.writestr(member, content)
        info = landing.verify_zip(dest, expected_member)
        return {"url": url, "etag": None, "zip_bytes": os.path.getsize(dest), "zip_sha256": "x",
                "csv_bytes": info.file_size, "csv_crc32": f"{info.CRC:08x}", "attempts": 1}

    download.calls = calls
    return download


def test_deliver_and_skip(tmp_path):
    dl = fake_downloader()
    args = dict(landing_dir=str(tmp_path / "landing"), manifest_dir=str(tmp_path / "m"),
                url_template="https://x/{year}/AIS_{year}_{month:02d}_{day:02d}.zip", tmp_dir=str(tmp_path / "tmp"))
    out = landing.deliver_day(DAY, downloader=dl, **args)
    assert out["status"] == "delivered"
    assert dl.calls == ["https://x/2023/AIS_2023_06_08.zip"]
    assert os.listdir(tmp_path / "landing") == ["AIS_2023_06_08.csv"]  # sin archivos parciales
    assert json.load(open(tmp_path / "m" / "AIS_2023_06_08.json"))["day"] == "2023-06-08"

    again = landing.deliver_day(DAY, downloader=dl, **args)
    assert again["status"] == "skipped" and len(dl.calls) == 1
    assert landing.next_pending_day(str(tmp_path / "m"), "2023-06-08") == date(2023, 6, 9)
    assert landing.next_pending_day(str(tmp_path / "nada"), "2023-06-08") == DAY


def test_bad_header_leaves_nothing(tmp_path):
    bad = "MMSI,LAT,LON\n1,2,3\n"
    with pytest.raises(landing.IntegrityError):
        landing.deliver_day(DAY, str(tmp_path / "landing"), str(tmp_path / "m"), "https://x/{year}{month}{day}",
                            tmp_dir=str(tmp_path / "tmp"), downloader=fake_downloader(bad))
    assert os.listdir(tmp_path / "landing") == []
    assert not os.path.exists(tmp_path / "m" / "AIS_2023_06_08.json")


def test_zip_with_wrong_member(tmp_path):
    with pytest.raises(landing.IntegrityError):
        landing.deliver_day(DAY, str(tmp_path / "l"), str(tmp_path / "m"), "https://x/{year}{month}{day}",
                            tmp_dir=str(tmp_path / "tmp"), downloader=fake_downloader(member="otro.csv"))


def test_retry_then_success(tmp_path, monkeypatch):
    """Dos fallos transitorios y luego éxito, con backoff 10 y 20 s."""
    attempts = {"n": 0}
    payload = tmp_path / "ok.zip"
    with zipfile.ZipFile(payload, "w") as zf:
        zf.writestr("AIS_2023_06_08.csv", CSV)
    data = payload.read_bytes()

    class Resp:
        headers = {"Content-Length": str(len(data)), "ETag": "e"}

        def __init__(self):
            self._chunks = [data]

        def read(self, n):
            return self._chunks.pop() if self._chunks else b""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(req, timeout):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TimeoutError("lento")
        return Resp()

    monkeypatch.setattr(landing.urllib.request, "urlopen", urlopen)
    sleeps = []
    meta = landing.download_with_retry("https://x", str(tmp_path / "d.zip"), "AIS_2023_06_08.csv", sleep=sleeps.append)
    assert meta["attempts"] == 3 and sleeps == [10, 20]
