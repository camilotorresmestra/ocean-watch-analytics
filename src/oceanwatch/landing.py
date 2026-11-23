"""Descarga del zip diario de NOAA y entrega del CSV en landing/.

Es la misma lógica de la sección 1.2 de la Entrega 1 (reintentos, integridad e
idempotencia), parametrizada por fecha para que sea una tarea del job.

El CSV se escribe primero con un nombre temporal que Auto Loader ignora y luego se
renombra: Auto Loader nunca ve un archivo a medio escribir.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from .schema import EXPECTED_COLUMNS

MAX_RETRIES = 5
BACKOFF_BASE = 10  # segundos: 10, 20, 40, 80
TIMEOUT = 120
CHUNK = 8 * 1024 * 1024
USER_AGENT = "ocean-watch-analytics/2.0"


class IntegrityError(Exception):
    pass


@dataclass(frozen=True)
class DayFile:
    day: date

    @property
    def stem(self) -> str:
        return f"AIS_{self.day:%Y_%m_%d}"

    @property
    def csv_name(self) -> str:
        return f"{self.stem}.csv"

    def url(self, template: str) -> str:
        return template.format(year=self.day.year, month=self.day.month, day=self.day.day)


def parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def next_pending_day(manifest_dir: str, start: str) -> date:
    """Primer día sin manifiesto a partir de `start`.

    Simula el feed diario: cada ejecución programada entrega el día siguiente al último
    ya entregado.
    """
    done = set()
    if os.path.isdir(manifest_dir):
        for name in os.listdir(manifest_dir):
            if name.endswith(".json"):
                done.add(name[: -len(".json")])
    day = parse_day(start)
    while DayFile(day).stem in done:
        day += timedelta(days=1)
    return day


def verify_zip(zip_path: str, expected_member: str) -> zipfile.ZipInfo:
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        if names != [expected_member]:
            raise IntegrityError(f"Contenido inesperado en el zip: {names}")
        bad = zf.testzip()
        if bad is not None:
            raise IntegrityError(f"CRC inválido en {bad}")
        return zf.getinfo(expected_member)


def download_with_retry(url: str, dest: str, expected_member: str, sleep=time.sleep) -> dict:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Intento {attempt}/{MAX_RETRIES}: {url}")
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            sha = hashlib.sha256()
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp, open(dest, "wb") as f:
                content_length = resp.headers.get("Content-Length")
                etag = resp.headers.get("ETag")
                while chunk := resp.read(CHUNK):
                    f.write(chunk)
                    sha.update(chunk)

            received = os.path.getsize(dest)
            if content_length is not None and received != int(content_length):
                raise IntegrityError(f"Recibidos {received} bytes, se esperaban {content_length}")
            info = verify_zip(dest, expected_member)
            return {
                "url": url,
                "etag": etag,
                "zip_bytes": received,
                "zip_sha256": sha.hexdigest(),
                "csv_bytes": info.file_size,
                "csv_crc32": f"{info.CRC:08x}",
                "attempts": attempt,
            }
        except urllib.error.HTTPError as exc:
            # Un 4xx (salvo 429) es una URL equivocada o un día que no existe: no se reintenta.
            if 400 <= exc.code < 500 and exc.code != 429:
                raise
            last_exc = exc
        except (urllib.error.URLError, TimeoutError, OSError, IntegrityError, zipfile.BadZipFile) as exc:
            last_exc = exc

        print(f"Falló el intento {attempt}: {last_exc!r}")
        if attempt < MAX_RETRIES:
            sleep(BACKOFF_BASE * 2 ** (attempt - 1))

    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_RETRIES} intentos") from last_exc


def read_header(csv_path: str) -> list[str]:
    with open(csv_path, encoding="utf-8") as f:
        return f.readline().strip().lstrip("﻿").split(",")


def deliver_day(
    day: date,
    landing_dir: str,
    manifest_dir: str,
    url_template: str,
    tmp_dir: str = "/tmp/ais_download",
    downloader=download_with_retry,
) -> dict:
    """Descarga, verifica y deja el CSV del día en landing/. Idempotente."""
    f = DayFile(day)
    csv_path = os.path.join(landing_dir, f.csv_name)
    manifest_path = os.path.join(manifest_dir, f"{f.stem}.json")

    if os.path.exists(manifest_path):
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        if os.path.exists(csv_path) and os.path.getsize(csv_path) == manifest["csv_bytes"]:
            print(f"{f.csv_name} ya entregado y verificado; se omite")
            return {**manifest, "status": "skipped"}

    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(manifest_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)
    zip_path = os.path.join(tmp_dir, f"{f.stem}.zip")
    # Auto Loader ignora los archivos que empiezan por "_" o ".".
    partial_path = os.path.join(landing_dir, f"_{f.csv_name}.partial")
    try:
        meta = downloader(f.url(url_template), zip_path, f.csv_name)
        with zipfile.ZipFile(zip_path) as zf, zf.open(f.csv_name) as src, open(partial_path, "wb") as dst:
            shutil.copyfileobj(src, dst, CHUNK)
        written = os.path.getsize(partial_path)
        if written != meta["csv_bytes"]:
            raise IntegrityError(f"Escritos {written} bytes, se esperaban {meta['csv_bytes']}")
        header = read_header(partial_path)
        if header != EXPECTED_COLUMNS:
            raise IntegrityError(f"Encabezado inesperado en {f.csv_name}: {header}")
        os.replace(partial_path, csv_path)
    finally:
        for path in (zip_path, partial_path):
            if os.path.exists(path):
                os.remove(path)

    manifest = {
        "day": day.isoformat(),
        "file": f.csv_name,
        **meta,
        "delivered_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    # El manifiesto se escribe al final: es la marca de "día entregado".
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)
    return {**manifest, "status": "delivered"}
