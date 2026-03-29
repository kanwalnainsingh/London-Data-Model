"""Fetch stage: download official source files before the pipeline runs."""

import csv
import logging
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

LOGGER = logging.getLogger(__name__)

_GIAS_URL_TEMPLATE = (
    "https://ea-edubase-api-prod.azurewebsites.net"
    "/edubase/downloads/public/edubasealldata{date}.csv"
)
_OFSTED_PUBLICATIONS_URL = (
    "https://www.gov.uk/government/publications/five-year-ofsted-inspection-data"
)

_ODS_NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}


@dataclass
class FetchResult:
    schools_path: Optional[Path] = None
    ofsted_path: Optional[Path] = None
    ks4_path: Optional[Path] = None
    ks5_path: Optional[Path] = None
    notes: List[str] = field(default_factory=list)


class FetchError(Exception):
    pass


def _gias_url(for_date: date) -> str:
    return _GIAS_URL_TEMPLATE.format(date=for_date.strftime("%Y%m%d"))


def _download_file(url: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "london-data-model/1.0"})
    LOGGER.info("Downloading %s", url)
    with urllib.request.urlopen(req, timeout=120) as response:
        with open(dest_path, "wb") as fh:
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                fh.write(chunk)
    LOGGER.info(
        "Saved %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1_048_576
    )


def _resolve_ofsted_url(publications_url: str) -> str:
    """Fetch the Ofsted publications page and extract the state-funded schools ODS URL."""
    req = urllib.request.Request(
        publications_url, headers={"User-Agent": "london-data-model/1.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        html = response.read().decode("utf-8", errors="replace")

    # Find lines containing an assets.publishing.service.gov.uk ODS link
    # for state-funded schools
    for line in html.splitlines():
        lower = line.lower()
        if "state-funded" in lower and ".ods" in lower and "assets.publishing" in lower:
            start = line.find("https://assets.publishing")
            if start == -1:
                start = line.find("//assets.publishing")
                if start != -1:
                    line = "https:" + line[start:]
                    start = 0
            end = line.find('"', start)
            if end == -1:
                end = line.find("'", start)
            if end != -1:
                return line[start:end]

    raise FetchError(
        "Could not find state-funded schools ODS link on {0}. "
        "Set official_input.fetch.ofsted_url in configs/pipeline/schools.yml "
        "to override automatic resolution.".format(publications_url)
    )


def _ods_to_csv(ods_path: Path, csv_path: Path, data_column: str = "URN") -> None:
    """Convert the data sheet of an ODS file to CSV using built-in zipfile + xml.

    Selects the sheet whose first non-empty row contains data_column as a header.
    Falls back to the last sheet if no match is found.
    """
    with zipfile.ZipFile(ods_path) as zf:
        with zf.open("content.xml") as fh:
            tree = ET.parse(fh)

    root = tree.getroot()
    table_tag = "{%s}table" % _ODS_NS["table"]
    row_tag = "{%s}table-row" % _ODS_NS["table"]
    cell_tag = "{%s}table-cell" % _ODS_NS["table"]
    repeat_attr = "{%s}number-columns-repeated" % _ODS_NS["table"]
    p_tag = "{%s}p" % _ODS_NS["text"]

    all_tables = root.findall(".//" + table_tag)
    if not all_tables:
        raise FetchError("No table found in ODS content.xml from {0}".format(ods_path))

    def _first_row_values(tbl):
        for row_el in tbl.findall(row_tag):
            cells = []
            for cell_el in row_el.findall(cell_tag):
                repeat = int(cell_el.get(repeat_attr, 1))
                text_nodes = cell_el.findall(p_tag)
                value = " ".join((t.text or "") for t in text_nodes).strip()
                cells.extend([value] * repeat)
            non_empty = [c for c in cells if c]
            if non_empty:
                return non_empty
        return []

    # Pick the sheet whose first row contains data_column; fall back to last sheet
    table = all_tables[-1]
    for tbl in all_tables:
        if data_column in _first_row_values(tbl):
            table = tbl
            break

    def _parse_row(row_el):
        cells = []
        for cell_el in row_el.findall(cell_tag):
            repeat = int(cell_el.get(repeat_attr, 1))
            text_nodes = cell_el.findall(p_tag)
            value = " ".join((t.text or "") for t in text_nodes).strip()
            cells.extend([value] * repeat)
        while cells and cells[-1] == "":
            cells.pop()
        return cells

    all_rows = [_parse_row(r) for r in table.findall(row_tag)]

    # Find the header row (first row containing data_column) and drop preamble rows
    header_idx = 0
    for i, row in enumerate(all_rows):
        if data_column in row:
            header_idx = i
            break
    rows = [r for r in all_rows[header_idx:] if any(r)]

    # Strip trailing empty rows
    while rows and not any(rows[-1]):
        rows.pop()

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)

    LOGGER.info(
        "Converted ODS → CSV: %s (%.1f MB)", csv_path.name, csv_path.stat().st_size / 1_048_576
    )


def _fetch_gias(fetch_cfg: Dict, dest: Path) -> str:
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)
    if skip_if_exists and dest.exists():
        LOGGER.info("GIAS file exists, skipping download: %s", dest)
        return "schools: skipped (file exists)"

    configured_url = str(fetch_cfg.get("schools_url") or "").strip()
    today_url = configured_url or _gias_url(date.today())

    try:
        _download_file(today_url, dest)
        return "schools: downloaded from GIAS ({0})".format(today_url)
    except Exception as exc:
        yesterday_url = _gias_url(date.today() - timedelta(days=1))
        if yesterday_url == today_url:
            raise FetchError("GIAS download failed: {0}".format(exc)) from exc
        LOGGER.warning("GIAS today URL failed (%s), trying yesterday: %s", exc, yesterday_url)
        try:
            _download_file(yesterday_url, dest)
            return "schools: downloaded from GIAS (yesterday export)"
        except Exception as exc2:
            raise FetchError(
                "GIAS download failed for both today and yesterday. "
                "Check https://get-information-schools.service.gov.uk/Downloads "
                "and set official_input.fetch.schools_url in configs/pipeline/schools.yml. "
                "Underlying error: {0}".format(exc2)
            ) from exc2


def _fetch_ofsted(fetch_cfg: Dict, dest: Path) -> str:
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)
    if skip_if_exists and dest.exists():
        LOGGER.info("Ofsted file exists, skipping download: %s", dest)
        return "ofsted: skipped (file exists)"

    configured_url = str(fetch_cfg.get("ofsted_url") or "").strip()
    if configured_url:
        ofsted_url = configured_url
    else:
        publications_url = fetch_cfg.get(
            "ofsted_publications_url", _OFSTED_PUBLICATIONS_URL
        )
        LOGGER.info("Resolving Ofsted ODS URL from %s", publications_url)
        ofsted_url = _resolve_ofsted_url(publications_url)
        LOGGER.info("Resolved Ofsted URL: %s", ofsted_url)

    ods_tmp = dest.with_suffix(".ods")
    try:
        _download_file(ofsted_url, ods_tmp)
        _ods_to_csv(ods_tmp, dest)
        return "ofsted: downloaded and converted from ODS ({0})".format(ofsted_url)
    finally:
        if ods_tmp.exists():
            ods_tmp.unlink()


def _extract_csv_from_zip(zip_path: Path, csv_dest: Path) -> None:
    """Extract the first CSV file found in a ZIP archive to csv_dest."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise FetchError(
                "No CSV file found inside ZIP: {0}".format(zip_path)
            )
        # Prefer a file whose name contains 'ks4' or 'ks5' or 'final'; else take first
        preferred = next(
            (n for n in csv_names if any(k in n.lower() for k in ("ks4", "ks5", "final", "school"))),
            csv_names[0],
        )
        csv_dest.parent.mkdir(parents=True, exist_ok=True)
        with zf.open(preferred) as src, open(csv_dest, "wb") as dst:
            while True:
                chunk = src.read(65536)
                if not chunk:
                    break
                dst.write(chunk)
    LOGGER.info(
        "Extracted %s → %s (%.1f MB)",
        preferred,
        csv_dest.name,
        csv_dest.stat().st_size / 1_048_576,
    )


def _fetch_performance_table(label: str, input_cfg: Dict, dest: Path) -> str:
    """Download a DfE performance-table CSV (or ZIP containing a CSV) if configured.

    Returns a short status note. Silently skips when no URL is configured —
    the caller must manually place the CSV at dest.
    """
    fetch_cfg = input_cfg.get("fetch", {})
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)
    if skip_if_exists and dest.exists():
        LOGGER.info("%s file exists, skipping download: %s", label, dest)
        return "{0}: skipped (file exists)".format(label)

    url = str(fetch_cfg.get("url") or "").strip()
    if not url:
        return (
            "{0}: skipped (no URL configured — download manually from "
            "https://www.find-school-performance-data.service.gov.uk/download-data "
            "and place at {1})".format(label, dest)
        )

    if url.lower().endswith(".zip"):
        zip_tmp = dest.with_suffix(".zip")
        try:
            _download_file(url, zip_tmp)
            _extract_csv_from_zip(zip_tmp, dest)
        finally:
            if zip_tmp.exists():
                zip_tmp.unlink()
    else:
        _download_file(url, dest)

    return "{0}: downloaded from {1}".format(label, url)


def fetch(pipeline_config: Dict, schools_dest: Path, ofsted_dest: Path) -> FetchResult:
    """Download GIAS, Ofsted, and (optionally) KS4/KS5 performance files."""
    fetch_cfg = pipeline_config.get("official_input", {}).get("fetch", {})

    result = FetchResult()
    result.notes.append(_fetch_gias(fetch_cfg, schools_dest))
    result.schools_path = schools_dest

    result.notes.append(_fetch_ofsted(fetch_cfg, ofsted_dest))
    result.ofsted_path = ofsted_dest

    # KS4 (GCSE) performance tables — optional; only fetch when enabled
    ks4_cfg = pipeline_config.get("ks4_input", {})
    if ks4_cfg.get("enabled", False):
        from london_data_model.settings import PROJECT_ROOT  # avoid circular at module level
        ks4_dest = PROJECT_ROOT / str(ks4_cfg.get("path", "data/raw/ks4_performance.csv"))
        result.notes.append(_fetch_performance_table("ks4", ks4_cfg, ks4_dest))
        result.ks4_path = ks4_dest

    # KS5 (A-level) performance tables — optional; only fetch when enabled
    ks5_cfg = pipeline_config.get("ks5_input", {})
    if ks5_cfg.get("enabled", False):
        from london_data_model.settings import PROJECT_ROOT  # noqa: PLC0415
        ks5_dest = PROJECT_ROOT / str(ks5_cfg.get("path", "data/raw/ks5_performance.csv"))
        result.notes.append(_fetch_performance_table("ks5", ks5_cfg, ks5_dest))
        result.ks5_path = ks5_dest

    return result
