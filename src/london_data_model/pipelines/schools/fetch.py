"""Fetch stage: download official source files before the pipeline runs."""

import csv
import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from london_data_model.types import SourceProvenance

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
    provenances: List[SourceProvenance] = field(default_factory=list)


# ── Static source catalogue ────────────────────────────────────────────────────
# Describes every raw data source used by the pipeline: what it is, who
# publishes it, how often it is updated and what licence it carries.
# Populated at run-time with fetch metadata (URL, timestamp, file size).
_SOURCE_CATALOGUE: Dict[str, Dict] = {
    "gias_establishments": {
        "label": "Get Information About Schools (GIAS)",
        "source_category": "administrative_register",
        "publisher": "Department for Education (DfE)",
        "licence": "Open Government Licence v3.0",
        "home_url": "https://get-information-schools.service.gov.uk/",
        "update_frequency": "Daily",
        "coverage": "All schools and educational establishments in England",
        "description": (
            "The definitive national register of every school in England. "
            "Used for school names, addresses, phase, establishment type and BNG coordinates. "
            "Updated daily; date of export is embedded in the download URL."
        ),
        "fields_used": [
            "URN", "EstablishmentName", "PhaseOfEducation (name)",
            "TypeOfEstablishment (name)", "EstablishmentStatus (name)",
            "LA (code)", "Easting", "Northing", "Postcode",
            "Street", "Locality", "Address3", "Town", "County (name)",
        ],
    },
    "ofsted_state_funded_schools": {
        "label": "Ofsted State-Funded Schools Inspection Outcomes",
        "source_category": "inspection_data",
        "publisher": "Ofsted (Office for Standards in Education, Children's Services and Skills)",
        "licence": "Open Government Licence v3.0",
        "home_url": "https://www.gov.uk/government/publications/five-year-ofsted-inspection-data",
        "update_frequency": "Monthly (updated after each inspection cycle)",
        "coverage": "All state-funded schools inspected in England (rolling 5-year window)",
        "description": (
            "Five-year rolling Ofsted inspection results for state-funded schools. "
            "Provides the headline grade (Outstanding/Good/Requires improvement/Inadequate) "
            "and the published inspection date for each school."
        ),
        "fields_used": ["URN", "Overall effectiveness", "Published date"],
    },
    "ks4_performance": {
        "label": "KS4 (GCSE) School Performance Tables",
        "source_category": "performance_statistics",
        "publisher": "Department for Education (DfE)",
        "licence": "Open Government Licence v3.0",
        "home_url": "https://www.find-school-performance-data.service.gov.uk/",
        "update_frequency": "Annual (revised data published each January for previous academic year)",
        "coverage": "Secondary and all-through state-funded schools in England",
        "description": (
            "Annual school-level GCSE outcomes. Includes Progress 8 (value-added measure), "
            "Attainment 8 (average grade score across 8 GCSEs), and the percentage of pupils "
            "achieving grade 4+ (standard pass) and grade 5+ (strong pass) in English and Maths."
        ),
        "fields_used": ["URN", "P8MEA", "ATT8SCR", "PTL2BASICS_95", "PTL2BASICS_94"],
    },
    "ks5_performance": {
        "label": "KS5 (A-Level) School Performance Tables",
        "source_category": "performance_statistics",
        "publisher": "Department for Education (DfE)",
        "licence": "Open Government Licence v3.0",
        "home_url": "https://www.find-school-performance-data.service.gov.uk/",
        "update_frequency": "Annual (published October/November for previous academic year)",
        "coverage": "Schools and sixth-form colleges in England",
        "description": (
            "Annual school-level A-level outcomes for schools with sixth forms. "
            "Includes average point score per entry, percentage of A*/A grades, "
            "A*–E pass rate, and total number of A-level students aged 16–18."
        ),
        "fields_used": [
            "URN", "A_ATT_AVG_PT_SCR", "A_A_STAR_A", "A_PASS", "TALLPPE_ALEV_1618",
        ],
    },
}


class FetchError(Exception):
    pass


def _gias_url(for_date: date) -> str:
    return _GIAS_URL_TEMPLATE.format(date=for_date.strftime("%Y%m%d"))


# ── Provenance helpers ─────────────────────────────────────────────────────────

def _file_stats(path: Path) -> Tuple[Optional[str], Optional[int], Optional[float]]:
    """Return (fetched_at_iso, size_bytes, size_mb) for an existing file, or Nones."""
    if not path.exists():
        return None, None, None
    stat = path.stat()
    fetched_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    size_bytes = stat.st_size
    size_mb = round(size_bytes / 1_048_576, 2)
    return fetched_at, size_bytes, size_mb


def _extract_gias_date(url: str) -> Optional[str]:
    """Extract ISO date from a GIAS URL, e.g. edubasealldata20260329.csv → 2026-03-29."""
    match = re.search(r"edubasealldata(\d{8})", url)
    if match:
        d = match.group(1)
        return "{0}-{1}-{2}".format(d[:4], d[4:6], d[6:8])
    return None


def _extract_year_from_url(url: str) -> Optional[str]:
    """Return the last 4-digit year found in a URL, e.g. .../outcomes_2024.ods → 2024."""
    years = re.findall(r"\b(20\d{2})\b", url)
    return years[-1] if years else None


def _build_provenance(
    source_name: str,
    path: Path,
    url: Optional[str],
    status: str,
    notes: List[str],
    source_date: Optional[str] = None,
    academic_year: Optional[str] = None,
) -> SourceProvenance:
    """Assemble a SourceProvenance from static catalogue + runtime fetch info."""
    cat = _SOURCE_CATALOGUE.get(source_name, {})
    fetched_at, size_bytes, size_mb = _file_stats(path)
    return SourceProvenance(
        source_name=source_name,
        source_type=source_name,
        label=cat.get("label", source_name),
        source_category=cat.get("source_category", "unknown"),
        publisher=cat.get("publisher", ""),
        licence=cat.get("licence", "Open Government Licence v3.0"),
        home_url=cat.get("home_url", ""),
        update_frequency=cat.get("update_frequency", ""),
        coverage=cat.get("coverage", ""),
        description=cat.get("description", ""),
        fields_used=list(cat.get("fields_used", [])),
        url=url,
        fetched_at=fetched_at,
        file_path=str(path),
        file_size_bytes=size_bytes,
        file_size_mb=size_mb,
        source_date=source_date,
        academic_year=academic_year,
        status=status,
        notes=notes,
    )


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


def _fetch_gias(fetch_cfg: Dict, dest: Path) -> Tuple[str, Optional[str]]:
    """Return (note, url_used)."""
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)
    configured_url = str(fetch_cfg.get("schools_url") or "").strip()
    today_url = configured_url or _gias_url(date.today())

    if skip_if_exists and dest.exists():
        LOGGER.info("GIAS file exists, skipping download: %s", dest)
        return "schools: skipped (file exists)", today_url

    try:
        _download_file(today_url, dest)
        return "schools: downloaded from GIAS ({0})".format(today_url), today_url
    except Exception as exc:
        yesterday_url = _gias_url(date.today() - timedelta(days=1))
        if yesterday_url == today_url:
            raise FetchError("GIAS download failed: {0}".format(exc)) from exc
        LOGGER.warning("GIAS today URL failed (%s), trying yesterday: %s", exc, yesterday_url)
        try:
            _download_file(yesterday_url, dest)
            return "schools: downloaded from GIAS (yesterday export)", yesterday_url
        except Exception as exc2:
            raise FetchError(
                "GIAS download failed for both today and yesterday. "
                "Check https://get-information-schools.service.gov.uk/Downloads "
                "and set official_input.fetch.schools_url in configs/pipeline/schools.yml. "
                "Underlying error: {0}".format(exc2)
            ) from exc2


def _fetch_ofsted(fetch_cfg: Dict, dest: Path) -> Tuple[str, Optional[str]]:
    """Return (note, url_used)."""
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)

    configured_url = str(fetch_cfg.get("ofsted_url") or "").strip()
    if not configured_url:
        # Resolve lazily so we can still return a URL even for skipped downloads
        publications_url = fetch_cfg.get("ofsted_publications_url", _OFSTED_PUBLICATIONS_URL)
        configured_url = publications_url  # placeholder; resolved below if needed

    if skip_if_exists and dest.exists():
        LOGGER.info("Ofsted file exists, skipping download: %s", dest)
        return "ofsted: skipped (file exists)", configured_url

    # Need to resolve the actual ODS URL if not directly set
    direct_url = str(fetch_cfg.get("ofsted_url") or "").strip()
    if direct_url:
        ofsted_url = direct_url
    else:
        publications_url = fetch_cfg.get("ofsted_publications_url", _OFSTED_PUBLICATIONS_URL)
        LOGGER.info("Resolving Ofsted ODS URL from %s", publications_url)
        ofsted_url = _resolve_ofsted_url(publications_url)
        LOGGER.info("Resolved Ofsted URL: %s", ofsted_url)

    ods_tmp = dest.with_suffix(".ods")
    try:
        _download_file(ofsted_url, ods_tmp)
        _ods_to_csv(ods_tmp, dest)
        return "ofsted: downloaded and converted from ODS ({0})".format(ofsted_url), ofsted_url
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


def _fetch_performance_table(label: str, input_cfg: Dict, dest: Path) -> Tuple[str, Optional[str]]:
    """Download a DfE performance-table CSV (or ZIP containing a CSV) if configured.

    Returns (note, url_used). Silently skips when no URL is configured — the
    caller must manually place the CSV at dest.
    """
    fetch_cfg = input_cfg.get("fetch", {})
    skip_if_exists = fetch_cfg.get("skip_if_exists", True)
    url = str(fetch_cfg.get("url") or "").strip()

    if skip_if_exists and dest.exists():
        LOGGER.info("%s file exists, skipping download: %s", label, dest)
        return "{0}: skipped (file exists)".format(label), url or None

    if not url:
        return (
            "{0}: skipped (no URL configured — download manually from "
            "https://www.find-school-performance-data.service.gov.uk/download-data "
            "and place at {1})".format(label, dest),
            None,
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

    return "{0}: downloaded from {1}".format(label, url), url


def fetch(pipeline_config: Dict, schools_dest: Path, ofsted_dest: Path) -> FetchResult:
    """Download GIAS, Ofsted, and (optionally) KS4/KS5 performance files.

    Returns a FetchResult whose ``provenances`` list records exactly where each
    file came from, when it was last written to disk, its size, and the date
    embedded in the source URL where extractable.
    """
    from london_data_model.settings import PROJECT_ROOT  # avoid top-level circular import

    fetch_cfg = pipeline_config.get("official_input", {}).get("fetch", {})
    result = FetchResult()

    # ── GIAS ────────────────────────────────────────────────────────────────
    gias_note, gias_url = _fetch_gias(fetch_cfg, schools_dest)
    result.notes.append(gias_note)
    result.schools_path = schools_dest
    result.provenances.append(_build_provenance(
        source_name="gias_establishments",
        path=schools_dest,
        url=gias_url,
        status="loaded" if schools_dest.exists() else "missing",
        notes=[gias_note],
        source_date=_extract_gias_date(gias_url) if gias_url else None,
    ))

    # ── Ofsted ──────────────────────────────────────────────────────────────
    ofsted_note, ofsted_url = _fetch_ofsted(fetch_cfg, ofsted_dest)
    result.notes.append(ofsted_note)
    result.ofsted_path = ofsted_dest
    result.provenances.append(_build_provenance(
        source_name="ofsted_state_funded_schools",
        path=ofsted_dest,
        url=ofsted_url,
        status="loaded" if ofsted_dest.exists() else "missing",
        notes=[ofsted_note],
        source_date=_extract_year_from_url(ofsted_url) if ofsted_url else None,
    ))

    # ── KS4 (GCSE) ──────────────────────────────────────────────────────────
    ks4_cfg = pipeline_config.get("ks4_input", {})
    if ks4_cfg.get("enabled", False):
        ks4_dest = PROJECT_ROOT / str(ks4_cfg.get("path", "data/raw/ks4_performance.csv"))
        ks4_note, ks4_url = _fetch_performance_table("ks4", ks4_cfg, ks4_dest)
        result.notes.append(ks4_note)
        result.ks4_path = ks4_dest
        result.provenances.append(_build_provenance(
            source_name="ks4_performance",
            path=ks4_dest,
            url=ks4_url,
            status="loaded" if ks4_dest.exists() else "missing",
            notes=[ks4_note],
            source_date=_extract_year_from_url(ks4_url) if ks4_url else None,
            academic_year=_extract_year_from_url(ks4_url) if ks4_url else None,
        ))
    else:
        ks4_path = PROJECT_ROOT / str(ks4_cfg.get("path", "data/raw/ks4_performance.csv"))
        result.provenances.append(_build_provenance(
            source_name="ks4_performance",
            path=ks4_path,
            url=str(ks4_cfg.get("fetch", {}).get("url") or "").strip() or None,
            status="not_configured",
            notes=["KS4 input disabled in config (ks4_input.enabled: false)."],
        ))

    # ── KS5 (A-level) ───────────────────────────────────────────────────────
    ks5_cfg = pipeline_config.get("ks5_input", {})
    if ks5_cfg.get("enabled", False):
        ks5_dest = PROJECT_ROOT / str(ks5_cfg.get("path", "data/raw/ks5_performance.csv"))
        ks5_note, ks5_url = _fetch_performance_table("ks5", ks5_cfg, ks5_dest)
        result.notes.append(ks5_note)
        result.ks5_path = ks5_dest
        result.provenances.append(_build_provenance(
            source_name="ks5_performance",
            path=ks5_dest,
            url=ks5_url,
            status="loaded" if ks5_dest.exists() else "missing",
            notes=[ks5_note],
            source_date=_extract_year_from_url(ks5_url) if ks5_url else None,
            academic_year=_extract_year_from_url(ks5_url) if ks5_url else None,
        ))
    else:
        ks5_path = PROJECT_ROOT / str(ks5_cfg.get("path", "data/raw/ks5_performance.csv"))
        result.provenances.append(_build_provenance(
            source_name="ks5_performance",
            path=ks5_path,
            url=str(ks5_cfg.get("fetch", {}).get("url") or "").strip() or None,
            status="not_configured",
            notes=["KS5 input disabled in config (ks5_input.enabled: false)."],
        ))

    return result
