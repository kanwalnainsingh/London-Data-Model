"""Tests ensuring no in-scope schools are incorrectly filtered out.

These tests guard against regressions where geographic pre-filters (e.g. la_code)
incorrectly exclude schools that are within the distance threshold.

Root cause fixed: KT19 had la_code=319 (Sutton only), silently dropping 87 schools
in Surrey, Kingston upon Thames, Merton and other LAs that are within range.
"""

import csv
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from london_data_model.pipelines.schools.extract import extract, filter_by_la_codes
from london_data_model.pipelines.schools.transform import transform
from london_data_model.types import PipelineContext
from london_data_model.utils.config import load_area_config, load_threshold_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline_config(schools_path: str, ofsted_path: str) -> dict:
    """Minimal pipeline config for official-mode fixture runs."""
    return {
        "version": 1,
        "input_mode": "official",
        "structured_output_format": "json",
        "manifest_enabled": True,
        "official_input": {
            "schools_path": schools_path,
            "schools_format": "csv",
            "ofsted_path": ofsted_path,
            "ofsted_format": "csv",
            "merge_key": "school_urn",
            "schools_column_map": {
                "la_code": "LA (code)",
                "school_urn": "URN",
                "school_name": "EstablishmentName",
                "address_line_1": "Street",
                "town": "Town",
                "postcode": "Postcode",
                "phase": "PhaseOfEducation",
                "establishment_type": "TypeOfEstablishment",
                "is_open": "EstablishmentStatus",
                "latitude": "Latitude",
                "longitude": "Longitude",
            },
            "ofsted_column_map": {
                "school_urn": "URN",
                "ofsted_rating_latest": "Overall effectiveness",
                "ofsted_inspection_date_latest": "Inspection end date",
            },
        },
    }


def _write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_context(area: str, pipeline_config: dict, area_config=None) -> PipelineContext:
    return PipelineContext(
        pipeline_name="schools",
        area=area,
        run_id="test-coverage",
        started_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        config_path=None,
        area_config=area_config or load_area_config(area),
        pipeline_config=pipeline_config,
        threshold_config=load_threshold_config(),
    )


# ---------------------------------------------------------------------------
# Unit tests for filter_by_la_codes
# ---------------------------------------------------------------------------

class FilterByLaCodesTestCase(unittest.TestCase):
    """Unit tests for the extract.filter_by_la_codes helper."""

    def _records(self, codes: List[str]) -> List[Dict]:
        return [{"school_urn": str(i), "la_code": code} for i, code in enumerate(codes)]

    def test_keeps_matching_records(self):
        records = self._records(["319", "319", "936"])
        result = filter_by_la_codes(records, [319])
        self.assertEqual(len(result), 2)
        self.assertTrue(all(r["la_code"] == "319" for r in result))

    def test_drops_non_matching_records(self):
        records = self._records(["936", "314"])
        result = filter_by_la_codes(records, [319])
        self.assertEqual(result, [])

    def test_multi_code_filter_keeps_all_matching(self):
        records = self._records(["319", "936", "314", "202"])
        result = filter_by_la_codes(records, [319, 936, 314])
        urns_kept = {r["school_urn"] for r in result}
        self.assertEqual(len(result), 3)
        self.assertNotIn("3", urns_kept)  # LA 202 dropped

    def test_empty_la_list_returns_nothing(self):
        records = self._records(["319", "936"])
        result = filter_by_la_codes(records, [])
        self.assertEqual(result, [])

    def test_missing_la_code_field_excluded(self):
        records = [{"school_urn": "x"}]  # no la_code key
        result = filter_by_la_codes(records, [319])
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Integration: multi-LA extraction (KT19 fix)
# ---------------------------------------------------------------------------

class MultiLaExtractionTestCase(unittest.TestCase):
    """Confirms schools from different LAs all pass through when la_code is None."""

    GIAS_FIELDS = ["URN", "EstablishmentName", "Street", "Town", "Postcode",
                   "PhaseOfEducation", "TypeOfEstablishment", "EstablishmentStatus",
                   "Latitude", "Longitude", "LA (code)"]
    OFSTED_FIELDS = ["URN", "Overall effectiveness", "Inspection end date"]

    # Schools that represent the KT19 cross-LA situation
    GIAS_ROWS = [
        # Sutton LA — already captured before the fix
        {"URN": "S001", "EstablishmentName": "Sutton Primary School",
         "Street": "1 High St", "Town": "Sutton", "Postcode": "SM1 1AA",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.361", "Longitude": "-0.193",
         "LA (code)": "319"},
        # Surrey LA — was wrongly excluded before the fix
        {"URN": "S002", "EstablishmentName": "Surrey Academy (Auriol-like)",
         "Street": "Vale Road", "Town": "Stoneleigh", "Postcode": "KT19 0PJ",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Academy converter",
         "EstablishmentStatus": "Open", "Latitude": "51.370", "Longitude": "-0.251",
         "LA (code)": "936"},
        # Kingston upon Thames LA — also outside Sutton
        {"URN": "S003", "EstablishmentName": "Kingston Primary School",
         "Street": "3 The Street", "Town": "Chessington", "Postcode": "KT9 1AA",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.357", "Longitude": "-0.303",
         "LA (code)": "314"},
    ]
    OFSTED_ROWS: List[Dict] = []  # no Ofsted data — schools will be partial quality

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.schools_path = tmp / "gias.csv"
        self.ofsted_path = tmp / "ofsted.csv"
        _write_csv(self.schools_path, self.GIAS_ROWS, self.GIAS_FIELDS)
        _write_csv(self.ofsted_path, self.OFSTED_ROWS, self.OFSTED_FIELDS)

    def tearDown(self):
        self._tmp.cleanup()

    def _run_extract(self, area_config=None):
        cfg = _make_pipeline_config(str(self.schools_path), str(self.ofsted_path))
        ctx = _make_context("KT19", cfg, area_config=area_config)
        return extract(ctx)

    def test_all_three_la_schools_extracted_when_no_la_filter(self):
        """All schools regardless of LA code pass through when area has no la_code."""
        result = self._run_extract()
        urns = {r["school_urn"] for r in result.records}
        self.assertEqual(urns, {"S001", "S002", "S003"})

    def test_surrey_school_not_dropped(self):
        """Surrey LA (936) school is present — this is the Auriol Junior School bug."""
        result = self._run_extract()
        urns = {r["school_urn"] for r in result.records}
        self.assertIn("S002", urns, "Surrey LA school missing — LA code filter may have been re-introduced")

    def test_kingston_school_not_dropped(self):
        """Kingston upon Thames LA (314) school is present."""
        result = self._run_extract()
        urns = {r["school_urn"] for r in result.records}
        self.assertIn("S003", urns, "Kingston LA school missing — LA code filter may have been re-introduced")

    def test_surrey_school_passes_transform(self):
        """Surrey academy within 6 km of KT19 search point survives transform."""
        result = self._run_extract()
        transformed = transform(result, _make_context("KT19", _make_pipeline_config(
            str(self.schools_path), str(self.ofsted_path)
        )))
        urns = {r.school_urn for r in transformed.records}
        self.assertIn("S002", urns, "Surrey school excluded by transform — check distance threshold")

    def test_kt19_config_has_no_la_code(self):
        """Regression guard: kt19.yml must not define la_code."""
        config = load_area_config("KT19")
        self.assertIsNone(
            config.la_code,
            "kt19.yml has la_code set — this will exclude cross-LA schools. Remove it.",
        )


# ---------------------------------------------------------------------------
# Distance threshold enforcement
# ---------------------------------------------------------------------------

class DistanceThresholdTestCase(unittest.TestCase):
    """Schools beyond the phase threshold are excluded by transform."""

    GIAS_FIELDS = ["URN", "EstablishmentName", "Street", "Town", "Postcode",
                   "PhaseOfEducation", "TypeOfEstablishment", "EstablishmentStatus",
                   "Latitude", "Longitude", "LA (code)"]
    OFSTED_FIELDS = ["URN", "Overall effectiveness", "Inspection end date"]
    OFSTED_ROWS: List[Dict] = []

    # KT19 search point: 51.349, -0.268
    # Thresholds: primary max 15 km, secondary max 20 km
    GIAS_ROWS = [
        # Primary within 15 km (at 0.0 km — right at search point)
        {"URN": "D001", "EstablishmentName": "Close Primary",
         "Street": "1 A", "Town": "Epsom", "Postcode": "KT19 0AA",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.349", "Longitude": "-0.268",
         "LA (code)": "319"},
        # Primary just beyond 15 km (at ~15.5 km north, delta_lat ≈ +0.140)
        {"URN": "D002", "EstablishmentName": "Too Far Primary",
         "Street": "2 B", "Town": "Somewhere", "Postcode": "KT1 9ZZ",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.489", "Longitude": "-0.268",
         "LA (code)": "319"},
        # Secondary within 20 km (at ~16 km north)
        {"URN": "D003", "EstablishmentName": "Distant Secondary",
         "Street": "3 C", "Town": "Kingston", "Postcode": "KT2 9ZZ",
         "PhaseOfEducation": "Secondary", "TypeOfEstablishment": "Academy converter",
         "EstablishmentStatus": "Open", "Latitude": "51.493", "Longitude": "-0.268",
         "LA (code)": "314"},
        # Secondary just beyond 20 km (at ~20.5 km north, delta_lat ≈ +0.185)
        {"URN": "D004", "EstablishmentName": "Very Far Secondary",
         "Street": "4 D", "Town": "London", "Postcode": "W1A 1AA",
         "PhaseOfEducation": "Secondary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.534", "Longitude": "-0.268",
         "LA (code)": "202"},
    ]

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.schools_path = tmp / "gias.csv"
        self.ofsted_path = tmp / "ofsted.csv"
        _write_csv(self.schools_path, self.GIAS_ROWS, self.GIAS_FIELDS)
        _write_csv(self.ofsted_path, self.OFSTED_ROWS, self.OFSTED_FIELDS)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self):
        cfg = _make_pipeline_config(str(self.schools_path), str(self.ofsted_path))
        ctx = _make_context("KT19", cfg)
        extracted = extract(ctx)
        return transform(extracted, ctx)

    def test_primary_within_threshold_included(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertIn("D001", urns)

    def test_primary_beyond_threshold_excluded(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertNotIn("D002", urns)

    def test_secondary_within_threshold_included(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertIn("D003", urns)

    def test_secondary_beyond_threshold_excluded(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertNotIn("D004", urns)


# ---------------------------------------------------------------------------
# Online / virtual schools excluded (no coordinates)
# ---------------------------------------------------------------------------

class OnlineSchoolExclusionTestCase(unittest.TestCase):
    """Schools with no physical location (no coordinates) must be excluded."""

    GIAS_FIELDS = ["URN", "EstablishmentName", "Street", "Town", "Postcode",
                   "PhaseOfEducation", "TypeOfEstablishment", "EstablishmentStatus",
                   "Latitude", "Longitude", "LA (code)"]
    OFSTED_FIELDS = ["URN", "Overall effectiveness", "Inspection end date"]
    OFSTED_ROWS: List[Dict] = []

    GIAS_ROWS = [
        # Normal physical school — should be included
        {"URN": "O001", "EstablishmentName": "Normal Primary School",
         "Street": "1 Road", "Town": "Epsom", "Postcode": "KT19 1AA",
         "PhaseOfEducation": "Primary", "TypeOfEstablishment": "Community school",
         "EstablishmentStatus": "Open", "Latitude": "51.349", "Longitude": "-0.268",
         "LA (code)": "319"},
        # Online school with no coordinates — must be excluded
        {"URN": "O002", "EstablishmentName": "Virtual Online Academy",
         "Street": "", "Town": "", "Postcode": "",
         "PhaseOfEducation": "Secondary", "TypeOfEstablishment": "Academy converter",
         "EstablishmentStatus": "Open", "Latitude": "", "Longitude": "",
         "LA (code)": "936"},
    ]

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.schools_path = tmp / "gias.csv"
        self.ofsted_path = tmp / "ofsted.csv"
        _write_csv(self.schools_path, self.GIAS_ROWS, self.GIAS_FIELDS)
        _write_csv(self.ofsted_path, self.OFSTED_ROWS, self.OFSTED_FIELDS)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self):
        cfg = _make_pipeline_config(str(self.schools_path), str(self.ofsted_path))
        ctx = _make_context("KT19", cfg)
        extracted = extract(ctx)
        return transform(extracted, ctx)

    def test_physical_school_included(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertIn("O001", urns)

    def test_online_school_without_coordinates_excluded(self):
        result = self._run()
        urns = {r.school_urn for r in result.records}
        self.assertNotIn("O002", urns, "Online/virtual school with no coordinates must be excluded")


if __name__ == "__main__":
    unittest.main()
