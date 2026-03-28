"""Tests for LA code filtering and multi-borough orchestration."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from london_data_model.pipelines.schools.extract import filter_by_la_codes


def _make_synthetic_records(la_codes_and_counts):
    """Generate synthetic GIAS-format records for testing.

    Args:
        la_codes_and_counts: List of (la_code, count) tuples.

    Returns:
        List of record dicts.
    """
    records = []
    for la_code, count in la_codes_and_counts:
        for i in range(count):
            records.append({
                "school_urn": str(100000 + la_code * 100 + i),
                "school_name": "School {0}-{1}".format(la_code, i),
                "la_code": str(la_code),
                "phase": "Primary" if i % 2 == 0 else "Secondary",
                "establishment_type": "Community School",
                "is_open": True,
                "address": "1 Test Street",
                "postcode": "N1 1AA",
                "latitude": 51.5 + (la_code - 300) * 0.01,
                "longitude": -0.1 + i * 0.005,
            })
    return records


class TestFilterByLaCodes(unittest.TestCase):
    """Tests for the filter_by_la_codes function."""

    def test_filter_keeps_matching_records(self):
        records = _make_synthetic_records([(302, 5), (306, 3)])
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 5)
        for r in result:
            self.assertEqual(r["la_code"], "302")

    def test_filter_excludes_non_matching_records(self):
        records = _make_synthetic_records([(302, 5), (306, 3)])
        result = filter_by_la_codes(records, [302])
        for r in result:
            self.assertNotEqual(r["la_code"], "306")

    def test_filter_multiple_la_codes(self):
        records = _make_synthetic_records([(302, 5), (306, 3), (202, 7)])
        result = filter_by_la_codes(records, [302, 306])
        self.assertEqual(len(result), 8)

    def test_filter_all_three_partitions_sum_correctly(self):
        records = _make_synthetic_records([(302, 5), (306, 3), (202, 7)])
        barnet = filter_by_la_codes(records, [302])
        croydon = filter_by_la_codes(records, [306])
        camden = filter_by_la_codes(records, [202])
        self.assertEqual(len(barnet) + len(croydon) + len(camden), len(records))

    def test_filter_empty_input(self):
        result = filter_by_la_codes([], [302])
        self.assertEqual(result, [])

    def test_filter_no_match(self):
        records = _make_synthetic_records([(999, 5)])
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 0)

    def test_filter_all_match(self):
        records = _make_synthetic_records([(302, 10)])
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 10)

    def test_filter_handles_missing_la_code_field(self):
        records = [{"school_name": "No LA field"}]
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 0)

    def test_filter_handles_none_la_code(self):
        records = [{"school_name": "None LA", "la_code": None}]
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 0)

    def test_filter_handles_empty_la_code_string(self):
        records = [{"school_name": "Empty LA", "la_code": ""}]
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 0)

    def test_filter_with_whitespace_in_la_code(self):
        records = [{"school_name": "Padded LA", "la_code": "  302  "}]
        result = filter_by_la_codes(records, [302])
        self.assertEqual(len(result), 1)

    def test_filter_returns_list(self):
        result = filter_by_la_codes([], [302])
        self.assertIsInstance(result, list)

    def test_filter_preserves_all_fields(self):
        records = [{"school_urn": "123456", "la_code": "302", "school_name": "Test"}]
        result = filter_by_la_codes(records, [302])
        self.assertEqual(result[0]["school_urn"], "123456")
        self.assertEqual(result[0]["school_name"], "Test")

    def test_filter_empty_la_codes_list(self):
        records = _make_synthetic_records([(302, 5)])
        result = filter_by_la_codes(records, [])
        self.assertEqual(len(result), 0)


class TestLondonIndexPublishing(unittest.TestCase):
    """Tests for _publish_london_index output structure."""

    def test_london_index_json_structure(self):
        """Verify london-index.json has the expected structure after a multi-borough run."""
        from london_data_model.pipelines.schools.orchestrate import _publish_london_index
        from london_data_model.types import AreaConfig, PublishResult, ValidateResult

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            borough_configs = [
                AreaConfig(
                    area_id="barnet",
                    area_type="borough",
                    label="London Borough of Barnet",
                    search_point_method="borough_centroid",
                    latitude=51.6252,
                    longitude=-0.1517,
                    la_code=302,
                ),
                AreaConfig(
                    area_id="camden",
                    area_type="borough",
                    label="London Borough of Camden",
                    search_point_method="borough_centroid",
                    latitude=51.5290,
                    longitude=-0.1255,
                    la_code=202,
                ),
            ]

            borough_results = [
                (
                    borough_configs[0],
                    PublishResult(record_count=12),
                    ValidateResult(quality_summary={"complete": 10, "partial": 2, "poor": 0}),
                ),
                (
                    borough_configs[1],
                    PublishResult(record_count=8),
                    ValidateResult(quality_summary={"complete": 6, "partial": 1, "poor": 1}),
                ),
            ]

            pipeline_config = {"input_mode": "sample"}

            # Patch DOCS_DATA_DIR to use temp directory
            with patch("london_data_model.pipelines.schools.orchestrate.DOCS_DATA_DIR", tmp_path):
                _publish_london_index(borough_results, pipeline_config)

            index_path = tmp_path / "london-index.json"
            self.assertTrue(index_path.exists())

            with open(index_path, encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["region_id"], "london")
            self.assertEqual(data["borough_count"], 2)
            self.assertEqual(data["school_count_total"], 20)
            self.assertEqual(data["input_mode"], "sample")
            self.assertIn("generated_at", data)
            self.assertIn("quality_counts", data)
            self.assertEqual(data["quality_counts"]["complete"], 16)
            self.assertEqual(data["quality_counts"]["partial"], 3)
            self.assertEqual(data["quality_counts"]["poor"], 1)

            # Areas sorted alphabetically
            area_ids = [a["area_id"] for a in data["areas"]]
            self.assertEqual(area_ids, sorted(area_ids))

            # Each area has required fields
            for area in data["areas"]:
                self.assertIn("area_id", area)
                self.assertIn("label", area)
                self.assertIn("school_count", area)
                self.assertIn("quality_counts", area)
                self.assertIn("status_url", area)

    def test_london_index_status_url_format(self):
        """Status URL should use area_id, not hardcoded kt19."""
        from london_data_model.pipelines.schools.orchestrate import _publish_london_index
        from london_data_model.types import AreaConfig, PublishResult, ValidateResult

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            borough_config = AreaConfig(
                area_id="tower-hamlets",
                area_type="borough",
                label="London Borough of Tower Hamlets",
                search_point_method="borough_centroid",
                latitude=51.5150,
                longitude=-0.0172,
                la_code=211,
            )

            borough_results = [
                (
                    borough_config,
                    PublishResult(record_count=5),
                    ValidateResult(quality_summary={"complete": 5, "partial": 0, "poor": 0}),
                ),
            ]

            with patch("london_data_model.pipelines.schools.orchestrate.DOCS_DATA_DIR", tmp_path):
                _publish_london_index(borough_results, {"input_mode": "sample"})

            with open(tmp_path / "london-index.json", encoding="utf-8") as f:
                data = json.load(f)

            area = data["areas"][0]
            self.assertIn("tower-hamlets", area["status_url"])
            self.assertNotIn("kt19", area["status_url"])


if __name__ == "__main__":
    unittest.main()
