import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from london_data_model.pipelines.schools.extract import extract
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import transform
from london_data_model.pipelines.schools.validate import validate
from london_data_model.types import PipelineContext
from london_data_model.utils.config import load_area_config, load_threshold_config


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "kt19_golden"


class GoldenPipelineRegressionTestCase(unittest.TestCase):
    def test_kt19_golden_fixture_pipeline_outputs_are_stable(self) -> None:
        context = PipelineContext(
            pipeline_name="schools",
            area="KT19",
            run_id="golden-run",
            started_at=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
            config_path=None,
            area_config=load_area_config("KT19"),
            pipeline_config={
                "version": 1,
                "input_mode": "official",
                "structured_output_format": "json",
                "manifest_enabled": True,
                "official_input": {
                    "schools_path": str(FIXTURES_DIR / "gias_establishments.csv"),
                    "schools_format": "csv",
                    "ofsted_path": str(FIXTURES_DIR / "ofsted_state_funded_schools.csv"),
                    "ofsted_format": "csv",
                    "merge_key": "school_urn",
                    "schools_column_map": {
                        "la_code": "LA (code)",
                        "school_urn": "URN",
                        "school_name": "EstablishmentName",
                        "address_line_1": "Street",
                        "town": "Town",
                        "county": "County",
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
                        "ofsted_report_url": "Web link",
                    },
                },
            },
            threshold_config=load_threshold_config(),
        )

        extracted = extract(context)
        transformed = transform(extracted, context)
        validated = validate(transformed, context)

        with tempfile.TemporaryDirectory() as tmp_dir:
            marts_dir = Path(tmp_dir) / "marts"
            manifests_dir = Path(tmp_dir) / "manifests"
            docs_data_dir = Path(tmp_dir) / "docs-data"
            marts_dir.mkdir()
            manifests_dir.mkdir()
            docs_data_dir.mkdir()

            with patch("london_data_model.pipelines.schools.publish.MARTS_DATA_DIR", marts_dir), patch(
                "london_data_model.pipelines.schools.publish.MANIFESTS_DATA_DIR", manifests_dir
            ), patch("london_data_model.pipelines.schools.publish.DOCS_DATA_DIR", docs_data_dir):
                published = publish(extracted, transformed, validated, context)

            records_payload = json.loads(
                Path(published.output_files["records_json"]).read_text(encoding="utf-8")
            )
            manifest_payload = json.loads(
                Path(published.output_files["manifest_json"]).read_text(encoding="utf-8")
            )
            public_manifest_payload = json.loads(
                Path(published.output_files["public_manifest_json"]).read_text(encoding="utf-8")
            )

        # 4 extracted: no LA-code filter (KT19 spans Sutton LA 319, Surrey LA 936, etc.)
        # URN 1001 Sutton primary, 1002 Sutton secondary, 1003 Sutton independent, 1004 Surrey academy
        self.assertEqual(len(extracted.records), 4)

        # 3 transformed: 1003 excluded (Independent school — not mainstream)
        self.assertEqual(len(transformed.records), 3)
        self.assertEqual(transformed.excluded_record_count, 1)
        self.assertEqual(len(validated.records), 3)
        self.assertEqual(published.record_count, 3)

        output_urns = [record["school_urn"] for record in records_payload]
        self.assertIn("1001", output_urns)  # Sutton primary
        self.assertIn("1002", output_urns)  # Sutton secondary
        self.assertIn("1004", output_urns)  # Surrey academy — confirms multi-LA extraction
        self.assertNotIn("1003", output_urns)  # Independent school correctly excluded

        record_1001 = next(r for r in records_payload if r["school_urn"] == "1001")
        self.assertEqual(record_1001["distance_km"], 0.0)
        self.assertEqual(record_1001["accessibility_band"], "very_close")
        self.assertEqual(record_1001["proximity_score"], 100.0)
        self.assertEqual(record_1001["data_quality_status"], "complete")

        record_1002 = next(r for r in records_payload if r["school_urn"] == "1002")
        self.assertEqual(record_1002["data_quality_status"], "partial")
        self.assertEqual(record_1002["data_quality_flags"], [
            "missing_ofsted_rating",
            "missing_inspection_date",
        ])

        record_1004 = next(r for r in records_payload if r["school_urn"] == "1004")
        self.assertAlmostEqual(record_1004["distance_km"], 2.59, delta=0.5)
        self.assertEqual(record_1004["data_quality_status"], "partial")  # No Ofsted data in fixture

        self.assertEqual(manifest_payload["input_mode"], "official")
        self.assertEqual(manifest_payload["record_counts"]["extracted"], 4)
        self.assertEqual(manifest_payload["record_counts"]["transformed_included"], 3)
        self.assertEqual(manifest_payload["record_counts"]["transformed_excluded"], 1)
        self.assertEqual(manifest_payload["quality_counts"], {"complete": 1, "partial": 2, "poor": 0})
        self.assertEqual(len(manifest_payload["input_sources"]), 2)
        self.assertEqual(public_manifest_payload["input_sources"][0]["source_path"], "tests/fixtures/kt19_golden/gias_establishments.csv")


if __name__ == "__main__":
    unittest.main()
