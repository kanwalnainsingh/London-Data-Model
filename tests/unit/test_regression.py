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

        self.assertEqual(len(extracted.records), 3)
        self.assertEqual(len(transformed.records), 2)
        self.assertEqual(transformed.excluded_record_count, 1)
        self.assertEqual(len(validated.records), 2)
        self.assertEqual(published.record_count, 2)

        self.assertEqual([record["school_urn"] for record in records_payload], ["1001", "1002"])
        self.assertEqual(records_payload[0]["distance_km"], 0.0)
        self.assertEqual(records_payload[0]["accessibility_band"], "very_close")
        self.assertEqual(records_payload[0]["proximity_score"], 100.0)
        self.assertEqual(records_payload[0]["data_quality_status"], "complete")
        self.assertEqual(records_payload[1]["data_quality_status"], "partial")
        self.assertEqual(records_payload[1]["data_quality_flags"], [
            "missing_ofsted_rating",
            "missing_inspection_date",
            "missing_ofsted_report_url",
        ])

        self.assertEqual(manifest_payload["input_mode"], "official")
        self.assertEqual(manifest_payload["record_counts"]["extracted"], 3)
        self.assertEqual(manifest_payload["record_counts"]["transformed_included"], 2)
        self.assertEqual(manifest_payload["record_counts"]["transformed_excluded"], 1)
        self.assertEqual(manifest_payload["quality_counts"], {"complete": 1, "partial": 1, "poor": 0})
        self.assertEqual(len(manifest_payload["input_sources"]), 2)
        self.assertEqual(public_manifest_payload["input_sources"][0]["source_path"], "tests/fixtures/kt19_golden/gias_establishments.csv")


if __name__ == "__main__":
    unittest.main()
