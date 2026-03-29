import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from london_data_model.pipelines.schools.publish import publish
from london_data_model.types import (
    AreaConfig,
    ExtractResult,
    PipelineContext,
    SchoolRecord,
    TransformResult,
    ValidateResult,
)
from london_data_model.utils.config import load_threshold_config


class PublishSummaryTestCase(unittest.TestCase):
    def test_publish_computes_non_empty_summary_metrics(self) -> None:
        context = PipelineContext(
            pipeline_name="schools",
            area="KT19",
            run_id="publish-metrics",
            started_at=datetime(2026, 3, 29, 17, 0, tzinfo=timezone.utc),
            config_path=None,
            area_config=AreaConfig(
                area_id="KT19",
                area_type="district",
                label="KT19",
                search_point_method="user_supplied",
                latitude=51.3490,
                longitude=-0.2680,
            ),
            pipeline_config={
                "version": 1,
                "input_mode": "official",
                "structured_output_format": "json",
                "manifest_enabled": True,
            },
            threshold_config=load_threshold_config(),
        )

        validated = ValidateResult(
            records=[
                SchoolRecord(
                    school_name="Primary Complete",
                    school_urn="1001",
                    address="1 Primary Road",
                    postcode="KT19 1AA",
                    latitude=51.35,
                    longitude=-0.26,
                    phase="primary",
                    is_open=True,
                    distance_km=0.8,
                    accessibility_band="very_close",
                    proximity_score=94.67,
                    ofsted_rating_latest="Good",
                    ofsted_inspection_date_latest="2024-01-01",
                    data_quality_status="complete",
                ),
                SchoolRecord(
                    school_name="Secondary Missing Ofsted",
                    school_urn="1002",
                    address="2 Secondary Road",
                    postcode="KT19 1AB",
                    latitude=51.36,
                    longitude=-0.27,
                    phase="secondary",
                    is_open=True,
                    distance_km=3.2,
                    accessibility_band="close",
                    proximity_score=84.0,
                    ofsted_rating_latest=None,
                    ofsted_inspection_date_latest=None,
                    data_quality_status="partial",
                ),
                SchoolRecord(
                    school_name="All Through",
                    school_urn="1003",
                    address="3 All Through Road",
                    postcode="KT19 1AC",
                    latitude=51.37,
                    longitude=-0.28,
                    phase="all_through",
                    is_open=True,
                    distance_km=6.4,
                    accessibility_band="moderate",
                    proximity_score=68.0,
                    ofsted_rating_latest="Outstanding",
                    ofsted_inspection_date_latest="2023-05-01",
                    data_quality_status="complete",
                ),
            ],
            quality_summary={"complete": 2, "partial": 1, "poor": 0},
        )

        transformed = TransformResult(
            records=validated.records,
            excluded_record_count=0,
        )

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
                result = publish(ExtractResult(), transformed, validated, context)

            summary_payload = json.loads(
                Path(result.output_files["summary_json"]).read_text(encoding="utf-8")
            )
            status_payload = json.loads(
                Path(result.output_files["public_status_json"]).read_text(encoding="utf-8")
            )

        self.assertEqual(summary_payload["school_count_total"], 3)
        self.assertEqual(
            summary_payload["school_count_by_phase"],
            {"primary": 1, "secondary": 1, "all_through": 1},
        )
        self.assertEqual(
            summary_payload["school_count_by_accessibility_band"],
            {"very_close": 1, "close": 1, "moderate": 1},
        )
        self.assertEqual(summary_payload["missing_ofsted_count"], 1)
        self.assertEqual(
            status_payload["school_count_by_phase"],
            {"primary": 1, "secondary": 1, "all_through": 1},
        )
        self.assertEqual(
            status_payload["school_count_by_accessibility_band"],
            {"very_close": 1, "close": 1, "moderate": 1},
        )
        self.assertEqual(status_payload["missing_ofsted_count"], 1)


if __name__ == "__main__":
    unittest.main()
