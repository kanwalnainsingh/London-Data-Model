import unittest
from datetime import datetime, timezone

from london_data_model.pipelines.schools.validate import (
    apply_data_quality,
    derive_data_quality_flags,
    derive_data_quality_status,
    summarize_quality,
    validate,
)
from london_data_model.types import AreaConfig, PipelineContext, SchoolRecord, TransformResult
from london_data_model.utils.config import load_threshold_config


class ValidateRulesTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.context = PipelineContext(
            pipeline_name="schools",
            area="KT19",
            run_id="test-run",
            started_at=datetime.now(timezone.utc),
            config_path=None,
            area_config=AreaConfig(
                area_id="KT19",
                area_type="district",
                label="KT19",
                search_point_method="user_supplied",
                latitude=51.4000,
                longitude=-0.3000,
            ),
            pipeline_config={"version": 1},
            threshold_config=load_threshold_config(),
        )

    def test_derive_data_quality_flags_identifies_missing_noncritical_fields(self) -> None:
        record = SchoolRecord(
            school_name="Sample School",
            school_urn="123",
            address="1 Sample Road",
            postcode="KT19 1AA",
            latitude=51.4000,
            longitude=-0.3000,
            phase="primary",
            distance_km=1.0,
        )

        flags = derive_data_quality_flags(record)

        self.assertIn("missing_ofsted_rating", flags)
        self.assertIn("missing_inspection_date", flags)
        self.assertNotIn("missing_ofsted_report_url", flags)

    def test_derive_data_quality_status_marks_critical_flags_as_poor(self) -> None:
        status = derive_data_quality_status(["missing_coordinates"])
        self.assertEqual(status, "poor")

    def test_derive_data_quality_status_marks_noncritical_flags_as_partial(self) -> None:
        status = derive_data_quality_status(["missing_ofsted_rating"])
        self.assertEqual(status, "partial")

    def test_apply_data_quality_sets_flags_and_status(self) -> None:
        record = SchoolRecord(
            school_name="Sample School",
            school_urn="123",
            address="",
            postcode=None,
            latitude=None,
            longitude=None,
            phase=None,
        )

        updated = apply_data_quality(record)

        self.assertEqual(updated.data_quality_status, "poor")
        self.assertIn("missing_postcode", updated.data_quality_flags)
        self.assertIn("missing_coordinates", updated.data_quality_flags)
        self.assertIn("invalid_phase", updated.data_quality_flags)

    def test_summarize_quality_counts_each_status(self) -> None:
        records = [
            SchoolRecord(data_quality_status="complete"),
            SchoolRecord(data_quality_status="partial"),
            SchoolRecord(data_quality_status="poor"),
        ]

        summary = summarize_quality(records)

        self.assertEqual(summary, {"complete": 1, "partial": 1, "poor": 1})

    def test_validate_preserves_records_and_applies_quality_summary(self) -> None:
        transformed = TransformResult(
            records=[
                SchoolRecord(
                    school_name="Complete School",
                    school_urn="111",
                    address="1 Good Road",
                    postcode="KT19 1AA",
                    latitude=51.4,
                    longitude=-0.3,
                    phase="primary",
                    distance_km=1.0,
                    accessibility_band="close",
                    proximity_score=80.0,
                    ofsted_rating_latest="Good",
                    ofsted_inspection_date_latest="2024-01-01",
                    ofsted_report_url="https://example.com/report",
                ),
                SchoolRecord(
                    school_name="Partial School",
                    school_urn="222",
                    address="1 Partial Road",
                    postcode="KT19 1AB",
                    latitude=51.41,
                    longitude=-0.31,
                    phase="secondary",
                    distance_km=2.0,
                ),
            ]
        )

        result = validate(transformed, self.context)

        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.quality_summary["complete"], 1)
        self.assertEqual(result.quality_summary["partial"], 1)
        self.assertEqual(result.quality_summary["poor"], 0)


if __name__ == "__main__":
    unittest.main()
