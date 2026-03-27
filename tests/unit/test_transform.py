import unittest
from datetime import datetime, timezone

from london_data_model.pipelines.schools.transform import (
    assign_accessibility_band,
    calculate_distance_km,
    calculate_proximity_score,
    is_in_scope_school,
    is_mainstream_establishment,
    normalize_phase,
    normalize_open_status,
    resolve_threshold_profile,
    transform,
)
from london_data_model.types import AreaConfig, ExtractResult, PipelineContext, SchoolRecord
from london_data_model.utils.config import load_threshold_config


class TransformRulesTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.threshold_config = load_threshold_config()
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
            threshold_config=self.threshold_config,
        )

    def test_calculate_distance_km_returns_zero_for_same_point(self) -> None:
        distance = calculate_distance_km(51.4000, -0.3000, 51.4000, -0.3000)
        self.assertEqual(distance, 0.0)

    def test_calculate_distance_km_returns_expected_short_distance(self) -> None:
        distance = calculate_distance_km(51.4000, -0.3000, 51.4090, -0.3000)
        self.assertAlmostEqual(distance, 1.001, places=3)

    def test_assign_accessibility_band_uses_primary_thresholds(self) -> None:
        band = assign_accessibility_band("primary", 1.5, self.threshold_config)
        self.assertEqual(band, "close")

    def test_assign_accessibility_band_uses_all_through_profile(self) -> None:
        band = assign_accessibility_band("all-through", 5.5, self.threshold_config)
        self.assertEqual(band, "moderate")

    def test_calculate_proximity_score_is_distance_only_and_bounded(self) -> None:
        score = calculate_proximity_score("secondary", 2.5, self.threshold_config)
        self.assertEqual(score, 75.0)

    def test_calculate_proximity_score_returns_zero_at_or_beyond_zero_point(self) -> None:
        score = calculate_proximity_score("primary", 7.0, self.threshold_config)
        self.assertEqual(score, 0.0)

    def test_normalize_phase_rejects_unknown_values(self) -> None:
        self.assertIsNone(normalize_phase("college"))

    def test_normalize_open_status_handles_source_strings(self) -> None:
        self.assertTrue(normalize_open_status("Open"))
        self.assertFalse(normalize_open_status("Closed"))
        self.assertIsNone(normalize_open_status("Proposed"))

    def test_is_mainstream_establishment_rejects_private_and_special(self) -> None:
        self.assertFalse(is_mainstream_establishment("Independent school"))
        self.assertFalse(is_mainstream_establishment("Special post 16 institution"))
        self.assertTrue(is_mainstream_establishment("Community school"))

    def test_is_in_scope_school_rejects_closed_school(self) -> None:
        record = SchoolRecord(phase="primary", establishment_type="Community school", is_open=False)
        self.assertFalse(is_in_scope_school(record))

    def test_resolve_threshold_profile_maps_all_through_explicitly(self) -> None:
        profile = resolve_threshold_profile("all_through", self.threshold_config)
        self.assertEqual(profile, "secondary")

    def test_transform_populates_distance_fields_for_valid_record(self) -> None:
        extracted = ExtractResult(
            records=[
                {
                    "school_name": "Sample School",
                    "school_urn": "123456",
                    "address": "1 Sample Road",
                    "postcode": "KT19 1AA",
                    "latitude": 51.4090,
                    "longitude": -0.3000,
                    "phase": "primary",
                    "establishment_type": "Community school",
                    "is_open": True,
                }
            ]
        )

        result = transform(extracted, self.context)

        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.excluded_record_count, 0)
        self.assertEqual(result.records[0].accessibility_band, "close")
        self.assertAlmostEqual(result.records[0].distance_km, 1.001, places=3)
        self.assertEqual(result.records[0].proximity_score, 83.32)

    def test_transform_excludes_private_special_college_and_closed_records(self) -> None:
        extracted = ExtractResult(
            records=[
                {
                    "school_name": "Private School",
                    "school_urn": "1",
                    "address": "1 Road",
                    "postcode": "KT19 1AA",
                    "latitude": 51.4010,
                    "longitude": -0.3000,
                    "phase": "primary",
                    "establishment_type": "Independent school",
                    "is_open": "Open",
                },
                {
                    "school_name": "Special School",
                    "school_urn": "2",
                    "address": "2 Road",
                    "postcode": "KT19 1AB",
                    "latitude": 51.4020,
                    "longitude": -0.3000,
                    "phase": "secondary",
                    "establishment_type": "Special school",
                    "is_open": "Open",
                },
                {
                    "school_name": "College",
                    "school_urn": "3",
                    "address": "3 Road",
                    "postcode": "KT19 1AC",
                    "latitude": 51.4030,
                    "longitude": -0.3000,
                    "phase": "secondary",
                    "establishment_type": "Sixth form college",
                    "is_open": "Open",
                },
                {
                    "school_name": "Closed School",
                    "school_urn": "4",
                    "address": "4 Road",
                    "postcode": "KT19 1AD",
                    "latitude": 51.4040,
                    "longitude": -0.3000,
                    "phase": "primary",
                    "establishment_type": "Community school",
                    "is_open": "Closed",
                },
                {
                    "school_name": "Included School",
                    "school_urn": "5",
                    "address": "5 Road",
                    "postcode": "KT19 1AE",
                    "latitude": 51.4050,
                    "longitude": -0.3000,
                    "phase": "all-through",
                    "establishment_type": "Academy sponsor led",
                    "is_open": "Open",
                },
            ]
        )

        result = transform(extracted, self.context)

        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0].school_urn, "5")
        self.assertEqual(result.excluded_record_count, 4)


if __name__ == "__main__":
    unittest.main()
