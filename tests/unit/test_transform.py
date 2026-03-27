import unittest

from london_data_model.pipelines.schools.transform import (
    assign_accessibility_band,
    calculate_distance_km,
    calculate_proximity_score,
    normalize_phase,
    resolve_threshold_profile,
    transform,
)
from london_data_model.types import AreaConfig, ExtractResult, PipelineContext
from london_data_model.utils.config import load_threshold_config


class TransformRulesTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.threshold_config = load_threshold_config()
        self.context = PipelineContext(
            pipeline_name="schools",
            area="KT19",
            run_id="test-run",
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
        self.assertEqual(result.records[0].accessibility_band, "close")
        self.assertAlmostEqual(result.records[0].distance_km, 1.001, places=3)
        self.assertEqual(result.records[0].proximity_score, 83.32)


if __name__ == "__main__":
    unittest.main()
