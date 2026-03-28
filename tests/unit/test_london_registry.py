"""Tests for London boroughs registry loading and borough config discovery."""

import unittest

from london_data_model.utils.config import (
    list_london_borough_ids,
    load_area_config,
    load_borough_configs,
    load_london_registry,
)


class TestLondonRegistry(unittest.TestCase):
    """Tests for load_london_registry and related helpers."""

    def test_load_registry_has_33_boroughs(self):
        registry = load_london_registry()
        boroughs = registry.get("boroughs", {})
        self.assertEqual(len(boroughs), 33)

    def test_registry_region_id(self):
        registry = load_london_registry()
        self.assertEqual(registry.get("region_id"), "london")

    def test_all_boroughs_have_la_code(self):
        registry = load_london_registry()
        for bid, info in registry["boroughs"].items():
            self.assertIn("la_code", info, "Missing la_code for: {0}".format(bid))
            self.assertIsInstance(info["la_code"], int)

    def test_inner_london_la_codes_201_to_213(self):
        registry = load_london_registry()
        boroughs = registry["boroughs"]
        inner_codes = sorted(
            info["la_code"]
            for info in boroughs.values()
            if 201 <= info["la_code"] <= 213
        )
        self.assertEqual(inner_codes, list(range(201, 214)))

    def test_outer_london_la_codes_301_to_320(self):
        registry = load_london_registry()
        boroughs = registry["boroughs"]
        outer_codes = sorted(
            info["la_code"]
            for info in boroughs.values()
            if 301 <= info["la_code"] <= 320
        )
        self.assertEqual(outer_codes, list(range(301, 321)))

    def test_all_boroughs_have_label(self):
        registry = load_london_registry()
        for bid, info in registry["boroughs"].items():
            self.assertIn("label", info, "Missing label for: {0}".format(bid))
            self.assertIsInstance(info["label"], str)
            self.assertTrue(len(info["label"]) > 0)

    def test_all_boroughs_have_coordinates(self):
        registry = load_london_registry()
        for bid, info in registry["boroughs"].items():
            self.assertIn("latitude", info, "Missing latitude for: {0}".format(bid))
            self.assertIn("longitude", info, "Missing longitude for: {0}".format(bid))
            lat = info["latitude"]
            lon = info["longitude"]
            self.assertIsNotNone(lat)
            self.assertIsNotNone(lon)
            # All London boroughs are within these approximate bounds
            self.assertGreater(lat, 51.2, "Latitude out of range for: {0}".format(bid))
            self.assertLess(lat, 51.75, "Latitude out of range for: {0}".format(bid))
            self.assertGreater(lon, -0.55, "Longitude out of range for: {0}".format(bid))
            self.assertLess(lon, 0.35, "Longitude out of range for: {0}".format(bid))

    def test_la_codes_are_unique(self):
        registry = load_london_registry()
        codes = [info["la_code"] for info in registry["boroughs"].values()]
        self.assertEqual(len(codes), len(set(codes)), "Duplicate LA codes found")

    def test_list_borough_ids_sorted(self):
        ids = list_london_borough_ids()
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(ids), 33)

    def test_list_borough_ids_contains_barnet(self):
        ids = list_london_borough_ids()
        self.assertIn("barnet", ids)

    def test_list_borough_ids_contains_all_expected(self):
        ids = list_london_borough_ids()
        expected_sample = [
            "barnet", "camden", "croydon", "greenwich", "hackney",
            "islington", "lambeth", "lewisham", "newham", "southwark",
            "tower-hamlets", "wandsworth", "westminster",
        ]
        for bid in expected_sample:
            self.assertIn(bid, ids)


class TestLoadBoroughConfigs(unittest.TestCase):
    """Tests for load_borough_configs."""

    def test_load_all_returns_33(self):
        configs = load_borough_configs()
        self.assertEqual(len(configs), 33)

    def test_load_subset(self):
        configs = load_borough_configs(["barnet", "camden", "croydon"])
        self.assertEqual(len(configs), 3)
        ids = {c.area_id for c in configs}
        self.assertEqual(ids, {"barnet", "camden", "croydon"})

    def test_barnet_config_has_correct_la_code(self):
        configs = load_borough_configs(["barnet"])
        self.assertEqual(len(configs), 1)
        barnet = configs[0]
        self.assertEqual(barnet.la_code, 302)
        self.assertEqual(barnet.area_type, "borough")

    def test_all_borough_configs_have_la_code(self):
        configs = load_borough_configs()
        for config in configs:
            self.assertIsNotNone(config.la_code, "la_code is None for: {0}".format(config.area_id))
            self.assertIsInstance(config.la_code, int)

    def test_all_borough_configs_area_type_borough(self):
        configs = load_borough_configs()
        for config in configs:
            self.assertEqual(config.area_type, "borough")

    def test_all_borough_configs_have_coordinates(self):
        configs = load_borough_configs()
        for config in configs:
            self.assertIsNotNone(config.latitude)
            self.assertIsNotNone(config.longitude)


class TestLoadAreaConfigWithLaCode(unittest.TestCase):
    """Tests for load_area_config reading la_code from borough YAML files."""

    def test_barnet_la_code(self):
        config = load_area_config(area="barnet")
        self.assertEqual(config.la_code, 302)

    def test_camden_la_code(self):
        config = load_area_config(area="camden")
        self.assertEqual(config.la_code, 202)

    def test_kt19_has_la_code(self):
        config = load_area_config(area="KT19")
        self.assertEqual(config.la_code, 319)

    def test_kingston_upon_thames_la_code(self):
        config = load_area_config(area="kingston-upon-thames")
        self.assertEqual(config.la_code, 314)


if __name__ == "__main__":
    unittest.main()
