import json
import tempfile
import unittest
from pathlib import Path

from london_data_model.pipelines.schools.extract import OfficialSourceConfigError, extract
from london_data_model.types import AreaConfig, PipelineContext
from london_data_model.utils.config import load_threshold_config


class ExtractStageTestCase(unittest.TestCase):
    def build_context(self, pipeline_config):
        return PipelineContext(
            pipeline_name="schools",
            area="KT19",
            run_id="test-run",
            config_path=None,
            area_config=AreaConfig(
                area_id="KT19",
                area_type="district",
                label="KT19",
                search_point_method="unresolved",
            ),
            pipeline_config=pipeline_config,
            threshold_config=load_threshold_config(),
        )

    def test_extract_loads_configured_sample_input_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            schools_path = Path(tmp_dir) / "schools.json"
            ofsted_path = Path(tmp_dir) / "ofsted.json"
            schools_path.write_text(
                json.dumps(
                    [
                        {
                            "school_name": "Sample School",
                            "school_urn": "123",
                            "address": "1 Sample Road",
                            "postcode": "KT19 1AA",
                            "latitude": 51.4,
                            "longitude": -0.3,
                            "phase": "primary",
                            "establishment_type": "Community school",
                            "is_open": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            ofsted_path.write_text("[]", encoding="utf-8")

            context = self.build_context(
                {
                    "version": 1,
                    "sample_input": {
                        "enabled": True,
                        "schools_path": str(schools_path),
                        "ofsted_path": str(ofsted_path),
                    },
                }
            )

            result = extract(context)

        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0]["school_name"], "Sample School")
        self.assertEqual(result.sources[0].status, "loaded")
        self.assertEqual(result.sources[1].status, "loaded")

    def test_extract_loads_and_merges_official_csv_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            schools_path = Path(tmp_dir) / "gias.csv"
            ofsted_path = Path(tmp_dir) / "ofsted.csv"
            schools_path.write_text(
                "\n".join(
                    [
                        "URN,EstablishmentName,Street,Town,County,Postcode,PhaseOfEducation,TypeOfEstablishment,EstablishmentStatus,Latitude,Longitude",
                        "123,Sample School,1 Sample Road,Epsom,Surrey,KT19 1AA,Primary,Community school,Open,51.4,-0.3",
                    ]
                ),
                encoding="utf-8",
            )
            ofsted_path.write_text(
                "\n".join(
                    [
                        "URN,Overall effectiveness,Inspection end date,Web link",
                        "123,Good,2024-01-01,https://reports.ofsted.gov.uk/provider/21/123",
                    ]
                ),
                encoding="utf-8",
            )

            context = self.build_context(
                {
                    "version": 1,
                    "input_mode": "official",
                    "official_input": {
                        "schools_path": str(schools_path),
                        "schools_format": "csv",
                        "ofsted_path": str(ofsted_path),
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
                }
            )

            result = extract(context)

        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0]["school_name"], "Sample School")
        self.assertEqual(result.records[0]["ofsted_rating_latest"], "Good")
        self.assertEqual(result.records[0]["school_urn"], "123")

    def test_extract_official_mode_raises_for_missing_file(self) -> None:
        context = self.build_context(
            {
                "version": 1,
                "input_mode": "official",
                "official_input": {
                    "schools_path": "/tmp/does-not-exist-gias.csv",
                    "schools_format": "csv",
                    "ofsted_path": "/tmp/does-not-exist-ofsted.csv",
                    "ofsted_format": "csv",
                    "schools_column_map": {"school_urn": "URN"},
                    "ofsted_column_map": {"school_urn": "URN"},
                },
            }
        )

        with self.assertRaises(OfficialSourceConfigError) as error:
            extract(context)

        self.assertIn("Official input file not found", str(error.exception))

    def test_extract_official_mode_raises_for_missing_required_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            schools_path = Path(tmp_dir) / "gias.csv"
            ofsted_path = Path(tmp_dir) / "ofsted.csv"
            schools_path.write_text("URN,EstablishmentName\n123,Sample School\n", encoding="utf-8")
            ofsted_path.write_text("URN,Overall effectiveness\n123,Good\n", encoding="utf-8")

            context = self.build_context(
                {
                    "version": 1,
                    "input_mode": "official",
                    "official_input": {
                        "schools_path": str(schools_path),
                        "schools_format": "csv",
                        "ofsted_path": str(ofsted_path),
                        "ofsted_format": "csv",
                        "schools_column_map": {
                            "school_urn": "URN",
                            "school_name": "EstablishmentName",
                            "postcode": "Postcode",
                        },
                        "ofsted_column_map": {
                            "school_urn": "URN",
                            "ofsted_rating_latest": "Overall effectiveness",
                        },
                    },
                }
            )

            with self.assertRaises(OfficialSourceConfigError) as error:
                extract(context)

        self.assertIn("schools_source is missing required headers", str(error.exception))


if __name__ == "__main__":
    unittest.main()
