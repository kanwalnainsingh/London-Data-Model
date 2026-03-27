import json
import tempfile
import unittest
from pathlib import Path

from london_data_model.pipelines.schools.extract import extract
from london_data_model.types import AreaConfig, PipelineContext
from london_data_model.utils.config import load_threshold_config


class ExtractStageTestCase(unittest.TestCase):
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

            context = PipelineContext(
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
                pipeline_config={
                    "version": 1,
                    "sample_input": {
                        "enabled": True,
                        "schools_path": str(schools_path),
                        "ofsted_path": str(ofsted_path),
                    },
                },
                threshold_config=load_threshold_config(),
            )

            result = extract(context)

        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0]["school_name"], "Sample School")
        self.assertEqual(result.sources[0].status, "loaded")
        self.assertEqual(result.sources[1].status, "loaded")


if __name__ == "__main__":
    unittest.main()
