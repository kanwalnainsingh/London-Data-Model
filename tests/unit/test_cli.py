import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from london_data_model.cli import build_parser, main
from london_data_model.pipelines.schools import publish as publish_module
from london_data_model.pipelines.schools.pipeline import run as run_pipeline


class CliTestCase(unittest.TestCase):
    def test_build_parser_accepts_schools_run_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["schools", "run", "--area", "KT19"])

        self.assertEqual(args.domain, "schools")
        self.assertEqual(args.action, "run")
        self.assertEqual(args.area, "KT19")

    def test_main_runs_stub_pipeline(self) -> None:
        with patch("sys.argv", ["ldm", "schools", "run", "--area", "KT19"]):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertIn("Schools pipeline skeleton completed for area=KT19.", stdout.getvalue())

    def test_pipeline_run_writes_placeholder_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            marts_dir = Path(tmp_dir) / "marts"
            manifests_dir = Path(tmp_dir) / "manifests"
            marts_dir.mkdir()
            manifests_dir.mkdir()

            with patch.object(publish_module, "MARTS_DATA_DIR", marts_dir), patch.object(
                publish_module, "MANIFESTS_DATA_DIR", manifests_dir
            ):
                result = run_pipeline(area="KT19")
                output_files = result.artifacts["output_files"]
                self.assertTrue(Path(output_files["csv"]).exists())
                self.assertTrue(Path(output_files["records_json"]).exists())
                self.assertTrue(Path(output_files["summary_json"]).exists())
                self.assertTrue(Path(output_files["manifest_json"]).exists())

                summary_payload = json.loads(
                    Path(output_files["summary_json"]).read_text(encoding="utf-8")
                )

        self.assertEqual(result.status, "success")
        self.assertEqual(summary_payload["area_id"], "KT19")
        self.assertEqual(summary_payload["school_count_total"], 0)


if __name__ == "__main__":
    unittest.main()
