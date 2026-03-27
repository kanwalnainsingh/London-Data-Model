import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from london_data_model.cli import build_parser, main


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
        self.assertIn("Schools pipeline scaffold is wired for area=KT19.", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
