import csv
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from london_data_model.pipelines.schools.fetch import (
    FetchError,
    _gias_url,
    _ods_to_csv,
    _resolve_ofsted_url,
    fetch,
)


def _make_ods(rows):
    """Build a minimal ODS (ZIP+XML) bytes object from a list of row lists."""
    ns_office = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    ns_table = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
    ns_text = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"

    cells_xml = ""
    for row in rows:
        cell_els = ""
        for value in row:
            cell_els += (
                '<table:table-cell xmlns:table="{ns_table}">'
                '<text:p xmlns:text="{ns_text}">{value}</text:p>'
                "</table:table-cell>"
            ).format(ns_table=ns_table, ns_text=ns_text, value=value)
        cells_xml += '<table:table-row xmlns:table="{ns_table}">{cells}</table:table-row>'.format(
            ns_table=ns_table, cells=cell_els
        )

    content = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<office:document-content xmlns:office="{ns_office}">'
        "<office:body>"
        '<office:spreadsheet xmlns:office="{ns_office}">'
        '<table:table xmlns:table="{ns_table}" table:name="Sheet1">'
        "{rows}"
        "</table:table>"
        "</office:spreadsheet>"
        "</office:body>"
        "</office:document-content>"
    ).format(ns_office=ns_office, ns_table=ns_table, rows=cells_xml)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("content.xml", content)
    return buf.getvalue()


class GiasUrlTestCase(unittest.TestCase):
    def test_url_contains_formatted_date(self):
        from datetime import date
        d = date(2026, 3, 28)
        url = _gias_url(d)
        self.assertIn("20260328", url)
        self.assertIn("edubasealldata", url)


class OdsToCsvTestCase(unittest.TestCase):
    def test_converts_rows_correctly(self):
        rows = [["URN", "Overall effectiveness", "Inspection end date"], ["123", "Good", "01/01/2024"]]
        ods_bytes = _make_ods(rows)
        with tempfile.TemporaryDirectory() as tmp:
            ods_path = Path(tmp) / "test.ods"
            csv_path = Path(tmp) / "test.csv"
            ods_path.write_bytes(ods_bytes)
            _ods_to_csv(ods_path, csv_path)
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                result = list(reader)
        self.assertEqual(result[0], ["URN", "Overall effectiveness", "Inspection end date"])
        self.assertEqual(result[1], ["123", "Good", "01/01/2024"])

    def test_strips_trailing_empty_cells_and_rows(self):
        rows = [["A", "B", ""], ["1", "", ""], ["", "", ""]]
        ods_bytes = _make_ods(rows)
        with tempfile.TemporaryDirectory() as tmp:
            ods_path = Path(tmp) / "test.ods"
            csv_path = Path(tmp) / "test.csv"
            ods_path.write_bytes(ods_bytes)
            _ods_to_csv(ods_path, csv_path)
            with open(csv_path, newline="", encoding="utf-8") as fh:
                result = list(csv.reader(fh))
        self.assertEqual(result, [["A", "B"], ["1"]])

    def test_raises_on_missing_table(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("content.xml", '<?xml version="1.0"?><root/>')
        with tempfile.TemporaryDirectory() as tmp:
            ods_path = Path(tmp) / "bad.ods"
            csv_path = Path(tmp) / "bad.csv"
            ods_path.write_bytes(buf.getvalue())
            with self.assertRaises(FetchError):
                _ods_to_csv(ods_path, csv_path)


class ResolveOfstedUrlTestCase(unittest.TestCase):
    def test_extracts_ods_url_from_page_html(self):
        html = (
            b'<a href="https://assets.publishing.service.gov.uk/media/abc123/'
            b'five-year-ofsted-inspection-data_state-funded-schools.ods">'
            b'State-funded schools</a>'
        )
        mock_response = MagicMock()
        mock_response.read.return_value = html
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            url = _resolve_ofsted_url("https://example.com/publications")

        self.assertIn("assets.publishing.service.gov.uk", url)
        self.assertIn("state-funded-schools.ods", url)

    def test_raises_when_no_ods_link_found(self):
        mock_response = MagicMock()
        mock_response.read.return_value = b"<html><body>No links here</body></html>"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            with self.assertRaises(FetchError):
                _resolve_ofsted_url("https://example.com/publications")


class FetchSkipIfExistsTestCase(unittest.TestCase):
    def test_skips_download_when_files_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            schools = Path(tmp) / "schools.csv"
            ofsted = Path(tmp) / "ofsted.csv"
            schools.write_text("URN,Name\n1,Test\n")
            ofsted.write_text("URN,Overall effectiveness\n1,Good\n")

            pipeline_config = {
                "official_input": {
                    "fetch": {"skip_if_exists": True}
                }
            }
            with patch("london_data_model.pipelines.schools.fetch._download_file") as mock_dl:
                result = fetch(pipeline_config, schools, ofsted)
                mock_dl.assert_not_called()

            self.assertIn("skipped", result.notes[0])
            self.assertIn("skipped", result.notes[1])

    def test_downloads_when_files_missing(self):
        rows = [["URN", "Overall effectiveness"], ["1", "Good"]]
        ods_bytes = _make_ods(rows)

        with tempfile.TemporaryDirectory() as tmp:
            schools = Path(tmp) / "schools.csv"
            ofsted = Path(tmp) / "ofsted.csv"

            pipeline_config = {
                "official_input": {
                    "fetch": {
                        "skip_if_exists": True,
                        "schools_url": "https://example.com/gias.csv",
                        "ofsted_url": "https://example.com/ofsted.ods",
                    }
                }
            }

            def fake_download(url, dest):
                if str(dest).endswith(".csv"):
                    dest.write_text("URN,EstablishmentName\n1,School\n")
                else:
                    dest.write_bytes(ods_bytes)

            with patch("london_data_model.pipelines.schools.fetch._download_file", side_effect=fake_download):
                result = fetch(pipeline_config, schools, ofsted)

            self.assertTrue(schools.exists())
            self.assertTrue(ofsted.exists())
            self.assertIn("downloaded", result.notes[0])
            self.assertIn("downloaded", result.notes[1])


if __name__ == "__main__":
    unittest.main()
