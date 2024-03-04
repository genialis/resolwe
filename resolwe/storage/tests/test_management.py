import os
from io import StringIO

from django.core.management import call_command
from django.db.models import Q
from django.test import TestCase

from resolwe.flow.models import Data, Process
from resolwe.storage.management.commands.compare_models_and_csv import (
    FileIterator,
    ModelIterator,
    map_subpath_locations,
    parse_line,
)
from resolwe.storage.models import FileStorage, ReferencedPath, StorageLocation


class CompareModelsTestCase(TestCase):
    def setUp(self):
        self.proc = Process.objects.create(contributor_id=1)
        # subpath 100
        self.fs100 = FileStorage.objects.create()
        self.d100 = Data.objects.create(
            process=self.proc,
            contributor_id=1,
            location=self.fs100,
        )
        self.sl100 = StorageLocation.objects.create(
            file_storage=self.fs100,
            url="100",
        )
        # subpath 123
        self.fs123 = FileStorage.objects.create()
        self.d123 = Data.objects.create(
            process=self.proc,
            contributor_id=1,
            location=self.fs123,
        )
        self.sl123 = StorageLocation.objects.create(
            file_storage=self.fs123,
            url="123",
        )

        self.make_file = lambda name, sl: ReferencedPath.objects.create(
            path=name, awss3etag="hashhashhash"
        ).storage_locations.add(sl)

        # all paths present in csv:
        self.make_file("names/000.json", self.sl100)
        self.make_file("names/001.json", self.sl100)
        self.make_file("names/002.json", self.sl100)
        self.make_file("names/003.json", self.sl100)
        self.make_file("names/004.json", self.sl100)
        self.make_file("names/005.json", self.sl100)

        self.make_file("d.json", self.sl123)
        self.make_file("f.json", self.sl123)
        self.make_file("j.json", self.sl123)
        self.make_file("k.json", self.sl123)
        self.make_file("l.json", self.sl123)

        self.csv_filename = os.path.abspath(
            os.path.dirname(__file__) + "/files/compare_models_and_csv.csv"
        )

        self.out = StringIO()

    def test_complete_match(self):
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("11 files OK", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_complete_mismatch(self):
        ReferencedPath.objects.all().delete()

        self.make_file("names/006.json", self.sl100)

        self.make_file("a.json", self.sl123)
        self.make_file("b.json", self.sl123)
        self.make_file("c.json", self.sl123)
        self.make_file("e.json", self.sl123)
        self.make_file("g.json", self.sl123)
        self.make_file("h.json", self.sl123)
        self.make_file("i.json", self.sl123)

        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/names/000.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertIn("CSV-ONLY 100/names/004.json", self.output)
        self.assertIn("CSV-ONLY 100/names/005.json", self.output)
        self.assertIn("MODEL-ONLY 100/names/006.json", self.output)
        self.assertIn("CSV-ONLY 123/d.json", self.output)
        self.assertIn("CSV-ONLY 123/f.json", self.output)
        self.assertIn("CSV-ONLY 123/j.json", self.output)
        self.assertIn("CSV-ONLY 123/k.json", self.output)
        self.assertIn("CSV-ONLY 123/l.json", self.output)
        self.assertIn("MODEL-ONLY 123/a.json", self.output)
        self.assertIn("MODEL-ONLY 123/b.json", self.output)
        self.assertIn("MODEL-ONLY 123/c.json", self.output)
        self.assertIn("MODEL-ONLY 123/e.json", self.output)
        self.assertIn("MODEL-ONLY 123/g.json", self.output)
        self.assertIn("MODEL-ONLY 123/h.json", self.output)
        self.assertIn("MODEL-ONLY 123/i.json", self.output)
        self.assertIn("0 files OK", self.output)
        self.assertIn("11 files in CSV only", self.output)
        self.assertIn("8 files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_subpath_in_models(self):
        self.sl100.files.all().delete()
        self.d100.delete()

        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/* (6 files)", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("5 files OK, 6 files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_subpath_in_csv(self):
        # subpath 200
        self.fs200 = FileStorage.objects.create()
        self.d200 = Data.objects.create(
            process=self.proc,
            contributor_id=1,
            location=self.fs200,
        )
        self.sl200 = StorageLocation.objects.create(
            file_storage=self.fs200,
            url="200",
        )

        self.make_file("x.json", self.sl200)
        self.make_file("y.json", self.sl200)
        self.make_file("z.json", self.sl200)

        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 200/* (3 files)", self.output)
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertIn("11 files OK, 3 files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_empty_models(self):
        self.sl100.files.all().delete()
        self.d100.delete()
        self.sl123.files.all().delete()
        self.d123.delete()

        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/* (6 files)", self.output)
        self.assertIn("CSV-ONLY 123/* (5 files)", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("0 files OK, 11 files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_empty_csv(self):
        call_command("compare_models_and_csv", os.devnull, stdout=self.out)
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/* (6 files)", self.output)
        self.assertIn("MODEL-ONLY 123/* (5 files)", self.output)
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertIn("0 files OK, 11 files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_first_file_in_models(self):
        ReferencedPath.objects.get(path="names/000.json").delete()
        ReferencedPath.objects.get(path="names/001.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/names/000.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("9 files OK, 2 files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_last_file_in_models(self):
        ReferencedPath.objects.get(path="names/004.json").delete()
        ReferencedPath.objects.get(path="names/005.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/names/004.json", self.output)
        self.assertIn("CSV-ONLY 100/names/005.json", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("9 files OK, 2 files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_first_file_in_csv(self):
        self.make_file("a.json", self.sl123)
        self.make_file("b.json", self.sl123)
        self.make_file("c.json", self.sl123)
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 123/a.json", self.output)
        self.assertIn("MODEL-ONLY 123/b.json", self.output)
        self.assertIn("MODEL-ONLY 123/c.json", self.output)
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertIn("11 files OK, 3 files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_last_file_in_csv(self):
        self.make_file("names/006.json", self.sl100)
        self.make_file("m.json", self.sl123)
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/names/006.json", self.output)
        self.assertIn("MODEL-ONLY 123/m.json", self.output)
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertIn("11 files OK, 2 files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_middle_file_in_models(self):
        ReferencedPath.objects.get(path="names/002.json").delete()
        ReferencedPath.objects.get(path="names/003.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertNotIn("MODEL-ONLY", self.output)
        self.assertIn("9 files OK, 2 files in CSV only", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_missing_middle_file_in_csv(self):
        self.make_file("e.json", self.sl123)
        self.make_file("g.json", self.sl123)
        self.make_file("h.json", self.sl123)
        self.make_file("i.json", self.sl123)
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 123/e.json", self.output)
        self.assertIn("MODEL-ONLY 123/g.json", self.output)
        self.assertIn("MODEL-ONLY 123/h.json", self.output)
        self.assertIn("MODEL-ONLY 123/i.json", self.output)
        self.assertNotIn("CSV-ONLY", self.output)
        self.assertIn("11 files OK, 4 files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_partial_mismatch(self):
        # Situation like this:
        # CSV | models
        #  0      0  (start with a match)
        #  1         (models leading)
        #  2         (no common entry for them to meet)
        #         25 (CSV starts to lead)
        #  3
        #         35 (mismatching entries)
        #  4
        #  5      5  (finish with another match)
        self.make_file("names/0025.json", self.sl100)
        self.make_file("names/0035.json", self.sl100)
        ReferencedPath.objects.get(path="names/001.json").delete()
        ReferencedPath.objects.get(path="names/002.json").delete()
        ReferencedPath.objects.get(path="names/003.json").delete()
        ReferencedPath.objects.get(path="names/004.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/names/0025.json", self.output)
        self.assertIn("MODEL-ONLY 100/names/0035.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertIn("CSV-ONLY 100/names/004.json", self.output)
        self.assertIn("7 files OK", self.output)
        self.assertIn("2 files in models only", self.output)
        self.assertIn("4 files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_partial_mismatch_no_common_finish(self):
        # same as test_partial_mismatch, but without the common finish
        self.make_file("names/0025.json", self.sl100)
        self.make_file("names/0035.json", self.sl100)
        ReferencedPath.objects.get(path="names/001.json").delete()
        ReferencedPath.objects.get(path="names/002.json").delete()
        ReferencedPath.objects.get(path="names/003.json").delete()
        ReferencedPath.objects.get(path="names/004.json").delete()
        ReferencedPath.objects.get(path="names/005.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/names/0025.json", self.output)
        self.assertIn("MODEL-ONLY 100/names/0035.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertIn("CSV-ONLY 100/names/004.json", self.output)
        self.assertIn("CSV-ONLY 100/names/005.json", self.output)
        self.assertIn("6 files OK", self.output)
        self.assertIn("2 files in models only", self.output)
        self.assertIn("5 files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_partial_mismatch_no_common_start(self):
        # same as test_partial_mismatch, but without the common beginning
        self.make_file("names/0025.json", self.sl100)
        self.make_file("names/0035.json", self.sl100)
        ReferencedPath.objects.get(path="names/000.json").delete()
        ReferencedPath.objects.get(path="names/001.json").delete()
        ReferencedPath.objects.get(path="names/002.json").delete()
        ReferencedPath.objects.get(path="names/003.json").delete()
        ReferencedPath.objects.get(path="names/004.json").delete()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/names/0025.json", self.output)
        self.assertIn("MODEL-ONLY 100/names/0035.json", self.output)
        self.assertIn("CSV-ONLY 100/names/000.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertIn("CSV-ONLY 100/names/004.json", self.output)
        self.assertIn("6 files OK", self.output)
        self.assertIn("2 files in models only", self.output)
        self.assertIn("5 files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_common_finish(self):
        # completely mismatched at first, then come together
        ReferencedPath.objects.get(path="names/000.json").delete()
        ReferencedPath.objects.get(path="names/001.json").delete()
        ReferencedPath.objects.get(path="names/002.json").delete()
        ReferencedPath.objects.get(path="names/003.json").delete()
        self.make_file("names/0025.json", self.sl100)
        self.make_file("names/0035.json", self.sl100)
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        self.assertIn("MODEL-ONLY 100/names/0025.json", self.output)
        self.assertIn("MODEL-ONLY 100/names/0035.json", self.output)
        self.assertIn("CSV-ONLY 100/names/000.json", self.output)
        self.assertIn("CSV-ONLY 100/names/001.json", self.output)
        self.assertIn("CSV-ONLY 100/names/002.json", self.output)
        self.assertIn("CSV-ONLY 100/names/003.json", self.output)
        self.assertIn("7 files OK", self.output)
        self.assertIn("2 files in models only", self.output)
        self.assertIn("4 files in CSV only", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_hash_mismatch(self):
        rp003 = ReferencedPath.objects.get(path="names/003.json")
        rp003.awss3etag = "hashhash"
        rp003.save()
        rp005 = ReferencedPath.objects.get(path="names/005.json")
        rp005.awss3etag = ""
        rp005.save()
        rpd = ReferencedPath.objects.get(path="d.json")
        rpd.awss3etag = "not hash"
        rpd.save()
        call_command(
            "compare_models_and_csv",
            self.csv_filename,
            stdout=self.out,
        )
        self.output = self.out.getvalue()
        longstring = "HASH 100/names/003.json hashhash != hashhashhash"
        self.assertIn(longstring, self.output)  # longstr; 79ch line len limit
        self.assertIn("HASH 100/names/005.json  != hashhashhash", self.output)
        self.assertIn("HASH 123/d.json not hash != hashhashhash", self.output)
        self.assertIn("8 files OK", self.output)
        self.assertNotIn("files in models only", self.output)
        self.assertNotIn("files in CSV only", self.output)
        self.assertIn("3 files do not match the hash", self.output)
        self.assertNotIn("Numbers don't add up.", self.output)

    def test_parse_line(self):
        line = (
            '"bucket","subpath/filename.extension",'
            '"filesize","hashhashhash","STANDARD",""'
        )
        line = parse_line(line)
        self.assertEqual(line.filename, "filename.extension")
        self.assertEqual(line.hash, "hashhashhash")
        self.assertEqual(line.subpath, "subpath")

    def test_parse_line_no_subpath(self):
        line = '"bucket","README.md","filesize","hashhashhash","STANDARD",""'
        line = parse_line(line)
        self.assertEqual(line.filename, "README.md")
        self.assertEqual(line.hash, "hashhashhash")
        self.assertEqual(line.subpath, "")

    def test_parse_line_empty_key(self):
        line = '"bucket","","filesize","hashhashhash","STANDARD",""'
        line = parse_line(line)
        self.assertEqual(line.filename, "")
        self.assertEqual(line.hash, "hashhashhash")
        self.assertEqual(line.subpath, "")

    def test_map_subpath_locations(self):
        fi = FileIterator(self.csv_filename)
        received = map_subpath_locations(fi)
        expected = {
            "100": {"start": 0, "end": 582, "linecount": 6},
            "123": {"start": 582, "end": 1027, "linecount": 5},
        }
        self.assertEqual(received, expected)
        self.assertEqual(fi.length, 11)

    def test_map_subpath_locations_empty_csv(self):
        fi = FileIterator(os.devnull)
        received = map_subpath_locations(fi)
        expected = {}
        self.assertEqual(received, expected)
        self.assertEqual(fi.length, 0)

    def test_fileiterator_tell_seek(self):
        fi = FileIterator(self.csv_filename)
        fi.seek(42)
        self.assertEqual(fi.tell(), 42)

    def test_file_iterator_readline(self):
        fi = FileIterator(self.csv_filename)
        line = (
            '"342286153875-genialis-dev-storage",'
            '"100/names/000.json","filesize","hashhashhash","STANDARD",""\n'
        )
        self.assertEqual(fi.readline(), line)
        self.assertEqual(fi.last_position, 0)

    def test_file_iterator_has_next(self):
        fi = FileIterator(self.csv_filename)
        self.assertTrue(fi.has_next())
        fi.seek(fi.size)
        self.assertFalse(fi.has_next())

    def test_file_iterator_next(self):
        fi = FileIterator(self.csv_filename)
        self.assertEqual(fi.next(), ("names/000.json", "hashhashhash"))

    def test_file_iterator_next_at_eof(self):
        fi = FileIterator(self.csv_filename)
        fi.seek(fi.size)
        self.assertEqual(fi.next(), ("", ""))

    def test_file_iterator_restrict(self):
        fi = FileIterator(self.csv_filename)
        fi.restrict(0, 10)
        self.assertTrue(fi.has_next())
        fi.seek(10)
        self.assertFalse(fi.has_next())

    def test_file_iterator_seek_relative(self):
        fi = FileIterator(self.csv_filename)
        fi.restrict(20, 40)
        fi.seek_relative(10)
        self.assertEqual(fi.tell(), 30)

    def test_model_iterator_next(self):
        urls = self.d123.location.files
        urls = urls.exclude(Q(path__endswith="/"))  # exclude directories
        urls = ModelIterator(urls.order_by("path"))
        self.assertEqual(urls.next(), ("d.json", "hashhashhash"))
        self.assertEqual(urls.next(), ("f.json", "hashhashhash"))
        self.assertEqual(urls.next(), ("j.json", "hashhashhash"))
        self.assertEqual(urls.next(), ("k.json", "hashhashhash"))
        self.assertEqual(urls.next(), ("l.json", "hashhashhash"))
        self.assertEqual(urls.next(), ("", ""))
        self.assertEqual(urls.next(), ("", ""))

    def test_model_iterator_has_next(self):
        urls = self.d123.location.files
        urls = urls.exclude(Q(path__endswith="/"))  # exclude directories
        urls = ModelIterator(urls.order_by("path"))
        self.assertTrue(urls.has_next())
        self.assertEqual(urls.next(), ("d.json", "hashhashhash"))
        self.assertTrue(urls.has_next())
        self.assertEqual(urls.next(), ("f.json", "hashhashhash"))
        self.assertTrue(urls.has_next())
        self.assertEqual(urls.next(), ("j.json", "hashhashhash"))
        self.assertTrue(urls.has_next())
        self.assertEqual(urls.next(), ("k.json", "hashhashhash"))
        self.assertTrue(urls.has_next())
        self.assertEqual(urls.next(), ("l.json", "hashhashhash"))
        self.assertFalse(urls.has_next())
