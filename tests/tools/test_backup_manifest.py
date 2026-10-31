import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backup_manifest.py"
SPEC = importlib.util.spec_from_file_location("backup_manifest", SCRIPT)
backup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backup)


class BackupManifestTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "backup"
        self.root.mkdir()
        (self.root / "group_1").mkdir()
        self.data = self.root / "group_1" / "entries"
        self.data.write_bytes(b"committed\x00log")
        self.manifest = self.base / "manifest.json"

    def test_round_trip_and_no_overwrite(self):
        self.assertEqual(backup.create(self.root, self.manifest), 1)
        self.assertEqual(backup.verify(self.root, self.manifest), 1)
        with self.assertRaises(FileExistsError):
            backup.create(self.root, self.manifest)

    def test_changed_missing_and_extra_files(self):
        backup.create(self.root, self.manifest)
        self.data.write_bytes(b"damaged")
        with self.assertRaisesRegex(ValueError, "changed=1"):
            backup.verify(self.root, self.manifest)
        self.data.unlink()
        (self.root / "unexpected").write_bytes(b"extra")
        with self.assertRaisesRegex(ValueError, "missing=1, extra=1"):
            backup.verify(self.root, self.manifest)

    def test_rejects_links_and_manifest_inside_backup(self):
        with self.assertRaisesRegex(ValueError, "outside"):
            backup.create(self.root, self.root / "manifest.json")
        (self.root / "outside").symlink_to(self.base)
        with self.assertRaisesRegex(ValueError, "symbolic link"):
            backup.create(self.root, self.manifest)

    def test_malformed_manifest_and_untrusted_paths(self):
        self.manifest.write_text("not JSON", encoding="utf-8")
        with self.assertRaises(ValueError):
            backup.verify(self.root, self.manifest)
        self.manifest.write_text(
            json.dumps({"version": 1, "files": {"../outside": "0" * 64}}), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "missing=1"):
            backup.verify(self.root, self.manifest)

    def test_cli_exit_status(self):
        args = [sys.executable, str(SCRIPT)]
        created = subprocess.run(args + ["create", str(self.root), str(self.manifest)],
                                 capture_output=True)
        self.assertEqual(created.returncode, 0, created.stderr)
        verified = subprocess.run(args + ["verify", str(self.root), str(self.manifest)],
                                  capture_output=True)
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.data.write_bytes(b"damaged")
        rejected = subprocess.run(args + ["verify", str(self.root), str(self.manifest)],
                                  capture_output=True)
        self.assertEqual(rejected.returncode, 1)


if __name__ == "__main__":
    unittest.main()
