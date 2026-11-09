#!/usr/bin/env python3
"""Create or verify a local integrity manifest for an immutable offline backup."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys


def inventory(directory):
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("backup root must be a real directory")
    root = directory.resolve()
    files = {}

    def walk_error(error):
        raise error

    for parent, directories, names in os.walk(root, onerror=walk_error, followlinks=False):
        for name in directories + names:
            path = Path(parent) / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise ValueError("backup contains a symbolic link")
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise ValueError("backup contains a non-regular file")
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            files[path.relative_to(root).as_posix()] = digest.hexdigest()
    return dict(sorted(files.items()))


def check_manifest_location(directory, manifest):
    manifest = Path(manifest)
    if manifest.is_symlink():
        raise ValueError("manifest must not be a symbolic link")
    if manifest.resolve().is_relative_to(Path(directory).resolve()):
        raise ValueError("manifest must be outside the backup directory")
    return manifest


def create(directory, manifest):
    manifest = check_manifest_location(directory, manifest)
    files = inventory(directory)
    payload = json.dumps({"version": 1, "files": files}, indent=2, sort_keys=True) + "\n"
    # Never replace an earlier manifest, including one for a different backup.
    with manifest.open("x", encoding="utf-8") as output:
        output.write(payload)
    return len(files)


def verify(directory, manifest):
    manifest = check_manifest_location(directory, manifest)
    with manifest.open(encoding="utf-8") as source:
        expected = json.load(source)
    if not isinstance(expected, dict) or expected.get("version") != 1:
        raise ValueError("unsupported manifest format")
    files = expected.get("files")
    if not isinstance(files, dict) or any(
        not isinstance(name, str)
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        for name, digest in files.items()
    ):
        raise ValueError("invalid manifest file inventory")
    # Compare inventories rather than opening paths supplied by the manifest.
    actual = inventory(directory)
    if actual != files:
        missing = len(files.keys() - actual.keys())
        extra = len(actual.keys() - files.keys())
        changed = sum(actual[name] != files[name] for name in actual.keys() & files.keys())
        raise ValueError(f"backup mismatch: missing={missing}, extra={extra}, changed={changed}")
    return len(actual)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "verify"))
    parser.add_argument("directory", help="immutable offline backup directory")
    parser.add_argument("manifest", help="manifest JSON outside the backup directory")
    args = parser.parse_args(argv)
    try:
        count = {"create": create, "verify": verify}[args.action](args.directory, args.manifest)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"{args.action}: {count} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
