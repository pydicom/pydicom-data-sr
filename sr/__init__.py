from pathlib import Path

from sr._version import __version__


PACKAGE_DIR = Path(__file__).parent.resolve(strict=True)
HASH_FILE = PACKAGE_DIR / "hashes.json"
VERSION_FILE = PACKAGE_DIR / "_version.py"
CID_FILE = PACKAGE_DIR / "_cid.py"
CONCEPTS_FILE = PACKAGE_DIR / "_concepts.py"
SNOMED_FILE = PACKAGE_DIR / "_snomed.py"
