from pathlib import Path

from sr._version import __version__


PACKAGE_DIR = Path(__file__).parent.resolve(strict=True)
HASH_FILE = PACKAGE_DIR / "hashes.json"
VERSION_FILE = PACKAGE_DIR / "_version.py"

SR_TABLES = PACKAGE_DIR / "tables"
CID_FILE = SR_TABLES / "_cid.py"
CONCEPTS_FILE = SR_TABLES / "_concepts.py"
SNOMED_FILE = SR_TABLES / "_snomed.py"
