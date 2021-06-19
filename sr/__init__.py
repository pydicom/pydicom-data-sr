from pathlib import Path
from types import ModuleType
from typing import Tuple

from sr._version import __version__
from sr.tables import _cid, _concepts, _snomed


PACKAGE_DIR = Path(__file__).parent.resolve(strict=True)
HASH_FILE = PACKAGE_DIR / "hashes.json"
VERSION_FILE = PACKAGE_DIR / "_version.py"

SR_TABLES = PACKAGE_DIR / "tables"
CID_FILE = SR_TABLES / "_cid.py"
CONCEPTS_FILE = SR_TABLES / "_concepts.py"
SNOMED_FILE = SR_TABLES / "_snomed.py"


def foo() -> Tuple[ModuleType, ModuleType, ModuleType]:
    return _cid, _concepts, _snomed
