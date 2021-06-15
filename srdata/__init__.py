from pathlib import Path

from ._version import __version__


PACKAGE_DIR = Path(__file__).parent.resolve(strict=True)
HASH_FILE = PACKAGE_DIR / "hashes.json"
