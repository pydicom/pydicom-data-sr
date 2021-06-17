from concurrent.futures import ThreadPoolExecutor
import ftplib
import hashlib
import json
import logging
from pathlib import Path
from typing import Tuple, List, Optional, Iterator

import requests


LOGGER = logging.getLogger(__name__)


def _fetch_uri(address: Tuple[Path, str, Path], timeout: int = 150) -> Path:
    """Download a file from an FTP server.

    Parameters
    ----------
    address : Tuple[Path, str, Path]
        The (destination directory, host, URI) of the file to download.
    timeout : int, optional
        The connection timeout to use, default 60 s.

    Returns
    -------
    pathlib.Path
        The path where the downloaded file was written.
    """
    dst, host, uri = address
    filename = dst / uri.name

    ftp = ftplib.FTP(host, timeout=timeout)
    ftp.login("anonymous")
    with open(filename, "wb") as f:
        ftp.retrbinary(f"RETR {uri}", f.write)

    return filename


def download_cid_files(
    address: Tuple[str, str], dst: Path, workers: int = 64
) -> List[Path]:
    """Download CID files from the DICOM FTP server.

    Parameters
    ----------
    address : Tuple[str, str]
        The (host, path) to the root directory where the CID are located.
    dst : pathlib.Path
        The destination directory for the downloaded files.
    workers : int, optional
        The number of workers to use when downloading the files.

    Returns
    -------
    list of pathlib.Path
        The paths to the downloaded files.
    """
    host, path = address
    user = "anonymous"

    LOGGER.info(f"Fetching list of CID files from '{host}'")
    LOGGER.info(f"Logging into FTP server as user '{user}'")
    ftp = ftplib.FTP(host, timeout=60)
    ftp.login(user)

    uris = ftp.nlst(path)
    addresses = [(dst, host, Path(uri)) for uri in uris]

    LOGGER.info(f"Downloading {len(uris)} *.json CID files from '{path}'...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        result = pool.map(_fetch_uri, addresses)

    # Check we have downloaded all the files
    if len(uris) != len(list(dst.glob("*.json"))):
        raise RuntimeError("The download of the CID files was not completed")

    return list(result)


def download_file(url: str, dst: Path) -> None:
    """Use requests to download the data at `url` and write it to `dst`.

    Parameters
    ----------
    url : str
        The URL of the data to be downloaded.
    dst : pathlib.Path
        The path where the data should be written.
    """
    LOGGER.info(f"Downloading '{url}'")
    r = requests.get(url)
    if r.status_code != 200 or not r.content:
        raise RuntimeError(
            f"An error occurred downloading from '{url}': {r.status_code}"
        )

    with open(dst, "wb") as f:
        f.write(r.content)


def _hash_func(path: Path) -> Tuple[Path, str]:
    """Return a hash for the file at `path`."""
    return path, hashlib.md5(open(path, "rb").read()).hexdigest()


def calculate_checksums(paths: List[Path]) -> Iterator[Tuple[Path, str]]:
    """Yield calculated checksums for `paths`.

    Parameters
    ----------
    paths : list of pathlib.Path
        A list of paths to calculate checksums for.

    Yields
    -------
    Tuple[Path, str]
        The (Path, hash str).
    """
    with ThreadPoolExecutor(max_workers=32) as pool:
        return pool.map(_hash_func, paths)


def compare_checksums(paths: List[Path], hash_file: Path) -> bool:
    """Return ``False`` if the checksums don"t match the reference.

    Parameters
    ----------
    paths : List[Path]
        A list of paths to the files that are being compared against the
        reference hashes.
    hash_file : Path
        The path to the JSON file containing the reference hashes.

    Returns
    -------
    bool
        ``True`` if the checksums match the reference, ``False`` otehrwise.
    """

    LOGGER.info(f"Performing checksum comparsion on source files")

    # True if the source files have changed
    has_changed = False

    with open(hash_file, "r") as f:
        reference = json.loads(f.read())

    checksums = calculate_checksums(paths)

    # Check for a change in source files
    current = {p.name for p, x in checksums}
    previous = set(reference.keys())

    # Added source files
    added = current - previous
    for name in added:
        LOGGER.info(f"Source file added: '{name}'")
        has_changed = True

    # Removed source files
    removed = previous - current
    for name in removed:
        # LOGGER.info(f"Source file removed: '{name}'")
        has_changed = True

    # Check for a change in checksums
    for path, _hash in checksums:
        if path.name not in reference:
            continue

        if reference[path.name] != _hash:
            LOGGER.info(f"Updated source file '{path.name}'")
            has_changed = True

    return not has_changed
