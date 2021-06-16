
from datetime import datetime
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Optional, List

from sr import (
    HASH_FILE,
    SNOMED_FILE,
    CID_FILE,
    CONCEPTS_FILE,
    VERSION_FILE,
    __version__,
)
from sr.process import process_source_data
from sr.utils import (
    download_cid_files,
    download_file,
    compare_checksums,
    calculate_checksums,
)


LOGGER = logging.getLogger(__name__)

CID_HOST = "medical.nema.org"
CID_PATH = "medical/dicom/resources/valuesets/fhir/json"

PART_16 = "http://dicom.nema.org/medical/dicom/current/output/chtml/part16"
TABLE_O1 = PART_16 + "/chapter_O.html#table_O-1"
TABLE_D1 = PART_16 + "/chapter_D.html#table_D-1"


def run(
    src: Optional[Path] = None,
    force_download: bool = False,
) -> bool:

    have_data = False

    if isinstance(src, Path) and src.exists():
        have_data = True
    elif src is None:
        t = TemporaryDirectory()
        src = Path(t.name)
        have_data = False
    else:
        raise ValueError(f"The data source path '{src}' does not exist")

    # 0. Download files (if necessary or forced)
    if not have_data or force_download:
        # Download all the data files:
        #   CID JSON files from ftp://medical.nema.org
        #   Tables O1 and D1 in Part 16 of the DICOM Standard
        start_time = time.time()

        download_cid_files((CID_HOST, CID_PATH), src)
        download_file(TABLE_D1, dst / "part16_d1.html")
        download_file(TABLE_O1, dst / "part16_o1.html")

        total_time = time.time() - start_time
        LOGGER.info(f"Source files downloaded in {total_time:.1f} s")

    cid_paths = list(src.glob('*.json'))
    table_paths = list(src.glob('part16_*.html'))
    paths = sorted(cid_paths + table_paths)

    # 1. Compare the data in `src` against the reference hashes
    if compare_checksums(paths, HASH_FILE):
        LOGGER.info("No change in source data found, exiting...")
        return False

    # 2. Source data has changed, regenerate the tables and update the package
    LOGGER.info("Source data has changed - updating package")

    table_o1 = src / "part16_o1.html"
    table_d1 = src / "part16_d1.html"

    # Rebuild the data tables
    tables = process_source_data(cid_paths, table_o1, table_d1)

    # Update the version file - fail early
    #write_version_file()

    # Recalculate the hashes
    #write_hash_file(paths)

    # write_snomed_file(tables[0])
    # write_cid_file(tables[0])
    # write_concepts_file(tables[0])

    return True


def write_hash_file(paths: List[Path]) -> None:
    """Update the hashes.json file with the hashes from `items`."""

    checksums = calculate_checksums(paths)

    with open(HASH_FILE, "w") as f:
        f.write("{\n")
        if checksums:
            for path, _hash in sorted(checksums[:-1], key=lambda x: x[0]):
                f.write(f"    \"{path.name}\": \"{_hash}\",\n")

            # The last line in the JSON dict can't have a trailing comma
            f.write(f"    \"{path.name}\": \"{_hash}\"\n")
        else:
            LOGGER.warning("No checksums available to write to 'hashes.json'")

        f.write("}")

    LOGGER.info(f"'hashes.json' written with {len(checksums)} entries")


def write_snomed_file(snomed_codes: List[str, str, str]) -> None:
    """"""

    with open(SNOMED_FILE, "w", encoding="utf8") as f:
        lines = [
            "# Dict with scheme designator keys; value format is:\n",
            "#   {concept_id1: snomed_id1, concept_id2: ...}\n",
            "# or\n",
            "#   {snomed_id1: concept_id1, snomed_id2: ...}\n",
            "\n",
        ]
        f.writelines(lines)

        f.write("mapping = {}\n")
        f.write("\nmapping['SCT'] = {{\n")
        for sct, srt, meaning in snomed_codes:
            f.write(f"    \"{sct}\": \"{srt}\",\n")

        f.write("}\n")
        f.write("\nmapping[\"SRT\"] = {{\n")

        for sct, srt, meaning in snomed_codes:
            f.write(f"     \"{srt}\": \"{sct}\",\n")

        f.write("}\n")


def write_cid_file(cid_lists, name_for_cid) -> None:
    with open(CID_FILE, 'w', encoding="utf8") as f:
        lines = [
            '# Dict with cid number as keys; value format is:\n',
            '#   {scheme designator: <list of keywords for current cid>\n',
            '#    scheme_designator: ...}\n',
            '\n',
        ]
        f.writelines(lines)
        f.write("name_for_cid = {}\n")
        f.write("cid_concepts = {}\n")
        for cid, value in cid_lists.items():
            f.write(f"\nname_for_cid[{cid}] = '{name_for_cid[cid]}'\n")
            f.write(f"cid_concepts[{cid}] = {{\n")
            for kk, vv in value:
                f.write(f"    \"{kk}\": [\n")
                for item in vv:
                    f.write(f"        \"{value}\",")
                f.write("    ],\n")

            f.write(f"}}")


def write_concepts_file(concepts) -> None:
    with open(CONCEPTS_FILE, 'w', encoding="utf8") as f:
        lines = [
            '# Dict with scheme designator keys; value format is:\n',
            '#   {keyword: {code1: (meaning, cid_list), code2: ...}\n',
            '#\n',
            '# Most keyword identifiers map to a single code, but not all\n',
            '\n',
        ]
        f.writelines(lines)
        f.write("concepts = {}\n")
        for scheme, value in concepts.items():
            f.write(f"\nconcepts['{scheme}'] = \\\n")
            pprint(value, f)


def write_version_file() -> None:
    new_version = datetime.now().strftime("%Y.%m.%d")

    if new_version == __version__:
        raise RuntimeError(
            "Error updating the package: no change in version number"
        )

    with open(VERSION_FILE, 'w') as f:
        f.write("\n")
        f.write(f"__version__: str = \"{new_version}\"\n")

    LOGGER.info(f"Package version updated to '{new_version}'")
