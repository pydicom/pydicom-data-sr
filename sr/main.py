import argparse
from datetime import datetime
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Optional, List, Tuple, Dict

from sr import (
    PACKAGE_DIR,
    SR_TABLES,
    HASH_FILE,
    SNOMED_FILE,
    CID_FILE,
    CONCEPTS_FILE,
    VERSION_FILE,
    __version__,
)
from sr.process import process_source_data, get_dicom_version
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
TABLE_O1 = PART_16 + "/chapter_O.html"
TABLE_D1 = PART_16 + "/chapter_D.html"

WORKERS = 64


def run(
    src: Optional[Path] = None,
    force_download: bool = False,
    force_regeneration: bool = False,
) -> bool:
    """Download and update the package data (if necessary).

    Parameters
    ----------
    src : pathlib.Path, optional
        The directory where the source data will be or is contained.
    force_download : bool, optional
        If ``True`` then force downloading the source data (default ``False``).
    force_regeneration : bool, optional
        If ``True`` then force regenerating the package data (default ``False).

    Returns
    -------
    bool
        ``True`` if the package has been updated, ``False`` otherwise.
    """
    have_data = False

    if isinstance(src, Path) and src.exists():
        have_data = bool(list(src.glob('*')))
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

        download_cid_files((CID_HOST, CID_PATH), src, WORKERS)
        download_file(TABLE_D1, src / "part16_d1.html")
        download_file(TABLE_O1, src / "part16_o1.html")

        total_time = time.time() - start_time
        LOGGER.info(f"Source files downloaded in {total_time:.1f} s")

    cid_paths = list(src.glob("*.json"))
    table_paths = list(src.glob("part16_*.html"))
    paths = sorted(cid_paths + table_paths)

    # 1. Compare the data in `src` against the reference hashes
    if compare_checksums(paths, HASH_FILE) and not force_regeneration:
        LOGGER.info("No change in source data found, exiting...")
        return False

    # 2. Source data has changed, regenerate the tables and update the package
    LOGGER.info("Source data has changed - updating package")

    table_o1 = src / "part16_o1.html"
    table_d1 = src / "part16_d1.html"

    dicom_version = get_dicom_version(table_d1)

    # Rebuild the data tables
    snomed, concepts, cid_lists, name_for_cid = process_source_data(
        cid_paths, table_o1, table_d1
    )

    # Update the version file
    write_version_file(dicom_version)

    # Recalculate the hashes
    write_hash_file(paths)

    # Write out the data tables
    write_snomed_file(snomed)
    write_cid_file(cid_lists, name_for_cid)
    write_concept_files(concepts)

    LOGGER.info("Package updates complete")

    return True


def write_hash_file(paths: List[Path]) -> None:
    """Update the hashes.json file with the hashes from `items`."""

    checksums = sorted(list(calculate_checksums(paths)), key=lambda x: x[0])

    indent = " " * 4
    with open(HASH_FILE, "w") as f:
        f.write("{\n")
        if checksums:
            for path, _hash in checksums[:-1]:
                f.write(f'{indent}"{path.name}": "{_hash}",\n')

            # The last line in the JSON dict can't have a trailing comma
            path, _hash = checksums[-1]
            f.write(f'{indent}"{path.name}": "{_hash}"\n')
        else:
            LOGGER.warning("No checksums available to write to 'hashes.json'")

        f.write("}")

    LOGGER.info(f"'hashes.json' written with {len(checksums)} entries")


def write_snomed_file(codes: List[Tuple[str, str, str]]) -> None:
    """Write the snomed data to file.

    Parameters
    ----------
    codes : list of Tuple[str, str, str]
        A list of (Concept ID (SCT), SNOMED ID (SRT0), Snomed Fully Specified
        Name).
    """

    LOGGER.info(f"Writing data to '{SNOMED_FILE}'")
    timestamp = datetime.now().strftime("%Y%m%d %H:%M%s")

    with open(SNOMED_FILE, "w", encoding="utf8") as f:
        f.writelines(
            [
                f"# Auto-generated on {timestamp}.\n",
                "# -*- coding: utf-8 -*-\n",
                "\n",
                "# Dict with scheme designator keys; format is:\n",
                "# mapping = {\n",
                "#   'SCT': {concept_id1: snomed_id1, concept_id2: ...},\n",
                "#   'SRT': {snomed_id1: concept_id1, snomed_id2: ...},\n",
                "# }\n",
                "\n",
            ]
        )

        f.write("mapping = {}\n")
        # Write the SCT to SRT mappings
        f.write("\nmapping['SCT'] = {\n")
        for sct, srt, meaning in sorted(codes, key=lambda x: x[0]):
            f.write(f'    "{sct}": "{srt}",\n')

        f.write("}\n")

        # Write the SRT to SCT mappings
        f.write('\nmapping["SRT"] = {\n')
        for sct, srt, meaning in sorted(codes, key=lambda x: x[1]):
            f.write(f'    "{srt}": "{sct}",\n')

        f.write("}\n")


def write_cid_file(
    cid_lists: Dict[int, Dict[str, List[str]]],
    name_for_cid: Dict[int, str],
) -> None:
    """Write the CID data to file.

    Parameters
    ----------
    cid_lists : dict
        A dict of {concept_id int: {scheme str: [str, str, ...]}}, where
        concept_id is the CID, scheme is the coding scheme designator and the
        list of str is a list of code meanings.
    name_for_cid : Dict[int, str]
        A dict linking the concept ID to the concept name, such as CID 2 ->
        "Anatomic Modifier".
    """

    LOGGER.info(f"Writing data to '{CID_FILE}'")
    timestamp = datetime.now().strftime("%Y%m%d %H:%M%s")

    top_indent = " " * 4
    bottom_indent = " " * 8
    with open(CID_FILE, "w", encoding="utf8") as f:
        f.writelines(
            [
                f"# Auto-generated on {timestamp}.\n",
                "# -*- coding: utf-8 -*-\n",
                "\n",
                "# Dict with cid number as keys; value format is:\n",
                "#   {scheme designator: <list of keywords for current cid>\n",
                "#    scheme_designator: ...}\n",
                "\n",
            ]
        )
        f.write("name_for_cid = {}\n")
        f.write("cid_concepts = {}\n")
        for cid, value in cid_lists.items():
            # cid: int
            # value: Dict[str, List[str]]
            # Write as:
            # name_for_id[cid] = <CID name>
            # cid_concepts[cid]: {
            #     <value>,
            # }
            f.write(f"\nname_for_cid[{cid}] = '{name_for_cid[cid]}'\n")
            f.write(f"cid_concepts[{cid}] = {{\n")
            for scheme, items in value.items():
                # scheme: str
                # items: List[str]
                # Write as:
                #     'scheme': [
                #         <items>,
                #     ]
                f.write(f'{top_indent}"{scheme}": [\n')
                for item in sorted(list(set(items))):  # ensure meanings are unique
                    f.write(f'{bottom_indent}"{item}",\n')
                f.write(f"{top_indent}],\n")

            f.write("}\n")


def write_concept_files(concepts) -> None:
    """Write the CID concepts to file.

    Parameters
    ----------
    concept : dict
        A dict containing the CID concepts.
    """

    LOGGER.info(f"Writing concept data files...")

    timestamp = datetime.now().strftime("%Y%m%d %H:%M%s")
    header = [
        f"# Auto-generated on {timestamp}.\n",
        "# -*- coding: utf-8 -*-\n",
        "\n",
    ]

    imports = []
    top_indent = " " * 4
    middle_indent = " " * 8
    bottom_indent = " " * 12
    for scheme, top_value in concepts.items():
        module = f"_concepts_{scheme}"
        variable = f"concepts_{scheme}"
        imports.append((scheme, module, variable))

        path = (SR_TABLES / module).with_suffix(".py")
        with open(path, "w", encoding="utf8") as f:
            f.writelines(header)
            # scheme: str
            # top_value: Dict[str, Dict[str, Tuple[str, List[int]]]]
            # Write as:
            # concepts_scheme: {
            #     <top_value>,
            # }
            f.write(f"{variable} = {{\n")
            for name, middle_value in top_value.items:
                # name: str
                # middle_value: Dict[str, Tuple[str, List[int]]]
                # Write as:
                #     name: {
                #         <middle_value>,
                #     },
                f.write(f'{top_indent}"{name}": {{\n')
                for key, val in middle_value.items:
                    # key: str
                    # val: Tuple[str, List[int]]
                    # Write as:
                    #         key: (
                    #             str, List[int],
                    #         ),
                    f.write(f'{middle_indent}"{key}": (\n')
                    f.write(f'{bottom_indent}"{val[0]}", {val[1]},\n')
                    f.write(f"{middle_indent}),\n")

                f.write(f"{top_indent}}},\n")

            f.write("}\n")

    # Write the main concepts file
    imports = sorted(imports, key=lambda x: x[0])
    with open(CONCEPTS_FILE, "w", encoding="utf8") as f:
        f.writelines(header)
        for scheme, module, variable in imports:
            # from sr._cid_concepts_scheme import scheme_concepts
            f.write(f"from sr.tables.{module} import {variable}\n")

        f.write("\n\n")
        f.write("concepts = {\n")
        for scheme, _, variable in imports:
            f.write(f'    "{scheme}": {variable},\n')
        f.write("}\n")


def write_version_file(dicom_version: str) -> None:
    """Write a new _version.py file"""

    new_version = datetime.now().strftime("%Y.%m.%d")
    if new_version == __version__:
        raise RuntimeError("Error updating the package: no change in version number")

    with open(VERSION_FILE, "w") as f:
        f.write("\n")
        f.write(f'__version__: str = "{new_version}"\n')
        f.write(f'__dicom_version__: str = "{dicom_version}"\n')

    LOGGER.info(f"Package version updated to '{new_version}'")


def _setup_argparser():
    """Setup the command line arguments"""
    # Description
    parser = argparse.ArgumentParser(description="", usage="")

    # General Options
    gen_opts = parser.add_argument_group("General Options")
    gen_opts.add_argument(
        "-d",
        "--dev",
        help="enable dev mode",
        action="store_true",
    )
    gen_opts.add_argument(
        "--force-download",
        help="force downloading the data tables",
        action="store_true",
        default=False,
    )
    gen_opts.add_argument(
        "--force-regeneration",
        help="force regenerating the data tables",
        action="store_true",
        default=False,
    )
    gen_opts.add_argument(
        "--clean",
        help="remove all data files",
        action="store_true",
        default=False,
    )

    return parser.parse_args()


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    args = _setup_argparser()

    src = None
    if args.dev:
        src = PACKAGE_DIR / "temp"

    if args.clean:
        pass

    run(src, args.force_download, args.force_regeneration)
