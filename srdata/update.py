
import argparse
from datetime import datetime
import ftplib
import hashlib
import json
import logging
from multiprocessing import Pool, cpu_count
from pathlib import Path
import re
import requests
import sys
from tempfile import TemporaryDirectory
import time
from typing import List, Callable, Tuple, Optional

from bs4 import BeautifulSoup


from srdata import HASH_FILE


LOGGER = logging.getLogger(__name__)


PART_16 = "http://dicom.nema.org/medical/dicom/current/output/chtml/part16"
TABLE_O1 = PART_16 + "/chapter_O.html#table_O-1"
TABLE_D1 = PART_16 + "/chapter_D.html#table_D-1"

# The list of scheme designators is not complete.
# For full list see table 8-1 in part 3.16 chapter 8:
FHIR_SYSTEM_TO_DICOM_SCHEME_DESIGNATOR = {
    'http://snomed.info/sct': 'SCT',
    'http://dicom.nema.org/resources/ontology/DCM': 'DCM',
    'http://loinc.org': 'LN',
    'http://www.radlex.org': 'RADLEX',
    'http://sig.biostr.washington.edu/projects/fm/AboutFM.html': 'FMA',
    'http://www.nlm.nih.gov/mesh/meshhome.html': 'MSH',
    'http://ncit.nci.nih.gov': 'NCIt',
    'http://unitsofmeasure.org': 'UCUM',
    'http://hl7.org/fhir/sid/ndc': 'NDC',
    'urn:iso:std:iso:11073:10101': 'MDC',
    'doi:10.1016/S0735-1097(99)00126-6': 'BARI',
    'http://www.nlm.nih.gov/research/umls': 'UMLS',
    'http://pubchem.ncbi.nlm.nih.gov': 'PUBCHEM_CID',
    'http://braininfo.rprc.washington.edu/aboutBrainInfo.aspx#NeuroNames': 'NEU',
    'http://www.itis.gov': 'ITIS_TSN',
    'http://arxiv.org/abs/1612.07003': 'IBSI',
    'http://www.nlm.nih.gov/research/umls/rxnorm': 'RXNORM',
}


def keyword_from_meaning(name):
    """Return a camel case valid python identifier"""
    # Try to adhere to keyword scheme in DICOM (CP850)

    # singular/plural alternative forms are made plural
    #     e.g., “Physician(s) of Record” becomes “PhysiciansOfRecord”
    name = name.replace("(s)", "s")

    # “Patient’s Name” -> “PatientName”
    # “Operators’ Name” -> “OperatorsName”
    name = name.replace("’s ", " ")
    name = name.replace("'s ", " ")
    name = name.replace("s’ ", "s ")
    name = name.replace("s' ", "s ")

    # Mathematical symbols
    name = name.replace("%", " Percent ")
    name = name.replace(">", " Greater Than ")
    name = name.replace("=", " Equals ")
    name = name.replace("<", " Lesser Than ")

    name = re.sub(r'([0-9]+)\.([0-9]+)', '\\1 Point \\2', name)
    name = re.sub(r'\s([0-9.]+)-([0-9.]+)\s', ' \\1 To \\2 ', name)

    name = re.sub(r'([0-9]+)day', '\\1 Day', name)
    name = re.sub(r'([0-9]+)y', '\\1 Years', name)

    # Remove category modifiers, such as "(specimen)", "(procedure)",
    # "(body structure)", etc.
    name = re.sub(r"^(.+) \([a-z ]+\)$", '\\1', name)

    name = camel_case(name.strip())

    # Python variables must not begin with a number.
    if re.match(r'[0-9]', name):
        name = "_" + name

    return name


def camel_case(s):
    #  "us"?-doesn"t seem to be there, probably need others
    leave_alone = (
        "mm",
        "cm",
        "km",
        "um",
        "ms",
        "ml",
        "mg",
        "kg",
    )

    return "".join(
        word.capitalize() if word != word.upper() and word not in leave_alone else word
        for word in re.split(r"\W", s, flags=re.UNICODE)
        if word.isalnum()
    )


def fetch_uri(address: Tuple[Path, str, Path]) -> Path:
    """Download a file.

    Parameters
    ----------
    address : Tuple[Path, str, Path]
        The (destination directory, host, URI) of the file to download.

    Returns
    -------
    pathlib.Path
        The path to the downloaded file.
    """
    dst, host, uri = address
    filename = dst / uri.name

    ftp = ftplib.FTP(host, timeout=60)
    ftp.login("anonymous")
    with open(filename, "wb") as f:
        ftp.retrbinary(f"RETR {uri}", f.write)

    return filename


def download_fhir_value_sets(dst) -> List[Path]:
    """Download CID files from the DICOM FTP server.

    Parameters
    ----------
    dst : pathlib.Path
        The destination directory for the downloaded files.

    Returns
    -------
    list of pathlib.Path
        The paths to the downloaded files.
    """
    host = "medical.nema.org"

    LOGGER.info(f"Fetching list of CID files from '{host}'")
    LOGGER.info("Logging into FTP server as user 'anonymous'")
    ftp = ftplib.FTP(host, timeout=60)
    ftp.login("anonymous")

    path = "medical/dicom/resources/valuesets/fhir/json"
    uris = ftp.nlst(path)
    uris = [(dst, host, Path(uri)) for uri in uris]

    LOGGER.info(f"Downloading {len(uris)} *.json CID files from '{path}'...")

    return Pool(cpu_count()).map(fetch_uri, uris)


def download_and_compare(
    dst: Path, download: bool = False, checksum: bool = False
) -> bool:
    """

    Parameters
    ----------
    dst : pathlib.Path
        The destination where the downloaded files will be saved.
    download : bool, optional
        If ``True`` then download the DICOM CID *.json files.
    checksum : bool, optional
        If ``True`` then perform the checksum comparsion on the files in
        `dst` against those in 'hashes.json'.

    Returns
    -------
    bool
        ``True`` if the checksums haven't changed, ``False`` otherwise.
    """
    if download:
        # Download the *.json CID files
        start_time = time.time()
        download_fhir_value_sets(dst)
        LOGGER.info(f"Download finished in {time.time() - start_time:.1f} s")

    if checksum:
        # Perform the checksum comparison
        checksums = calculate_checksums(dst.glob("CID*.json"))
        return compare_checksums(checksums)

    return True


def compare_checksums(checksums: List[Tuple[Path, str]]) -> bool:
    """Return ``False`` if the checksums don"t match the reference."""
    LOGGER.info(f"Performing checksum comparsion on downloaded files")

    with open(HASH_FILE, "r") as f:
        reference = json.loads(f.read())

    for path, _hash in checksums:
        if path.name not in reference:
            return False

        if reference[path.name] != _hash:
            return False

    return True


def hash_func(path: Path) -> str:
    """Return a hash for the file at `path`."""
    return path, hashlib.md5(open(path, "rb").read()).hexdigest()


def calculate_checksums(paths: List[Path]) -> List[Tuple[Path, str]]:
    """Return calculated checksums for `paths`.

    Parameters
    ----------
    paths : list of pathlib.Path
        A list of paths to calculate checksums for.

    Returns
    -------
    list of Tuple[Path, str]
        A list of (Path, hash str).
    """
    return Pool(cpu_count()).map(hash_func, paths)


def update_package(dst: Path) -> None:
    # Checksums have changed or new files found -> update hashes
    update_hashes(dst)

    # Regenerate the data tables
    update_tables(dst)

    # Bump version
    update_version()


def build_snomed(concepts) -> None:
    pass


def generate_concepts(dst: Path):
    CID_REGEX = re.compile('^dicom-cid-([0-9]+)-[a-zA-Z]+')

    concepts = {}
    cid_lists = {}
    name_for_cid = {}

    for path in dst.glob("CID*.json"):
        with open(path, 'r') as f:
            data = json.loads(f.read())

            match = CID_REGEX.search(data['id'])
            if not match:
                continue

            # e.g. for 'dicom-cid-2-AnatomicModifier' -> cid = 2
            cid = int(match.group(1))
            concept_name = data['name']

            cid_concepts = {}
            for group in data['compose']['include']:
                system = group['system']
                try:
                    scheme_designator = FHIR_SYSTEM_TO_DICOM_SCHEME_DESIGNATOR[system]
                except KeyError:
                    raise NotImplementedError(
                        f"The DICOM scheme designator for the '{system}' FHIR "
                        "system has not been specified"
                    )
                if scheme_designator not in concepts:
                    concepts[scheme_designator] = dict()

                for concept in group['concept']:
                    name = keyword_from_meaning(concept['display'])
                    code = concept['code'].strip()
                    display = concept['display'].strip()

                    # If new name under this scheme, start dict of
                    #   codes/cids that use that code
                    if name not in concepts[scheme_designator]:
                        concepts[scheme_designator][name] = {code: (display, [cid])}
                    else:
                        prior = concepts[scheme_designator][name]
                        if code in prior:
                            prior[code][1].append(cid)
                        else:
                            prior[code] = (display, [cid])

                        if prior[code][0].lower() != display.lower():
                            # Meanings can only be different by symbols, etc.
                            #    because converted to same keyword.
                            #    Nevertheless, print as info
                            LOGGER.info(
                                f"'{name}': Meaning '{display}' in "
                                f"cid_{cid}, previously '{prior[code][0]}' "
                                f"in cids {prior[code][1]}"
                            )

                    # Keep track of this cid referencing that name
                    if scheme_designator not in cid_concepts:
                        cid_concepts[scheme_designator] = []

                    if name in cid_concepts[scheme_designator]:
                        LOGGER.warning(
                            f"'{name}': Meaning '{concept['display']}' in "
                            f"cid_{cid} is duplicated!"
                        )

                    cid_concepts[scheme_designator].append(name)

            cid_lists[cid] = cid_concepts


def update_hashes(dst: Path) -> None:
    """Update the hashes.json file with the hashes from `items`."""

    checksums = calculate_checksums(dst.glob("CID*.json"))

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

    LOGGER.info(f"  hashes.json updated with {len(checksums)} entries")


def update_tables(dst: Path) -> None:
    LOGGER.info("  Updating data tables...")

    start_time = time.time()

    concepts = generate_concepts(dst)
    snomed = build_snomed(concepts)


    LOGGER.info(f"  Data tables updated in {time.time() - start_time:.2f} s")


def update_version() -> None:
    timestamp = datetime.now().strftime("%Y.%m.%d")

    with open("_version.py", 'w') as f:
        f.write("\n")
        f.write(f"__version__: str = \"{timestamp}\"\n")

    LOGGER.info(f"  Package version updated to '{timestamp}'")


def write_concepts(concepts, cid_concepts, cid_lists, name_for_cid) -> None:
    """"""
    lines = [
        f"# Auto-generated by {'foo'}.\n",
        "# -*- coding: utf-8 -*-\n",
        "\n",
        "# Dict with scheme designator keys; value format is:\n",
        "#   {keyword: {code1: (meaning, cid_list}, code2:...}\n",
        "#\n",
        "# Most keyword identifiers map to a single code, but not all\n",
        "\n",
    ]

    TABLE_FILE = DATA_DIR / "_concepts_dict.py"
    with open(TABLE_FILE, "w", encoding="UTF8") as f:
        f.writelines(lines)
        f.write("concepts = {}\n")
        for scheme, value in concepts.items():
            f.write("\nconcepts['{}'] = \\\n".format(scheme))

    lines = DOC_LINES + [
        "# Dict with cid number as keys; value format is:\n",
        "#   {scheme designator: <list of keywords for current cid>\n",
        "#    scheme_designator: ...}\n",
        "\n",
    ]

    TABLE_FILE = DATA_DIR / "_cid_dict.py"
    with open(TABLE_FILE, "w", encoding="utf8") as f:
        f.writelines(lines)
        f.write("name_for_cid = {}\n")
        f.write("cid_concepts = {}\n")

        for cid, value in cid_lists.items():
            f.write("\nname_for_cid[{}] = '{}'\n".format(cid, name_for_cid[cid]))
            f.write("cid_concepts[{}] = \\\n".format(cid))


def write_snomed_mapping(snomed_codes) -> None:
    """

    _snomed_dict.py

    """
    TABLE_FILE = DATA_DIR / "_snomed_dict.py"
    with open(TABLE_FILE, "w", encoding="utf8") as f:
        lines = DOC_LINES + [
            "# Dict with scheme designator keys; value format is:\n",
            "#   {concept_id1: snomed_id1, concept_id2: ...}\n",
            "# or\n",
            "#   {snomed_id1: concept_id1, snomed_id2: ...}\n",
            "\n",
        ]
        f.writelines(lines)
        f.write("mapping = {}\n")  # start with empty dict
        f.write("\nmapping['SCT'] = {{\n")
        for sct, srt, meaning in snomed_codes:
            f.write("    \"{}\": \"{}\",\n".format(sct, srt))

        f.write("}\n")
        f.write("\nmapping[\"{}\"] = {{\n".format("SRT"))

        for sct, srt, meaning in snomed_codes:
            f.write("     \"{}\": \"{}\",\n".format(srt, sct))

        f.write("}")


def get_table_o1():
    logger.info('process Table O1')

    root = BeautifulSoup(_download_html(TABLE_O1))
    namespaces = {'w3': root.tag.split('}')[0].strip('{')}
    body = root.find('w3:body', namespaces=namespaces)
    table = body.findall('.//w3:tbody', namespaces=namespaces)[0]
    rows = table.findall('./w3:tr', namespaces=namespaces)
    data = []
    for row in rows:
        data.append((
            _get_text(row[0].findall('.//w3:p', namespaces=namespaces)[-1]),
            _get_text(row[1].findall('.//w3:p', namespaces=namespaces)[0]),
            _get_text(row[2].findall('.//w3:p', namespaces=namespaces)[0]),
        ))

    return data


def get_table_d1():
    logger.info('process Table D1')

    root = _parse_html(_download_html(TABLE_D1))
    namespaces = {'w3': root.tag.split('}')[0].strip('{')}
    body = root.find('w3:body', namespaces=namespaces)
    table = body.findall('.//w3:tbody', namespaces=namespaces)[0]
    rows = table.findall('./w3:tr', namespaces=namespaces)
    return [
        (
             _get_text(row[0].findall('.//w3:p', namespaces=namespaces)[0]),
             _get_text(row[1].findall('.//w3:p', namespaces=namespaces)[0])
        )
        for row in rows
    ]


def _setup_argparser():
    """Setup the command line arguments"""
    # Description
    parser = argparse.ArgumentParser(
        description="",
        usage=""
    )

    # General Options
    gen_opts = parser.add_argument_group('General Options')
    gen_opts.add_argument(
        "-d", "--dev",
        help="enable dev mode",
        action="store_true",
    )

    # Development Options
    dev_opts = parser.add_argument_group('Development Options')
    dev_opts.add_argument(
        "--download",
        help="download the CID files",
        action="store_true",
        default=False,
    )
    dev_opts.add_argument(
        "--checksum",
        help="perform the checksum comparison",
        action="store_true",
        default=False,
    )
    dev_opts.add_argument(
        "--force-update",
        help="force regenerating the data tables",
        action="store_true",
        default=False,
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    cli_args = _setup_argparser()

    if cli_args.dev:
        LOGGER.debug("Running in development mode")
        LOGGER.debug(cli_args)

        dst = Path(__file__).parent.resolve() / "temp"
        dst.mkdir(exist_ok=True)

        result = download_and_compare(
            dst, cli_args.download, cli_args.checksum
        )

        if not result or cli_args.force_update:
            LOGGER.info("CID checksum mismatch - updating package!")
            update_package(dst)
        else:
            # No change necessary
            LOGGER.info("No changes required - going back to sleep")

        sys.exit()

    with TemporaryDirectory() as dirname:
        dst = Path(dirname)
        LOGGER.info(f"Downloading to {dst}")
        result = download_and_compare(dst, True, True)
        if not result:
            # Checksums have changed or new files found -> update package
            LOGGER.info("CID checksum mismatch - updating package!")
            update_package(dst)
        else:
            # No change necessary
            LOGGER.info("CID checksums match 'hashes.json'")
            LOGGER.info("No changes required - going back to sleep")
