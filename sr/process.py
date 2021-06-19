import json
import logging
from pathlib import Path
import re
import sys
import time
from typing import List, Callable, Tuple, Optional, cast, Dict

from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

# The list of scheme designators is not complete.
# For full list see table 8-1 in part 3.16 chapter 8:
FHIR_LOOKUP: Dict[str, str] = {
    "http://snomed.info/sct": "SCT",
    "http://dicom.nema.org/resources/ontology/DCM": "DCM",
    "http://loinc.org": "LN",
    "http://www.radlex.org": "RADLEX",
    "http://sig.biostr.washington.edu/projects/fm/AboutFM.html": "FMA",
    "http://www.nlm.nih.gov/mesh/meshhome.html": "MSH",
    "http://ncit.nci.nih.gov": "NCIt",
    "http://unitsofmeasure.org": "UCUM",
    "http://hl7.org/fhir/sid/ndc": "NDC",
    "urn:iso:std:iso:11073:10101": "MDC",
    "doi:10.1016/S0735-1097(99)00126-6": "BARI",
    "http://www.nlm.nih.gov/research/umls": "UMLS",
    "http://pubchem.ncbi.nlm.nih.gov": "PUBCHEM_CID",
    "http://braininfo.rprc.washington.edu/aboutBrainInfo.aspx#NeuroNames": "NEU",
    "http://www.itis.gov": "ITIS_TSN",
    "http://arxiv.org/abs/1612.07003": "IBSI",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "RXNORM",
}


ConceptType = Dict[str, Dict[str, Dict[str, Tuple[str, List[int]]]]]
SnomedType = List[Tuple[str, str, str]]
CIDListType = Dict[int, Dict[str, List[str]]]
NameForCIDType = Dict[int, str]
ProcessReturnType = Tuple[
    SnomedType,
    ConceptType,
    CIDListType,
    NameForCIDType,
]


def process_source_data(
    cid_paths: List[Path],
    table_o1: Path,
    table_d1: Path,
) -> ProcessReturnType:
    """Process the downloaded souce data.

    Parameters
    ----------
    cid_paths : List[pathlib.Path]
        A list of paths to the CID *.json files.
    table_o1 : pathlib.Path
        The path to the Part 16, Table O-1 HTML file, contains the
        'SNOMED Concept ID to SNOMED ID Mapping' data.
    table_d1 : pathlib.Path
        The path to the Part 16, Table D-1 HTML file, contains the
        'DICOM Controlled Terminology Definitions' data.

    Returns
    -------
    (list, dict, dict, dict)
        The SNOMED mappings and concepts, CID list and CID to name
        dictionaries.
    """
    CID_REGEX = re.compile("^dicom-cid-([0-9]+)-[a-zA-Z]+")
    concepts: ConceptType = {}
    cid_lists: CIDListType = {}
    name_for_cid: NameForCIDType = {}

    for path in sorted(cid_paths, key=lambda x: int(x.stem.split("_")[-1])):
        with open(path, "r") as f:
            data = json.loads(f.read())

            # e.g. 'dicom-cid-2-AnatomicModifier'
            match = CID_REGEX.search(data["id"])
            if not match:
                continue

            cid = int(match.group(1))
            name_for_cid[cid] = cast(str, data["name"])

            cid_concepts: Dict[str, List[str]] = {}
            for group in data["compose"]["include"]:
                # e.g. "system":"http://dicom.nema.org/resources/ontology/DCM"
                system: str = group["system"]
                try:
                    scheme_designator = FHIR_LOOKUP[system]
                except KeyError:
                    raise NotImplementedError(
                        f"The DICOM scheme designator for the '{system}' FHIR "
                        "system has not been specified"
                    )

                if scheme_designator not in concepts:
                    concepts[scheme_designator] = dict()

                for concept in cast(List[Dict[str, str]], group["concept"]):
                    # "concept":[
                    #     {
                    #         "code":"14414005",
                    #         "display":"Peripheral"
                    #     },
                    #     {
                    #         "code":"57195005",
                    #         "display":"Basal"
                    #     },
                    # ]
                    # Not all display values are identical for the same code
                    # Mostly differences in capitalisation and punctuation
                    attr = identifier_from_meaning(
                        concept["display"], scheme_designator == "UCUM"
                    )
                    code = concept["code"].strip()
                    display = concept["display"].strip()

                    # If new name under this scheme, start dict of
                    #   codes/cids that use that code
                    if attr not in concepts[scheme_designator]:
                        concepts[scheme_designator][attr] = {code: (display, [cid])}
                    else:
                        prior = concepts[scheme_designator][attr]
                        if code in prior:
                            prior[code][1].append(cid)
                        else:
                            prior[code] = (display, [cid])

                    # Keep track of this cid referencing that name
                    if scheme_designator not in cid_concepts:
                        cid_concepts[scheme_designator] = []

                    if attr in cid_concepts[scheme_designator]:
                        LOGGER.error(
                            f"'{attr}': '{concept['display']}' in "
                            f"CID {cid} is duplicated!"
                        )

                    cid_concepts[scheme_designator].append(attr)

            cid_lists[cid] = cid_concepts

    snomed, concepts = process_table_o1(concepts, table_o1)
    dicom, concepts = process_table_d1(concepts, table_d1)

    cid_lists = {k: v for k, v in sorted(cid_lists.items(), key=lambda x: x[0])}

    return snomed, concepts, cid_lists, name_for_cid


def process_table_o1(
    concepts: ConceptType,
    table: Path,
) -> Tuple[List[Tuple[str, str, str]], ConceptType]:
    """Process the Part 16, O-1 table and add the data to `concepts`

    Parameters
    ----------
    concepts : dict
        A dict containing the processed CID files.
    table : pathlib.Path
        The path to the Part 16, Table O-1 HTML file, contains the
        'SNOMED Concept ID to SNOMED ID Mapping' data.

    Returns
    -------
    codes : List[Tuple[str, str, str]]
        A list of SNOMED codes as (Concept ID, SNOMED ID, SNOMED Fully
        Specified Name).
    concepts : dict
        A dict containing the processed CID files with added SNOMED concepts.
    """
    LOGGER.info(f"Processing 'SCT' table from '{table.name}'")
    scheme = "SCT"

    with open(table, "rb") as f:
        doc = BeautifulSoup(f.read(), "html.parser")

    # List[(Concept ID, SNOMED ID, SNOMED Fully Specified Name)]
    codes: List[Tuple[str, str, str]] = []
    data = doc.find_all("table")[2]
    for row in data.tbody.find_all("tr"):
        [code, srt_code, meaning] = [
            cell.get_text().strip() for cell in row.find_all("td")
        ]
        name = identifier_from_meaning(meaning)
        if name not in concepts[scheme]:
            concepts[scheme][name] = {code: (meaning, [])}
        else:
            prior = concepts[scheme][name]
            if code not in prior:
                prior[code] = (meaning, [])

        codes.append((code, srt_code, meaning))

    return codes, concepts


def process_table_d1(
    concepts: ConceptType,
    table: Path,
) -> Tuple[List[Tuple[str, str, str, str]], ConceptType]:
    """Process the Part 16 D-1 table and add the data to `concepts`.

    Parameters
    ----------
    concepts : dict
        A dict containing the processed CID files.
    table : pathlib.Path
        The path to the Part 16, Table D-1 HTML file, contains the
        'DICOM Controlled Terminology Definitions' data.

    Returns
    -------
    codes : List[Tuple[str, str, str, str]]
        A list of code values and meanings as (Code Value, Code Meaning,
        Definition, Notes).
    concepts : dict
        A dict containing the processed CID files with added DICOM concepts.
    """
    LOGGER.info(f"Processing 'DCM' table from '{table.name}'")
    scheme = "DCM"

    with open(table, "rb") as f:
        doc = BeautifulSoup(f.read(), "html.parser")

    # (Code Value, Code Meaning, Definition, Notes)
    codes: List[Tuple[str, str, str, str]] = []
    data = doc.find_all("table")[2]
    for row in data.tbody.find_all("tr"):
        [code, meaning, definition, notes] = [
            cell.get_text().strip() for cell in row.find_all("td")
        ]
        if code == "...":
            continue

        name = identifier_from_meaning(meaning)
        if name not in concepts[scheme]:
            concepts[scheme][name] = {code: (meaning, [])}
        else:
            prior = concepts[scheme][name]
            if code not in prior:
                prior[code] = (meaning, [])

        codes.append((code, meaning, definition, notes))

    return codes, concepts


def identifier_from_meaning(name: str, units: bool = False) -> str:
    """Return a camel case valid Python identifier.

    Parameters
    ----------
    name : str
        The meaning to convert to a valid Python identifier.
    units : bool, optional
        If ``True`` then treat `name` as containing scientific quantities or
        units.
    """
    # Try to adhere to keyword scheme in DICOM (CP850)
    original = name

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

    if units:
        name = name.replace("/", " Per ")
        name = name.replace("**2", " Squared ")

    # Custom
    name = name.replace("(symmetric placement)", "Symmetric Placement")
    name = name.replace("Beat detected (rejected)", "Beat Detected Rejected")
    name = name.replace("Beat detected (accepted)", "Beat Detected Accepted")
    name = name.replace(
        "atrial contraction (subsequent)", "atrial contraction subsequent"
    )
    name = name.replace("ratio (greater)", "ratio greater")
    name = name.replace("ratio (lesser)", "ratio lesser")
    name = name.replace("AP+45", "AP Plus 45")
    name = name.replace("AP-45", "AP Minus 45")
    name = name.replace("R2*", "R2 Star")
    name = name.replace("T2*", "T2 Star")
    name = name.replace("tau_m", "tau m")

    name = re.sub(r"([0-9]+)\.([0-9]+)", "\\1 Point \\2", name)
    name = re.sub(r"\s([0-9.]+)-([0-9.]+)\s", " \\1 To \\2 ", name)

    name = re.sub(r"([0-9]+)day", "\\1 Day", name)
    name = re.sub(r"([0-9]+)y", "\\1 Years", name)

    # Remove category modifiers, such as "(specimen)", "(procedure)",
    # "(body structure)", etc.
    name = re.sub(r"^(.+) \([a-z ]+\)$", "\\1", name)

    name = camel_case(name.strip())

    # Python variables must not begin with a number.
    if re.match(r"[0-9]", name):
        name = "_" + name

    if not name.isidentifier():
        raise ValueError(f"Invalid Python identifier: '{name}' from '{original}'")

    return name


def camel_case(s: str) -> str:
    """Return a camel case version of `s`."""
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
    words = re.split(r"\W", s, flags=re.UNICODE)
    if s == "tau_m":
        print(words)
        w = words[0]
        print(w.isalnum())

    words = s if not words else words
    words = [
        w.capitalize() if w != w.upper() and w not in leave_alone else w
        for w in words
        if w.isalnum()
    ]

    if s == "tau_m":
        print(words)

    return "".join(words)


def get_dicom_version(path: Path) -> str:
    """Return the DICOM version from a Part 16, Table O-1 HTML file"""
    with open(path, "rb") as f:
        doc = BeautifulSoup(f.read(), "html.parser")
        table = doc.find_all("table")[0]
        version: str = table.tr.th.get_text().strip().split()[2]

        LOGGER.debug(f"DICOM version is '{version}'")
        return version
