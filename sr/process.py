import json
import logging
from pathlib import Path
import re
import sys
import time
from typing import List, Callable, Tuple, Optional

from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

# The list of scheme designators is not complete.
# For full list see table 8-1 in part 3.16 chapter 8:
FHIR_LOOKUP = {
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


def process_source_data(
    cid_paths: List[Path],
    snomed_table: Path,
    dicom_table: Path,
) -> None:
    CID_REGEX = re.compile("^dicom-cid-([0-9]+)-[a-zA-Z]+")
    concepts = {}
    cid_lists = {}
    name_for_cid = {}

    for path in cid_paths:
        with open(path, "r") as f:
            data = json.loads(f.read())

            match = CID_REGEX.search(data["id"])
            if not match:
                continue

            # e.g. for 'dicom-cid-2-AnatomicModifier' -> cid = 2
            cid = int(match.group(1))
            name_for_cid[cid] = data["name"]

            cid_concepts = {}
            for group in data["compose"]["include"]:
                system = group["system"]
                try:
                    scheme_designator = FHIR_LOOKUP[system]
                except KeyError:
                    raise NotImplementedError(
                        f"The DICOM scheme designator for the '{system}' FHIR "
                        "system has not been specified"
                    )
                if scheme_designator not in concepts:
                    concepts[scheme_designator] = dict()

                for concept in group["concept"]:
                    name = keyword_from_meaning(concept["display"])
                    code = concept["code"].strip()
                    display = concept["display"].strip()

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
                                f"'{name}': '{display}' in "
                                f"cid_{cid}, previously '{prior[code][0]}' "
                                f"in cids {prior[code][1]}"
                            )

                    # Keep track of this cid referencing that name
                    if scheme_designator not in cid_concepts:
                        cid_concepts[scheme_designator] = []

                    if name in cid_concepts[scheme_designator]:
                        LOGGER.warning(
                            f"'{name}': '{concept['display']}' in "
                            f"cid_{cid} is duplicated!"
                        )

                    cid_concepts[scheme_designator].append(name)

            cid_lists[cid] = cid_concepts

    snomed, concepts = process_table_o1(concepts, snomed_table)
    dicom, concepts = process_table_d1(concepts, dicom_table)

    cid_lists = {k: v for k, v in sorted(cid_lists.items(), key=lambda x: x[0])}

    return snomed, concepts, cid_lists, name_for_cid


def process_table_o1(concepts, table: Path):
    LOGGER.info(f"Processing 'SCT' table from '{table.name}'")
    scheme = "SCT"

    with open(table, "rb") as f:
        doc = BeautifulSoup(f.read(), "html.parser")

    codes = []
    data = doc.find_all("table")[2]
    for row in data.tbody.find_all("tr"):
        [code, srt_code, meaning] = [
            cell.get_text().strip() for cell in row.find_all("td")
        ]
        name = keyword_from_meaning(meaning)
        if name not in concepts[scheme]:
            concepts[scheme][name] = {code: (meaning, [])}
        else:
            prior = concepts[scheme][name]
            if code not in prior:
                prior[code] = (meaning, [])

        codes.append([code, srt_code, meaning])

    return codes, concepts


def process_table_d1(concepts, table: Path):
    LOGGER.info(f"Processing 'DCM' table from '{table.name}'")
    scheme = "DCM"

    with open(table, "rb") as f:
        doc = BeautifulSoup(f.read(), "html.parser")

    codes = []
    data = doc.find_all("table")[2]
    for row in data.tbody.find_all("tr"):
        [code, meaning, definition, notes] = [
            cell.get_text().strip() for cell in row.find_all("td")
        ]
        name = keyword_from_meaning(meaning)
        if name not in concepts[scheme]:
            concepts[scheme][name] = {code: (meaning, [])}
        else:
            prior = concepts[scheme][name]
            if code not in prior:
                prior[code] = (meaning, [])

        codes.append([code, meaning, definition, notes])

    return codes, concepts


def keyword_from_meaning(name: str) -> str:
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

    return name


def camel_case(s: str) -> str:
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
        w.capitalize() if w != w.upper() and w not in leave_alone else w
        for w in re.split(r"\W", s, flags=re.UNICODE)
        if w.isalnum()
    )
