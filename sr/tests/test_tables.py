import pytest

from sr.tables import _cid, _concepts, _snomed


def test_cid_table():
    """Basic _cid table functionality tests."""
    assert _cid.name_for_cid[2] == "AnatomicModifier"
    assert "SCT" in _cid.cid_concepts[2]
    assert "DCM" in _cid.cid_concepts[2]


def test_concepts_table():
    """Basic _concepts table functionality tests."""
    assert isinstance(_concepts.concepts["SCT"], dict)


def test_snomed_table():
    """Basic _concepts table functionality tests."""
    assert isinstance(_snomed.mapping["SCT"], dict)
    assert isinstance(_snomed.mapping["SRT"], dict)
