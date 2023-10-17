"""Tests explorer"""
import sys
if not sys.platform.startswith('darwin'):
    import openvds
import logging
import json
from pathlib import Path
from uuid import UUID
import pytest
from xtgeo import RegularSurface
from context import (
    Explorer,
    Utils,
    Case,
    CaseCollection,
    SurfaceCollection,
)

from sumo.wrapper import SumoClient


TEST_DATA = Path("data")
logging.basicConfig(level="DEBUG")
LOGGER = logging.getLogger()


@pytest.fixture(name="the_logger")
def fixture_the_logger():
    """Defining a logger"""
    return LOGGER  # ut.init_logging("tests", "debug")


@pytest.fixture(name="case_name")
def fixture_case_name() -> str:
    """Returns case name"""
    return "drogon_design_small-2023-01-18"


@pytest.fixture(name="case_uuid")
def fixture_case_uuid() -> str:
    """Returns case uuid"""
    return "2c2f47cf-c7ab-4112-87f9-b4797ec51cb6"

@pytest.fixture(name="seismic_case_uuid")
def fixture_seismic_case_uuid() -> str:
    """Returns seismic case uuid"""
    return "c616019d-d344-4094-b2ee-dd4d6d336217"

@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns explorer"""
    return Explorer("dev", token=token)


@pytest.fixture(name="test_case")
def fixture_test_case(explorer: Explorer, case_name: str) -> Case:
    """Basis for test of method get_case_by_name for Explorer,
    but also other attributes
    """
    return explorer.cases.filter(name=case_name)[0]


@pytest.fixture(name="sumo_client")
def fixture_sumo_client(token: str):
    """Returns SumoClient for dev env"""
    return SumoClient("dev", token=token)


@pytest.fixture(name="utils")
def fixture_utils(sumo_client: SumoClient):
    """Returns utils object"""
    return Utils(sumo_client)


def write_json(result_file, results):
    """writes json files to disc
    args:
    result_file (str): path to file relative to TEST_DATA
    """
    result_file = TEST_DATA / result_file
    with open(result_file, "w", encoding="utf-8") as json_file:
        json.dump(results, json_file)


def read_json(input_file):
    """read json from disc
    args:
    result_file (str): path to file relative to TEST_DATA
    returns:
    content (dict): results from file
    """
    result_file = TEST_DATA / input_file
    with open(result_file, "r", encoding="utf-8") as json_file:
        contents = json.load(json_file)
    return contents


def assert_correct_uuid(uuid_to_check, version=4):
    """Checks if uuid has correct structure
    args:
    uuid_to_check (str): to be checked
    version (int): what version of uuid to compare to
    """
    # Concepts stolen from stackoverflow.com
    # questions/19989481/how-to-determine-if-a-string-is-a-valid-v4-uuid
    type_mess = f"{uuid_to_check} is not str ({type(uuid_to_check)}"
    assert isinstance(uuid_to_check, str), type_mess
    works_for_me = True
    try:
        UUID(uuid_to_check, version=version)
    except ValueError:
        works_for_me = False
    structure_mess = f"{uuid_to_check}, does not have correct structure"
    assert works_for_me, structure_mess


def assert_uuid_dict(uuid_dict):
    """Tests that dict has string keys, and valid uuid's as value
    args:
    uuid_dict (dict): dict to test
    """
    for key in uuid_dict:
        assert_mess = f"{key} is not of type str"
        assert isinstance(key, str), assert_mess
        assert_correct_uuid(uuid_dict[key])


def assert_dict_equality(results, correct):
    """Asserts whether two dictionaries are the same
    args:
    results (dict): the one to check
    correct (dict): the one to compare to
    """
    incorrect_mess = (
        f"the dictionary produced ({results}) is not equal to \n"
        + f" ({correct})"
    )
    assert results == correct, incorrect_mess


def test_get_cases(explorer: Explorer):
    """Test the get_cases method."""

    cases = explorer.cases
    assert isinstance(cases, CaseCollection)
    assert isinstance(cases[0], Case)


def test_get_cases_fields(explorer: Explorer):
    """Test CaseCollection.filter method with the field argument.

    Shall be case insensitive.
    """

    cases = explorer.cases.filter(field="DROGON")
    for case in cases:
        assert case.field.lower() == "drogon"


def test_get_cases_status(explorer: Explorer):
    """Test the CaseCollection.filter method with the status argument."""

    cases = explorer.cases.filter(status="keep")
    for case in cases:
        assert case.status == "keep"


def test_get_cases_user(explorer: Explorer):
    """Test the CaseCollection.filter method with the user argument."""

    cases = explorer.cases.filter(user="peesv")
    for case in cases:
        assert case.user == "peesv"


def test_get_cases_combinations(explorer: Explorer):
    """Test the CaseCollection.filter method with combined arguments."""

    cases = explorer.cases.filter(
        field=["DROGON", "JOHAN SVERDRUP"],
        user=["peesv", "dbs"],
        status=["keep"],
    )
    for case in cases:
        assert (
            case.user in ["peesv", "dbs"]
            and case.field in ["DROGON", "JOHAN SVERDRUP"]
            and case.status == "keep"
        )


def test_case_surfaces_type(test_case: Case):
    """Test that Case.surfaces property is of rype SurfaceCollection"""
    assert isinstance(test_case.surfaces, SurfaceCollection)


def test_case_surfaces_size(test_case: Case):
    """Test that Case.surfaces has the correct size"""
    assert len(test_case.surfaces) == 219


def test_case_surfaces_filter(test_case: Case):
    """Test that Case.surfaces has the correct size"""
    # filter on iteration stage
    agg_surfs = test_case.surfaces.filter(stage="iteration")
    assert len(agg_surfs) == 7

    agg_surfs = test_case.surfaces.filter(aggregation=True)
    assert len(agg_surfs)

    # filter on realization stage
    real_surfs = test_case.surfaces.filter(stage="realization")
    assert len(real_surfs) == 212

    real_surfs = test_case.surfaces.filter(realization=True)
    assert len(real_surfs) == 212

    # filter on iteration
    real_surfs = real_surfs.filter(iteration="iter-0")
    assert len(real_surfs) == 212

    for surf in real_surfs:
        assert surf.iteration == "iter-0"

    # filter on name
    real_surfs = real_surfs.filter(name="Valysar Fm.")
    assert len(real_surfs) == 56

    for surf in real_surfs:
        assert surf.iteration == "iter-0"
        assert surf.name == "Valysar Fm."

    # filter on tagname
    real_surfs = real_surfs.filter(tagname="FACIES_Fraction_Channel")
    assert len(real_surfs) == 4

    for surf in real_surfs:
        assert surf.iteration == "iter-0"
        assert surf.name == "Valysar Fm."
        assert surf.tagname == "FACIES_Fraction_Channel"

    # filter on realization
    real_surfs = real_surfs.filter(realization=0)
    assert len(real_surfs) == 1

    assert real_surfs[0].iteration == "iter-0"
    assert real_surfs[0].name == "Valysar Fm."
    assert real_surfs[0].tagname == "FACIES_Fraction_Channel"
    assert real_surfs[0].realization == 0
    assert isinstance(real_surfs[0].to_regular_surface(), RegularSurface)


def test_case_surfaces_pagination(test_case: Case):
    """Test the pagination logic of SurfaceCollection (DocumentCollection)"""
    surfs = test_case.surfaces
    count = 0

    for _ in surfs:
        count += 1

    assert count == len(surfs)


def test_get_case_by_uuid(explorer: Explorer, case_uuid: str, case_name: str):
    """Test that explorer.get_case_by_uuid returns the specified case"""
    case = explorer.get_case_by_uuid(case_uuid)

    assert isinstance(case, Case)
    assert case.uuid == case_uuid
    assert case.name == case_name

@pytest.mark.skipif(sys.platform.startswith('darwin'), reason="do not run OpenVDS SEGYImport on mac os")
def test_seismic_case_by_uuid(explorer: Explorer, seismic_case_uuid: str):
    """Test that explorer returns openvds compatible cubes for seismic case"""
    case = explorer.get_case_by_uuid(seismic_case_uuid)

    assert isinstance(case, Case)
    assert case.uuid == seismic_case_uuid
    assert len(case.cubes) == 10
    cube = case.cubes[0]
    openvds_handle = cube.openvds_handle

    layout = openvds.getLayout(openvds_handle)
    channel_count = layout.getChannelCount()
    assert channel_count == 3
    channel_list = []
    for i in range(channel_count):
        channel_list.append(layout.getChannelName(i))
    assert 'Amplitude' in channel_list
    assert 'Trace' in channel_list
    assert 'SEGYTraceHeader' in channel_list

def test_utils_extend_query_object(utils: Utils):
    """Test extension of query"""
    old = {"bool": {"must": [{"term": {"class.keyword": "surface"}}]}}
    new = {
        "bool": {"must": [{"term": {"fmu.aggregation.operation": "mean"}}]},
        "terms": {"fmu.iteration.name.keyword": ["iter-0", "iter-1"]},
    }
    extended = utils.extend_query_object(old, new)

    assert len(extended["bool"]["must"]) == 2
    assert isinstance(extended["terms"], dict)

    new = {
        "bool": {
            "must_not": [{"term": {"key": "value"}}],
            "must": [{"term": {"key": "value"}}],
        }
    }

    extended = utils.extend_query_object(extended, new)

    assert len(extended["bool"]["must"]) == 3
