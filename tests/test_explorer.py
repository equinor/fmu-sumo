"""Tests explorer"""

import sys

if not sys.platform.startswith("darwin") and sys.version_info < (3, 12):
    import openvds
import json
import logging
from pathlib import Path
from uuid import UUID

import pytest
from context import (
    Case,
    Explorer,
    SearchContext,
)
from sumo.wrapper import SumoClient
from xtgeo import RegularSurface

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
    assert isinstance(cases, SearchContext)
    assert isinstance(cases[0], Case)


def test_get_cases_fields(explorer: Explorer):
    """Test SearchContext.filter method with the field argument.

    Shall be case insensitive.
    """

    cases = explorer.cases.filter(field="DROGON")
    for case in cases:
        assert case.field.lower() == "drogon"


def test_get_cases_status(explorer: Explorer):
    """Test the SearchContext.filter method with the status argument."""

    cases = explorer.cases.filter(status="keep")
    for case in cases:
        assert case.status == "keep"


def test_get_cases_user(explorer: Explorer):
    """Test the SearchContext.filter method with the user argument."""

    cases = explorer.cases.filter(user="peesv")
    for case in cases:
        assert case.user == "peesv"


def test_get_cases_combinations(explorer: Explorer):
    """Test the SearchContext.filter method with combined arguments."""

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
    """Test that all objects in Case.surfaces class surface"""
    # assert all([x.metadata["class"] == "surface" for x in test_case.surfaces])
    classes = test_case.surfaces.get_field_values("class.keyword")
    assert len(classes) == 1
    assert classes[0] == "surface"


def test_case_surfaces_size(test_case: Case):
    """Test that Case.surfaces has the correct size"""
    assert len(test_case.surfaces) == 271


def test_case_surfaces_filter(test_case: Case):
    """Test that Case.surfaces has the correct size"""
    # filter on iteration stage
    agg_surfs = test_case.surfaces.filter(stage="iteration")
    assert len(agg_surfs) == 59

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

    # for surf in real_surfs:
    #     assert surf.iteration == "iter-0"
    its = real_surfs.get_field_values("fmu.iteration.name.keyword")
    assert len(its) == 1 and its[0] == "iter-0"

    # filter on name
    non_valid_name_surfs = real_surfs.filter(name="___not_valid")
    assert len(non_valid_name_surfs) == 0

    real_surfs = real_surfs.filter(name="Valysar Fm.")
    assert len(real_surfs) == 56

    # for surf in real_surfs:
    #     assert surf.iteration == "iter-0"
    #     assert surf.name == "Valysar Fm."
    its = real_surfs.get_field_values("fmu.iteration.name.keyword")
    assert len(its) == 1 and its[0] == "iter-0"
    names = real_surfs.get_field_values("data.name.keyword")
    assert len(names) == 1 and names[0] == "Valysar Fm."

    # filter on content
    non_valid_content_surfs = real_surfs.filter(content="___not_valid")
    assert len(non_valid_content_surfs) == 0

    real_surfs = real_surfs.filter(content="depth")
    assert len(real_surfs) == 56

    # filter on tagname
    non_valid_tagname_surfs = real_surfs.filter(tagname="___not_valid")
    assert len(non_valid_tagname_surfs) == 0

    real_surfs = real_surfs.filter(tagname="FACIES_Fraction_Channel")
    assert len(real_surfs) == 4

    # for surf in real_surfs:
    #     assert surf.iteration == "iter-0"
    #     assert surf.name == "Valysar Fm."
    #     assert surf.tagname == "FACIES_Fraction_Channel"
    its = real_surfs.get_field_values("fmu.iteration.name.keyword")
    assert len(its) == 1 and its[0] == "iter-0"
    names = real_surfs.get_field_values("data.name.keyword")
    assert len(names) == 1 and names[0] == "Valysar Fm."
    tagnames = real_surfs.get_field_values("data.tagname.keyword")
    assert len(tagnames) == 1 and tagnames[0] == "FACIES_Fraction_Channel"

    # filter on data format
    non_valid_format_surfs = real_surfs.filter(dataformat="___not_valid")
    assert len(non_valid_format_surfs) == 0

    real_surfs = real_surfs.filter(dataformat="irap_binary")
    assert len(real_surfs) == 4

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


@pytest.mark.skipif(
    sys.platform.startswith("darwin") or sys.version_info > (3, 12),
    reason="do not run OpenVDS SEGYImport on mac os or python 3.12",
)
def test_seismic_case_by_uuid(explorer: Explorer, seismic_case_uuid: str):
    """Test that explorer returns openvds compatible cubes for seismic case"""
    case = explorer.get_case_by_uuid(seismic_case_uuid)

    assert isinstance(case, Case)
    assert case.uuid == seismic_case_uuid
    assert len(case.cubes) == 10
    cube = case.cubes[0]
    openvds_handle = cube.openvds_handle

    layout = openvds.getLayout(openvds_handle)  # type: ignore
    channel_count = layout.getChannelCount()
    assert channel_count == 3
    channel_list = []
    for i in range(channel_count):
        channel_list.append(layout.getChannelName(i))
    assert "Amplitude" in channel_list
    assert "Trace" in channel_list
    assert "SEGYTraceHeader" in channel_list


def test_grids_and_properties(explorer: Explorer):
    cases_with_grids = explorer.grids.cases.filter(status="keep")
    cases_with_gridprops = explorer.grid_properties.cases.filter(status="keep")
    cgs = {case.uuid for case in cases_with_grids}
    cgps = {case.uuid for case in cases_with_gridprops}
    assert cgs == cgps
    case = cases_with_grids[0]
    grids = case.grids
    gridprops = case.grid_properties
    xtgrid = grids[0].to_cpgrid()
    gridspec = grids[0].metadata["data"]["spec"]
    assert xtgrid.nlay == gridspec["nlay"]
    assert xtgrid.nrow == gridspec["nrow"]
    assert xtgrid.ncol == gridspec["ncol"]
    xtgridprop = gridprops[0].to_cpgrid_property()
    gridpropspec = gridprops[0].metadata["data"]["spec"]
    assert xtgridprop.nlay == gridpropspec["nlay"]
    assert xtgridprop.nrow == gridpropspec["nrow"]
    assert xtgridprop.ncol == gridpropspec["ncol"]


def test_search_context_select(test_case: Case):
    surfs = test_case.surfaces.filter(realization=True)
    assert "_sumo" in surfs[0].metadata
    surfs.select("fmu")
    assert "_sumo" not in surfs[0].metadata
    assert "fmu" in surfs[0].metadata
    surfs.select(["fmu"])
    assert "_sumo" not in surfs[0].metadata
    assert "fmu" in surfs[0].metadata
    surfs.select({"excludes": ["fmu"]})
    assert "_sumo" in surfs[0].metadata
    assert "fmu" not in surfs[0].metadata
    surfs.select({"includes": ["_sumo"], "excludes": ["_sumo.timestamp"]})
    assert "_sumo" in surfs[0].metadata
    assert "fmu" not in surfs[0].metadata
    assert "timestamp" not in surfs[0].metadata["_sumo"]
