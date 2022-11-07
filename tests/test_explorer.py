"""Tests explorer"""
import logging
import warnings
import json
from pathlib import Path
from uuid import UUID
import pytest
import context
# Runs function in context. Adds src to path. This should be done automatically,
# but not working need to figure out
context.add_path()
from fmu.sumo.explorer._utils import TooManyCasesWarning, TooLowSizeWarning
from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as ut


TEST_DATA = Path("data")
logging.basicConfig(level="DEBUG")
LOGGER = logging.getLogger()
LOGGER.debug("Tjohei")


@pytest.fixture
def the_logger():
    """Defining a logger"""
    return ut.init_logging("tests", "debug")


@pytest.fixture
def case_name():
    """Returns case name
    """
    return "21.x.0.dev_rowh2022_08-17"


@pytest.fixture
def test_explorer():
    """Returns explorer"""
    return Explorer("test")


@pytest.fixture
def prod_explorer():
    """Returns explorer"""
    return Explorer("prod")


@pytest.fixture
def test_case(test_explorer, case_name):
    """Basis for test of method get_case_by_name for Explorer,
       but also other attributes
    """
    return test_explorer.get_case_by_name(case_name)


@pytest.fixture
def sum_case():
    """Gets case with summary data from prod"""
    exp = Explorer("prod")
    return exp.get_case_by_name("drogon_design_2022_11-01")


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
        f"the dictionary produced ({results}) is not equal to \n" +
        f" ({correct})")
    assert results == correct, incorrect_mess

# Come back to this
# def test_logger(caplog):
#     """Tests the defined logger in explorer"""
#     logger_name = "tests"
#     logger = ut.init_logging(logger_name, "debug")
#     message = "works!"
#     logger.debug(message)
#     with caplog:
#         assert caplog.record_tuples == [(logger_name, logging.DEBUG, message)]


def test_cast_toomany_warning():
    """Tests custom made warning"""
    with pytest.warns(TooManyCasesWarning):
        warnings.warn("Dummy", TooManyCasesWarning)


def test_toomany_warning_content():
    """Tests case name in custom warning"""
    test_message = "testing testing"

    with warnings.catch_warnings(record=True) as w_list:
        warnings.warn(test_message, TooManyCasesWarning)
        warn_message = str(w_list[0].message)
        print(warn_message)
        print(test_message)
        assert_mess = (
            f"wrong message in warning, is |{warn_message}| not |{test_message}|"
        )

        assert warn_message == test_message, assert_mess


def test_cast_toolowsize_warning():
    """Tests custom made warning"""
    with pytest.warns(TooLowSizeWarning):
        warnings.warn("Dummy", TooLowSizeWarning)


def test_toolowsize_warning_content():
    """Tests case name in custom warning"""
    test_message = "testing testing"

    with warnings.catch_warnings(record=True) as w_list:
        warnings.warn(test_message, TooManyCasesWarning)
        warn_message = str(w_list[0].message)
        print(warn_message)
        print(test_message)
        assert_mess = (
            f"wrong message in warning, is |{warn_message}| not |{test_message}|"
        )

        assert warn_message == test_message, assert_mess


def test_sumo_id_attribute(sum_case):
    """Tests getting sumo_id
    args
    test_explorer (sumo.Explorer):
    """
    assert_correct_uuid(sum_case.sumo_id)


def test_get_dict_of_case_names(prod_explorer):
    """tests method get_dict_of_cases
    """

    assert_uuid_dict(prod_explorer.get_dict_of_case_names())


def test_func_get_object_surface_blob_ids(the_logger, sum_case):
    """Tests method get_object_blob_ids"""

    results = ut.get_object_blob_ids(sum_case, data_type="surface", content="depth",
                                     name="VOLANTIS GP. Base",
                                     tag="FACIES_Fraction_Offshore", iteration=0,
                                     size=309
    )
    # |result_file = "dict_of_surface_blob_ids.json"

    # write_json(result_file, results)
    # correct = read_json(result_file)

    assert len(results) == 155
    assert_uuid_dict(results)
    # assert_dict_equality(results, correct)


def test_func_get_object_sum_blob_ids(the_logger, sum_case):
    """Tests method get_object_blob_ids"""
    results = ut.get_object_blob_ids(sum_case, data_type="table",
                                     content="timeseries",
                                     size=974
    )
    # result_file = "dict_of_sum_blob_ids.json"

    # write_json(result_file, results)

    # correct = read_json(result_file)

    assert len(results) == 974
    assert_uuid_dict(results)
    # assert_dict_equality(results, correct)


def test_method_get_object_surface_blob_ids(the_logger, sum_case):
    """Tests method get_object_blob_ids"""

    results = sum_case.get_blob_ids("VOLANTIS GP. Base",
                                    "FACIES_Fraction_Offshore", size=309)

    # result_file = "dict_of_surface_blob_ids.json"

    # write_json(result_file, results)
    # correct = read_json(result_file)

    assert len(results) == 155
    assert_uuid_dict(results)
    # assert_dict_equality(results, correct)


def test_method_get_object_sum_blob_ids(the_logger, sum_case):
    """Tests method get_object_blob_ids"""
    results = sum_case.get_summary_blob_ids(size=974)
    # result_file = "dict_of_sum_blob_ids.json"

    # write_json(result_file, results)

    # |correct = read_json(result_file)

    assert len(results) == 974
    assert_uuid_dict(results)
    # assert_dict_equality(results, correct)
