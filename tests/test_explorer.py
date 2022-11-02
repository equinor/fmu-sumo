"""Tests explorer"""
import logging
import warnings
import json
from pathlib import Path
import pytest
import context
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
def the_explorer():
    """Returns explorer"""
    return Explorer("test")

@pytest.fixture
def the_case(the_explorer, case_name):
    """Basis for test of method get_case_by_name for Explorer,
       but also other attributes
    """
    return the_explorer.get_case_by_name(case_name)


@pytest.fixture
def sum_case():
    """Gets prod case"""
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


def test_get_sumo_id(the_case):
    """Tests getting sumo_id
    args
    test_explorer (sumo.Explorer):
    """
    test_id = "67d105f3-a2b1-693e-7653-090d91fb1981"
    case_id = the_case.sumo_id
    assert_mess = f"sumo id for was {case_id}, should be {test_id}"
    assert case_id == test_id, assert_mess


def test_get_dict_of_cases(the_explorer):
    """tests method get_dict_of_cases
    """

    results = the_explorer.get_dict_of_cases()
    result_file = "dict_of_cases.json"
    # write_json(result_file, results)
    correct = read_json(result_file)
    assert_dict_equality(results, correct)


def test_func_get_object_surface_blobs(the_logger, sum_case):
    """Tests method get_object_blobs"""

    results = ut.get_object_blobs(sum_case, data_type="surface", content="depth",
                                  name="VOLANTIS GP. Base",
                                  tag="FACIES_Fraction_Offshore", iteration=0,
                                  size=309
    )
    result_file = "dict_of_surface_blobs.json"

    # write_json(result_file, results)
    correct = read_json(result_file)

    assert len(results) == 155
    # assert_dict_equality(results, correct)


def test_func_get_object_sum_blobs(the_logger, sum_case):
    """Tests method get_object_blobs"""
    results = ut.get_object_blobs(sum_case, data_type="table",
                                  content="timeseries",
                                  size=974
    )
    result_file = "dict_of_sum_blobs.json"

    # write_json(result_file, results)

    correct = read_json(result_file)

    assert len(results) == 974
    # assert_dict_equality(results, correct)


def test_method_get_object_surface_blobs(the_logger, sum_case):
    """Tests method get_object_blobs"""

    results = sum_case.get_blob_paths("VOLANTIS GP. Base",
                                      "FACIES_Fraction_Offshore", size=309)
    result_file = "dict_of_surface_blobs.json"

    # write_json(result_file, results)
    correct = read_json(result_file)

    assert len(results) == 155
    # assert_dict_equality(results, correct)


def test_method_get_object_sum_blobs(the_logger, sum_case):
    """Tests method get_object_blobs"""
    results = sum_case.get_summary_blob_paths(size=974)
    result_file = "dict_of_sum_blobs.json"

    # write_json(result_file, results)

    correct = read_json(result_file)

    assert len(results) == 974
    # assert_dict_equality(results, correct)


def test_vector_names(the_explorer):
    """Test method get_vector_names"""

# if __name__ == "__main__":
#     test_get_object_blobs()
