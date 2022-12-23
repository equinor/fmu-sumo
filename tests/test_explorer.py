"""Tests explorer"""
import logging
import warnings
import json
from pathlib import Path
from uuid import UUID
import pytest
from context import Explorer, Case, DocumentCollection, ut


TEST_DATA = Path("data")
logging.basicConfig(level="DEBUG")
LOGGER = logging.getLogger()


@pytest.fixture(name="the_logger")
def fixture_the_logger():
    """Defining a logger"""
    return ut.init_logging("tests", "debug")


@pytest.fixture(name="case_name")
def fixture_case_name():
    """Returns case name"""
    return "21.x.0.dev_rowh2022_08-17"


@pytest.fixture(name="explorer")
def fixture_explorer(token):
    """Returns explorer"""
    return Explorer("dev", token=token)


@pytest.fixture(name="test_case")
def fixture_test_case(test_explorer, case_name):
    """Basis for test of method get_case_by_name for Explorer,
    but also other attributes
    """
    return test_explorer.get_case_by_name(case_name)


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
        f"the dictionary produced ({results}) is not equal to \n" + f" ({correct})"
    )
    assert results == correct, incorrect_mess


def test_cast_toomany_warning():
    """Tests custom made warning"""
    with pytest.warns(ut.TooManyCasesWarning):
        warnings.warn("Dummy", ut.TooManyCasesWarning)


def test_toomany_warning_content():
    """Tests case name in custom warning"""
    test_message = "testing testing"

    with warnings.catch_warnings(record=True) as w_list:
        warnings.warn(test_message, ut.TooManyCasesWarning)
        warn_message = str(w_list[0].message)
        print(warn_message)
        print(test_message)
        assert_mess = (
            f"wrong message in warning, is |{warn_message}| not |{test_message}|"
        )

        assert warn_message == test_message, assert_mess


def test_cast_toolowsize_warning():
    """Tests custom made warning"""
    with pytest.warns(ut.TooLowSizeWarning):
        warnings.warn("Dummy", ut.TooLowSizeWarning)


def test_toolowsize_warning_content():
    """Tests case name in custom warning"""
    test_message = "testing testing"

    with warnings.catch_warnings(record=True) as w_list:
        warnings.warn(test_message, ut.TooManyCasesWarning)
        warn_message = str(w_list[0].message)
        print(warn_message)
        print(test_message)
        assert_mess = (
            f"wrong message in warning, is |{warn_message}| not |{test_message}|"
        )

        assert warn_message == test_message, assert_mess


def test_get_cases(explorer):
    """Test the get_cases method."""

    cases = explorer.get_cases()
    assert isinstance(cases, DocumentCollection)
    assert isinstance(cases[0], Case)


def test_get_cases_fields(explorer):
    """Test get_cases method with the fields argument.

    Shall be case insensitive.
    """

    cases = explorer.get_cases(fields=["dRoGoN"])
    for case in cases:
        assert case.field_name.lower() == "drogon"


def test_get_cases_status(explorer):
    """Test the get_cases method with the status argument."""

    cases = explorer.get_cases(status=["keep"])
    for case in cases:
        assert case.status == "keep"


def test_get_cases_user(explorer):
    """Test the get_cases method with the users argument."""

    cases = explorer.get_cases(users=["peesv"])
    for case in cases:
        assert case.user == "peesv"


def test_get_cases_combinations(explorer):
    """Test the get_cases method with combined arguments."""

    cases = explorer.get_cases(
        fields=["Drogon", "Johan_Sverdrup"], users=["peesv", "dbs"], status=["keep"]
    )
    for case in cases:
        assert (
            case.user in ["peesv", "dbs"]
            and case.field_name.lower() in ["drogon", "johan_sverdrup"]
            and case.status == "keep"
        )
