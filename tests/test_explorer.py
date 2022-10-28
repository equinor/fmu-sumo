"""Tests explorer"""
import warnings
import pytest
from fmu.sumo.explorer._utils import TooManyCasesWarning
from fmu.sumo.explorer import Explorer
import context

@pytest.fixture
def the_explorer():
    """Returns explorer"""
    return Explorer("test")


def test_cast_warning():
    """Tests custom made warning"""
    with pytest.warns(TooManyCasesWarning):
        warnings.warn("Dummy", TooManyCasesWarning)


def test_warning_content():
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


def test_get_sumo_id(the_explorer):
    """Tests getting sumo_id
    args
    test_explorer (sumo.Explorer):
    """
    test_name = "21.x.0.dev_rowh2022_08-17"
    test_id = "67d105f3-a2b1-693e-7653-090d91fb1981"
    case = the_explorer.get_case_by_name(test_name)
    case_id = case.sumo_id
    assert_mess = f"sumo id for {test_name} was {case_id}, should be {test_id}"
    assert case_id == test_id, assert_mess


def test_vector_names(the_explorer):
    """Test method get_vector_names"""
