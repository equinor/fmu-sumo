"""Test table objects.

* Table
* AggregatedTable
* TableCollection

"""

import pandas as pd
import pyarrow as pa
import pytest

from fmu.sumo.explorer import Explorer

# Fixed test case ("Drogon_AHM_2023-02-22") in Sumo/DEV
TESTCASE_UUID = "10f41041-2c17-4374-a735-bb0de62e29dc"


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns explorer"""
    return Explorer("dev", token=token)


@pytest.fixture(name="case")
def fixture_case(explorer: Explorer):
    """Return fixed testcase."""
    return explorer.get_case_by_uuid(TESTCASE_UUID)


@pytest.fixture(name="table")
def fixture_table(case):
    """Get one table for further testing."""
    return case.tables[0]


### Table


def test_table_to_pandas(table):
    """Test the to_pandas method."""
    df = table.to_pandas()
    assert isinstance(df, pd.DataFrame)


def test_table_to_arrow(table):
    """Test the to_arrow() method"""
    arrow = table.to_arrow()
    assert isinstance(arrow, pa.Table)
