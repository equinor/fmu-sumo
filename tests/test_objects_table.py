"""Test table objects.

  * Table
  * AggregatedTable
  * TableCollection

"""
import pandas as pd
import pyarrow as pa
from fmu.sumo.explorer import Explorer, AggregatedTable
import pytest

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


### Aggregated Table

def test_aggregated_summary_arrow(case):
    """Test usage of Aggregated class with default type"""

    table = AggregatedTable(case, "summary", "eclipse", "iter-0")

    assert len(table.columns) == 972 + 2
    column = table["FOPT"]

    assert isinstance(column.to_arrow(), pa.Table)
    with pytest.raises(IndexError) as e_info:
        table = table["banana"]
        assert (
            e_info.value.args[0] == "Column: 'banana' does not exist try again"
        )


def test_aggregated_summary_pandas(case):
    """Test usage of Aggregated class with item_type=pandas"""
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table["FOPT"].to_pandas(), pd.DataFrame)


def test_get_fmu_iteration_parameters(case):
    """Test getting the metadata of of an object"""
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table.parameters, dict)
