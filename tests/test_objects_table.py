"""Test table objects.

  * Table
  * AggregatedTable
  * TableCollection

"""
import pandas as pd
import pyarrow as pa
from fmu.sumo.explorer import Explorer, AggregatedTable
import pytest


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns explorer"""
    return Explorer("dev", token=token)


@pytest.fixture(name="table")
def fixture_table(token: str, explorer: Explorer):
    """Get one table for further testing."""
    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]
    return case.tables[0]
    
### Table

def test_table_dataframe(table):
    """Test the dataframe property."""
    with pytest.warns(DeprecationWarning, match=".dataframe is deprecated"):
        df = table.dataframe
    assert isinstance(df, pd.DataFrame)


def test_table_to_pandas(table):
    """Test the to_pandas method."""
    df = table.to_pandas()
    assert isinstance(df, pd.DataFrame)


def test_arrowtable(table):
    """Test the arrowtable property."""
    with pytest.warns(DeprecationWarning, match=".arrowtable is deprecated"):
        arrow = table.arrowtable
    assert isinstance(arrow, pa.Table)


def test_table_to_arrow(table):
    """Test the to_arrow() method"""
    arrow = table.to_arrow()
    assert isinstance(arrow, pa.Table)


### Aggregated Table

def test_aggregated_summary_arrow(explorer: Explorer):
    """Test usage of Aggregated class with default type"""

    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]

    table = AggregatedTable(case, "summary", "eclipse", "iter-0")

    assert len(table.columns) == 972 + 2
    column = table["FOPT"]

    assert isinstance(column.to_arrow(), pa.Table)
    with pytest.raises(IndexError) as e_info:
        table = table["banana"]
        assert (
            e_info.value.args[0] == "Column: 'banana' does not exist try again"
        )


def test_aggregated_summary_arrow_with_deprecated_function_name(
    explorer: Explorer,
):
    """Test usage of Aggregated class with default type with deprecated function name"""

    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]

    table = AggregatedTable(case, "summary", "eclipse", "iter-0")

    assert len(table.columns) == 972 + 2
    column = table["FOPT"]

    with pytest.warns(
        DeprecationWarning,
        match=".arrowtable is deprecated, renamed to .to_arrow()",
    ):
        assert isinstance(column.arrowtable, pa.Table)
    with pytest.raises(IndexError) as e_info:
        table = table["banana"]
        assert (
            e_info.value.args[0] == "Column: 'banana' does not exist try again"
        )


def test_aggregated_summary_pandas(explorer: Explorer):
    """Test usage of Aggregated class with item_type=pandas"""
    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table["FOPT"].to_pandas(), pd.DataFrame)


def test_get_fmu_iteration_parameters(explorer: Explorer):
    """Test getting the metadata of of an object"""
    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table.parameters, dict)
