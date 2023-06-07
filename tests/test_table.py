from fmu.sumo.explorer import Explorer
import pytest


@pytest.fixture(name="doc", scope="function")
def _fixture_doc():
    exp = Explorer("dev")
    case = exp.get_case_by_uuid("8575ed9a-b0c1-4f64-b9ab-fab9cec31f4a")
    pvt_table = case.tables.filter(
        name="SNORRE",
        tagname="pvt",
        iteration="iter-0",
        realization=113,
    )
    return pvt_table[0]


@pytest.fixture(name="correct_columns", scope="session")
def _fixture_cols():
    return [
        "PRESSURE",
        "VOLUMEFACTOR",
        "VISCOSITY",
        "RS",
        "PVTNUM",
        "KEYWORD",
        "OILDENSITY",
        "WATERDENSITY",
        "GASDENSITY",
        "COMPRESSIBILITY",
        "VISCOSIBILITY",
    ]


def test_to_pandas_with_csv(doc, correct_columns):
    """Test method to_pandas

    Args:
        doc (fmu.sumo.Document): the document to read from
        correct_columns (list): list of correct columns
    """
    returned = doc.to_pandas
    check_columns = returned.columns.tolist()
    correct_columns.sort()
    check_columns.sort()
    assert (
        check_columns == correct_columns
    ), f"Cols should be {correct_columns}, {check_columns}, when csv"


def test_to_arrow_with_csv(doc, correct_columns):
    """Test method to_arrow

    Args:
        doc (fmu.sumo.Document): the document to read from
        correct_columns (list): list of correct columns
    """
    returned = doc.to_arrow
    correct_columns.sort()
    check_columns = returned.column_names
    check_columns.sort()

    assert (
        check_columns == correct_columns
    ), f"Cols should be {correct_columns}, {check_columns}, when arrow"
