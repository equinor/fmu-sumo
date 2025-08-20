from contextlib import nullcontext

import pandas as pd
import pyarrow as pa
import pytest

from fmu.sumo.explorer.objects import Case, SearchContext
from fmu.sumo.explorer.objects.table import Table
from fmu.sumo.explorer.summary import Realization, Summary, get_case


@pytest.fixture(name="case")
def fixture_case() -> Case:
    return get_case("a3678922-3a1b-41a3-a57c-15cb3e6d6983", env="prod")


@pytest.fixture(name="vectors")
def fixture_vectors() -> list:
    return ["FWPT", "FOPT", "FOPR"]


@pytest.fixture(name="tables")
def fixture_tables(case: Case) -> SearchContext:
    return Realization(
        case, 0, ensemble="iter-0", vectors=["FOPR", "FOPT"]
    ).tables


@pytest.fixture(name="table")
def fixture_table(tables: SearchContext) -> Table:
    return tables[0]


@pytest.fixture(name="pyarrow_table")
def fixture_pyarrow_table(tables: SearchContext) -> Table:
    return tables[0].to_arrow()


@pytest.fixture(name="df_duplicate_dates")
def fixture_df_duplicate_dates(tables: SearchContext) -> pd.DataFrame:
    df = tables[0].to_pandas()
    df = pd.concat([df, df.loc[0:5]], ignore_index=True).copy()
    return df


""" Tests for Summary class """


@pytest.mark.parametrize(
    "vectors,expected_result",
    [(["FWPT", "FOP*"], True), (["FWPT", "FOPT", "FOPR"], False)],
)
def test_regex_in_vectors(case: Case, vectors: list, expected_result: bool):
    summary = Summary(case, vectors)
    assert summary._regex_in_vectors == expected_result


@pytest.mark.parametrize(
    "vectors,expected_result",
    [(["FWPT", "FOPT*"], ["FWPT", "FOPT", "FOPTF", "FOPTH", "FOPTS"])],
)
def test_expand_wildcard_vectors(
    case: Case, tables: SearchContext, vectors: list, expected_result: bool
):
    summary = Summary(case, vectors)  # Required to access method to be tested
    columns = tables[0].columns
    expanded_vectors = summary._expand_wildcard_vectors(columns)
    assert set(expected_result) == set(expanded_vectors)


def test_check_number_summary_files():
    # Difficult to test without a case with multiple summary files
    # unless one is run. Can make a mock case?
    pass


""" Tests for Summary class methods which are called by the Realization and
Ensemble classes. Here the methods are tested using the Realization class. """


def test_check_number_ensembles(case: Case, vectors: list):
    with pytest.raises(ValueError):
        # More than one ensemble in this case
        Realization(case, 0, vectors=vectors)

    with pytest.raises(ValueError):
        # Filter to something which doesn't exist to get 0 ensembles
        Realization(case, 0, ensemble="filter_out_all", vectors=vectors)

    with nullcontext():
        # No error in this case
        Realization(case, 0, ensemble="iter-0", vectors=vectors)


@pytest.mark.parametrize(
    "frequency,expected_result",
    [
        ("yearly", 4),
        ("monthly", 31),
        ("weekly", 132),
        ("daily", 912),
        ("raw", 116),
    ],
)
def test_filter_time_index(
    case: Case,
    vectors: list,
    pyarrow_table: pa.Table,
    frequency: str,
    expected_result: int,  # length of re-indexed list
):
    summary = Summary(case, vectors)  # Required to access method to be tested
    time_index = summary._filter_time_index(pyarrow_table, frequency)

    assert isinstance(time_index, pd.DatetimeIndex)
    assert len(time_index) == expected_result


@pytest.mark.parametrize(
    "frequency",
    ("daily", "weekly", "monthly", "yearly", "raw"),
)
def test_reindex_dates(
    case: Case,
    vectors: list,
    frequency: str,
):
    realisation = Realization(case, 0, vectors, ensemble="iter-0")
    table = realisation.tables[0].to_arrow()
    time_index = realisation._filter_time_index(table, frequency)
    df = table.to_pandas()

    # Create duplicate date entries in dataframe
    df_duplicate_dates = pd.concat([df, df.loc[0:5]], ignore_index=True).copy()
    # Need to set index to DATE
    df_duplicate_dates = df_duplicate_dates.set_index("DATE")

    with pytest.raises(ValueError):
        # Raise ValueError if duplicate dates found
        realisation._reindex_dates(df_duplicate_dates, time_index)

    with nullcontext():
        # No error in this case
        realisation._reindex_dates(df, time_index)


@pytest.mark.parametrize(
    "vectors,expected_result",
    [
        (
            ["FOPT", "FOPR", "DUMMY"],
            {"FOPT": "interpolate", "FOPR": "fill", "DUMMY": None},
        ),
    ],
)
def test_get_column_imputation_methods(
    case: Case, vectors: list, expected_result: dict
):
    realisation = Realization(case, 0, vectors=vectors, ensemble="iter-0")
    df = pd.DataFrame(columns=vectors)
    imputation_methods = realisation._get_column_imputation_methods(df)
    assert imputation_methods == expected_result


@pytest.mark.parametrize(
    "frequency",
    ("daily", "weekly", "monthly", "yearly", "raw"),
)
def test_change_data_frequency_sr(case: Case, frequency: str):
    vectors = [
        "DATE",
        "FOPT",
        "FGPT",
        "FWIT",
        "FOPR",
        "FOIP",
        "FGIR",
        "FPR",
        "WBHP:A1",
    ]
    realisation = Realization(
        case,
        0,
        vectors=vectors,
        ensemble="iter-0",
    )
    table = realisation.tables[0].to_arrow()
    table = table.select(vectors)

    time_index = realisation._filter_time_index(table, frequency)
    imputed_table = realisation._change_data_frequency_sr(table, time_index)
    df = imputed_table.to_pandas()

    assert len(df) > 0
    # A key check that imputation has worked correctly is that there are no
    # missing values in the reindexed dataframe
    assert not df.isna().to_numpy().any()

    # TODO test against stored summary data? This has been done separately to
    # compare results from this module with those from other packages used to
    # obtain summary data from the scratch disk.


""" Tests for Realization class """


@pytest.mark.parametrize(
    "vectors,expected_result",
    [(["FOPR", "FWPT"], ["DATE", "FOPR", "FWPT"]), ("FOPR", ["DATE", "FOPR"])],
)
def test_format_vectors_realization(
    case: Case, vectors: list, expected_result: list
):
    realisation = Realization(case, 0, vectors=vectors, ensemble="iter-0")
    realisation_vectors = realisation.vectors
    assert set(expected_result) == set(realisation_vectors)


def test_realization_df():
    pass


""" Tests for Ensemble class """


def test_ensemble_change_data_frequency():
    pass


def test_ensemble_df():
    pass
