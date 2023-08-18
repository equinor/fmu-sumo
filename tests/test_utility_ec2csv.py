"""Test utility ecl2csv"""
from pathlib import Path
import pandas as pd
import pytest
from fmu.sumo.utilities.ecl2csv import define_submodules
from fmu.sumo.utilities.ecl2csv import get_dataframe
from fmu.sumo.utilities.ecl2csv import export_csv

REEK_DATA = Path(__file__).parent / "data/reek/eclipse/model/"
REEK_DATA_FILE = REEK_DATA / "2_R001_REEK-0.DATA"


def test_submodules_list():
    """Test generation of submodule list"""
    submods = define_submodules()
    assert isinstance(submods, list)
    for submod in submods:
        assert isinstance(submod, str)
        assert "/" not in submod
    print(submods)


@pytest.mark.parametrize("submod", define_submodules())
def test_get_dataframe(submod):
    """Test fetching of dataframe"""
    frame = get_dataframe(REEK_DATA_FILE, submod)
    assert isinstance(frame, pd.DataFrame)


def test_export_csv(submod="summary"):
    """Test writing of csv file"""
    assert isinstance(
        export_csv(
            REEK_DATA_FILE,
            submod,
            "data/reek/fmuconfig/output/global_variables.yml",
        ),
        str,
    )
