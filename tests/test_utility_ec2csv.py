"""Test utility ecl2csv"""
from pathlib import Path
import pandas as pd
import pytest
from fmu.sumo.utilities.ecl2csv import SUBMODULES
from fmu.sumo.utilities.ecl2csv import SUBMOD_DICT
from fmu.sumo.utilities.ecl2csv import define_submodules
from fmu.sumo.utilities.ecl2csv import get_dataframe
from fmu.sumo.utilities.ecl2csv import export_csv
from fmu.sumo.utilities.ecl2csv import parse_args


REEK_DATA = Path(__file__).parent / "data/reek/eclipse/model/"
REEK_DATA_FILE = REEK_DATA / "2_R001_REEK-0.DATA"


def test_submodules_dict():
    """Test generation of submodule list"""
    sublist, submods = define_submodules()
    assert isinstance(sublist, tuple)
    assert isinstance(submods, dict)
    for submod_name, submod_input in submods.items():
        assert isinstance(submod_name, str)
        assert "/" not in submod_name
        assert isinstance(submod_input, tuple)
    print(submods)


@pytest.mark.parametrize("submod", SUBMODULES)
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


def test_parse_args(tmp_path, mocker):
    mocker.path(
        "sys.argv",
        [
            "ecl2csv",
            Path(__file__).parent
            / "data/reek/fmuconfig/output/global_variables.yml",
            "summary",
        ],
    )
    parse_args()


if __name__ == "__main__":
    test_parse_args()
