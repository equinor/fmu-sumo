"""Test utility ecl2csv"""
import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pytest
import fmu.sumo.utilities.ecl2csv as sumo_ecl2csv

from fmu.sumo.uploader import CaseOnDisk, SumoConnection
from sumo.wrapper import SumoClient

REEK_ROOT = Path(__file__).parent / "data/reek"
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_ROOT / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_ROOT / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"


def test_submodules_dict():
    """Test generation of submodule list"""
    sublist, submods = sumo_ecl2csv._define_submodules()
    print(submods)
    assert isinstance(sublist, tuple)
    assert isinstance(submods, dict)
    for submod_name, submod_dict in submods.items():
        print(submod_name)
        print(submod_dict)
        assert isinstance(submod_name, str)
        assert (
            "/" not in submod_name
        ), f"Left part of folder path for {submod_name}"
        assert isinstance(submod_dict, dict), f"{submod_name} has no subdict"
        assert (
            "options" in submod_dict.keys()
        ), f"{submod_name} does not have any options"

        assert isinstance(
            submod_dict["options"], tuple
        ), f"options for {submod_name} not tuple"


@pytest.mark.parametrize(
    "submod",
    (name for name in sumo_ecl2csv.SUBMODULES if name != "wellcompletiondata"),
)
# Skipping wellcompletion data, since this needs zonemap, which none of the others do
def test_get_dataframe(submod):
    """Test fetching of dataframe"""
    extras = {}
    if submod == "wellcompletiondata":
        extras["zonemap"] = "data/reek/zones.lyr"
    frame = sumo_ecl2csv.get_dataframe(REEK_DATA_FILE, submod)
    assert isinstance(
        frame, pd.DataFrame
    ), f"Call for get_dataframe should produce dataframe, but produces {type(frame)}"
    frame = sumo_ecl2csv.get_dataframe(REEK_DATA_FILE, submod, arrow=True)
    assert isinstance(
        frame, pa.Table
    ), f"Call for get_dataframe with arrow=True should produce pa.Table, but produces {type(frame)}"


@pytest.mark.parametrize(
    "submod",
    (name for name in sumo_ecl2csv.SUBMODULES if name != "wellcompletiondata"),
)
def test_export_csv(tmp_path, submod):
    """Test writing of csv file"""
    os.chdir(tmp_path)
    export_path = (
        tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.csv".lower()
    )
    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = sumo_ecl2csv.export_csv(
        REEK_DATA_FILE,
        submod,
        CONFIG_PATH,
    )
    print(actual_path)
    assert isinstance(
        actual_path,
        str,
    ), "No string returned for path"
    assert export_path.exists(), f"No export of data to {export_path}"
    assert meta_path.exists(), f"No export of metadata to {meta_path}"


def test_export_csv_w_options(tmp_path, submod="summary"):
    """Test writing of csv file"""
    os.chdir(tmp_path)
    export_path = (
        tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.csv".lower()
    )
    key_args = {
        "time_index": "daily",
        "start_date": "2002-01-02",
        "end_date": "2003-01-02",
    }

    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = sumo_ecl2csv.export_csv(
        REEK_DATA_FILE, submod, CONFIG_PATH, **key_args
    )
    print(actual_path)
    assert isinstance(
        actual_path,
        str,
    ), "No string returned for path"
    assert export_path.exists(), f"No export of data to {export_path}"
    assert meta_path.exists(), f"No export of metadata to {meta_path}"


# Extra checks to be used with parametrize below
CHECK_DICT = {
    "global_variables_w_eclpath.yml": {
        "nrdatafile": 1,
        "nrsubmods": 16,
        "nroptions": 1,
        "arrow": False,
    },
    "global_variables_w_eclpath_and_extras.yml": {
        "nrdatafile": 1,
        "nrsubmods": 3,
        "nroptions": 4,
        "arrow": True,
    },
    "global_variables.yml": {
        "nrdatafile": 2,
        "nrsubmods": 16,
        "nroptions": 1,
        "arrow": False,
    },
}


def _assert_right_len(checks, key, to_messure, name):
    """Assert length when reading config

    Args:
        checks (dict): the answers
        key (str): the answer to check
        to_messure (list): the generated answer
        name (str): name of the file to check against
    """
    # Helper for test_read_config
    right_len = checks[key]
    actual_len = len(to_messure)
    assert (
        actual_len == right_len
    ), f"For {name}-{key} actual length is {actual_len}, but should be {right_len}"


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_read_config(config_path):
    """Test reading of config file via read_config function"""
    os.chdir(REEK_ROOT)
    print(config_path)
    config = sumo_ecl2csv.yaml_load(config_path)["ecl2csv"]
    assert isinstance(config, (dict, bool))
    dfiles, submods, opts = sumo_ecl2csv.read_config(config)
    name = config_path.name
    checks = CHECK_DICT[name]
    print(config)
    print(dfiles)
    print(submods)
    print(opts)
    _assert_right_len(checks, "nrdatafile", dfiles, name)
    _assert_right_len(checks, "nrsubmods", submods, name)
    _assert_right_len(checks, "nroptions", opts, name)

    assert (
        opts["arrow"] == checks["arrow"]
    ), f"Wrong choice for arrow for {name}"


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_export_w_config(tmp_path, config_path):
    """Test function export with config"""
    os.chdir(tmp_path)
    # The line below is needed for test to work when def of datafile not in
    # config
    sim_path = tmp_path / "eclipse"
    conf_path = tmp_path / "fmuconfig/output/"
    sim_path.mkdir(parents=True)
    (sim_path / "model").symlink_to(REEK_ECL_MODEL)
    conf_path.mkdir(parents=True)
    (conf_path / config_path.name).symlink_to(config_path)

    sumo_ecl2csv.export_with_config(config_path)


def test_parse_args(mocker, submod):
    """Test parse args

    Args:
        mocker (pytest.fixture): to mock command line like
        submod (str): name of submodule
    """
    config_path = str(
        Path(__file__).parent
        / "data/reek/fmuconfig/output/global_variables.yml",
    )

    reek_datafile_str = str(REEK_DATA_FILE)
    commands = [config_path, submod, reek_datafile_str]

    print(commands)
    mocker.patch(
        "sys.argv",
        commands,
    )
    args = sumo_ecl2csv.parse_args()
    assert isinstance(args, dict), "Args not converted to dict"
    assert (
        args["subcommand"] == submod
    ), f"For {submod} subcommand is set to {args['subcommand']}"
    assert (
        args["DATAFILE"] == reek_datafile_str
    ), f"For {submod} datafile is {args['DATAFILE']}"
    options = sumo_ecl2csv.SUBMOD_DICT[submod]
    arg_keys = args.keys()
    print(options)
    print(arg_keys)
    unknowns = [arg_key for arg_key in arg_keys if arg_key not in options]
    assert (
        len(unknowns) == 0
    ), f"for {submod} these unknown arguments were passed {unknowns}"

    assert all([arg in options for arg in arg_keys]), "Not all passed"


def test_upload():
    sumo = SumoClient("test")
    sumocon = SumoConnection("test")
    case_metadata_path = REEK_ROOT / "share/metadata/fmu_case.yml"
    print(f"This is the case metadata %s", case_metadata_path)
    # case_metadata_path = "share/metadata/fmu_case.yml"

    case = CaseOnDisk(
        case_metadata_path=case_metadata_path,
        sumo_connection=sumocon,
        verbosity="DEBUG",
    )

    case_uuid = case.register()
    sumo_ecl2csv.upload(
        str(REEK_ROOT / "realization-0/iter-0/share/results/tables"),
        ["csv"],
        "test",
    )
    path = f"/objects('{case_uuid}')"

    # sumo.delete(path)


if __name__ == "__main__":
    pass
