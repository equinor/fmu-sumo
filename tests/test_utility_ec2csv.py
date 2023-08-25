"""Test utility ecl2csv"""
import sys
import os
from time import sleep
import logging
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pytest
from sumo.wrapper import SumoClient
import fmu.sumo.utilities.ecl2csv as sumo_ecl2csv
from fmu.sumo.uploader import CaseOnDisk, SumoConnection


REEK_ROOT = Path(__file__).parent / "data/reek"
REAL_PATH = "realization-0/iter-0/"
REEK_REAL = REEK_ROOT / REAL_PATH
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_REAL / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_REAL / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"


logging.basicConfig(
    level=logging.info, format=" %(name)s :: %(levelname)s :: %(message)s"
)
LOGGER = logging.getLogger(__file__)


def test_submodules_dict():
    """Test generation of submodule list"""
    sublist, submods = sumo_ecl2csv._define_submodules()
    LOGGER.info(submods)
    assert isinstance(sublist, tuple)
    assert isinstance(submods, dict)
    for submod_name, submod_dict in submods.items():
        LOGGER.info(submod_name)
        LOGGER.info(submod_dict)
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
    LOGGER.info(actual_path)
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
    LOGGER.info(actual_path)
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
    LOGGER.info(config_path)
    config = sumo_ecl2csv.yaml_load(config_path)["ecl2csv"]
    assert isinstance(config, (dict, bool))
    dfiles, submods, opts = sumo_ecl2csv.read_config(config)
    name = config_path.name
    checks = CHECK_DICT[name]
    LOGGER.info(config)
    LOGGER.info(dfiles)
    LOGGER.info(submods)
    LOGGER.info(opts)
    _assert_right_len(checks, "nrdatafile", dfiles, name)
    _assert_right_len(checks, "nrsubmods", submods, name)
    _assert_right_len(checks, "nroptions", opts, name)

    assert (
        opts["arrow"] == checks["arrow"]
    ), f"Wrong choice for arrow for {name}"


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_export_w_config(tmp_path, config_path):
    """Test function export with config"""
    # Make exec path, needs to be at real..-0/iter-0
    exec_path = tmp_path / REAL_PATH
    exec_path.mkdir(parents=True)
    # Symlink in case meta at root of run
    case_share_meta = "share/metadata/"
    (tmp_path / case_share_meta).mkdir(parents=True)
    case_meta_path = "share/metadata/fmu_case.yml"
    (tmp_path / case_meta_path).symlink_to(REEK_ROOT / case_meta_path)
    # Run tests from exec path to get metadata in ship shape
    os.chdir(exec_path)
    # The lines below is needed for test to work when definition of datafile
    #  not in config symlink to model folder, code autodetects
    sim_path = tmp_path / REAL_PATH / "eclipse"
    sim_path.mkdir(parents=True)
    (sim_path / "model").symlink_to(REEK_ECL_MODEL)
    # Symlink in config, this is also autodetected
    conf_path = tmp_path / REAL_PATH / "fmuconfig/output/"
    conf_path.mkdir(parents=True)
    (conf_path / config_path.name).symlink_to(config_path)
    # THE TEST
    sumo_ecl2csv.export_with_config(config_path)


@pytest.mark.parametrize(
    "input_args",
    ((), *[("help", sub) for sub in sumo_ecl2csv.SUBMODULES]),
)
def test_parse_args(mocker, input_args):
    """Test parse args

    Args:
        mocker (pytest.fixture): to mock command line like
        submod (str): name of submodule
    """
    commands = list(input_args)
    assert isinstance(commands, list)
    assert len(commands) == 2
    assert "help" in commands

    print(commands)
    mocker.patch("sys.argv", commands)
    print(sys.argv)
    # sumo_ecl2csv.parse_args()


def test_upload():
    """Test the upload function"""
    sumo_env = "dev"
    sumo = SumoClient(sumo_env)
    sumocon = SumoConnection(sumo_env)
    case_metadata_path = REEK_ROOT / "share/metadata/fmu_case.yml"
    LOGGER.info("This is the case metadata %s", case_metadata_path)

    case = CaseOnDisk(
        case_metadata_path=case_metadata_path,
        sumo_connection=sumocon,
        verbosity="DEBUG",
    )

    case_uuid = case.register()
    sumo_ecl2csv.upload(
        str(REEK_ROOT / "realization-0/iter-0/share/results/tables"),
        ["csv"],
        sumo_env,
    )
    # There has been instances when this fails, probably because of
    # some time delay, have introduced a little sleep to get it to be quicker
    sleep(2)
    results = sumo.get(
        "/search", query=f"fmu.case.uuid:{case_uuid} AND class:table", size=0
    )
    print(results["hits"])
    correct = 2
    returned = results["hits"]["total"]["value"]
    print("This is returned ", returned)
    assert (
        returned == correct
    ), f"Tried to upload {correct}, but only managed {returned}"
    path = f"/objects('{case_uuid}')"

    sumo.delete(path)


if __name__ == "__main__":
    pass
