"""Export with metadata"""
from pathlib import Path
import logging
import importlib
from inspect import signature, Parameter
import pandas as pd
import ecl2df
from fmu.config.utilities import yaml_load
from fmu.dataio import ExportData

UNWANTEDS = ["inferdims", "__init__", "version", "ecl2csv", "common"]


def define_submodules():
    """Fetch all submodules

    Returns:
        list: list of submodules
    """
    package_path = Path(ecl2df.__file__).parent

    # submodules = [
    #     str(submod.name).replace(".py", "")
    #     for submod in list(submod_path.glob("*.py"))
    # ]
    logger = logging.getLogger(__file__ + "define_submodules")
    submodules = []
    for submod_path in package_path.glob("*.py"):
        submod = str(submod_path.name.replace(".py", ""))
        try:
            func = importlib.import_module("ecl2df." + submod).df
        except AttributeError:
            logger.debug("No df function in %s", submod_path)
            continue
        sig_items = signature(func).parameters.items()
        print(sig_items)

        logger.debug(
            "%s has the following parameters %s", func.__name__, sig_items
        )
        submodules.append(submod)

    # submodules = [
    #     "summary",
    #     "rft",
    #     "satfunc",
    #     "grid",
    #     "faults",
    #     "pillars",
    #     "pvt",
    #     "trans",
    #     "nnc",
    #     "compdat",
    #     "gruptree",
    #     "equil",
    # ]
    logger.debug(submodules)
    return submodules


def give_name(eclpath: str) -> str:
    """Return name to assign in metadata

    Args:
        eclpath (str): path to the eclipse datafile

    Returns:
        str: derived name
    """
    eclpath_posix = Path(eclpath)
    base_name = eclpath_posix.name.replace(eclpath_posix.suffix, "")
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    return base_name


def get_dataframe(eclpath: str, submod: str) -> pd.DataFrame:
    """Fetch dataframe from eclipse results

    Args:
        eclpath (str): the path to the eclipse datafile
        submod (str): the name of the submodule to extract with

    Returns:
        pd.DataFrame: the extracted data
    """
    extract_df = importlib.import_module("ecl2df." + submod).df
    return extract_df(ecl2df.EclFiles(eclpath))


def export_csv(
    eclpath: str,
    submod: str,
    global_variables_file="fmuconfig/output/global_variables.yml",
) -> str:
    """Export csv file with specified datatype

    Args:
        eclpath (str): the path to the eclipse datafile
        submod (str): the name of the submodule to extract with

    Returns:
        str: path of export
    """
    frame = get_dataframe(eclpath, submod)
    logger = logging.getLogger(__file__ + "export_csv")
    logger.debug("Reading global variables from %s", global_variables_file)
    cfg = yaml_load(global_variables_file)
    exp = ExportData(
        config=cfg,
        name=give_name(eclpath),
        tagname=submod,
    )
    return exp.export(frame)
