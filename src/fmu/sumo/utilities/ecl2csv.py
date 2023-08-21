"""Export with metadata"""
from pathlib import Path
import logging
import importlib
import argparse
from inspect import signature, Parameter
import pandas as pd
import ecl2df
from fmu.config.utilities import yaml_load
from fmu.dataio import ExportData


def define_submodules():
    """Fetch all submodules

    Returns:
        list: list of submodules
    """

    logger = logging.getLogger(__file__ + "define_submodules")
    package_path = Path(ecl2df.__file__).parent

    submodules = {}
    for submod_path in package_path.glob("*.py"):
        submod = str(submod_path.name.replace(".py", ""))
        try:
            func = importlib.import_module("ecl2df." + submod).df
        except AttributeError:
            logger.debug("No df function in %s", submod_path)
            continue
        submodules[submod] = tuple(signature(func).parameters.keys())
        logger.debug("Assigning %s to %s", submodules[submod], submod)

    logger.debug(submodules)
    return tuple(submodules.keys()), submodules


SUBMODULES, SUBMOD_DICT = define_submodules()


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


def get_dataframe(eclpath: str, submod: str, **kwargs) -> pd.DataFrame:
    """Fetch dataframe from eclipse results

    Args:
        eclpath (str): the path to the eclipse datafile
        submod (str): the name of the submodule to extract with
        kwargs (dict): other options

    Returns:
        pd.DataFrame: the extracted data
    """
    extract_df = importlib.import_module("ecl2df." + submod).df
    right_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key in SUBMOD_DICT[submod]
    }
    return extract_df(ecl2df.EclFiles(eclpath), **right_kwargs)


def export_csv(
    eclpath: str,
    submod: str,
    global_variables_file="fmuconfig/output/global_variables.yml",
    **kwargs,
) -> str:
    """Export csv file with specified datatype

    Args:
        eclpath (str): the path to the eclipse datafile
        submod (str): the name of the submodule to extract with

    Returns:
        str: path of export
    """
    frame = get_dataframe(eclpath, submod, **kwargs)
    logger = logging.getLogger(__file__ + "export_csv")
    logger.debug("Reading global variables from %s", global_variables_file)
    cfg = yaml_load(global_variables_file)
    exp = ExportData(
        config=cfg,
        name=give_name(eclpath),
        tagname=submod,
    )
    return exp.export(frame)


def parse_args():
    """Parse arguments"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "ecl2csv ("
            # + __version__
            + ") is a modified command line frontend to ecl2df. "
        ),
    )
    # parser.add_argument(
    #     "--datafile_path",
    #     type=str,
    #     help="path to eclipse datafile for run",
    # )
    subparsers = parser.add_subparsers(
        required=True,
        dest="subcommand",
        parser_class=argparse.ArgumentParser,
    )
    for submod in SUBMODULES:
        subparser = subparsers.add_parser(submod)
        importlib.import_module("ecl2df." + submod).fill_parser(subparser)
    parser.add_argument(
        "--config_path",
        type=str,
        help="fmu config file to enable exporting with metadata",
        default="fmuconfig/output/global_variables.yml",
    )
    args = parser.parse_args()
    return vars(args)


def export_csv_from_command_line():
    """Execute from command line"""
    submod = "summary"
    args = parse_args()
    export_csv(
        args.eclpath,
        submod,
        global_variables_file="fmuconfig/output/global_variables.yml",
    )


def main():
    parse_args()
