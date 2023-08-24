"""Export with metadata"""
import re
from typing import Tuple
from pathlib import Path
import logging
import importlib
import argparse
from inspect import signature
import pandas as pd
import ecl2df
from pyarrow import Table
from fmu.config.utilities import yaml_load
from fmu.dataio import ExportData
from fmu.sumo.uploader.scripts.sumo_upload import sumo_upload_main

logging.basicConfig(level=logging.DEBUG)


def _define_submodules():  #  -> Tuple(tuple, dict):
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
        submodules[submod] = {"extract": func}
        submodules[submod]["options"] = tuple(
            name
            for name in signature(func).parameters.keys()
            if name not in ["deck", "eclfiles"]
        )
        submodules[submod]["doc"] = func.__doc__
        try:
            submodules[submod]["arrow_convertor"] = importlib.import_module(
                "ecl2df." + submod
            )._df2pyarrow
        except AttributeError:
            logger.info(
                "No premade function for converting to arrow in %s",
                submod_path,
            )
        logger.debug("Assigning %s to %s", submodules[submod], submod)

    logger.debug(
        "Returning the submodule names as a list: %s ", submodules.keys()
    )
    logger.debug(
        "Returning the submodules extra args as a dictionary: %s ", submodules
    )

    return tuple(submodules.keys()), submodules


SUBMODULES, SUBMOD_DICT = _define_submodules()


def give_name(datafile_path: str) -> str:
    """Return name to assign in metadata

    Args:
        datafile_path (str): path to the simulator datafile

    Returns:
        str: derived name
    """
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    return base_name


def convert_to_arrow(frame):
    """Convert pd.DataFrame to arrow

    Args:
        frame (pd.DataFrame): the frame to convert

    Returns:
        pa.Table: the converted dataframe
    """
    table = Table.from_pandas(frame)
    return table


def get_dataframe(
    datafile_path: str, submod: str, print_help=False, **kwargs
) -> pd.DataFrame:
    """Fetch dataframe from simulator results

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with
        kwargs (dict): other options

    Returns:
        pd.DataFrame: the extracted data
    """
    logger = logging.getLogger(__file__ + ".get_dataframe")
    extract_df = SUBMOD_DICT[submod]["extract"]
    arrow = kwargs.get("arrow", False)
    frame = None
    if print_help:
        print(SUBMOD_DICT[submod]["doc"])
    else:
        right_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in SUBMOD_DICT[submod]
        }
        logger.debug("Exporting with arguments %s", right_kwargs)
        try:
            frame = extract_df(ecl2df.EclFiles(datafile_path), **right_kwargs)
            if arrow:
                try:
                    frame = kwargs["arrow_convertor"](frame)
                except KeyError:
                    logger.debug("No arrow convertor defined for %s", submod)
                    frame = convert_to_arrow(frame)

        except TypeError:
            logger.warning(
                "Couldn't produce results!! Most likely something with you call"
            )
        except FileNotFoundError:
            logger.warning(
                "Cannot produce results!!, most likely no results present"
            )
    return frame


def check_options(submod, key_args):
    """Check keyword args against possible optiopns

    Args:
        submod (str): the submodule to use from ecl2df
        key_args (dict): the keyword arguments supplied

    Raises:
        KeyError: if key(s) in key_args not available for submodule
    """
    unknowns = [
        key
        for key in key_args.keys()
        if key not in SUBMOD_DICT[submod]["options"]
    ]
    if len(unknowns) > 0:
        raise KeyError(
            (
                f"these unknown options are included in call {unknowns}\n"
                + f"please read the docs: \n {SUBMOD_DICT[submod]['doc']}"
            )
        )


def export_csv(
    datafile_path: str,
    submod: str,
    global_variables_file="fmuconfig/output/global_variables.yml",
    **kwargs,
) -> str:
    """Export csv file with specified datatype

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with

    Returns:
        str: path of export
    """
    logger = logging.getLogger(__file__ + ".export_csv")
    # check_options(submod, kwargs)
    frame = get_dataframe(datafile_path, submod, **kwargs)
    if frame is not None:
        logger.debug("Reading global variables from %s", global_variables_file)
        cfg = yaml_load(global_variables_file)
        exp = ExportData(
            config=cfg,
            name=give_name(datafile_path),
            tagname=submod,
        )
        exp_path = exp.export(frame)
    else:
        exp_path = "Nothing produced"
    return exp_path


def read_config(config):
    """Read config settings

    Args:
        config (dict): the settings for export of simulator results

    Returns:
        tuple: datafiles as list, submodules to use as list, and options as kwargs
    """
    # datafile can be read as list, or string which can be either folder or filepath
    logger = logging.getLogger(__file__ + ".read_config")
    if isinstance(config, bool):
        config = {}
    datafile = config.get("datafile", "eclipse/model/")
    if isinstance(datafile, str):
        datafile_posix = Path(datafile)
        if datafile_posix.is_dir():
            datafiles = list(datafile_posix.glob("*.DATA"))
        else:
            datafiles = [datafile]
    else:
        datafiles = datafile
    try:
        submods = config["datatypes"]
    except KeyError:
        submods = SUBMODULES
    try:
        options = config["options"]
    except KeyError:
        logger.info("No special options selected")
        options = {}
    options["arrow"] = options.get("arrow", False)
    return datafiles, submods, options


def export_with_config(config_path):
    """Export several datatypes with yaml config file

    Args:
        config_path (str): path to existing yaml file
    """
    logger = logging.getLogger(__file__ + ".export_w_config")
    config = yaml_load(config_path)
    export_path = None
    count = 0
    suffixes = set()
    try:
        sim_specifics = config["ecl2csv"]
        datafiles, submods, options = read_config(sim_specifics)
        for datafile in datafiles:
            for submod in submods:
                export_path = export_csv(
                    datafile,
                    submod,
                    global_variables_file=config_path,
                    **options,
                )
                count += 1
                export_path = Path(export_path)
                suffixes.add(export_path.suffix)
    except KeyError:
        logger.warning(
            "No export from reservoir simulator, forgot to include ecl2csv in config?"
        )
    export_folder = str(export_path.parent)
    logger.info("Exported %i files to %s", count, export_folder)
    return export_folder, suffixes


def upload(upload_folder, suffixes, env="prod", threads=5, start_del="real"):
    """Upload to sumo

    Args:
        upload_folder (str): folder to upload from
        suffixes (set, list, tuple): suffixes to include in upload
        env (str, optional): sumo environment to upload to. Defaults to "prod".
        threads (int, optional): Threads to use in upload. Defaults to 5.
    """
    logger = logging.getLogger(__file__ + ".upload")
    case_path = Path(re.sub(rf"\/{start_del}.*", "", upload_folder))
    logger.info("Case to upload from %s", case_path)
    case_meta_path = case_path / "share/metadata/fmu_case.yml"
    logger.info("Case meta object %s", case_meta_path)
    for suffix in suffixes:
        logger.info(suffix)
        upload_search = f"{upload_folder}/*.{suffix}"
        logger.info("Upload folder %s", upload_search)
        sumo_upload_main(
            case_path,
            upload_search,
            env,
            case_meta_path,
            threads,
        )
        logger.debug("Uploaded")


def parse_args():
    """Parse arguments for command line tool

    Returns:
        argparse.NameSpace: the arguments parsed
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Parsing input to control export of simulator data",
    )
    parser.add_argument(
        "--global_config",
        type=str,
        help="config that controls the export",
        default="fmuconfig/output/global_variables.yml",
    )
    parser.add_argument(
        "--env",
        type=str,
        help="Which sumo environment to upload to",
        default="prod",
    )
    parser.add_argument(
        "--help_on",
        type=str,
        help=(
            "Use this to get documentation of one of the datatypes to upload\n"
            + f"valid options are \n{', '.join(SUBMODULES)}"
        ),
    )
    return parser.parse_args()


def give_help(submod, only_general=False):
    """Give descriptions of variables available for submodule

    Args:
        submod (str): submodule

    Returns:
        str: description of submodule input
    """
    general_info = """
    This utility uses the library ecl2csv, but uploads directly to sumo. Required options are:
    A config file in yaml format, where you specifiy the variables to extract. What is required
    is a keyword in the config saying sim2sumo. under there you have three optional arguments:
    * datafile: this can be a string, a list, or it can be absent altogether
    * datatypes: this needs to be a list, or non existent
    * options: The options are listed below in the orignal documentation from ecl2csv. The eclfiles
               option is replaced with what is under datafile

    """
    if only_general:
        text_to_return = general_info
    else:
        try:
            text_to_return = general_info + SUBMOD_DICT[submod]["doc"]
        except KeyError:
            text_to_return = f"subtype {submod} does not exist"

    return text_to_return


def main():
    """Main function to be called"""
    args = parse_args()
    print(args.help_on)
    if args.help_on is not None:
        print(give_help(args.help_on))
    else:
        upload_folder, suffixes = export_with_config(args.config_path)
        upload(upload_folder, suffixes, args.env)


if __name__ == "__main__":
    main()
