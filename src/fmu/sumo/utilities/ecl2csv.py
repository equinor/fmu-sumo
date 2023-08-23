"""Export with metadata"""
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
        exp_path = "Nothing produces"
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
    try:
        sim_specifics = config["ecl2csv"]
        datafiles, submods, options = read_config(sim_specifics)
        for datafile in datafiles:
            for submod in submods:
                export_csv(
                    datafile,
                    submod,
                    global_variables_file=config_path,
                    **options,
                )
    except KeyError:
        logger.warning(
            "No export from reservoir simulator, forgot to include ecl2csv in config?"
        )


# def filter_args_w_options(args):
#     """Filter the command line arguments to fit to function

#     Args:
#         args (argparse.NameSpace): the arguments are namespace

#     Returns:
#         dict: the filtered arguments
#     """
#     logger = logging.getLogger(__file__ + ".filter_args_w_options")
#     export_arrow = False
#     try:
#         export_arrow = args.arrow = True
#     except AttributeError:
#         logger.info("No option for export to arrow in %s", args.subcommand)
#     key_args = {
#         key: value
#         for key, value in vars(args).items()
#         if key in SUBMOD_DICT[args.subcommand]
#     }
#     if export_arrow:
#         key_args["arrow_convertor"] = SUBMOD_DICT[args.subcommand][
#             "arrow_convertor"
#         ]
#     logger.debug("Arguments filtered: %s", key_args)
#     return key_args


# def parse_args():
#     """Parse arguments"""
#     parser = argparse.ArgumentParser(
#         formatter_class=argparse.ArgumentDefaultsHelpFormatter,
#         description=(
#             "ecl2csv ("
#             # + __version__
#             + ") is a modified command line frontend to ecl2df. "
#         ),
#     )
#     # parser.add_argument(
#     #     "--datafile_path",
#     #     type=str,
#     #     help="path to simulator datafile for run",
#     # )
#     subparsers = parser.add_subparsers(
#         required=True,
#         dest="subcommand",
#         parser_class=argparse.ArgumentParser,
#     )
#     for submod in SUBMODULES:
#         subparser = subparsers.add_parser(submod)
#         importlib.import_module("ecl2df." + submod).fill_parser(subparser)
#     parser.add_argument(
#         "--config_path",
#         type=str,
#         help="fmu config file to enable exporting with metadata",
#         default="fmuconfig/output/global_variables.yml",
#     )
#     args = filter_args_w_options(parser.parse_args())

#     return args


# def export_csv_with_args(
#     args,
# ):
#     """Export datatypes from simulator from command line

#     Args:
#         args (argparse.NameSpace): Input arguments
#     """
#     logger = logging.getLogger(__file__ + ".export_csv_with_args")
#     export_csv(
#         args.DATAFILE, args.subcommand, global_variables_file, **key_args
#     )


# def main():
#     # export_csv_with_args(parse_args())
#     print(parse_args())


if __name__ == "__main__":
    main()
