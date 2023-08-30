#!/usr/bin/env python

"""Upload data to Sumo from FMU."""

import warnings
import os
import argparse
import logging
from pathlib import Path
from ert.shared.plugins.plugin_manager import hook_implementation
from ert.shared.plugins.plugin_response import plugin_response
from ert import ErtScript
from fmu.sumo.utilities.sim2sumo import parse_args, upload_with_config

LOGGER = logger = logging.getLogger(__name__)

DESCRIPTION = """SIM2SUMO uploads results from reservoir simulators directly to sumo.
Typically this is run per realization. The upload is controlled by a yaml config file

SIM2SUMO is implemented both as a FORWARD_JOB and can be called only from this context
 when running ERT."""

EXAMPLES = """

FOR full blown example add this to your ert config:

FORWARD_MODEL SIM2SUMO(<S2S_CONF_PATH>= <your config location>, <SUMO_ENV>=<sumo env to upload to>)
<S2S_CONFIG_PATH> refers to the config file that controls the upload. This file can be a regular
fmu config file, or a completely separate file, but needs to be in yaml format, and contain
a section called sim2sumo to produce any results. Defaults to fmuconfig/output/global_variables.yml
<SUMO_ENV> refers to the sumo environment to upload to. Defaults to prod

For minimum amount of clutter in your ert config utilize the defaults. This means that if you
add section sim2sumo to the fmu config file, and store it at the recommendation and upload to
the prod environment for sumo then your call in the ert config can be reduced to

FORWARD_MODEL SIM2SUMO."""


class Sim2Sumo(ErtScript):
    """A class with a run() function that can be registered as an ERT plugin.

    This is used for the ERT workflow context."""

    # pylint: disable=too-few-public-methods
    def run(self):
        # pylint: disable=no-self-use
        """Parse with a simplified command line parser, for ERT only,
        call sumo_upload_main()"""
        logger.debug("Calling run() on Sim2Sumo")
        args = parse_args()

        upload_with_config(args.config_path, args.env)


# @hook_implementation
# def legacy_ertscript_workflow(config):
#     """Hook the Sim2Sumo class into ERT with the name SIM2SUMO,
#     and inject documentation"""
#     workflow = config.add_workflow(Sim2Sumo, "SIM2SUMO")
#     workflow.parser = _get_parser
#     workflow.description = DESCRIPTION
#     workflow.examples = EXAMPLES
#     workflow.category = "export"


def main():
    """Main function, to be executed as console script"""
    args = parse_args()
    upload_with_config(args.config_path, args.env)


@hook_implementation
@plugin_response(plugin_name="SIM2SUMO")
def job_documentation(job_name):
    """Add job documentation for forward model

    Args:
        job_name (str): name of job

    Returns:
        dict: the documentation to be provided
    """
    if job_name != "SIM2SUMO":
        return None

    return {
        "description": DESCRIPTION,
        "examples": EXAMPLES,
        "category": "export",
    }
