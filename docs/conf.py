#!/usr/bin/env python3

# flake8: noqa
# pylint: skip-file
import os
import sys
from pathlib import Path
import sphinx
from datetime import date

# directly inject into sys.path the path where all modules are installed
sys.path.insert(0, str(Path(sphinx.__file__).parent.parent))

# import fmu.sumo
# from fmu.sumo import explorer, uploader

# -- General configuration ---------------------------------------------

# The full version, including alpha/beta/rc tags.
# release = fmu.sumo.__version__

extensions = [
    "sphinxcontrib.apidoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.mathjax",
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

autodoc_mock_imports = [
    "ert",
    "ert_shared",
    "sumo",
    "xtgeo",
    "pandas",
]

os.environ["SPHINX_APIDOC_OPTIONS"] = (
    "members,show-inheritance,inherited-members"
)

apidoc_module_dir = "../src/fmu"
apidoc_output_dir = "apiref"
apidoc_excluded_paths = ["_version.py", "hook_implementations"]
apidoc_separate_modules = True
apidoc_module_first = True
apidoc_extra_args = ["-H", "API reference for fmu.sumo"]

autoclass_content = "both"

napoleon_include_special_with_doc = False

# The suffix of source filenames.
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "fmu.sumo"
current_year = date.today().year
# copyright = "Equinor " + str(current_year) + f" (fmu-sumo release {release})"


# Sort members by input order in classes
autodoc_member_order = "bysource"
autodoc_default_flags = ["members", "show-inheritance", "inherited-members"]

exclude_patterns = ["_build"]

pygments_style = "sphinx"

html_theme = "sphinx_rtd_theme"

# html_theme_options = {
#     "style_nav_header_background": "#C0C0C0",
# }

# Output file base name for HTML help builder.
htmlhelp_basename = "fmu-sumo"

html_logo = "_static/equinor-logo2.jpg"
