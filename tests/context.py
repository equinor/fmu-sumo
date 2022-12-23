"""context  pytest"""
import sys
from pathlib import Path


def add_path():
    """Way to add package path to sys.path for testing"""
    # Adapted from https://docs.python-guide.org/writing/structure/
    # Turned into function because the details here didn't work
    package_path = str(Path(__file__).parent.absolute() / "../src/")
    while package_path in sys.path:
        sys.path.remove(package_path)
    sys.path.insert(0, package_path)


add_path()

from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as ut
from fmu.sumo.explorer._document_collection import DocumentCollection
from fmu.sumo.explorer._case import Case
