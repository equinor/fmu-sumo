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
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer.objects._document import Document
from fmu.sumo.explorer.objects._document_collection import DocumentCollection
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.objects.case_collection import CaseCollection
from fmu.sumo.explorer.objects.surface import Surface
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons import Polygons
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table import Table
from fmu.sumo.explorer.objects.table_collection import TableCollection
