from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table_collection import TableCollection
from fmu.sumo.explorer.objects.document import Document
from typing import Dict


class Case(Document):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        super().__init__(metadata)
        self._sumo = sumo
        self._surfaces = SurfaceCollection(self._sumo, self.id)
        self._polygons = PolygonsCollection(self._sumo, self.id)
        self._tables = TableCollection(self._sumo, self.id)

    @property
    def name(self):
        return self._get_property(["fmu", "case", "name"])

    @property
    def status(self):
        return self._get_property(["_sumo", "status"])

    @property
    def user(self):
        return self._get_property(["fmu", "case", "user", "id"])

    @property
    def asset(self):
        return self._get_property(["access", "asset", "name"])

    @property
    def field(self):
        fields = self._get_property(["masterdata", "smda", "field"])
        return fields[0]["identifier"]

    @property
    def surfaces(self):
        """Case surfaces"""
        return self._surfaces

    @property
    def polygons(self):
        """Case polygons"""
        return self._polygons

    @property
    def tables(self):
        """Case tables"""
        return self._tables
