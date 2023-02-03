from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.document import Document
from fmu.sumo.explorer.contexts.realization import RealizationContext
from fmu.sumo.explorer.contexts.aggregation import AggregationContext
from fmu.sumo.explorer.contexts.observation import ObservationContext
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table_collection import TableCollection
from typing import Dict


class Case(Document):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        super().__init__(metadata)
        self._sumo = sumo

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
    def realization(self):
        return RealizationContext(self._sumo, self._id)

    @property
    def aggregation(self):
        return AggregationContext(self._sumo, self._id)

    @property
    def observation(self):
        return ObservationContext(self._sumo, self._id)

    @property
    def surfaces(self):
        return SurfaceCollection(self._sumo, self._id)

    @property
    def polygons(self):
        return PolygonsCollection(self._sumo, self._id)

    @property
    def tables(self):
        return TableCollection(self._sumo, self._id)
