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
    def name(self) -> str:
        return self._get_property(["fmu", "case", "name"])

    @property
    def status(self) -> str:
        return self._get_property(["_sumo", "status"])

    @property
    def user(self) -> str:
        return self._get_property(["fmu", "case", "user", "id"])

    @property
    def asset(self) -> str:
        return self._get_property(["access", "asset", "name"])

    @property
    def field(self) -> str:
        fields = self._get_property(["masterdata", "smda", "field"])
        return fields[0]["identifier"]

    @property
    def realization(self) -> RealizationContext:
        return RealizationContext(self._sumo, self._id)

    @property
    def aggregation(self) -> AggregationContext:
        return AggregationContext(self._sumo, self._id)

    @property
    def observation(self) -> ObservationContext:
        return ObservationContext(self._sumo, self._id)

    @property
    def surfaces(self) -> SurfaceCollection:
        return SurfaceCollection(self._sumo, self._id)

    @property
    def polygons(self) -> PolygonsCollection:
        return PolygonsCollection(self._sumo, self._id)

    @property
    def tables(self) -> TableCollection:
        return TableCollection(self._sumo, self._id)
