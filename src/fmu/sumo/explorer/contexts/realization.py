from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table_collection import TableCollection


REALIZATION_FILTER = {
    "bool": {
        "must": [
            {"exists": {"field": "fmu.iteration.name.keyword"}},
            {"exists": {"field": "fmu.realization.id"}},
        ]
    }
}


class RealizationContext:
    def __init__(self, sumo: SumoClient, case_id: str) -> None:
        self._sumo = sumo
        self._case_id = case_id

    @property
    def surfaces(self) -> SurfaceCollection:
        return SurfaceCollection(self._sumo, self._case_id, REALIZATION_FILTER)

    @property
    def polygons(self) -> PolygonsCollection:
        return PolygonsCollection(self._sumo, self._case_id, REALIZATION_FILTER)

    @property
    def tables(self) -> TableCollection:
        return TableCollection(self._sumo, self._case_id, REALIZATION_FILTER)
