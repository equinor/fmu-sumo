from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection


OBSERVATION_FILTER = {
    "bool": {
        "must_not": [
            {"exists": {"field": "fmu.iteration.name.keyword"}},
            {"exists": {"field": "fmu.realization.id"}},
        ]
    }
}


class ObservationContext:
    def __init__(self, sumo: SumoClient, case_id: str) -> None:
        self._sumo = sumo
        self._case_id = case_id

    @property
    def surfaces(self) -> SurfaceCollection:
        return SurfaceCollection(self._sumo, self._case_id, OBSERVATION_FILTER)
