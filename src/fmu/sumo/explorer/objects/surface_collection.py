from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.surface import Surface
import xtgeo
from io import BytesIO
from typing import Union, List, Dict


class SurfaceCollection(ChildCollection):
    """Class for representing a collection of surface objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_id: str, filter: List[Dict] = None):
        super().__init__("surface", sumo, case_id, filter)
        self._aggregations = {}

    def __getitem__(self, index) -> Surface:
        doc = super().__getitem__(index)
        return Surface(self._sumo, doc)

    def _aggregate(self, operation: str) -> xtgeo.RegularSurface:
        if operation not in self._aggregations:
            must = self._base_filter
            objects = self._utils.get_objects(500, must, ["_id"])
            object_ids = List(map(lambda obj: obj["_id"], objects))

            res = self._sumo.post(
                "/aggregate",
                json={"operation": [operation], "object_ids": object_ids},
            )

            self._aggregations[operation] = xtgeo.surface_from_file(
                BytesIO(res.content)
            )

        return self._aggregations[operation]

    def filter(
        self,
        name: Union[str, List[str]] = None,
        tagname: Union[str, List[str]] = None,
        iteration: Union[int, List[int]] = None,
        realization: Union[int, List[int]] = None,
        aggregation: Union[str, List[str]] = None,
    ) -> "SurfaceCollection":
        filter = super()._add_filter(name, tagname, iteration, realization, aggregation)
        return SurfaceCollection(self._sumo, self._case_id, filter)

    def mean(self):
        return self._aggregate("mean")

    def min(self):
        return self._aggregate("min")

    def max(self):
        return self._aggregate("max")

    def std(self):
        return self._aggregate("std")

    def p10(self):
        return self._aggregate("p10")

    def p50(self):
        return self._aggregate("p50")

    def p90(self):
        return self._aggregate("p90")
