from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.polygons import Polygons
from typing import Union


class PolygonsCollection(ChildCollection):
    """Class for representing a collection of polygons objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_id: str, filter: list[dict] = None):
        super().__init__("polygons", sumo, case_id, filter)

    def __getitem__(self, index) -> Polygons:
        doc = super().__getitem__(index)
        return Polygons(self._sumo, doc)

    def filter(
        self,
        name: Union[str, list[str]] = None,
        tagname: Union[str, list[str]] = None,
        iteration: Union[int, list[int]] = None,
        realization: Union[int, list[int]] = None,
    ) -> "PolygonsCollection":
        filter = super()._add_filter(name, tagname, iteration, realization)
        return PolygonsCollection(self._sumo, self._case_id, filter)
