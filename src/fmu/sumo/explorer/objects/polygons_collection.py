"""Module containing class for colection of polygons """
from typing import Union, List, Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child_collection import ChildCollection
from fmu.sumo.explorer.objects.polygons import Polygons
from fmu.sumo.explorer.pit import Pit


class PolygonsCollection(ChildCollection):
    """Class for representing a collection of polygons objects in Sumo"""

    def __init__(
        self,
        sumo: SumoClient,
        case_uuid: str,
        query: Dict = None,
        pit: Pit = None,
    ):
        """
        Args:
            sumo (SumoClient): connection to Sumo
            case_uuid (str): parent case uuid
            query (dict): elastic query object
            pit (Pit): point in time
        """
        super().__init__("polygons", sumo, case_uuid, query, pit)

    def __getitem__(self, index) -> Polygons:
        doc = super().__getitem__(index)
        return Polygons(self._sumo, doc)

    async def getitem_async(self, index: int) -> Polygons:
        doc = await super().getitem_async(index)
        return Polygons(self._sumo, doc)

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        uuid: Union[str, List[str], bool] = None,
    ) -> "PolygonsCollection":
        """Filter polygons

        Args:
            name (Union[str, List[str], bool]): polygon name
            tagname (Union[str, List[str], bool]): polygon tagname
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            uuid (Union[str, List[str], bool]): polygons object uuid

        Returns:
            PolygonsCollection: A filtered PolygonsCollection
        """
        query = super()._add_filter(
            name=name,
            tagname=tagname,
            iteration=iteration,
            realization=realization,
            uuid=uuid,
        )

        return PolygonsCollection(
            self._sumo, self._case_uuid, query, self._pit
        )
