"""Module containing class for colection of dictionaries """
from typing import Union, List, Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child_collection import ChildCollection
from fmu.sumo.explorer.pit import Pit
from fmu.sumo.explorer.objects.dictionary import Dictionary


class DictionaryCollection(ChildCollection):
    """Class for representing a collection of dictionaries objects in Sumo"""

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
        super().__init__("dictionary", sumo, case_uuid, query, pit)

    def __getitem__(self, index) -> Dictionary:
        doc = super().__getitem__(index)
        return Dictionary(self._sumo, doc)

    async def getitem_async(self, index: int) -> Dictionary:
        doc = await super().getitem_async(index)
        return Dictionary(self._sumo, doc)

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        uuid: Union[str, List[str], bool] = None,
    ) -> "DictionaryCollection":
        """Filter dictionaries

        Args:
            name (Union[str, List[str], bool]): polygon name
            tagname (Union[str, List[str], bool]): polygon tagname
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            uuid (Union[str, List[str], bool]): polygons object uuid

        Returns:
            DictionaryCollection: A filtered DictionaryCollection
        """
        query = super()._add_filter(
            name=name,
            tagname=tagname,
            iteration=iteration,
            realization=realization,
            aggregation=aggregation,
            stage=stage,
            uuid=uuid,
        )

        return DictionaryCollection(
            self._sumo, self._case_uuid, query, self._pit
        )
