"""Module containing class for collection of tables"""
from typing import Union, List, Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child_collection import ChildCollection
from fmu.sumo.explorer.objects.table import Table
from fmu.sumo.explorer.pit import Pit


class TableCollection(ChildCollection):
    """Class for representing a collection of table objects in Sumo"""

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
        super().__init__("table", sumo, case_uuid, query, pit)

    def __getitem__(self, index) -> Table:
        doc = super().__getitem__(index)
        return Table(self._sumo, doc)

    async def getitem_async(self, index: int) -> Table:
        doc = await super().getitem_async(index)
        return Table(self._sumo, doc)

    @property
    def columns(self) -> List[str]:
        """List of unique column names"""
        return self._get_field_values("data.spec.columns.keyword")

    @property
    async def columns_async(self) -> List[str]:
        """List of unique column names"""
        return await self._get_field_values_async("data.spec.columns.keyword")

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        column: Union[str, List[str], bool] = None,
        uuid: Union[str, List[str], bool] = None,
    ) -> "TableCollection":
        """Filter tables

        Arguments:
            name (Union[str, List[str], bool]): table name
            tagname (Union[str, List[str], bool]): table tagname
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            aggregation (Union[str, List[str], bool]): aggregation operation
            stage (Union[str, List[str], bool]): context/stage
            uuid (Union[str, List[str], bool]): table object uuid

        Returns:
            TableCollection: A filtered TableCollection
        """

        query = super()._add_filter(
            name=name,
            tagname=tagname,
            iteration=iteration,
            realization=realization,
            aggregation=aggregation,
            stage=stage,
            column=column,
            uuid=uuid,
        )
        return TableCollection(self._sumo, self._case_uuid, query, self._pit)
