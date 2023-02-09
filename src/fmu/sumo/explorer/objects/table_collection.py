from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.table import Table
from typing import Union, List, Dict


class TableCollection(ChildCollection):
    """Class for representing a collection of table objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_id: str, query: Dict = None):
        super().__init__("table", sumo, case_id, query)

    def __getitem__(self, index) -> Table:
        doc = super().__getitem__(index)
        return Table(self._sumo, doc)

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[int, List[int], bool] = None,
        realization: Union[int, List[int], bool] = None,
        operation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
    ) -> "TableCollection":
        query = super()._add_filter(
            name, tagname, iteration, realization, operation, stage
        )
        return TableCollection(self._sumo, self._case_id, query)
