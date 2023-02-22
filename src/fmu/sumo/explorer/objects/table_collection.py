from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.table import Table
from typing import Union, List, Dict


class TableCollection(ChildCollection):
    """Class for representing a collection of table objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_uuid: str, query: Dict = None):
        super().__init__("table", sumo, case_uuid, query)

    def __getitem__(self, index) -> Table:
        doc = super().__getitem__(index)
        return Table(self._sumo, doc)

    @property
    def columns(self) -> List[str]:
        """List of unique column names"""
        return self._get_field_values("data.spec.columns.keyword")

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        column: Union[str, List[str], bool] = None,
    ) -> "TableCollection":
        """Filter tables

        Arguments:
            - name (Union[str, List[str], bool]): table name
            - tagname (Union[str, List[str], bool]): table tagname
            - iteration (Union[int, List[int], bool]): iteration id
            - realization Union[int, List[int], bool]: realization id
            - aggregation (Union[str, List[str], bool]): aggregation operation
            - stage (Union[str, List[str], bool]): context/stage

        Returns:
            A filtered TableCollection
        """

        query = super()._add_filter(
            name,
            tagname,
            iteration,
            realization,
            aggregation,
            stage,
            column=column,
        )
        return TableCollection(self._sumo, self._case_uuid, query)
