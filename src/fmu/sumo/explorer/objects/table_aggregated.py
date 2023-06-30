"""module containing class for table"""
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer.objects.table import Table


class AggregatedTable:
    """Class for representing an aggregated table in Sumo"""

    def __init__(
        self,
        case: Case,
        name: str,
        tag: str,
        iteration: str,
        aggregation: str = "collection",
    ) -> None:
        """Init of aggregated table

        Args:
            case (Sumo.Case): given case object
            name (str): name of table
            tag (str): name of tag
            iteration (str): name of interation
            aggregation (str, optional): aggregation type
        """
        self._sumo = case._sumo
        self._utils = Utils(self._sumo)
        self._case = case
        self._name = name
        self._tag = tag
        self._iteration = iteration
        self._aggregation = aggregation
        self._parameters = None
        self._collection = case.tables.filter(
            name=name,
            tagname=tag,
            iteration=iteration,
            aggregation=aggregation,
        )

    @property
    def columns(self):
        """Return column names

        Returns:
            list: the column names available
        """
        return self._collection.columns

    @property
    def parameters(self):
        """Return parameter set for iteration

        Returns:
            dict: parameters connected to iteration
        """
        if not self._parameters:
            must = self._utils.build_terms(
                {
                    "class.keyword": "table",
                    "_sumo.parent_object.keyword": self._case.uuid,
                    "data.name.keyword": self._name,
                    "data.tagname.keyword": self._tag,
                    "fmu.iteration.name.keyword": self._iteration,
                    "fmu.aggregation.operation.keyword": "collection",
                }
            )

            query = {
                "size": 1,
                "_source": ["fmu.iteration.parameters"],
                "query": {"bool": {"must": must}},
            }

            res = self._sumo.post("/search", json=query)
            doc = res.json()["hits"]["hits"][0]
            self._parameters = doc["_source"]["fmu"]["iteration"]["parameters"]

        return self._parameters

    @property
    async def parameters_async(self):
        """Return parameter set for iteration

        Returns:
            dict: parameters connected to iteration
        """
        if not self._parameters:
            must = self._utils.build_terms(
                {
                    "class.keyword": "table",
                    "_sumo.parent_object.keyword": self._case.uuid,
                    "data.name.keyword": self._name,
                    "data.tagname.keyword": self._tag,
                    "fmu.iteration.name.keyword": self._iteration,
                    "fmu.aggregation.operation.keyword": "collection",
                }
            )

            query = {
                "size": 1,
                "_source": ["fmu.iteration.parameters"],
                "query": {"bool": {"must": must}},
            }

            res = await self._sumo.post_async("/search", json=query)
            doc = res.json()["hits"]["hits"][0]
            self._parameters = doc["_source"]["fmu"]["iteration"]["parameters"]

        return self._parameters

    def __len__(self):
        return len(self._collection)

    def __getitem__(self, column) -> Table:
        try:
            return self._collection.filter(column=column)[0]
        except IndexError as i_ex:
            raise IndexError(
                f"Column: '{column}' does not exist, try again"
            ) from i_ex
