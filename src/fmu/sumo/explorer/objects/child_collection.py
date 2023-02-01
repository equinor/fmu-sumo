from fmu.sumo.explorer.objects.document_collection import DocumentCollection
from typing import List, Dict, Union
from sumo.wrapper import SumoClient


class ChildCollection(DocumentCollection):
    """Class for representing a collection of child objects in Sumo"""

    def __init__(
        self, type: str, sumo: SumoClient, case_id: str, filter: List[Dict] = None
    ):
        self._case_id = case_id
        super().__init__(type, sumo, filter)

    def _init_base_filter(self, type: str, filters=None) -> List[Dict]:
        base_filter = super()._init_base_filter(type, filters)

        if filters is None:
            base_filter.append({"term": {"_sumo.parent_object.keyword": self._case_id}})

        return base_filter

    @property
    def names(self) -> List[str]:
        """List of unique object names"""
        return self._get_field_values("data.name.keyword")

    @property
    def tagnames(self) -> List[str]:
        """List of unqiue object tagnames"""
        return self._get_field_values("data.tagname.keyword")

    @property
    def iterations(self) -> List[int]:
        """List of unique object iteration ids"""
        return self._get_field_values("fmu.iteration.id")

    @property
    def realizations(self) -> List[int]:
        """List of unique object realization ids"""
        return self._get_field_values("fmu.realization.id")

    @property
    def aggregations(self) -> List[str]:
        """List of unique object aggregation operations"""
        return self._get_field_values("fmu.aggregation.operation.keyword")

    def _add_filter(
        self,
        name: Union[str, List[str]] = None,
        tagname: Union[str, List[str]] = None,
        iteration: Union[int, List[int]] = None,
        realization: Union[int, List[int]] = None,
        aggregation: Union[str, List[str]] = None,
    ):
        filter = {}

        if name is not None:
            filter["data.name.keyword"] = name

        if tagname is not None:
            filter["data.tagname.keyword"] = tagname

        if iteration is not None:
            filter["fmu.iteration.id"] = iteration

        if realization is not None:
            filter["fmu.realization.id"] = realization

        if aggregation is not None:
            filter["fmu.aggregation.operation"] = aggregation

        return super()._add_filter(filter)
