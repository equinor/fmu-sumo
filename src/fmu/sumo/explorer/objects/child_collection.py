from fmu.sumo.explorer.objects.document_collection import DocumentCollection
from typing import List, Dict, Union
from sumo.wrapper import SumoClient


class ChildCollection(DocumentCollection):
    """Class for representing a collection of child objects in Sumo"""

    def __init__(self, type: str, sumo: SumoClient, case_id: str, query: Dict = None):
        self._case_id = case_id
        super().__init__(type, sumo, query)

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
    def operations(self) -> List[str]:
        """List of unique object aggregation operations"""
        return self._get_field_values("fmu.aggregation.operation.keyword")

    @property
    def stages(self) -> List[str]:
        """List of unique stages"""
        return self._get_field_values("fmu.context.stage.keyword")

    def _init_query(self, type: str, query: Dict = None) -> Dict:
        new_query = super()._init_query(type, query)
        case_filter = {
            "bool": {"must": [{"term": {"_sumo.parent_object.keyword": self._case_id}}]}
        }

        return self._utils.extend_query_object(new_query, case_filter)

    def _add_filter(
        self,
        name: Union[str, List[str]] = None,
        tagname: Union[str, List[str]] = None,
        iteration: Union[int, List[int]] = None,
        realization: Union[int, List[int]] = None,
        operation: Union[str, List[str]] = None,
        stage: Union[str, List[str]] = None,
    ):
        must = self._utils.build_terms(
            {
                "data.name.keyword": name,
                "data.tagname.keyword": tagname,
                "fmu.iteration.id": iteration,
                "fmu.realization.id": realization,
                "fmu.aggregation.operation.keyword": operation,
                "fmu.context.stage.keyword": stage,
            }
        )

        if len(must) > 0:
            return super()._add_filter({"bool": {"must": must}})

        return self._query
