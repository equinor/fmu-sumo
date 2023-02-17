from fmu.sumo.explorer.objects.document_collection import DocumentCollection
from typing import List, Dict, Union
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.timefilter import TimeFilter


class ChildCollection(DocumentCollection):
    """Class for representing a collection of child objects in Sumo"""

    def __init__(
        self, type: str, sumo: SumoClient, case_uuid: str, query: Dict = None
    ):
        self._case_uuid = case_uuid
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
        """List of unique object iteration names"""
        return self._get_field_values("fmu.iteration.name.keyword")

    @property
    def realizations(self) -> List[int]:
        """List of unique object realization ids"""
        return self._get_field_values("fmu.realization.id")

    @property
    def aggregations(self) -> List[str]:
        """List of unique object aggregation operations"""
        return self._get_field_values("fmu.aggregation.operation.keyword")

    @property
    def stages(self) -> List[str]:
        """List of unique stages"""
        return self._get_field_values("fmu.context.stage.keyword")

    def _init_query(self, type: str, query: Dict = None) -> Dict:
        new_query = super()._init_query(type, query)
        case_filter = {
            "bool": {
                "must": [
                    {"term": {"_sumo.parent_object.keyword": self._case_uuid}}
                ]
            }
        }

        return self._utils.extend_query_object(new_query, case_filter)

    def _add_filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        time: TimeFilter = None,
    ):
        must = []
        must_not = []

        prop_map = {
            "data.name.keyword": name,
            "data.tagname.keyword": tagname,
            "fmu.iteration.name.keyword": iteration,
            "fmu.realization.id": realization,
            "fmu.aggregation.operation.keyword": aggregation,
            "fmu.context.stage.keyword": stage,
        }

        for prop in prop_map:
            value = prop_map[prop]

            if value is not None:
                if type(value) == bool:
                    if value:
                        must.append({"exists": {"field": prop}})
                    else:
                        must_not.append({"exists": {"field": prop}})
                else:
                    term = "terms" if type(value) == list else "term"
                    must.append({term: {prop: value}})

        query = {"bool": {}}

        if len(must) > 0:
            query["bool"]["must"] = must

        if len(must_not) > 0:
            query["bool"]["must_not"] = must_not

        if time:
            query = self._utils.extend_query_object(query, time.get_query())

        return super()._add_filter(query)
