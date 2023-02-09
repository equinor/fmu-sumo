from fmu.sumo.explorer.objects.document_collection import DocumentCollection
from typing import List, Dict, Union, Tuple
from sumo.wrapper import SumoClient

TIMESTAMP_QUERY = {
    "bool": {
        "must": [{"exists": {"field": "data.time.t0"}}],
        "must_not": [{"exists": {"field": "data.time.t1"}}],
    }
}


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
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[int, List[int], bool] = None,
        realization: Union[int, List[int], bool] = None,
        operation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        interval: Union[Tuple[str], bool] = None,
        timestamp: Union[str, List[str], bool] = None,
    ):
        must = []
        must_not = []

        prop_map = {
            "data.name.keyword": name,
            "data.tagname.keyword": tagname,
            "fmu.iteration.id": iteration,
            "fmu.realization.id": realization,
            "fmu.aggregation.operation.keyword": operation,
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

        if interval is not None:
            if type(interval) == list:
                count = len(interval)
                if count > 2 or count < 2:
                    raise Exception(
                        f"Interval tuple expected two elements, got {count}"
                    )

            if type(interval) == bool:
                if interval:
                    must.append({"exists": {"field": "data.time.t0"}})
                    must.append({"exists": {"field": "data.time.t1"}})
                else:
                    must_not.append({"exists": {"field": "data.time.t1"}})
            else:
                must.append({"term": {"data.time.t0.value": interval[0]}})
                must.append({"term": {"data.time.t1.value": interval[1]}})

        if timestamp is not None:
            if type(timestamp) == bool:
                if timestamp:
                    must.append({"exists": {"field": "data.time.t0"}})
                    must_not.append({"exists": {"field": "data.time.t1"}})
                else:
                    must_not.append(
                        {
                            "bool": {
                                "must": [{"exists": {"field": "data.time.t0"}}],
                                "must_not": [
                                    {"exists": {"field": "data.time.t1"}},
                                ],
                            }
                        }
                    )
            else:
                must.append({"term": {"data.time.t0.value": timestamp}})
                must_not.append({"exists": {"field": "data.time.t1"}})

        query = {"bool": {}}

        if len(must) > 0:
            query["bool"]["must"] = must

        if len(must_not) > 0:
            query["bool"]["must_not"] = must_not

        return super()._add_filter(query)
