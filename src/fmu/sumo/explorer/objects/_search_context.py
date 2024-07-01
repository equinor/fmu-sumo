import json
from typing import List, Dict, Union
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.timefilter import TimeFilter


def extend_query_object(self, old: Dict, new: Dict) -> Dict:
    """Extend query object

    Args:
        old (Dict): old query object
        new (Dict): new query object

    Returns:
        Dict: Extended query object
    """
    extended = json.loads(json.dumps(old))
    if new is not None:
        for key in new:
            if key in extended:
                if isinstance(new[key], dict):
                    extended[key] = self.extend_query_object(
                        extended[key], new[key]
                    )
                elif isinstance(new[key], list):
                    for val in new[key]:
                        if val not in extended[key]:
                            extended[key].append(val)
                else:
                    extended[key] = new[key]
            else:
                extended[key] = new[key]
    return extended


def _build_bucket_query(query, field):
    return {
        "size": 0,
        "query": query,
        "aggs": {
            f"{field}": {
                "composite": {
                    "size": 1000,
                    "sources": [{f"{field}": {"terms": {"field": field}}}],
                }
            }
        },
    }


def _set_after_key(query, field, after_key):
    if after_key is not None:
        query["aggs"][field]["composite"]["after"] = after_key
        pass
    return query


class Pit:
    def __init__(self, sumo: SumoClient, keepalive="5m"):
        self._sumo = sumo
        self._keepalive = keepalive
        self._id = None
        return

    def __enter__(self):
        res = self._sumo.post("/pit", params={"keep-alive": self._keepalive})
        self._id = res.json()["id"]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._id is not None:
            self._sumo.delete("/pit", params={"id": self._id})
            pass
        return False

    def __aenter__(self):
        res = self._sumo.post_async(
            "/pit", params={"keep-alive": self._keepalive}
        )
        self._id = res.json()["id"]
        return self

    def __aexit__(self, exc_type, exc_value, traceback):
        if self._id is not None:
            self._sumo.delete_async("/pit", params={"id": self._id})
            pass
        return False

    def stamp_query(self, query):
        query["pit"] = {"id": self._id, "keep_alive": self._keepalive}
        return query

    def update_from_result(self, result):
        self._id = result["pit_id"]
        return


class SearchContext:
    def __init__(
        self, sumo: SumoClient, query: Dict = None, select: List[str] = None
    ):
        self._sumo = sumo
        self._select = select
        self._field_values = {}
        return

    def _get_buckets(
        self,
        field: str,
        sort: List = None,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """

        query = _build_bucket_query(self._query, field)
        all_buckets = []
        after_key = None
        with Pit(self._sumo) as pit:
            while True:
                query = pit.stamp_query(
                    _set_after_key(query, field, after_key)
                )
                res = self._sumo.post("/search", json=query).json()
                pit.update_from_result(res)
                buckets = res["aggregations"][field]["buckets"]
                if len(buckets) == 0:
                    break
                after_key = res["aggregations"][field]["after_key"]
                buckets = [
                    {
                        "key": bucket["key"][field],
                        "doc_count": bucket["doc_count"],
                    }
                    for bucket in buckets
                ]
                all_buckets = all_buckets + buckets
                pass

        return all_buckets

    async def _get_buckets_async(
        self,
        field: str,
        sort: List = None,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """

        query = _build_bucket_query(self._query, field)
        all_buckets = []
        after_key = None
        async with Pit(self._sumo) as pit:
            while True:
                query = pit.stamp_query(
                    _set_after_key(query, field, after_key)
                )
                res = await self._sumo.post_async("/search", json=query)
                res = res.json()
                pit.update_from_result(res)
                buckets = res["aggregations"][field]["buckets"]
                if len(buckets) == 0:
                    break
                after_key = res["aggregations"][field]["after_key"]
                buckets = [
                    {
                        "key": bucket["key"][field],
                        "doc_count": bucket["doc_count"],
                    }
                    for bucket in buckets
                ]
                all_buckets = all_buckets + buckets
                pass

        return all_buckets

    def _get_field_values(
        self, field: str, key_as_string: bool = False
    ) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            key = "key_as_string" if key_as_string is True else "key"
            buckets = self._get_buckets(field)
            self._field_values[field] = list(
                map(lambda bucket: bucket[key], buckets)
            )

        return self._field_values[field]

    async def _get_field_values_async(
        self, field: str, key_as_string: bool = False
    ) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            key = "key_as_string" if key_as_string is True else "key"
            buckets = await self._get_buckets_async(field)
            self._field_values[field] = list(
                map(lambda bucket: bucket[key], buckets)
            )

        return self._field_values[field]

    @property
    def names(self) -> List[str]:
        """List of unique object names"""
        return self._get_field_values("data.name.keyword")

    @property
    async def names_async(self) -> List[str]:
        """List of unique object names"""
        return await self._get_field_values_async("data.name.keyword")

    @property
    def tagnames(self) -> List[str]:
        """List of unqiue object tagnames"""
        return self._get_field_values("data.tagname.keyword")

    @property
    async def tagnames_async(self) -> List[str]:
        """List of unqiue object tagnames"""
        return await self._get_field_values_async("data.tagname.keyword")

    @property
    def dataformats(self) -> List[str]:
        """List of unique data.format values"""
        return self._get_field_values("data.format.keyword")

    @property
    async def dataformats_async(self) -> List[str]:
        """List of unique data.format values"""
        return await self._get_field_values_async("data.format.keyword")

    @property
    def iterations(self) -> List[int]:
        """List of unique object iteration names"""
        return self._get_field_values("fmu.iteration.name.keyword")

    @property
    async def iterations_async(self) -> List[int]:
        """List of unique object iteration names"""
        return await self._get_field_values_async("fmu.iteration.name.keyword")

    @property
    def realizations(self) -> List[int]:
        """List of unique object realization ids"""
        return self._get_field_values("fmu.realization.id")

    @property
    async def realizations_async(self) -> List[int]:
        """List of unique object realization ids"""
        return await self._get_field_values_async("fmu.realization.id")

    @property
    def aggregations(self) -> List[str]:
        """List of unique object aggregation operations"""
        return self._get_field_values("fmu.aggregation.operation.keyword")

    @property
    async def aggregations_async(self) -> List[str]:
        """List of unique object aggregation operations"""
        return await self._get_field_values_async(
            "fmu.aggregation.operation.keyword"
        )

    @property
    def stages(self) -> List[str]:
        """List of unique stages"""
        return self._get_field_values("fmu.context.stage.keyword")

    @property
    async def stages_async(self) -> List[str]:
        """List of unique stages"""
        return await self._get_field_values_async("fmu.context.stage.keyword")

    @property
    def stratigraphic(self) -> List[str]:
        """List of unqiue object stratigraphic"""
        return self._get_field_values("data.stratigraphic")

    @property
    async def stratigraphic_async(self) -> List[str]:
        """List of unqiue object stratigraphic"""
        return await self._get_field_values_async("data.stratigraphic")

    @property
    def vertical_domain(self) -> List[str]:
        """List of unqiue object vertical domain"""
        return self._get_field_values("data.vertical_domain")

    @property
    async def vertical_domain_async(self) -> List[str]:
        """List of unqiue object vertical domain"""
        return await self._get_field_values_async("data.vertical_domain")

    @property
    def contents(self) -> List[str]:
        """List of unique contents"""
        return self._get_field_values("data.content.keyword")

    @property
    async def contents_async(self) -> List[str]:
        """List of unique contents"""
        return self._get_field_values_async("data.content.keyword")

    def _add_filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        dataformat: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        column: Union[str, List[str], bool] = None,
        time: TimeFilter = None,
        uuid: Union[str, List[str], bool] = None,
        stratigraphic: Union[str, List[str], bool] = None,
        vertical_domain: Union[str, List[str], bool] = None,
        content: Union[str, List[str], bool] = None,
        is_observation: bool = None,
        is_prediction: bool = None,
    ):
        must = []
        must_not = []

        prop_map = {
            "data.name.keyword": name,
            "data.tagname.keyword": tagname,
            "data.format": dataformat,
            "fmu.iteration.name.keyword": iteration,
            "fmu.realization.id": realization,
            "fmu.aggregation.operation.keyword": aggregation,
            "fmu.context.stage.keyword": stage,
            "data.spec.columns.keyword": column,
            "_id": uuid,
            "data.vertical_domain.keyword": vertical_domain,
            "data.content.keyword": content,
        }

        for prop, value in prop_map.items():
            if value is not None:
                if isinstance(value, bool):
                    if value:
                        must.append({"exists": {"field": prop}})
                    else:
                        must_not.append({"exists": {"field": prop}})
                else:
                    term = "terms" if isinstance(value, list) else "term"
                    must.append({term: {prop: value}})

        bool_prop_map = {
            "data.stratigraphic": stratigraphic,
            "data.is_observation": is_observation,
            "data.is_prediction": is_prediction,
        }
        for prop, value in bool_prop_map.items():
            if value is not None:
                must.append({"term": {prop: value}})

        query = {"bool": {}}

        if len(must) > 0:
            query["bool"]["must"] = must

        if len(must_not) > 0:
            query["bool"]["must_not"] = must_not

        if time:
            query = _extend_query_object(query, time._get_query())

        return query

    
