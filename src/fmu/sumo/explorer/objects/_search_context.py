import json
from typing import List, Dict, Tuple
from datetime import datetime
from sumo.wrapper import SumoClient
import fmu.sumo.explorer.objects as objects
from fmu.sumo.explorer.cache import LRUCache


def _gen_filter_none():
    def _fn(value):
        return None, None

    return _fn


def _gen_filter_id():
    def _fn(value):
        if value is None:
            return None, None
        else:
            return {
                "ids": {
                    "values": value if isinstance(value, list) else [value]
                }
            }, None

    return _fn


def _gen_filter_gen(attr):
    def _fn(value):
        if value is None:
            return None, None
        elif value is True:
            return {"exists": {"field": attr}}, None
        elif value is False:
            return None, {"exists": {"field": attr}}
        elif isinstance(value, list):
            return {"terms": {attr: value}}, None
        else:
            return {"term": {attr: value}}, None

    return _fn


def _gen_filter_name():
    def _fn(value):
        if value is None:
            return None, None
        else:
            return {
                "bool": {
                    "minimum_should_match": 1,
                    "should": [
                        {"term": {"data.name.keyword": value}},
                        {
                            "bool": {
                                "must": [
                                    {"term": {"class.keyword": "case"}},
                                    {"term": {"fmu.case.name.keyword": value}},
                                ]
                            }
                        },
                    ],
                }
            }, None

    return _fn


def _gen_filter_time():
    def _fn(value):
        if value is None:
            return None, None
        else:
            return value._get_query(), None

    return _fn


def _gen_filter_bool(attr):
    def _fn(value):
        if value is None:
            return None, None
        else:
            return {"term": {attr: value}}, None

    return _fn


def _gen_filter_complex():
    def _fn(value):
        if value is None:
            return None, None
        else:
            return value, None

    return _fn


filters = {
    "id": _gen_filter_id(),
    "cls": _gen_filter_gen("class.keyword"),
    "time": _gen_filter_time(),
    "name": _gen_filter_name(),
    "uuid": _gen_filter_gen("fmu.case.uuid.keyword"),
    "tagname": _gen_filter_gen("data.tagname.keyword"),
    "dataformat": _gen_filter_gen("data.format.keyword"),
    "iteration": _gen_filter_gen("fmu.iteration.name.keyword"),
    "realization": _gen_filter_gen("fmu.realization.id"),
    "aggregation": _gen_filter_gen("fmu.aggregation.operation.keyword"),
    "stage": _gen_filter_gen("fmu.context.stage.keyword"),
    "column": _gen_filter_gen("data.spec.columns.keyword"),
    "vertical_domain": _gen_filter_gen("data.vertical_domain.keyword"),
    "content": _gen_filter_gen("data.content.keyword"),
    "status": _gen_filter_gen("_sumo.status.keyword"),
    "user": _gen_filter_gen("fmu.user.keyword"),
    "asset": _gen_filter_gen("access.asset.name.keyword"),
    "field": _gen_filter_gen("masterdata.smda.field.keyword"),
    "stratigraphic": _gen_filter_bool("data.stratigraphic"),
    "is_observation": _gen_filter_bool("data.is_observation"),
    "is_prediction": _gen_filter_bool("data.is_prediction"),
    "complex": _gen_filter_complex(),
    "has": _gen_filter_none(),
}


def _build_bucket_query(query, field):
    return {
        "size": 0,
        "query": query,
        "aggs": {
            f"{field}": {
                "composite": {
                    "size": 10000,
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


def _set_search_after(query, after):
    if after is not None:
        query["search_after"] = after
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

    async def __aenter__(self):
        res = await self._sumo.post_async(
            "/pit", params={"keep-alive": self._keepalive}
        )
        self._id = res.json()["id"]
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._id is not None:
            await self._sumo.delete_async("/pit", params={"id": self._id})
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
        self,
        sumo: SumoClient,
        must: List = [],
        must_not: List = [],
        select: List[str] = None,
    ):
        self._sumo = sumo
        self._must = must[:]
        self._must_not = must_not[:]
        self._select = select
        self._field_values = {}
        self._hits = None
        self._cache = LRUCache(capacity=200)
        self._length = None
        return

    @property
    def _query(self):
        return {"bool": {"must": self._must, "must_not": self._must_not}}

    def _to_sumo(self, obj):
        cls = obj["_source"]["class"]
        constructor = {
            "case": objects.Case,
            "cube": objects.Cube,
            "dictionary": objects.Dictionary,
            "polygons": objects.Polygons,
            "surface": objects.Surface,
            "table": objects.Table,
        }.get(cls)
        assert constructor is not None
        return constructor(self._sumo, obj)

    def __len__(self):
        if self._hits is not None:
            return len(self._hits)
        if self._length is None:
            query = {"query": self._query, "size": 0, "track_total_hits": True}
            res = self._sumo.post("/search", json=query).json()
            self._length = res["hits"]["total"]["value"]
        return self._length

    async def length_async(self):
        if self._hits is not None:
            return len(self._hits)
        if self._length is None:
            query = {"query": self._query, "size": 0, "track_total_hits": True}
            res = (await self._sumo.post_async("/search", json=query)).json()
            self._length = res["hits"]["total"]["value"]
        return self._length

    def __search_all(self, query, size=1000, select=False, maxhits=None):
        all_hits = []
        query = {
            "query": query,
            "size": size,
            "_source": select,
            "sort": {"_doc": {"order": "asc"}},
        }
        after = None
        with Pit(self._sumo, "1m") as pit:
            while maxhits is None or len(all_hits) < maxhits:
                query = pit.stamp_query(_set_search_after(query, after))
                query["size"] = size if maxhits is None else min(size, maxhits-len(all_hits))
                res = self._sumo.post("/search", json=query).json()
                pit.update_from_result(res)
                hits = res["hits"]["hits"]
                if len(hits) == 0:
                    break
                after = hits[-1]["sort"]
                if select is False:
                    all_hits = all_hits + [hit["_id"] for hit in hits]
                else:
                    all_hits = all_hits + hits
                pass
            pass
        return all_hits

    def _search_all(self):
        return self.__search_all(query=self._query, size=1000, select=False)

    async def __search_all_async(self, query, size=1000, select=False, maxhits=None):
        all_hits = []
        query = {
            "query": query,
            "size": size,
            "_source": select,
            "sort": {"_doc": {"order": "asc"}},
        }
        after = None
        with Pit(self._sumo, "1m") as pit:
            while maxhits is None or len(all_hits) < maxhits:
                query = pit.stamp_query(_set_search_after(query, after))
                query["size"] = size if maxhits is None else min(size, maxhits-len(all_hits))
                res = (await self._sumo.post("/search", json=query)).json()
                pit.update_from_result(res)
                hits = res["hits"]["hits"]
                if len(hits) == 0:
                    break
                after = hits[-1]["sort"]
                if select is False:
                    all_hits = all_hits + [hit["_id"] for hit in hits]
                else:
                    all_hits = all_hits + hits
                pass
            pass
        return all_hits

    async def _search_all_async(self):
        return await __search_all_async(
            query=self._query, size=1000, select=False
        )

    @property
    def uuids(self):
        if self._hits is None:
            self._hits = self._search_all()
        return self._hits

    @property
    async def uuids_async(self):
        if self._hits is None:
            self._hits = await self._search_all_async()
        return self._hits

    def __iter__(self):
        self._curr_index = 0
        if self._hits is None:
            self._hits = self._search_all()
            pass
        return self

    def __next__(self):
        if self._curr_index < len(self._hits):
            uuid = self._hits[self._curr_index]
            self._maybe_prefetch(self._curr_index)
            res = self.get_object(uuid)
            self._curr_index += 1
            return self._to_sumo(res)
        else:
            raise StopIteration

    async def __aiter__(self):
        self._curr_index = 0
        if self._hits is None:
            self._hits = await self._search_all_async()
            pass
        return self

    async def __anext__(self):
        if self._curr_index < len(self._hits):
            uuid = self._hits[self._curr_index]
            await self._maybe_prefetch_async(self._curr_index)
            res = await self.get_object_async(uuid)
            self._curr_index += 1
            return self._to_sumo(res)
        else:
            raise StopIteration

    def __getitem__(self, index):
        if self._hits is None:
            self._hits = self._search_all()
            pass
        uuid = self._hits[index]
        obj = self.get_object(uuid)
        return self._to_sumo(obj)

    async def getitem_async(self, index):
        if self._hits is None:
            self._hits = await self._search_all_async()
            pass
        uuid = self._hits[index]
        obj = self.get_object_async(uuid)
        return self._to_sumo(obj)

    def get_object(self, uuid: str, select: List[str] = None) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """
        obj = self._cache.get(uuid)
        assert obj is not None
        if obj is None:
            query = {
                "query": {"ids": {"values": [uuid]}},
                "size": 1,
            }

            if select is not None:
                query["_source"] = select

            res = self._sumo.post("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return obj

    async def get_object_async(
        self, uuid: str, select: List[str] = None
    ) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """

        obj = self._cache.get(uuid)
        if obj is None:
            query = {
                "query": {"ids": {"values": [uuid]}},
                "size": 1,
            }

            if select is not None:
                query["_source"] = select

            res = await self._sumo.post_async("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return obj

    def _maybe_prefetch(self, index):
        uuid = self._hits[index]
        if self._cache.has(uuid):
            return
        uuids = self._hits[index : min(index + 100, len(self._hits))]
        uuids = [uuid for uuid in uuids if not self._cache.has(uuid)]
        hits = self.__search_all(
            {"ids": {"values": uuids}},
            select={
                "exclude": ["data.spec.columns", "fmu.realization.parameters"],
            },
            maxhits=len(uuids),
        )
        if len(hits) == 0:
            return
        for hit in hits:
            self._cache.put(hit["_id"], hit)
        return

    async def _maybe_prefetch_async(self, index):
        uuid = self._hits[index]
        if self._cache.has(uuid):
            return
        uuids = self._hits[index : min(index + 100, len(self._hits))]
        uuids = [uuid for uuid in uuids if not self._cache.has(uuid)]
        hits = await self.__search_all_async(
            {"ids": {"values": uuids}},
            select={
                "exclude": ["data.spec.columns", "fmu.realization.parameters"],
            },
            maxhits=len(uuids),
        )
        if len(hits) == 0:
            return
        for hit in hits:
            self._cache.put(hit["_id"], hit)
        return

    def get_objects(
        self,
        uuids: List[str],
        select: List[str] = None,
    ) -> List[Dict]:
        size = (
            1000
            if select is False
            else 100 if isinstance(select, list) else 10
        )
        return self.__search_all(
            {"ids": {"values": uuids}}, size=size, select=select
        )

    async def get_objects_async(
        self,
        uuids: List[str],
        select: List[str] = None,
    ) -> List[Dict]:
        size = (
            1000
            if select is False
            else 100 if isinstance(select, list) else 10
        )
        return await self.__search_all_async(
            {"ids": {"values": uuids}}, size=size, select=select
        )

    def _get_buckets(
        self,
        field: str,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata

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
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
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

    def _get_field_values(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            buckets = self._get_buckets(field)
            self._field_values[field] = list(
                map(lambda bucket: bucket["key"], buckets)
            )

        return self._field_values[field]

    async def _get_field_values_async(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            buckets = await self._get_buckets_async(field)
            self._field_values[field] = list(
                map(lambda bucket: bucket["key"], buckets)
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

    @property
    def columns(self) -> List[str]:
        """List of unique column names"""
        return self._get_field_values("data.spec.columns.keyword")

    @property
    async def columns_async(self) -> List[str]:
        """List of unique column names"""
        return await self._get_field_values_async("data.spec.columns.keyword")

    _timestamp_query = {
        "bool": {
            "must": [{"exists": {"field": "data.time.t0"}}],
            "must_not": [{"exists": {"field": "data.time.t1"}}],
        }
    }

    @property
    def timestamps(self) -> List[str]:
        """List of unique timestamps in SearchContext"""
        ts = self.filter(complex=self._timestamp_query)._get_field_values(
            "data.time.t0.value"
        )
        return [datetime.fromtimestamp(t / 1000).isoformat() for t in ts]

    @property
    async def timestamps_async(self) -> List[str]:
        """List of unique timestamps in SearchContext"""
        ts = await self._get_field_values_async(
            "data.time.t0.value", self._timestamp_query
        )
        return [datetime.fromtimestamp(t / 1000).isoformat() for t in ts]

    def _extract_intervals(self, res):
        buckets = res.json()["aggregations"]["t0"]["buckets"]
        intervals = []

        for bucket in buckets:
            t0 = bucket["key_as_string"]

            for t1 in bucket["t1"]["buckets"]:
                intervals.append((t0, t1["key_as_string"]))

        return intervals

    _intervals_aggs = {
        "t0": {
            "terms": {"field": "data.time.t0.value", "size": 50},
            "aggs": {
                "t1": {
                    "terms": {
                        "field": "data.time.t1.value",
                        "size": 50,
                    }
                }
            },
        }
    }

    @property
    def intervals(self) -> List[Tuple]:
        """List of unique intervals in SearchContext"""
        res = self._sumo.post(
            "/search",
            json={
                "query": self._query,
                "size": 0,
                "aggs": self._intervals_aggs,
            },
        )

        return self._extract_intervals(res)

    @property
    async def intervals_async(self) -> List[Tuple]:
        """List of unique intervals in SearchContext"""
        res = await self._sumo.post_async(
            "/search",
            json={
                "query": self._query,
                "size": 0,
                "aggs": self._intervals_aggs,
            },
        )

        return self._extract_intervals(res)

    def filter(self, **kwargs) -> "SearchContext":
        """Filter SearchContext

        Apply additional filters to SearchContext and get a new filtered
        instance.

        Args:
            name (Union[str, List[str], bool]): name
            tagname (Union[str, List[str], bool]): tagname
            dataformat (Union[str, List[str], bool]): data format
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            aggregation (Union[str, List[str], bool]): aggregation operation
            stage (Union[str, List[str], bool]): context/stage
            time (TimeFilter): time filter
            uuid (Union[str, List[str], bool]): object uuid
            stratigraphic (Union[str, List[str], bool]): stratigraphic
            vertical_domain (Union[str, List[str], bool]): vertical_domain
            content (Union[str, List[str], bool): = content

        Returns:
            SearchContext: A filtered SearchContext

        Examples:

            Match one value::

                surfs = case.surfaces.filter(
                    iteration="iter-0"
                    name="my_surface_name"
                )

            Match multiple values::

                surfs = case.surfaces.filter(
                    name=["one_name", "another_name"]
                )

            Get aggregated surfaces with specific operation::

                surfs = case.surfaces.filter(
                    aggregation="max"
                )

            Get all aggregated surfaces::

                surfs = case.surfaces.filter(
                    aggregation=True
                )

            Get all non-aggregated surfaces::

                surfs = case.surfaces.filter(
                    aggregation=False
                )

        """

        must = self._must[:]
        must_not = self._must_not[:]
        for k, v in kwargs.items():
            f = filters.get(k)
            if f is None:
                raise Exception(f"Don't know how to generate filter for {k}")
                pass
            _must, _must_not = f(v)
            if _must:
                must.append(_must)
            if _must_not is not None:
                must_not.append(_must_not)

        sc = SearchContext(
            self._sumo, must=must, must_not=must_not, select=self._select
        )

        if "has" in kwargs:
            # Get list of cases matched by current filter set
            uuids = sc._get_field_values("fmu.case.uuid.keyword")
            # Generate new searchcontext for objects that match the uuids
            # and also satisfy the "has" filter
            sc = SearchContext(
                self._sumo,
                must=[
                    {"terms": {"fmu.case.uuid.keyword": uuids}},
                    kwargs["has"],
                ],
                select=self._select,
            )
            uuids = sc._get_field_values("fmu.case.uuid.keyword")
            sc = SearchContext(
                self._sumo,
                must=[{"ids": {"values": uuids}}],
                select=self._select,
            )

        return sc

    def _context_for_class(self, cls):
        return self.filter(cls=cls)

    @property
    def cases(self):
        return self._context_for_class("case")

    @property
    def surfaces(self):
        return self._context_for_class("surface")

    @property
    def tables(self):
        return self._context_for_class("table")

    @property
    def cubes(self):
        return self._context_for_class("cube")

    @property
    def polygons(self):
        return self._context_for_class("polygons")

    @property
    def dictionaries(self):
        return self._context_for_class("dictionary")
