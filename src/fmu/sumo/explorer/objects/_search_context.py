import uuid
import httpx
import deprecation
import warnings
from typing import List, Dict, Tuple
from datetime import datetime
from io import BytesIO
from sumo.wrapper import SumoClient
import fmu.sumo.explorer.objects as objects
from fmu.sumo.explorer.cache import LRUCache


def _gen_filter_none():
    def _fn(value):
        return None, None

    return _fn


def _gen_filter_id():
    """Match against document id(s) (in uuid format)."""

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
    """Match property against either single value or list of values.
    If the value given is a boolean, tests for existence or not of the property.
    """

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
    """Match against \"data.name\", or \"case.name\" for case objects."""

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
    """Match against a TimeFilter instance."""

    def _fn(value):
        if value is None:
            return None, None
        else:
            return value._get_query(), None

    return _fn


def _gen_filter_bool(attr):
    """Match boolean value."""

    def _fn(value):
        if value is None:
            return None, None
        else:
            return {"term": {attr: value}}, None

    return _fn


def _gen_filter_complex():
    """Match against user-supplied query, which is a structured
    Elasticsearch query in dictionary form."""

    def _fn(value):
        if value is None:
            return None, None
        else:
            return value, None

    return _fn


_filterspec = {
    "id": [_gen_filter_id, None],
    "cls": [_gen_filter_gen, "class.keyword"],
    "time": [_gen_filter_time, None],
    "name": [_gen_filter_name, None],
    "uuid": [_gen_filter_gen, "fmu.case.uuid.keyword"],
    "tagname": [_gen_filter_gen, "data.tagname.keyword"],
    "dataformat": [_gen_filter_gen, "data.format.keyword"],
    "iteration": [_gen_filter_gen, "fmu.iteration.name.keyword"],
    "realization": [_gen_filter_gen, "fmu.realization.id"],
    "aggregation": [_gen_filter_gen, "fmu.aggregation.operation.keyword"],
    "stage": [_gen_filter_gen, "fmu.context.stage.keyword"],
    "column": [_gen_filter_gen, "data.spec.columns.keyword"],
    "vertical_domain": [_gen_filter_gen, "data.vertical_domain.keyword"],
    "content": [_gen_filter_gen, "data.content.keyword"],
    "status": [_gen_filter_gen, "_sumo.status.keyword"],
    "user": [_gen_filter_gen, "fmu.case.user.id.keyword"],
    "asset": [_gen_filter_gen, "access.asset.name.keyword"],
    "field": [_gen_filter_gen, "masterdata.smda.field.identifier.keyword"],
    "stratigraphic": [_gen_filter_bool, "data.stratigraphic"],
    "is_observation": [_gen_filter_bool, "data.is_observation"],
    "is_prediction": [_gen_filter_bool, "data.is_prediction"],
    "complex": [_gen_filter_complex, None],
    "has": [_gen_filter_none, None],
}


def _gen_filters(spec):
    res = {}
    for name, desc in spec.items():
        gen, param = desc
        if param is None:
            res[name] = gen()
        else:
            res[name] = gen(param)
            pass
    return res


filters = _gen_filters(_filterspec)


_bucket_spec = {
    "names": ["data.name.keyword", "List of unique object names."],
    "tagnames": ["data.tagname.keyword", "List of unique object tagnames."],
    "dataformats": [
        "data.format.keyword",
        "List of unique data.format values.",
    ],
    "aggregations": [
        "fmu.aggregation.operation.keyword",
        "List of unique object aggregation operations.",
    ],
    "stages": ["fmu.context.stage.keyword", "List of unique stages."],
    # "stratigraphic": [
    #     "data.stratigraphic",
    #     "List of unique object stratigraphic.",
    # ],
    "vertical_domains": [
        "data.vertical_domain",
        "List of unique object vertical domains.",
    ],
    "contents": ["data.content.keyword", "List of unique contents."],
    "columns": ["data.spec.columns.keyword", "List of unique column names."],
    "statuses": ["_sumo.status.keyword", "List of unique case statuses."],
    "users": ["fmu.case.user.id.keyword", "List of unique user names."]
}


def _build_bucket_query(query, field, size):
    return {
        "size": 0,
        "query": query,
        "aggs": {
            f"{field}": {
                "composite": {
                    "size": size,
                    "sources": [{f"{field}": {"terms": {"field": field}}}],
                }
            }
        },
    }


def _build_bucket_query_simple(query, field, size):
    return {
        "size": 0,
        "query": query,
        "aggs": {f"{field}": {"terms": {"field": field, "size": size}}},
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
        hidden = False,
        visible = True
    ):
        self._sumo = sumo
        self._must = must[:]
        self._must_not = must_not[:]
        self._visible = visible
        self._hidden = hidden
        self._field_values = {}
        self._hits = None
        self._cache = LRUCache(capacity=200)
        self._length = None
        self._select = {
            "excludes": ["fmu.realization.parameters"],
        }
        return

    @property
    def _query(self):
        must = self._must[:]
        must_not = self._must_not[:]
        if self._visible and not self._hidden:
            must_not.append({"term": {"_sumo.hidden": True}})
        elif not self._visible and self._hidden:
            must.append({"term": {"_sumo.hidden": True}})
            pass
        if len(must_not) == 0:
            if len(must) == 1:
                return must[0]
            else:
                return {"bool": {"must": must}}
        else:
            if len(must) == 0:
                return {"bool": {"must_not": must_not}}
            else:
                return {
                    "bool": {"must": must, "must_not": must_not}
                }

    def _to_sumo(self, obj, blob=None):
        cls = obj["_source"]["class"]
        if cls == "case":
            return objects.Case(self._sumo, obj)
        # ELSE
        constructor = {
            "cube": objects.Cube,
            "dictionary": objects.Dictionary,
            "polygons": objects.Polygons,
            "surface": objects.Surface,
            "table": objects.Table,
            "cpgrid": objects.CPGrid,
            "cpgrid_property": objects.CPGridProperty
        }.get(cls)
        if constructor is None:
            warnings.warn(f"No constructor for class {cls}")
            constructor = objects.Child
        return constructor(self._sumo, obj, blob)

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

    def __search_all(self, query, size=1000, select=False):
        all_hits = []
        query = {
            "query": query,
            "size": size,
            "_source": select,
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
        }
        # fast path: try searching without Pit
        res = self._sumo.post("/search", json=query).json()
        total_hits = res["hits"]["total"]["value"]
        if total_hits <= size:
            hits = res["hits"]["hits"]
            if select is False:
                return [hit["_id"] for hit in hits]
            else:
                return hits
        after = None
        with Pit(self._sumo, "1m") as pit:
            while True:
                query = pit.stamp_query(_set_search_after(query, after))
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
            pass
        return all_hits

    def _search_all(self, select=False):
        return self.__search_all(query=self._query, size=1000, select=select)

    async def __search_all_async(self, query, size=1000, select=False):
        all_hits = []
        query = {
            "query": query,
            "size": size,
            "_source": select,
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
        }
        # fast path: try searching without Pit
        res = (await self._sumo.post_async("/search", json=query)).json()
        total_hits = res["hits"]["total"]["value"]
        if total_hits <= size:
            hits = res["hits"]["hits"]
            if select is False:
                return [hit["_id"] for hit in hits]
            else:
                return hits
        after = None
        with Pit(self._sumo, "1m") as pit:
            while True:
                query = pit.stamp_query(_set_search_after(query, after))
                res = (
                    await self._sumo.post_async("/search", json=query)
                ).json()
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
            pass
        return all_hits

    async def _search_all_async(self, select=False):
        return await self.__search_all_async(
            query=self._query, size=1000, select=select
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
        return self

    def __next__(self):
        if self._hits is None:
            self._hits = self._search_all()
            pass
        if self._curr_index < len(self._hits):
            uuid = self._hits[self._curr_index]
            self._maybe_prefetch(self._curr_index)
            self._curr_index += 1
            return self.get_object(uuid)
        else:
            raise StopIteration

    def __aiter__(self):
        self._curr_index = 0
        return self

    async def __anext__(self):
        if self._hits is None:
            self._hits = await self._search_all_async()
            pass
        if self._curr_index < len(self._hits):
            uuid = self._hits[self._curr_index]
            await self._maybe_prefetch_async(self._curr_index)
            self._curr_index += 1
            return await self.get_object_async(uuid)
        else:
            raise StopAsyncIteration

    def __getitem__(self, index):
        if self._hits is None:
            self._hits = self._search_all()
            pass
        uuid = self._hits[index]
        return self.get_object(uuid)

    async def getitem_async(self, index):
        if self._hits is None:
            self._hits = await self._search_all_async()
            pass
        uuid = self._hits[index]
        return await self.get_object_async(uuid)

    def select(self, sel):
        """Specify what should be returned from elasticsearch.
        Has the side effect of clearing the lru cache.
        sel is either a single string value, a list of string value,
        or a dictionary with keys "includes" and/or "excludes" and
        the values are lists of strings. The string values are nested
        property names.

        Args:
            sel (str | List(str) | Dict(str, List[str]): select specification
        Returns:
            None
        """

        required = set(["class"])
        def extreq(lst):
            if isinstance(lst, str):
                lst = [lst]
            return list(set(lst) | required)
        if isinstance(sel, str):
            self._select = extreq([sel])
        elif isinstance(sel, list):
            self._select = extreq(sel)
        elif isinstance(sel, dict):
            inc = sel.get("includes")
            exc = sel.get("excludes")
            slct = {}
            if inc is not None:
                slct["includes"] = extreq(inc)
                pass
            if exc is not None:
                slct["excludes"] = exc
                pass
            self._select = slct
            pass
        self._cache.clear()

    def get_object(self, uuid: str) -> Dict:
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
                "_source": self._select
            }

            res = self._sumo.post("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return self._to_sumo(obj)

    async def get_object_async(self, uuid: str) -> Dict:
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
                "_source": self._select
            }

            res = await self._sumo.post_async("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return self._to_sumo(obj)

    def _maybe_prefetch(self, index):
        uuid = self._hits[index]
        if self._cache.has(uuid):
            return
        uuids = self._hits[index:min(index + 100, len(self._hits))]
        uuids = [uuid for uuid in uuids if not self._cache.has(uuid)]
        hits = self.__search_all(
            {"ids": {"values": uuids}},
            select=self._select,
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
        uuids = self._hits[index:min(index + 100, len(self._hits))]
        uuids = [uuid for uuid in uuids if not self._cache.has(uuid)]
        hits = await self.__search_all_async(
            {"ids": {"values": uuids}},
            select=self._select,
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

        buckets_per_batch = 1000

        # fast path: try without Pit
        query = _build_bucket_query_simple(
            self._query, field, buckets_per_batch
        )
        res = self._sumo.post("/search", json=query).json()
        other_docs_count = res["aggregations"][field]["sum_other_doc_count"]
        if other_docs_count == 0:
            buckets = res["aggregations"][field]["buckets"]
            buckets = [
                {
                    "key": bucket["key"],
                    "doc_count": bucket["doc_count"],
                }
                for bucket in buckets
            ]
            return buckets

        query = _build_bucket_query(self._query, field, buckets_per_batch)
        all_buckets = []
        after_key = None
        with Pit(self._sumo, "1m") as pit:
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
                if len(buckets) < buckets_per_batch:
                    break
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

        buckets_per_batch = 1000

        # fast path: try without Pit
        query = _build_bucket_query_simple(
            self._query, field, buckets_per_batch
        )
        res = (await self._sumo.post_async("/search", json=query)).json()
        other_docs_count = res["aggregations"][field]["sum_other_doc_count"]
        if other_docs_count == 0:
            buckets = res["aggregations"][field]["buckets"]
            buckets = [
                {
                    "key": bucket["key"],
                    "doc_count": bucket["doc_count"],
                }
                for bucket in buckets
            ]
            return buckets

        query = _build_bucket_query(self._query, field, buckets_per_batch)
        all_buckets = []
        after_key = None
        async with Pit(self._sumo, "1m") as pit:
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
                if len(buckets) < buckets_per_batch:
                    break
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

    _timestamp_query = {
        "bool": {
            "must": [{"exists": {"field": "data.time.t0"}}],
            "must_not": [{"exists": {"field": "data.time.t1"}}],
        }
    }

    def _context_for_class(self, cls):
        return self.filter(cls=cls)

    @property
    def hidden(self):
        return SearchContext(sumo=self._sumo,
                             must=self._must,
                             must_not = self._must_not,
                             hidden = True,
                             visible = False)

    @property
    def visible(self):
        return SearchContext(sumo=self._sumo,
                             must=self._must,
                             must_not = self._must_not,
                             hidden = False,
                             visible = True)

    @property
    def all(self):
        return SearchContext(sumo=self._sumo,
                             must=self._must,
                             must_not = self._must_not,
                             hidden = True,
                             visible = True)

    @property
    def cases(self):
        """Cases from current selection."""
        uuids = self._get_field_values("fmu.case.uuid.keyword")
        return objects.Cases(self, uuids)

    @property
    async def cases_async(self):
        """Cases from current selection."""
        uuids = await self._get_field_values_async("fmu.case.uuid.keyword")
        return objects.Cases(self, uuids)

    @property
    def iterations(self):
        """Iterations from current selection."""
        uuids = self._get_field_values("fmu.iteration.uuid.keyword")
        return objects.Iterations(self, uuids)

    @property
    async def iterations_async(self):
        """Iterations from current selection."""
        uuids = await self._get_field_values_async("fmu.iteration.uuid.keyword")
        return objects.Iterations(self, uuids)

    @property
    def realizations(self):
        """Realizations from current selection."""
        uuids = self._get_field_values("fmu.realization.uuid.keyword")
        return objects.Realizations(self, uuids)

    @property
    async def realizations_async(self):
        """Realizations from current selection."""
        uuids = await self._get_field_values_async("fmu.realization.uuid.keyword")
        return objects.Realizations(self, uuids)

    @property
    def template_paths(sc):
        return set([obj.template_path for obj in sc])

    @property
    def metrics(self):
        """Metrics for current search context."""
        return objects.Metrics(self)

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
        """Filter SearchContext"""

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

        sc = SearchContext(self._sumo, must=must, must_not=must_not, hidden=self._hidden, visible = self._visible)

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
            )
            uuids = sc._get_field_values("fmu.case.uuid.keyword")
            sc = SearchContext(
                self._sumo,
                must=[{"ids": {"values": uuids}}],
            )

        return sc

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

    @property
    def grids(self):
        return self._context_for_class("cpgrid")

    @property
    def grid_properties(self):
        return self._context_for_class("cpgrid_property")

    def _get_object_by_class_and_uuid(self, cls, uuid):
        obj = self.get_object(uuid)
        if obj.metadata["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return obj

    async def _get_object_by_class_and_uuid_async(self, cls, uuid):
        obj = self.get_object_async(uuid)
        if obj.metadata["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return obj

    def get_case_by_uuid(self, uuid: str):
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return self._get_object_by_class_and_uuid("case", uuid)

    async def get_case_by_uuid_async(self, uuid: str):
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return await self._get_object_by_class_and_uuid_async("case", uuid)

    def _iteration_query(self, uuid):
        return {
            "query": {"term": {"fmu.iteration.uuid.keyword": {"value": uuid}}},
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                ],
            },
        }

    def get_iteration_by_uuid(self, uuid: str):
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        res = self._sumo.post(
            "/search", json=self._iteration_query(uuid)
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return objects.Iteration(self._sumo, obj)

    async def get_iteration_by_uuid_async(self, uuid: str):
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        res = (
            await self._sumo.post("/search", json=self._iteration_query(uuid))
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return objects.Iteration(self._sumo, obj)

    def _realization_query(self, uuid):
        return {
            "query": {
                "term": {"fmu.realization.uuid.keyword": {"value": uuid}}
            },
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                    "fmu.realization",
                ],
            },
        }

    def get_realization_by_uuid(self, uuid: str):
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        res = self._sumo.post(
            "/search", json=self._realization_query(uuid)
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return objects.Realization(self._sumo, obj)

    async def get_realization_by_uuid_async(self, uuid: str):
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        res = (
            await self._sumo.post(
                "/search", json=self._realization_query(uuid)
            )
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return objects.Realization(self._sumo, obj)

    def get_surface_by_uuid(self, uuid: str):
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = self.get_object(uuid)
        return objects.Surface(self._sumo, metadata)

    async def get_surface_by_uuid_async(self, uuid: str):
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = await self.get_object_async(uuid)
        return objects.Surface(self._sumo, metadata)

    def get_polygons_by_uuid(self, uuid: str):
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = self.get_object(uuid)
        return objects.Polygons(self._sumo, metadata)

    async def get_polygons_by_uuid_async(self, uuid: str):
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = await self.get_object_async(uuid)
        return objects.Polygons(self._sumo, metadata)

    def get_table_by_uuid(self, uuid: str):
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = self.get_object(uuid)
        return objects.Table(self._sumo, metadata)

    async def get_table_by_uuid_async(self, uuid: str):
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = await self.get_object_async(uuid)
        return objects.Table(self._sumo, metadata)

    def _verify_aggregation_operation(self):
        query = {
            "query": self._query,
            "size": 1,
            "track_total_hits": True,
            "aggs": {
                k: {"terms": {"field": k + ".keyword", "size": 1}}
                for k in [
                    "fmu.case.uuid",
                    "class",
                    "fmu.iteration.name",
                    "data.name",
                    "data.tagname",
                    "data.content",
                ]
            },
        }
        sres = self._sumo.post("/search", json=query).json()
        prototype = sres["hits"]["hits"][0]
        conflicts = [
            k
            for (k, v) in sres["aggregations"].items()
            if (
                ("sum_other_doc_count" in v) and (v["sum_other_doc_count"] > 0)
            )
        ]
        if len(conflicts) > 0:
            raise Exception(f"Conflicting values for {conflicts}")

        hits = self._search_all(select=["fmu.realization.id"])

        if any(
            [hit["_source"]["fmu"].get("realization") is None for hit in hits]
        ):
            raise Exception("Selection contains non-realization data.")

        uuids = [hit["_id"] for hit in hits]
        rids = [hit["_source"]["fmu"]["realization"]["id"] for hit in hits]
        return prototype, uuids, rids

    def _aggregate(self, columns=None, operation=None):
        prototype, uuids, rids = self._verify_aggregation_operation()
        spec = {
            "object_ids": uuids,
            "operations": [operation],
        }
        del prototype["_source"]["fmu"]["realization"]
        del prototype["_source"]["_sumo"]
        del prototype["_source"]["file"]
        del prototype["_source"]["access"]
        if "context" in prototype["_source"]["fmu"]:
            prototype["_source"]["fmu"]["context"]["stage"] = "iteration"
            pass
        prototype["_source"]["fmu"]["aggregation"] = {
            "id": str(uuid.uuid4()),
            "realization_ids": rids,
            "operation": operation,
        }
        if columns is not None:
            spec["columns"] = columns
            cols = columns[:]
            table_index = prototype["_source"]["data"].get("table_index")
            if table_index is not None and len(table_index) != 0 and table_index[0] not in cols:
                cols.insert(0, table_index[0])
                pass
            prototype["_source"]["data"]["spec"]["columns"] = cols
            pass
        try:
            res = self._sumo.post("/aggregations", json=spec)
        except httpx.HTTPStatusError as ex:
            print(ex.response.reason_phrase)
            print(ex.response.text)
            raise ex
        blob = BytesIO(res.content)
        res = self._to_sumo(prototype, blob)
        res._blob = blob
        return res

    def aggregate(self, columns=None, operation=None):
        if len(self.hidden) > 0:
            return self.hidden._aggregate(columns=columns, operation=operation)
        else:
            return self.visible._aggregate(columns=columns, operation=operation)

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def min(self):
        return self.aggregate(operation="min")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def max(self):
        return self.aggregate(operation="max")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def mean(self):
        return self.aggregate(operation="mean")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def std(self):
        return self.aggregate(operation="std")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def p10(self):
        return self.aggregate(operation="p10")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def p50(self):
        return self.aggregate(operation="p50")

    @deprecation.deprecated(details="Use the method 'aggregate' instead, with parameter 'operation'.")
    def p90(self):
        return self.aggregate(operation="p90")


def _gen_filter_doc(spec):
    fmap = {
        _gen_filter_id: "Id",
        _gen_filter_bool: "Boolean",
        _gen_filter_name: "Name",
        _gen_filter_gen: "General",
        _gen_filter_time: "Time",
        _gen_filter_complex: "Complex",
    }
    ret = """\
Filter SearchContext.

Apply additional filters to SearchContext and return a new
filtered instance.

The filters (specified as keyword args) are of these formats:

"""
    for gen, name in fmap.items():
        ret = ret + f"    {name}:  {gen.__doc__}\n"
    ret = (
        ret
        + """
Args:

"""
    )
    for name in sorted(spec.keys()):
        gen, property = spec[name]
        if gen in [_gen_filter_complex, _gen_filter_none]:
            continue
        typ = fmap.get(gen)
        if typ is not None:
            if property is None:
                ret = ret + f"    {name} ({typ})\n"
            else:
                ret = ret + f'    {name} ({typ}): "{property}"\n'
                pass
            pass
        pass
    ret = ret + "    has (Complex)\n"
    ret = ret + "    complex (Complex)\n"
    ret = (
        ret
        + """
Returns:
    SearchContext: A filtered SearchContext.

Examples:

    Match one value::

        surfs = case.surfaces.filter(
                    iteration="iter-0",
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
    )
    return ret


SearchContext.filter.__doc__ = _gen_filter_doc(_filterspec)


def _build_bucket_fn(property, docstring):
    def fn(self):
        return self._get_field_values(property)

    return fn


def _build_bucket_fn_async(property, docstring):
    async def fn(self):
        return await self._get_field_values_async(property)

    return fn


def _inject_bucket_fns(spec):
    for name, defn in spec.items():
        prop, docstring = defn
        fn = _build_bucket_fn(prop, docstring)
        setattr(SearchContext, name, property(fn, None, None, docstring))
        afn = _build_bucket_fn_async(prop, docstring)
        setattr(
            SearchContext,
            name + "_async",
            property(afn, None, None, docstring),
        )
        pass
    return


_inject_bucket_fns(_bucket_spec)
