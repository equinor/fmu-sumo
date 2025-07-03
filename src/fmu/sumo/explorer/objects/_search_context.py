from __future__ import annotations

import json
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

import deprecation
import httpx

from fmu.sumo.explorer import objects
from fmu.sumo.explorer.cache import LRUCache

if TYPE_CHECKING:
    from sumo.wrapper import SumoClient


# Type aliases
SelectArg = Union[bool, str, Dict[str, Union[str, List[str]]], List[str]]


def _gen_filter_none():
    def _fn(_):
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


def _gen_filter_stage(attr):
    """Match property against either single value or list of values.
    If the value given is a boolean, tests for existence or not of the property.
    In addition, if the value is or includes either "iteration" or "ensemble",
    expand to include both values.
    """

    _inner = _gen_filter_gen(attr)

    def _fn(value):
        if value == "iteration" or value == "ensemble":
            return _inner(["iteration", "ensemble"])
        elif isinstance(value, list) and set(value).intersection(
            {"iteration", "ensemble"}
        ):
            return _inner(list(set(value).union({"iteration", "ensemble"})))
        else:
            return _inner(value)

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
    "relative_path": [_gen_filter_gen, "file.relative_path.keyword"],
    "tagname": [_gen_filter_gen, "data.tagname.keyword"],
    "dataformat": [_gen_filter_gen, "data.format.keyword"],
    "iteration": [
        _gen_filter_gen,
        "fmu.iteration.name.keyword",
    ],  # FIXME: to be removed
    "ensemble": [_gen_filter_gen, "fmu.ensemble.name.keyword"],
    "realization": [_gen_filter_gen, "fmu.realization.id"],
    "aggregation": [_gen_filter_gen, "fmu.aggregation.operation.keyword"],
    "stage": [_gen_filter_stage, "fmu.context.stage.keyword"],
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
    "standard_result": [_gen_filter_gen, "data.standard_result.name.keyword"],
    "entity": [_gen_filter_gen, "fmu.entity.uuid.keyword"],
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

    def __exit__(self, *_):
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

    async def __aexit__(self, *_):
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
        hidden=False,
        visible=True,
    ):
        self._sumo = sumo
        self._must = must[:]
        self._must_not = must_not[:]
        self._visible = visible
        self._hidden = hidden
        self._field_values = {}
        self._field_values_and_counts = {}
        self._hits = None
        self._cache = LRUCache(capacity=200)
        self._length = None
        self._select: SelectArg = {
            "excludes": ["fmu.realization.parameters"],
        }
        return

    def __str__(self):
        length = len(self)
        if length == 0:
            return "None"
        else:
            preview = [self[i].metadata for i in range(min(5, length))]
            return f"Data Preview:\n{json.dumps(preview, indent=4)}"

    def __repr__(self):
        cls = self.__class__.__name__
        length = len(self)
        if length == 0:
            return f"<{cls}: {length} objects>"
        else:
            if len(self.classes) == 1:
                return f"<{cls}: {length} objects of type {self.classes[0]}>"
            else:
                return f"<{cls}: {length} objects of types {self.classes}>"

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
                return {"bool": {"must": must, "must_not": must_not}}

    def _to_sumo(self, obj, blob=None) -> objects.Document:
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
            "cpgrid_property": objects.CPGridProperty,
            "iteration": objects.Iteration,  # FIXME: to be removed
            "ensemble": objects.Ensemble,
            "realization": objects.Realization,
        }.get(cls)
        if constructor is None:
            warnings.warn(f"No constructor for class {cls}")
            constructor = objects.Child
        return constructor(self._sumo, obj, blob)

    def __len__(self):
        if self._hits is not None:
            return len(self._hits)
        if self._length is None:
            query = {"query": self._query}
            res = self._sumo.post("/count", json=query).json()
            self._length = res["count"]
        return self._length

    async def length_async(self):
        if self._hits is not None:
            return len(self._hits)
        if self._length is None:
            query = {"query": self._query}
            res = (await self._sumo.post_async("/count", json=query)).json()
            self._length = res["count"]
        return self._length

    def __search_all(self, query, size: int = 1000, select: SelectArg = False):
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

    def _search_all(self, select: SelectArg = False):
        return self.__search_all(query=self._query, size=1000, select=select)

    async def __search_all_async(
        self, query, size: int = 1000, select: SelectArg = False
    ):
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

    async def _search_all_async(self, select: SelectArg = False):
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

    @property
    def single(self):
        """Verifies that SearchContext contains exactly one object,
        and returns it.
        """
        assert len(self) == 1
        return self[0]

    @property
    async def single_async(self):
        """Verifies that SearchContext contains exactly one object,
        and returns it.
        """
        assert await self.length_async() == 1
        return await self.getitem_async(0)

    def select(self, sel) -> SearchContext:
        """Specify what should be returned from elasticsearch.
        Has the side effect of clearing the lru cache.
        sel is either a single string value, a list of string value,
        or a dictionary with keys "includes" and/or "excludes" and
        the values are lists of strings. The string values are nested
        property names.

        This method returns itself, so it is chainable, but the select
        settings will not propagate into a new SearchContext
        (specifically, it will not be passed into the result of .filter()).

        Args:
            sel (str | List(str) | Dict(str, List[str]): select specification
        Returns:
            itself (SearchContext)
        """

        required = {"class"}

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
        return self

    def get_object(self, uuid: str) -> objects.Document:
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
                "_source": self._select,
            }

            res = self._sumo.post("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return self._to_sumo(obj)

    async def get_object_async(self, uuid: str) -> objects.Document:
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
                "_source": self._select,
            }

            res = await self._sumo.post_async("/search", json=query)
            hits = res.json()["hits"]["hits"]

            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            self._cache.put(uuid, obj)

        return self._to_sumo(obj)

    def _maybe_prefetch(self, index):
        assert isinstance(self._hits, list)
        uuid = self._hits[index]
        if self._cache.has(uuid):
            return
        uuids = self._hits[index : min(index + 100, len(self._hits))]
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
        assert isinstance(self._hits, list)
        uuid = self._hits[index]
        if self._cache.has(uuid):
            return
        uuids = self._hits[index : min(index + 100, len(self._hits))]
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
        select: SelectArg,
    ) -> List[Dict]:
        size = (
            1000
            if select is False
            else 100
            if isinstance(select, (list, dict))
            else 10
        )
        return self.__search_all(
            {"ids": {"values": uuids}}, size=size, select=select
        )

    async def get_objects_async(
        self, uuids: List[str], select: SelectArg
    ) -> List[Dict]:
        size = (
            1000
            if select is False
            else 100
            if isinstance(select, (list, dict))
            else 10
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

    def get_field_values_and_counts(self, field: str) -> Dict[str, int]:
        """Get List of unique values with occurrence counts for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A mapping from unique values to count.
        """
        if field not in self._field_values_and_counts:
            buckets = {
                b["key"]: b["doc_count"] for b in self._get_buckets(field)
            }
            self._field_values_and_counts[field] = buckets

        return self._field_values_and_counts[field]

    def get_field_values(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            buckets = self._get_buckets(field)
            self._field_values[field] = [bucket["key"] for bucket in buckets]

        return self._field_values[field]

    @deprecation.deprecated(
        details="Use the method 'get_field_values' instead."
    )
    def _get_field_values(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        return self.get_field_values(field)

    async def get_field_values_and_counts_async(
        self, field: str
    ) -> Dict[str, int]:
        """Get List of unique values with occurrence counts for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A mapping from unique values to count.
        """
        if field not in self._field_values_and_counts:
            buckets = {
                b["key"]: b["doc_count"]
                for b in await self._get_buckets_async(field)
            }
            self._field_values_and_counts[field] = buckets

        return self._field_values_and_counts[field]

    async def get_field_values_async(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            buckets = await self._get_buckets_async(field)
            self._field_values[field] = [bucket["key"] for bucket in buckets]

        return self._field_values[field]

    @deprecation.deprecated(
        details="Use the method 'get_field_values' instead."
    )
    async def _get_field_values_async(self, field: str) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        return await self.get_field_values_async(field)

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
        return SearchContext(
            sumo=self._sumo,
            must=self._must,
            must_not=self._must_not,
            hidden=True,
            visible=False,
        )

    @property
    def visible(self):
        return SearchContext(
            sumo=self._sumo,
            must=self._must,
            must_not=self._must_not,
            hidden=False,
            visible=True,
        )

    @property
    def all(self):
        return SearchContext(
            sumo=self._sumo,
            must=self._must,
            must_not=self._must_not,
            hidden=True,
            visible=True,
        )

    @property
    def cases(self):
        """Cases from current selection."""
        uuids = self.get_field_values("fmu.case.uuid.keyword")
        return objects.Cases(self, uuids)

    @property
    async def cases_async(self):
        """Cases from current selection."""
        uuids = await self.get_field_values_async("fmu.case.uuid.keyword")
        return objects.Cases(self, uuids)

    @property
    @deprecation.deprecated(details="Use the method 'ensembles' instead.")
    def iterations(self):
        """Iterations from current selection."""
        uuids = self.get_field_values("fmu.iteration.uuid.keyword")
        return objects.Iterations(self, uuids)

    @property
    @deprecation.deprecated(
        details="Use the method 'ensembles_async' instead."
    )
    async def iterations_async(self):
        """Iterations from current selection."""
        uuids = await self.get_field_values_async("fmu.iteration.uuid.keyword")
        return objects.Iterations(self, uuids)

    @property
    def ensembles(self):
        """Ensembles from current selection."""
        uuids = self.get_field_values("fmu.ensemble.uuid.keyword")
        return objects.Ensembles(self, uuids)

    @property
    async def ensembles_async(self):
        """Ensembles from current selection."""
        uuids = await self.get_field_values_async("fmu.ensemble.uuid.keyword")
        return objects.Ensembles(self, uuids)

    @property
    def realizations(self):
        """Realizations from current selection."""
        uuids = self.get_field_values("fmu.realization.uuid.keyword")
        return objects.Realizations(self, uuids)

    @property
    async def realizations_async(self):
        """Realizations from current selection."""
        uuids = await self.get_field_values_async(
            "fmu.realization.uuid.keyword"
        )
        return objects.Realizations(self, uuids)

    @property
    def template_paths(self) -> List[str]:
        return {obj.template_path for obj in self}

    @property
    def metrics(self):
        """Metrics for current search context."""
        return objects.Metrics(self)

    @property
    def timestamps(self) -> List[str]:
        """List of unique timestamps in SearchContext"""
        ts = self.filter(complex=self._timestamp_query).get_field_values(
            "data.time.t0.value"
        )
        return [datetime.fromtimestamp(t / 1000).isoformat() for t in ts]

    @property
    async def timestamps_async(self) -> List[str]:
        """List of unique timestamps in SearchContext"""
        ts = await self.filter(
            complex=self._timestamp_query
        ).get_field_values_async("data.time.t0.value")
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
            _must, _must_not = f(v)
            if _must:
                must.append(_must)
            if _must_not is not None:
                must_not.append(_must_not)

        sc = SearchContext(
            self._sumo,
            must=must,
            must_not=must_not,
            hidden=self._hidden,
            visible=self._visible,
        )

        if "has" in kwargs:
            # Get list of cases matched by current filter set
            uuids = sc.get_field_values("fmu.case.uuid.keyword")
            # Generate new searchcontext for objects that match the uuids
            # and also satisfy the "has" filter
            sc = SearchContext(
                self._sumo,
                must=[
                    {"terms": {"fmu.case.uuid.keyword": uuids}},
                    kwargs["has"],
                ],
            )
            uuids = sc.get_field_values("fmu.case.uuid.keyword")
            sc = SearchContext(
                self._sumo,
                must=[{"ids": {"values": uuids}}],
            )

        return sc

    @property
    def surfaces(self) -> SearchContext:
        return self._context_for_class("surface")

    @property
    def tables(self) -> SearchContext:
        return self._context_for_class("table")

    @property
    def cubes(self) -> SearchContext:
        return self._context_for_class("cube")

    @property
    def polygons(self) -> SearchContext:
        return self._context_for_class("polygons")

    @property
    def dictionaries(self) -> SearchContext:
        return self._context_for_class("dictionary")

    @property
    def grids(self) -> SearchContext:
        return self._context_for_class("cpgrid")

    @property
    def grid_properties(self) -> SearchContext:
        return self._context_for_class("cpgrid_property")

    @property
    def parameters(self) -> SearchContext:
        return self.filter(
            complex={
                "bool": {
                    "must": [
                        {"term": {"data.name.keyword": "parameters"}},
                        {"term": {"data.content.keyword": "parameters"}},
                    ],
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {"term": {"class.keyword": "dictionary"}},
                                    {
                                        "exists": {
                                            "field": "fmu.realization.id"
                                        }
                                    },
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"class.keyword": "table"}},
                                    {
                                        "exists": {
                                            "field": "fmu.aggregation.operation"
                                        }
                                    },
                                ]
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    def _get_object_by_class_and_uuid(self, cls, uuid) -> Any:
        obj = self.get_object(uuid)
        if obj.metadata["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return obj

    async def _get_object_by_class_and_uuid_async(self, cls, uuid) -> Any:
        obj = await self.get_object_async(uuid)
        if obj.metadata["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return obj

    def get_case_by_uuid(self, uuid: str) -> objects.Case:
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return self._get_object_by_class_and_uuid("case", uuid)

    async def get_case_by_uuid_async(self, uuid: str) -> objects.Case:
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
                    "class",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                ],
            },
        }

    def get_iteration_by_uuid(self, uuid: str) -> objects.Iteration:
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        try:
            obj = self.get_object(uuid)
            assert isinstance(obj, objects.Iteration)
            return obj
        except Exception:
            res = self._sumo.post(
                "/search", json=self._iteration_query(uuid)
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "iteration"
            ret = self._to_sumo(obj)
            self._cache.put(uuid, ret)
            return ret

    async def get_iteration_by_uuid_async(
        self, uuid: str
    ) -> objects.Iteration:
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        try:
            obj = await self.get_object_async(uuid)
            assert isinstance(obj, objects.Iteration)
            return obj
        except Exception:
            res = (
                await self._sumo.post_async(
                    "/search", json=self._iteration_query(uuid)
                )
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "iteration"
            ret = self._to_sumo(obj)
            self._cache.put(uuid, ret)
            return ret

    def _ensemble_query(self, uuid):
        return {
            "query": {"term": {"fmu.ensemble.uuid.keyword": {"value": uuid}}},
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "class",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.ensemble",
                ],
            },
        }

    def get_ensemble_by_uuid(self, uuid: str) -> objects.Ensemble:
        """Get ensemble object by uuid

        Args:
            uuid (str): ensemble uuid

        Returns: ensemble object
        """
        try:
            obj = self.get_object(uuid)
            assert isinstance(obj, objects.Ensemble)
            return obj
        except Exception:
            res = self._sumo.post(
                "/search", json=self._ensemble_query(uuid)
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "ensemble"
            ret = self._to_sumo(obj)
            self._cache.put(uuid, ret)
            return ret

    async def get_ensemble_by_uuid_async(self, uuid: str) -> objects.Ensemble:
        """Get ensemble object by uuid

        Args:
            uuid (str): ensemble uuid

        Returns: ensemble object
        """
        try:
            obj = await self.get_object_async(uuid)
            assert isinstance(obj, objects.Ensemble)
            return obj
        except Exception:
            res = (
                await self._sumo.post_async(
                    "/search", json=self._ensemble_query(uuid)
                )
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "ensemble"
            ret = self._to_sumo(obj)
            self._cache.put(uuid, ret)
            return ret

    def _realization_query(self, uuid) -> Dict:
        return {
            "query": {
                "term": {"fmu.realization.uuid.keyword": {"value": uuid}}
            },
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "class",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                    "fmu.ensemble",
                    "fmu.realization",
                ],
            },
        }

    def get_realization_by_uuid(self, uuid: str) -> objects.Realization:
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        try:
            obj = self.get_object(uuid)
            assert isinstance(obj, objects.Realization)
            return obj
        except Exception:
            res = self._sumo.post(
                "/search", json=self._realization_query(uuid)
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "realization"
            return self._to_sumo(obj)

    async def get_realization_by_uuid_async(
        self, uuid: str
    ) -> objects.Realization:
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        try:
            obj = await self.get_object_async(uuid)
            assert isinstance(obj, objects.Realization)
            return obj
        except Exception:
            res = (
                await self._sumo.post_async(
                    "/search", json=self._realization_query(uuid)
                )
            ).json()
            hits = res["hits"]["hits"]
            if len(hits) == 0:
                raise Exception(f"Document not found: {uuid}")
            obj = hits[0]
            obj["_id"] = uuid
            obj["_source"]["class"] = "realization"
            return self._to_sumo(obj)

    def get_surface_by_uuid(self, uuid: str) -> objects.Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        return self._get_object_by_class_and_uuid("surface", uuid)

    async def get_surface_by_uuid_async(self, uuid: str) -> objects.Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        return await self._get_object_by_class_and_uuid_async("surface", uuid)

    def get_polygons_by_uuid(self, uuid: str) -> objects.Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        return self._get_object_by_class_and_uuid("polygons", uuid)

    async def get_polygons_by_uuid_async(self, uuid: str) -> objects.Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        return await self._get_object_by_class_and_uuid_async("polygons", uuid)

    def get_table_by_uuid(self, uuid: str) -> objects.Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        return self._get_object_by_class_and_uuid("table", uuid)

    async def get_table_by_uuid_async(self, uuid: str) -> objects.Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        return await self._get_object_by_class_and_uuid_async("table", uuid)

    def __prepare_verify_aggregation_query(self) -> Dict:
        return {
            "query": self._query,
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                k: {"terms": {"field": k + ".keyword", "size": 1}}
                for k in [
                    "fmu.case.uuid",
                    "class",
                    "fmu.ensemble.name",
                    "fmu.entity.uuid",
                    "data.name",
                    "data.tagname",
                    "data.content",
                ]
            },
        }

    def __verify_aggregation_operation(
        self, sres
    ) -> Tuple[str, str, str, str]:
        tot_hits = sres["hits"]["total"]["value"]
        if tot_hits == 0:
            raise Exception("No matching realizations found.")
        conflicts = [
            k
            for (k, v) in sres["aggregations"].items()
            if (
                ("sum_other_doc_count" in v)
                and (v["sum_other_doc_count"] > 0)
                or (
                    "buckets" in v
                    and len(v["buckets"]) > 0
                    and v["buckets"][0]["doc_count"] != tot_hits
                )
            )
        ]
        if len(conflicts) > 0:
            raise Exception(f"Conflicting values for {conflicts}")
        entityuuid = sres["aggregations"]["fmu.entity.uuid"]["buckets"][0][
            "key"
        ]
        caseuuid = sres["aggregations"]["fmu.case.uuid"]["buckets"][0]["key"]
        ensemblename = sres["aggregations"]["fmu.ensemble.name"]["buckets"][0][
            "key"
        ]
        classname = sres["aggregations"]["class"]["buckets"][0]["key"]
        return caseuuid, classname, entityuuid, ensemblename

    def _verify_aggregation_operation(
        self, columns, operation
    ) -> Tuple[str, str, str, str]:
        assert columns is None or len(columns) == 1, (
            "Exactly one column required for collection aggregation."
        )
        sc = self if columns is None else self.filter(column=columns)
        query = sc.__prepare_verify_aggregation_query()
        sres = sc._sumo.post("/search", json=query).json()
        return sc.__verify_aggregation_operation(sres)

    def __prepare_aggregation_spec(
        self, caseuuid, classname, entityuuid, ensemblename, operation, columns
    ):
        spec = {
            "case_uuid": caseuuid,
            "class": classname,
            "entity_uuid": entityuuid,
            "ensemble_name": ensemblename,
            "iteration_name": ensemblename,
            "operations": [operation],
        }
        if columns is not None:
            spec["columns"] = columns
        return spec

    def _aggregate(self, columns=None, operation=None) -> objects.Child:
        caseuuid, classname, entityuuid, ensemblename = (
            self._verify_aggregation_operation(columns, operation)
        )
        spec = self.__prepare_aggregation_spec(
            caseuuid, classname, entityuuid, ensemblename, operation, columns
        )
        spec["object_ids"] = self.uuids
        try:
            res = self._sumo.post("/aggregations", json=spec)
        except httpx.HTTPStatusError as ex:
            print(ex.response.reason_phrase)
            print(ex.response.text)
            raise ex
        res = self._sumo.poll(res).json()
        return self._to_sumo(res)

    def aggregate(self, columns=None, operation=None) -> objects.Child:
        sc = self.filter(realization=True, column=columns)
        if len(sc.hidden) > 0:
            return sc.hidden._aggregate(columns=columns, operation=operation)
        else:
            return sc.visible._aggregate(columns=columns, operation=operation)

    async def _verify_aggregation_operation_async(
        self, columns, operation
    ) -> Tuple[str, str, str, str]:
        assert (
            operation != "collection"
            or columns is not None
            and len(columns) == 1
        ), "Exactly one column required for collection aggregation."
        sc = self if columns is None else self.filter(column=columns)
        query = sc.__prepare_verify_aggregation_query()
        sres = (await self._sumo.post_async("/search", json=query)).json()
        return sc.__verify_aggregation_operation(sres)

    async def _aggregate_async(
        self, columns=None, operation=None
    ) -> objects.Child:
        (
            caseuuid,
            classname,
            entityuuid,
            ensemblename,
        ) = await self._verify_aggregation_operation_async(columns, operation)
        spec = self.__prepare_aggregation_spec(
            caseuuid, classname, entityuuid, ensemblename, operation, columns
        )
        spec["object_ids"] = await self.uuids_async
        try:
            res = await self._sumo.post_async("/aggregations", json=spec)
        except httpx.HTTPStatusError as ex:
            print(ex.response.reason_phrase)
            print(ex.response.text)
            raise ex
        res = (await self._sumo.poll_async(res)).json()
        return self._to_sumo(res)

    async def aggregate_async(
        self, columns=None, operation=None
    ) -> objects.Child:
        sc = self.filter(realization=True, column=columns)
        length_hidden = await sc.hidden.length_async()
        if length_hidden > 0:
            return await sc.hidden._aggregate_async(
                columns=columns, operation=operation
            )
        else:
            return await sc.visible._aggregate_async(
                columns=columns, operation=operation
            )

    def aggregation(self, column=None, operation=None) -> objects.Child:
        assert operation is not None
        assert column is None or isinstance(column, str)
        sc = self.filter(aggregation=operation, column=column)
        numaggs = len(sc)
        assert numaggs <= 1
        if numaggs == 1:
            agg = sc.single
            assert isinstance(agg, objects.Child)
            ts = agg.metadata["_sumo"]["timestamp"]
            reals = self.filter(
                realization=True,
                complex={"range": {"_sumo.timestamp": {"lt": ts}}},
            ).realizationids
            if set(reals) == set(
                agg.metadata["fmu"]["aggregation"]["realization_ids"]
            ) and len(reals) == len(self.filter(realization=True)):
                return agg
        # ELSE
        return self.filter(realization=True).aggregate(
            columns=[column] if column is not None else None,
            operation=operation,
        )

    async def aggregation_async(
        self, column=None, operation=None
    ) -> objects.Child:
        assert operation is not None
        assert column is None or isinstance(column, str)
        sc = self.filter(aggregation=operation, column=column)
        numaggs = await sc.length_async()
        assert numaggs <= 1
        if numaggs == 1:
            agg = await sc.single_async
            assert isinstance(agg, objects.Child)
            ts = agg.metadata["_sumo"]["timestamp"]
            reals = await self.filter(
                realization=True,
                complex={"range": {"_sumo.timestamp": {"lt": ts}}},
            ).realizationids_async
            if (
                set(reals)
                == set(agg.metadata["fmu"]["aggregation"]["realization_ids"])
                and len(reals)
                == await self.filter(realization=True).length_async()
            ):
                return agg
        # ELSE
        return await self.filter(realization=True).aggregate_async(
            columns=[column] if column is not None else None,
            operation=operation,
        )

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def min(self):
        return self.aggregate(operation="min")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def max(self):
        return self.aggregate(operation="max")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def mean(self):
        return self.aggregate(operation="mean")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def std(self):
        return self.aggregate(operation="std")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def p10(self):
        return self.aggregate(operation="p10")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def p50(self):
        return self.aggregate(operation="p50")

    @deprecation.deprecated(
        details="Use the method 'aggregate' instead, with parameter 'operation'."
    )
    def p90(self):
        return self.aggregate(operation="p90")

    @property
    def realizationids(self) -> List[int]:
        """List of unique realization ids."""
        return self.get_field_values("fmu.realization.id")

    @property
    async def realizationids_async(self) -> List[int]:
        """List of unique realization ids."""
        return await self.get_field_values_async("fmu.realization.id")

    @property
    def stratcolumnidentifiers(self) -> List[str]:
        """List of unique stratigraphic column names."""
        return self.get_field_values(
            "masterdata.smda.stratigraphic_column.identifier.keyword"
        )

    @property
    async def stratcolumnidentifiers_async(self) -> List[str]:
        """List of unique stratigraphic column names."""
        return await self.get_field_values_async(
            "masterdata.smda.stratigraphic_column.identifier.keyword"
        )

    @property
    def fieldidentifiers(self) -> List[str]:
        """List of unique field names."""
        return self.get_field_values(
            "masterdata.smda.field.identifier.keyword"
        )

    @property
    async def fieldidentifiers_async(self) -> List[str]:
        """List of unique field names."""
        return await self.get_field_values_async(
            "masterdata.smda.field.identifier.keyword"
        )

    @property
    def users(self) -> List[str]:
        """List of unique user names."""
        return self.get_field_values("fmu.case.user.id.keyword")

    @property
    async def users_async(self) -> List[str]:
        """List of unique user names."""
        return await self.get_field_values_async("fmu.case.user.id.keyword")

    @property
    def statuses(self) -> List[str]:
        """List of unique case statuses."""
        return self.get_field_values("_sumo.status.keyword")

    @property
    async def statuses_async(self) -> List[str]:
        """List of unique case statuses."""
        return await self.get_field_values_async("_sumo.status.keyword")

    @property
    def columns(self) -> List[str]:
        """List of unique column names."""
        return self.get_field_values("data.spec.columns.keyword")

    @property
    async def columns_async(self) -> List[str]:
        """List of unique column names."""
        return await self.get_field_values_async("data.spec.columns.keyword")

    @property
    def contents(self) -> List[str]:
        """List of unique contents."""
        return self.get_field_values("data.content.keyword")

    @property
    async def contents_async(self) -> List[str]:
        """List of unique contents."""
        return await self.get_field_values_async("data.content.keyword")

    @property
    def vertical_domains(self) -> List[str]:
        """List of unique object vertical domains."""
        return self.get_field_values("data.vertical_domain.keyword")

    @property
    async def vertical_domains_async(self) -> List[str]:
        """List of unique object vertical domains."""
        return await self.get_field_values_async(
            "data.vertical_domain.keyword"
        )

    @property
    def stages(self) -> List[str]:
        """List of unique stages."""
        return self.get_field_values("fmu.context.stage.keyword")

    @property
    async def stages_async(self) -> List[str]:
        """List of unique stages."""
        return await self.get_field_values_async("fmu.context.stage.keyword")

    @property
    def aggregations(self) -> List[str]:
        """List of unique object aggregation operations."""
        return self.get_field_values("fmu.aggregation.operation.keyword")

    @property
    async def aggregations_async(self) -> List[str]:
        """List of unique object aggregation operations."""
        return await self.get_field_values_async(
            "fmu.aggregation.operation.keyword"
        )

    @property
    def dataformats(self) -> List[str]:
        """List of unique data.format values."""
        return self.get_field_values("data.format.keyword")

    @property
    async def dataformats_async(self) -> List[str]:
        """List of unique data.format values."""
        return await self.get_field_values_async("data.format.keyword")

    @property
    def tagnames(self) -> List[str]:
        """List of unique object tagnames."""
        return self.get_field_values("data.tagname.keyword")

    @property
    async def tagnames_async(self) -> List[str]:
        """List of unique object tagnames."""
        return await self.get_field_values_async("data.tagname.keyword")

    @property
    def names(self) -> List[str]:
        """List of unique object names."""
        return self.get_field_values("data.name.keyword")

    @property
    async def names_async(self) -> List[str]:
        """List of unique object names."""
        return await self.get_field_values_async("data.name.keyword")

    @property
    def classes(self) -> List[str]:
        """List of class names."""
        return self.get_field_values("class.keyword")

    @property
    async def classes_async(self) -> List[str]:
        """List of class names."""
        return await self.get_field_values_async("class.keyword")

    @property
    def standard_results(self) -> List[str]:
        """List of standard result names."""
        return self.get_field_values("data.standard_result.name.keyword")

    @property
    async def standard_results_async(self) -> List[str]:
        """List of standard result names."""
        return await self.get_field_values_async(
            "data.standard_result.name.keyword"
        )

    @property
    def entities(self) -> List[str]:
        """List of entity uuids."""
        return self.get_field_values("fmu.entity.uuid.keyword")

    @property
    async def entities_async(self) -> List[str]:
        """List of entity uuids."""
        return await self.get_field_values_async("fmu.entity.uuid.keyword")


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
                    ensemble="iter-0",
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
