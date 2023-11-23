"""Module containing utility class"""
from typing import List, Dict
import json
from sumo.wrapper import SumoClient

def _build_bucket_query(query, field):
    return {
        "size": 0,
        "query": query,
        "aggs": {
            f"{field}": {
                "composite": {
                    "size": 1000,
                    "sources": [
                        {
                            f"{field}": {
                                "terms": {
                                    "field": field
                                }
                            }
                        }
                    ]
                }
            }
        }
    }

def _set_after_key(query, field, after_key):
    if after_key is not None:
        query["aggs"][field]["composite"]["after"] = after_key
        pass
    return query

class Utils:
    """A class with utility functions for communicating with Sumo API"""

    def __init__(self, sumo: SumoClient) -> None:
        self._sumo = sumo

    def get_buckets(
        self,
        field: str,
        query: Dict,
        sort: List = None,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
            - query (List[Dict] or None): filter options
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """

        query = _build_bucket_query(query, field)
        all_buckets = []
        after_key = None
        while True:
            query = _set_after_key(query, field, after_key)
            res = self._sumo.post("/search", json=query).json()
            buckets = res["aggregations"][field]["buckets"]
            if len(buckets) == 0:
                break
            after_key = res["aggregations"][field]["after_key"]
            buckets = [{"key": bucket["key"][field], "doc_count": bucket["doc_count"]} for bucket in buckets]
            all_buckets = all_buckets + buckets
            pass

        return all_buckets

    async def get_buckets_async(
        self,
        field: str,
        query: Dict,
        sort: List = None,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
            - query (List[Dict] or None): filter options
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """

        query = _build_bucket_query(query, field)
        all_buckets = []
        after_key = None
        while True:
            query = _set_after_key(query, field, after_key)
            res = await self._sumo.post_async("/search", json=query)
            res = res.json()
            buckets = res["aggregations"][field]["buckets"]
            if len(buckets) == 0:
                break
            after_key = res["aggregations"][field]["after_key"]
            buckets = [{"key": bucket["key"][field], "doc_count": bucket["doc_count"]} for bucket in buckets]
            all_buckets = all_buckets + buckets
            pass

        return all_buckets

    def get_objects(
        self,
        size: int,
        query: Dict,
        select: List[str] = None,
    ) -> List[Dict]:
        """Get objects

        Args:
            size (int): number of objects to return
            query (List[Dict] or None): filter options
            select (List[str] or None): list of metadata fields to return

        Returns:
            List[Dict]: A List of metadata
        """
        query = {"size": size, "query": query}

        if select is not None:
            query["_source"] = select

        res = self._sumo.post("/search", json=query)

        return res.json()["hits"]["hits"]

    async def get_objects_async(
        self,
        size: int,
        query: Dict,
        select: List[str] = None,
    ) -> List[Dict]:
        """Get objects

        Args:
            size (int): number of objects to return
            query (List[Dict] or None): filter options
            select (List[str] or None): list of metadata fields to return

        Returns:
            List[Dict]: A List of metadata
        """
        query = {"size": size, "query": query}

        if select is not None:
            query["_source"] = select

        res = await self._sumo.post_async("/search", json=query)

        return res.json()["hits"]["hits"]

    def get_object(self, uuid: str, select: List[str] = None) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """

        query = {
            "query": {"term": {"_id": uuid}},
            "size": 1,
        }

        if select is not None:
            query["_source"] = select

        res = self._sumo.post("/search", json=query)
        hits = res.json()["hits"]["hits"]

        if len(hits) == 0:
            raise Exception(f"Document not found: {uuid}")

        return hits[0]

    async def get_object_async(self, uuid: str, select: List[str] = None) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """

        query = {
            "query": {"term": {"_id": uuid}},
            "size": 1,
        }

        if select is not None:
            query["_source"] = select

        res = await self._sumo.post_async("/search", json=query)
        hits = res.json()["hits"]["hits"]

        if len(hits) == 0:
            raise Exception(f"Document not found: {uuid}")

        return hits[0]

    def extend_query_object(self, old: Dict, new: Dict) -> Dict:
        """Extend query object

        Args:
            old (Dict): old query object
            new (Dict): new query object

        Returns:
            Dict: Extended query object
        """
        return_value = old
        if new is not None:
            stringified = json.dumps(old)
            extended = json.loads(stringified)

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
            return_value = extended
        return return_value

    def build_terms(self, keys_vals: Dict) -> List[Dict]:
        """Build a list of term objects

        `key_vals` expects a dictionary with the following structure:

        ```
        {
            "my.property.1": "value",
            "my.property.2": ["list", "of", "values"]
        }
        ```

        Returns a list of term expressions:

        ```
        [
            {"term": {"my.property.1": "value"}},
            {"terms": {"my.property.2": ["list", "of", "values"]}}
        ]
        ```

        Args:
            key_vals: Dict

        Returns:
            List[Dict]: list of term expressions
        """
        terms = []

        for key in keys_vals:
            val = keys_vals[key]
            if val is not None:
                items = [val] if not isinstance(val, list) else val
                terms.append({"terms": {key: items}})

        return terms
