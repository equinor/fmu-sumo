from sumo.wrapper import SumoClient
from typing import List, Dict


class Utils:
    """A class with utility functions for communicating with Sumo API"""

    def __init__(self, sumo: SumoClient) -> None:
        self._sumo = sumo

    def get_buckets(
        self,
        field: str,
        must: List[Dict] = None,
        sort: List = None,
    ) -> List[Dict]:
        """Get a List of buckets

        Arguments:
            - field (str): a field in the metadata
            - must (List[Dict] or None): filter options
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """
        query = {
            "size": 0,
            "aggs": {f"{field}": {"terms": {"field": field, "size": 50}}},
        }

        if must is not None:
            query["query"] = {"bool": {"must": must}}

        if sort is not None:
            query["sort"] = sort

        res = self._sumo.post("/search", json=query)
        buckets = res.json()["aggregations"][field]["buckets"]

        return list(map(lambda bucket: bucket["key"], buckets))

    def get_objects(
        self,
        size: int,
        must: List[Dict] = None,
        select: List[str] = None,
    ) -> List[Dict]:
        """Get objects

        Arguments:
            - size (int): number of objects to return
            - must (List[Dict] or None): filter options
            - select (List[str] or None): List of metadata fields to return

        Returns:
            A List of metadata
        """
        query = {"size": size}

        if must is not None:
            query["query"] = {"bool": {"must": must}}

        if select is not None:
            query["_source"] = select

        res = self._sumo.post("/search", json=query)

        return res.json()["hits"]["hits"]
