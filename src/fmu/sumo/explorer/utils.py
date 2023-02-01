from sumo.wrapper import SumoClient


class Utils:
    """A class with utility functions for communicating with Sumo API"""

    def __init__(self, sumo: SumoClient) -> None:
        self._sumo = sumo

    def get_buckets(
        self,
        field: str,
        must: list[dict] = None,
        must_not: list[dict] = None,
        sort: list = None,
    ) -> list[dict]:
        """Get a list of buckets

        Arguments:
            - field (str): a field in the metadata
            - must (list[dict] or None): filter options
            - sort (list or None): sorting options

        Returns:
            A list of unique values for a given field
        """
        query = {
            "size": 0,
            "aggs": {f"{field}": {"terms": {"field": field, "size": 30}}},
            "query": {"bool": {}},
        }

        if must is not None:
            query["query"]["bool"]["must"] = must

        if must_not is not None:
            query["query"]["bool"]["must_not"] = must_not

        if sort is not None:
            query["sort"] = sort

        res = self._sumo.post("/search", json=query)
        buckets = res.json()["aggregations"][field]["buckets"]

        return list(map(lambda bucket: bucket["key"], buckets))

    def get_objects(
        self,
        size: int,
        must: list[dict] = None,
        must_not: list[dict] = None,
        select: list[str] = None,
    ) -> list[dict]:
        """Get objects

        Arguments:
            - size (int): number of objects to return
            - must (list[dict] or None): filter options
            - select (list[str] or None): list of metadata fields to return

        Returns:
            A list of metadata
        """
        query = {"size": size, "query": {"bool": {}}}

        if must is not None:
            query["query"]["bool"]["must"] = must

        if must_not is not None:
            query["query"]["bool"]["must_not"] = must_not

        if select is not None:
            query["_source"] = select

        res = self._sumo.post("/search", json=query)

        return res.json()["hits"]["hits"]
