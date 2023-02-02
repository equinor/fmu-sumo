from sumo.wrapper import SumoClient
from typing import List, Dict
import json


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
            - must (List[Dict] or None): filter options
            - sort (List or None): sorting options

        Returns:
            A List of unique values for a given field
        """
        query = {
            "size": 0,
            "aggs": {f"{field}": {"terms": {"field": field, "size": 50}}},
            "query": query,
        }

        if sort is not None:
            query["sort"] = sort

        res = self._sumo.post("/search", json=query)
        buckets = res.json()["aggregations"][field]["buckets"]

        return list(map(lambda bucket: bucket["key"], buckets))

    def get_objects(
        self,
        size: int,
        query: Dict,
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
        query = {"size": size, "query": query}

        if select is not None:
            query["_source"] = select

        res = self._sumo.post("/search", json=query)

        return res.json()["hits"]["hits"]

    def extend_query_object(self, old: Dict, new: Dict) -> Dict:
        """Extend query object

        Arguments:
            - old (Dict): old query object
            - new (Dict): new query object

        Returns:
            Extender query object
        """
        stringified = json.dumps(old)
        extended = json.loads(stringified)

        for key in new:
            if key in extended:
                if type(new[key]) == dict:
                    extended[key] = self.extend_query_object(extended[key], new[key])
                elif type(new[key]) == list:
                    for val in new[key]:
                        if val not in extended[key]:
                            extended[key].append(val)
                else:
                    extended[key] = new[key]
            else:
                extended[key] = new[key]

        return extended

    def build_terms(self, keys_vals: Dict):
        terms = []

        for key in keys_vals:
            val = keys_vals[key]
            if val is not None:
                items = [val] if type(val) != list else val
                terms.append({"terms": {key: items}})

        return terms
