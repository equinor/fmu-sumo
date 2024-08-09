"""Module containing case class"""

from typing import Dict, List
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document
from fmu.sumo.explorer.objects._search_context import SearchContext

_prop_desc = [
    ("name", "fmu.case.name", "Case name"),
    ("status", "_sumo.status", "Case status"),
    ("user", "fmu.case.user.id", "Name of user who uploaded case."),
    ("asset", "access.asset.name", "Case asset"),
    ("field", "masterdata.smda.field[0].identifier", "Case field"),
]


class Case(Document, SearchContext):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        Document.__init__(self, metadata)
        SearchContext.__init__(self, sumo, must=[{"term": {"fmu.case.uuid.keyword": self.uuid}}])
        self._overview = None
        self._iterations = None

    @property
    def overview(self):
        """Overview of case contents."""
        return self._overview

    @property
    def iterations(self) -> List[Dict]:
        """List of case iterations"""
        if self._iterations is None:
            query = {
                "query": {"term": {"_sumo.parent_object.keyword": self.uuid}},
                "aggs": {
                    "uuid": {
                        "terms": {
                            "field": "fmu.iteration.uuid.keyword",
                            "size": 50,
                        },
                        "aggs": {
                            "name": {
                                "terms": {
                                    "field": "fmu.iteration.name.keyword",
                                    "size": 1,
                                }
                            },
                            "realizations": {
                                "cardinality": {
                                    "field": "fmu.realization.id",
                                }
                            },
                        },
                    },
                },
                "size": 0,
            }

            res = self._sumo.post("/search", json=query)
            buckets = res.json()["aggregations"]["uuid"]["buckets"]
            iterations = []

            for bucket in buckets:
                iterations.append(
                    {
                        "uuid": bucket["key"],
                        "name": bucket["name"]["buckets"][0]["key"],
                        "realizations": bucket["realizations"]["value"],
                    }
                )

            self._iterations = iterations

        return self._iterations

    @property
    async def iterations_async(self) -> List[Dict]:
        """List of case iterations"""
        if self._iterations is None:
            query = {
                "query": {"term": {"_sumo.parent_object.keyword": self.uuid}},
                "aggs": {
                    "id": {
                        "terms": {"field": "fmu.iteration.id", "size": 50},
                        "aggs": {
                            "name": {
                                "terms": {
                                    "field": "fmu.iteration.name.keyword",
                                    "size": 1,
                                }
                            },
                            "realizations": {
                                "terms": {
                                    "field": "fmu.realization.id",
                                    "size": 1000,
                                }
                            },
                        },
                    },
                },
                "size": 0,
            }

            res = await self._sumo.post_async("/search", json=query)
            buckets = res.json()["aggregations"]["id"]["buckets"]
            iterations = []

            for bucket in buckets:
                iterations.append(
                    {
                        "id": bucket["key"],
                        "name": bucket["name"]["buckets"][0]["key"],
                        "realizations": len(bucket["realizations"]["buckets"]),
                    }
                )

            self._iterations = iterations

        return self._iterations

    def get_realizations(self, iteration: str = None) -> List[int]:
        """Get a list of realization ids

        Calling this method without the iteration argument will
        return a list of unique realization ids across iterations.
        It is not guaranteed that all realizations in this list exists
        in all case iterations.

        Args:
            iteration (str): iteration name

        Returns:
            List[int]: realization ids
        """
        must = [{"term": {"_sumo.parent_object.keyword": self.uuid}}]

        if iteration:
            must.append({"term": {"fmu.iteration.name.keyword": iteration}})

        buckets = self._utils.get_buckets(
            "fmu.realization.id",
            query={"bool": {"must": must}},
            sort=["fmu.realization.id"],
        )

        return list(map(lambda b: b["key"], buckets))

    async def get_realizations_async(self, iteration: str = None) -> List[int]:
        """Get a list of realization ids

        Calling this method without the iteration argument will
        return a list of unique realization ids across iterations.
        It is not guaranteed that all realizations in this list exists
        in all case iterations.

        Args:
            iteration (str): iteration name

        Returns:
            List[int]: realization ids
        """
        must = [{"term": {"_sumo.parent_object.keyword": self.uuid}}]

        if iteration:
            must.append({"term": {"fmu.iteration.name.keyword": iteration}})

        buckets = await self._utils.get_buckets_async(
            "fmu.realization.id",
            query={"bool": {"must": must}},
            sort=["fmu.realization.id"],
        )

        return list(map(lambda b: b["key"], buckets))


Case.map_properties(Case, _prop_desc)
