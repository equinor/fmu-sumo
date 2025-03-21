"""Module containing case class"""

from typing import Dict

from sumo.wrapper import SumoClient

from ._document import Document
from ._search_context import SearchContext


def _make_overview_query(id) -> Dict:
    return {
        "query": {"term": {"fmu.case.uuid.keyword": id}},
        "aggs": {
            "iteration_uuids": {
                "terms": {"field": "fmu.iteration.uuid.keyword", "size": 100}
            },
            "iteration_names": {
                "terms": {"field": "fmu.iteration.name.keyword", "size": 100}
            },
            "data_types": {"terms": {"field": "class.keyword", "size": 100}},
            "iterations": {
                "terms": {"field": "fmu.iteration.uuid.keyword", "size": 100},
                "aggs": {
                    "iteration_name": {
                        "terms": {
                            "field": "fmu.iteration.name.keyword",
                            "size": 100,
                        }
                    },
                    "numreal": {
                        "cardinality": {"field": "fmu.realization.id"}
                    },
                    "maxreal": {"max": {"field": "fmu.realization.id"}},
                    "minreal": {"min": {"field": "fmu.realization.id"}},
                },
            },
        },
        "size": 0,
    }


class Case(Document, SearchContext):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        Document.__init__(self, metadata)
        SearchContext.__init__(
            self, sumo, must=[{"term": {"fmu.case.uuid.keyword": self.uuid}}]
        )
        self._overview = None
        self._iterations = None

    @property
    def overview(self) -> Dict:
        """Overview of case contents."""

        def extract_bucket_keys(bucket, name):
            return [b["key"] for b in bucket[name]["buckets"]]

        if self._overview is None:
            query = _make_overview_query(self._uuid)
            res = self._sumo.post("/search", json=query)
            data = res.json()
            aggs = data["aggregations"]
            iteration_names = extract_bucket_keys(aggs, "iteration_names")
            iteration_uuids = extract_bucket_keys(aggs, "iteration_uuids")
            data_types = extract_bucket_keys(aggs, "data_types")
            iterations = {}
            for bucket in aggs["iterations"]["buckets"]:
                iterid = bucket["key"]
                itername = extract_bucket_keys(bucket, "iteration_name")
                minreal = bucket["minreal"]["value"]
                maxreal = bucket["maxreal"]["value"]
                numreal = bucket["numreal"]["value"]
                iterations[iterid] = {
                    "name": itername,
                    "minreal": minreal,
                    "maxreal": maxreal,
                    "numreal": numreal,
                }
            self._overview = {
                "iteration_names": iteration_names,
                "iteration_uuids": iteration_uuids,
                "data_types": data_types,
                "iterations": iterations,
            }

        return self._overview

    @property
    def field(self) -> str:
        """Case field"""
        return self.get_property("masterdata.smda.field[0].identifier")

    @property
    def asset(self) -> str:
        """Case asset"""
        return self.get_property("access.asset.name")

    @property
    def user(self) -> str:
        """Name of user who uploaded case."""
        return self.get_property("fmu.case.user.id")

    @property
    def status(self) -> str:
        """Case status"""
        return self.get_property("_sumo.status")

    @property
    def name(self) -> str:
        """Case name"""
        return self.get_property("fmu.case.name")
