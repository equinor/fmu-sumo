"""Module containing case class"""

from typing import Dict

from sumo.wrapper import SumoClient

from ._document import Document
from ._search_context import SearchContext


def _make_overview_query(id) -> Dict:
    return {
        "query": {"term": {"fmu.case.uuid.keyword": id}},
        "aggs": {
            "ensemble_uuids": {
                "terms": {"field": "fmu.ensemble.uuid.keyword", "size": 100}
            },
            "ensemble_names": {
                "terms": {"field": "fmu.ensemble.name.keyword", "size": 100}
            },
            "data_types": {"terms": {"field": "class.keyword", "size": 100}},
            "ensembles": {
                "terms": {"field": "fmu.ensemble.uuid.keyword", "size": 100},
                "aggs": {
                    "ensemble_name": {
                        "terms": {
                            "field": "fmu.ensemble.name.keyword",
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
        self._ensembles = None

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
            ensemble_names = extract_bucket_keys(aggs, "ensemble_names")
            ensemble_uuids = extract_bucket_keys(aggs, "ensemble_uuids")
            data_types = extract_bucket_keys(aggs, "data_types")
            ensembles = {}
            for bucket in aggs["ensembles"]["buckets"]:
                iterid = bucket["key"]
                itername = extract_bucket_keys(bucket, "ensemble_name")
                minreal = bucket["minreal"]["value"]
                maxreal = bucket["maxreal"]["value"]
                numreal = bucket["numreal"]["value"]
                ensembles[iterid] = {
                    "name": itername,
                    "minreal": minreal,
                    "maxreal": maxreal,
                    "numreal": numreal,
                }
            self._overview = {
                "ensemble_names": ensemble_names,
                "ensemble_uuids": ensemble_uuids,
                "data_types": data_types,
                "ensembles": ensembles,
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
