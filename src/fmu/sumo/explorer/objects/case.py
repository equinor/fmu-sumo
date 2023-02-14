from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.document import Document
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table_collection import TableCollection
from fmu.sumo.explorer.utils import Utils
from typing import Dict, List


class Case(Document):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        super().__init__(metadata)
        self._sumo = sumo
        self._utils = Utils(sumo)
        self._iterations = None

    @property
    def name(self) -> str:
        return self._get_property(["fmu", "case", "name"])

    @property
    def status(self) -> str:
        return self._get_property(["_sumo", "status"])

    @property
    def user(self) -> str:
        return self._get_property(["fmu", "case", "user", "id"])

    @property
    def asset(self) -> str:
        return self._get_property(["access", "asset", "name"])

    @property
    def field(self) -> str:
        fields = self._get_property(["masterdata", "smda", "field"])
        return fields[0]["identifier"]

    @property
    def iterations(self) -> List[Dict]:
        if self._iterations is None:
            query = {
                "query": {"term": {"_sumo.parent_object.keyword": self.id}},
                "aggs": {
                    "id": {
                        "terms": {"field": "fmu.iteration.id", "size": 50},
                        "aggs": {
                            "name": {
                                "terms": {"field": "fmu.iteration.name.keyword", "size": 1}
                            },
                            "realizations": {
                                "terms": {"field": "fmu.realization.id", "size": 1000}
                            }
                        },
                    },
                },
                "size": 0,
            }

            res = self._sumo.post("/search", json=query)
            buckets = res.json()["aggregations"]["id"]["buckets"]
            iterations = []

            for bucket in buckets:
                iterations.append({
                    "id": bucket["key"],
                    "name": bucket["name"]["buckets"][0]["key"],
                    "realizations": len(bucket["realizations"]["buckets"])
                })

            self._iterations = iterations

        return self._iterations

    @property
    def surfaces(self) -> SurfaceCollection:
        return SurfaceCollection(self._sumo, self._uuid)

    @property
    def polygons(self) -> PolygonsCollection:
        return PolygonsCollection(self._sumo, self._uuid)

    @property
    def tables(self) -> TableCollection:
        return TableCollection(self._sumo, self._uuid)
