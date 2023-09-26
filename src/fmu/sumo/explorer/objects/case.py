"""Module containing case class"""
from typing import Dict, List
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.objects.table_collection import TableCollection
from fmu.sumo.explorer.objects.cube_collection import CubeCollection
from fmu.sumo.explorer.objects.dictionary_collection import (
    DictionaryCollection,
)
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer.pit import Pit


class Case(Document):
    """Class for representing a case in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, pit: Pit = None):
        super().__init__(metadata)
        self._pit = pit
        self._sumo = sumo
        self._utils = Utils(sumo)
        self._iterations = None

    @property
    def name(self) -> str:
        """Case name"""
        return self._get_property(["fmu", "case", "name"])

    @property
    def status(self) -> str:
        """Case status"""
        return self._get_property(["_sumo", "status"])

    @property
    def user(self) -> str:
        """Name of user who uploaded the case"""
        return self._get_property(["fmu", "case", "user", "id"])

    @property
    def asset(self) -> str:
        """Case asset"""
        return self._get_property(["access", "asset", "name"])

    @property
    def field(self) -> str:
        """Case field"""
        fields = self._get_property(["masterdata", "smda", "field"])
        return fields[0]["identifier"]

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

    @property
    def surfaces(self) -> SurfaceCollection:
        """List of case surfaces"""
        return SurfaceCollection(self._sumo, self._uuid, pit=self._pit)

    @property
    def polygons(self) -> PolygonsCollection:
        """List of case polygons"""
        return PolygonsCollection(self._sumo, self._uuid, pit=self._pit)

    @property
    def tables(self) -> TableCollection:
        """List of case tables"""
        return TableCollection(self._sumo, self._uuid, pit=self._pit)

    @property
    def cubes(self) -> CubeCollection:
        """List of case tables"""
        return CubeCollection(self._sumo, self._uuid, pit=self._pit)

    @property
    def dictionaries(self) -> DictionaryCollection:
        """List of case dictionaries"""
        return DictionaryCollection(self._sumo, self._uuid, pit=self._pit)
