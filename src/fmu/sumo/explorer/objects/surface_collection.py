"""Module containing class for collection of surfaces"""
from typing import Union, List, Dict, Tuple
from io import BytesIO
import xtgeo
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child_collection import ChildCollection
from fmu.sumo.explorer.objects.surface import Surface
from fmu.sumo.explorer.timefilter import TimeFilter
from fmu.sumo.explorer.pit import Pit

TIMESTAMP_QUERY = {
    "bool": {
        "must": [{"exists": {"field": "data.time.t0"}}],
        "must_not": [{"exists": {"field": "data.time.t1"}}],
    }
}


class SurfaceCollection(ChildCollection):
    """Class representing a collection of surface objects in Sumo"""

    def __init__(
        self,
        sumo: SumoClient,
        case_uuid: str,
        query: Dict = None,
        pit: Pit = None,
    ):
        """
        Args:
            sumo (SumoClient): connection to Sumo
            case_uuid (str): parent case uuid
            query (dict): elastic query object
            pit (Pit): point in time
        """
        super().__init__("surface", sumo, case_uuid, query, pit)

        self._aggregation_cache = {}

    def __getitem__(self, index) -> Surface:
        doc = super().__getitem__(index)
        return Surface(self._sumo, doc)

    async def getitem_async(self, index: int) -> Surface:
        doc = await super().getitem_async(index)
        return Surface(self._sumo, doc)

    @property
    def timestamps(self) -> List[str]:
        """List of unique timestamps in SurfaceCollection"""
        return self._get_field_values(
            "data.time.t0.value", TIMESTAMP_QUERY, True
        )

    @property
    async def timestamps_async(self) -> List[str]:
        """List of unique timestamps in CubeCollection"""
        return await self._get_field_values_async(
            "data.time.t0.value", TIMESTAMP_QUERY, True
        )

    @property
    def intervals(self) -> List[Tuple]:
        """List of unique intervals in SurfaceCollection"""
        res = self._sumo.post(
            "/search",
            json={
                "query": self._query,
                "aggs": {
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
                },
            },
        )

        buckets = res.json()["aggregations"]["t0"]["buckets"]
        intervals = []

        for bucket in buckets:
            t0 = bucket["key_as_string"]

            for t1 in bucket["t1"]["buckets"]:
                intervals.append((t0, t1["key_as_string"]))

        return intervals

    @property
    async def intervals_async(self) -> List[Tuple]:
        """List of unique intervals in SurfaceCollection"""
        res = await self._sumo.post_async(
            "/search",
            json={
                "query": self._query,
                "aggs": {
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
                },
            },
        )

        buckets = res.json()["aggregations"]["t0"]["buckets"]
        intervals = []

        for bucket in buckets:
            t0 = bucket["key_as_string"]

            for t1 in bucket["t1"]["buckets"]:
                intervals.append((t0, t1["key_as_string"]))

        return intervals

    def _aggregate(self, operation: str) -> xtgeo.RegularSurface:
        if operation not in self._aggregation_cache:
            objects = self._utils.get_objects(500, self._query, ["_id"])
            object_ids = list(map(lambda obj: obj["_id"], objects))

            res = self._sumo.post(
                "/surface/aggregate",
                json={"operation": [operation], "object_ids": object_ids},
            )

            self._aggregation_cache[operation] = xtgeo.surface_from_file(
                BytesIO(res.content)
            )

        return self._aggregation_cache[operation]

    async def _aggregate_async(self, operation: str) -> xtgeo.RegularSurface:
        if operation not in self._aggregation_cache:
            objects = await self._utils.get_objects_async(
                500, self._query, ["_id"]
            )
            object_ids = list(map(lambda obj: obj["_id"], objects))

            res = await self._sumo.post_async(
                "/surface/aggregate",
                json={"operation": [operation], "object_ids": object_ids},
            )

            self._aggregation_cache[operation] = xtgeo.surface_from_file(
                BytesIO(res.content)
            )

        return self._aggregation_cache[operation]

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        stratigraphic: Union[str, List[str], bool] = None,
        vertical_domain: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        time: TimeFilter = None,
        uuid: Union[str, List[str], bool] = None,
    ) -> "SurfaceCollection":
        """Filter surfaces

        Apply filters to the SurfaceCollection and get a new filtered instance.

        Args:
            name (Union[str, List[str], bool]): surface name
            tagname (Union[str, List[str], bool]): surface tagname
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            aggregation (Union[str, List[str], bool]): aggregation operation
            stage (Union[str, List[str], bool]): context/stage
            time (TimeFilter): time filter
            uuid (Union[str, List[str], bool]): surface object uuid
            stratigraphic (Union[str, List[str], bool]): surface stratigraphic
            vertical_domain (Union[str, List[str], bool]): surface vertical_domain

        Returns:
            SurfaceCollection: A filtered SurfaceCollection

        Examples:

            Match one value::

                surfs = case.surfaces.filter(
                    iteration="iter-0"
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

                surfs = case.surfacse.filter(
                    aggregation=True
                )

            Get all non-aggregated surfaces::

                surfs = case.surfaces.filter(
                    aggregation=False
                )

        """

        query = super()._add_filter(
            name=name,
            tagname=tagname,
            iteration=iteration,
            realization=realization,
            aggregation=aggregation,
            stage=stage,
            time=time,
            uuid=uuid,
            stratigraphic=stratigraphic,
            vertical_domain=vertical_domain,
        )

        return SurfaceCollection(self._sumo, self._case_uuid, query, self._pit)

    def mean(self) -> xtgeo.RegularSurface:
        """Perform a mean aggregation"""
        return self._aggregate("mean")

    async def mean_async(self) -> xtgeo.RegularSurface:
        """Perform a mean aggregation"""
        return await self._aggregate_async("mean")

    def min(self) -> xtgeo.RegularSurface:
        """Perform a minimum aggregation"""
        return self._aggregate("min")

    async def min_async(self) -> xtgeo.RegularSurface:
        """Perform a minimum aggregation"""
        return await self._aggregate_async("min")

    def max(self) -> xtgeo.RegularSurface:
        """Perform a maximum aggregation"""
        return self._aggregate("max")

    async def max_async(self) -> xtgeo.RegularSurface:
        """Perform a maximum aggregation"""
        return await self._aggregate_async("max")

    def std(self) -> xtgeo.RegularSurface:
        """Perform a standard deviation aggregation"""
        return self._aggregate("std")

    async def std_async(self) -> xtgeo.RegularSurface:
        """Perform a standard deviation aggregation"""
        return await self._aggregate_async("std")

    def p10(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p10")

    async def p10_async(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return await self._aggregate_async("p10")

    def p50(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p50")

    async def p50_async(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return await self._aggregate_async("p50")

    def p90(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p90")

    async def p90_async(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return await self._aggregate_async("p90")
