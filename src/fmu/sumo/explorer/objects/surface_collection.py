from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.surface import Surface
from fmu.sumo.explorer.timefilter import TimeFilter
import xtgeo
from io import BytesIO
from typing import Union, List, Dict, Tuple

TIMESTAMP_QUERY = {
    "bool": {
        "must": [{"exists": {"field": "data.time.t0"}}],
        "must_not": [{"exists": {"field": "data.time.t1"}}],
    }
}


class SurfaceCollection(ChildCollection):
    """Class for representing a collection of surface objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_uuid: str, query: Dict = None):
        super().__init__("surface", sumo, case_uuid, query)
        self._aggregation_cache = {}

    def __getitem__(self, index) -> Surface:
        doc = super().__getitem__(index)
        return Surface(self._sumo, doc)

    @property
    def timestamps(self) -> List[str]:
        return self._get_field_values(
            "data.time.t0.value", TIMESTAMP_QUERY, True
        )

    @property
    def intervals(self) -> List[Tuple]:
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

    def _aggregate(self, operation: str) -> xtgeo.RegularSurface:
        if operation not in self._aggregation_cache:
            objects = self._utils.get_objects(500, self._query, ["_id"])
            object_ids = list(map(lambda obj: obj["_id"], objects))

            res = self._sumo.post(
                "/aggregate",
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
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        aggregation: Union[str, List[str], bool] = None,
        stage: Union[str, List[str], bool] = None,
        time: TimeFilter = None,
    ) -> "SurfaceCollection":
        """Filter surfaces

        Arguments:
            - name (Union[str, List[str], bool]): surface name
            - tagname (Union[str, List[str], bool]): surface tagname
            - iteration (Union[int, List[int], bool]): iteration id
            - realization Union[int, List[int], bool]: realization id
            - aggregation (Union[str, List[str], bool]): aggregation operation
            - stage (Union[str, List[str], bool]): context/stage
            - time (TimeFilter): time filter

        Returns:
            A filtered SurfaceCollection
        """

        query = super()._add_filter(
            name, tagname, iteration, realization, aggregation, stage, time
        )

        return SurfaceCollection(self._sumo, self._case_uuid, query)

    def mean(self) -> xtgeo.RegularSurface:
        """Perform a mean aggregation"""
        return self._aggregate("mean")

    def min(self) -> xtgeo.RegularSurface:
        """Perform a minimum aggregation"""
        return self._aggregate("min")

    def max(self) -> xtgeo.RegularSurface:
        """Perform a maximum aggregation"""
        return self._aggregate("max")

    def std(self) -> xtgeo.RegularSurface:
        """Perform a standard deviation aggregation"""
        return self._aggregate("std")

    def p10(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p10")

    def p50(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p50")

    def p90(self) -> xtgeo.RegularSurface:
        """Perform a percentile aggregation"""
        return self._aggregate("p90")
