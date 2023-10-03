"""Module containing class for collection of cubes """
from typing import Union, List, Dict, Tuple
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child_collection import ChildCollection
from fmu.sumo.explorer.objects.cube import Cube
from fmu.sumo.explorer.pit import Pit
from fmu.sumo.explorer.timefilter import TimeFilter

TIMESTAMP_QUERY = {
    "bool": {
        "must": [{"exists": {"field": "data.time.t0"}}],
        "must_not": [{"exists": {"field": "data.time.t1"}}],
    }
}


class CubeCollection(ChildCollection):
    """Class for representing a collection of seismic cube objects in Sumo"""

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
        super().__init__("cube", sumo, case_uuid, query, pit)

    def __getitem__(self, index) -> Cube:
        doc = super().__getitem__(index)
        return Cube(self._sumo, doc)

    async def getitem_async(self, index: int) -> Cube:
        doc = await super().getitem_async(index)
        return Cube(self._sumo, doc)

    @property
    def timestamps(self) -> List[str]:
        """List of unique timestamps in CubeCollection"""
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
        """List of unique intervals in CubeCollection"""
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
        """List of unique intervals in CubeCollection"""
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

    def filter(
        self,
        name: Union[str, List[str], bool] = None,
        tagname: Union[str, List[str], bool] = None,
        iteration: Union[str, List[str], bool] = None,
        realization: Union[int, List[int], bool] = None,
        stage: Union[str, List[str], bool] = None,
        time: TimeFilter = None,
        uuid: Union[str, List[str], bool] = None,
        is_observation: bool = None,
        is_prediction: bool = None,
    ) -> "CubeCollection":
        """Filter cubes

        Args:
            name (Union[str, List[str], bool]): cube name
            tagname (Union[str, List[str], bool]): cube tagname
            iteration (Union[int, List[int], bool]): iteration id
            realization Union[int, List[int], bool]: realization id
            stage (Union[str, List[str], bool]): context/stage
            time (TimeFilter): time filter
            uuid (Union[str, List[str], bool]): cube object uuid
            is_observation (bool): cube is_observation
            is_prediction (bool): cube is_prediction

        Returns:
            CubeCollection: A filtered CubeCollection
        """
        query = super()._add_filter(
            name=name,
            tagname=tagname,
            iteration=iteration,
            realization=realization,
            stage=stage,
            time=time,
            uuid=uuid,
            is_observation=is_observation,
            is_prediction=is_prediction,
        )

        return CubeCollection(self._sumo, self._case_uuid, query, self._pit)
