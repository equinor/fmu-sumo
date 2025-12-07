"""Module for searchcontext for collection of ensembles."""

from copy import deepcopy
from typing import List

from fmu.sumo.explorer.objects.ensemble import Ensemble

from ._search_context import SearchContext


class Ensembles(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        self._prototype = None
        self._map = {}
        return

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return

    def filter(self, **kwargs):
        sc = super().filter(**kwargs)
        uuids = sc.get_field_values("fmu.ensemble.uuid.keyword")
        return Ensembles(sc, uuids)

    def get_object(self, uuid):
        if self._prototype is None:
            obj = super().get_object(uuid)
            if len(self.get_field_values("fmu.case.uuid.keyword")) > 1:
                return obj
            # ELSE
            self._prototype = obj.metadata
            buckets = self.get_composite_agg(
                {
                    "uuid": "fmu.ensemble.uuid.keyword",
                    "name": "fmu.ensemble.name.keyword",
                }
            )
            self._map = {b["uuid"]: b for b in buckets}
            pass
        metadata = deepcopy(self._prototype)
        b = self._map[uuid]
        metadata["fmu"]["ensemble"] = b
        return Ensemble(self._sumo, {"_id": uuid, "_source": metadata})

    async def get_object_async(self, uuid):
        if self._prototype is None:
            obj = await super().get_object_async(uuid)
            if (
                len(await self.get_field_values_async("fmu.case.uuid.keyword"))
                > 1
            ):
                return obj
            # ELSE
            self._prototype = obj.metadata
            buckets = await self.get_composite_agg_async(
                {
                    "uuid": "fmu.ensemble.uuid.keyword",
                    "name": "fmu.ensemble.name.keyword",
                }
            )
            self._map = {b["uuid"]: b for b in buckets}
            pass
        metadata = deepcopy(self._prototype)
        b = self._map[uuid]
        metadata["fmu"]["ensemble"] = b
        return Ensemble(self._sumo, {"_id": uuid, "_source": metadata})

    @property
    def classes(self) -> List[str]:
        return ["ensemble"]

    @property
    async def classes_async(self) -> List[str]:
        return ["ensemble"]

    @property
    def ensemblenames(self) -> List[str]:
        return [self.get_object(uuid).ensemblename for uuid in self._hits]

    @property
    async def ensemblenames_async(self) -> List[str]:
        return [
            (await self.get_object_async(uuid)).ensemblename
            for uuid in self._hits
        ]
