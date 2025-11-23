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
            self._prototype = super().get_object(uuid).metadata
            buckets = self.get_composite_agg(
                {
                    "uuid": "fmu.ensemble.uuid.keyword",
                    "name": "fmu.ensemble.name.keyword",
                }
            )
            self._map = {b["uuid"]: b for b in buckets}
            pass
        obj = deepcopy(self._prototype)
        b = self._map[uuid]
        obj["fmu"]["ensemble"] = b
        return Ensemble(self._sumo, {"_id": uuid, "_source": obj})

    async def get_object_async(self, uuid):
        if self._prototype is None:
            self._prototype = (await super().get_object_async(uuid)).metadata
            buckets = self.get_composite_agg(
                {
                    "uuid": "fmu.realization.uuid.keyword",
                    "name": "fmu.realization.name.keyword",
                    "id": "fmu.realization.id",
                }
            )
            self._map = {b["uuid"]: b for b in buckets}
            pass
        obj = deepcopy(self._prototype)
        b = self._map[uuid]
        obj["fmu"]["realization"] = b
        return Ensemble(self._sumo, {"_id": uuid, "_source": obj})

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
