"""Module for searchcontext for collection of realizations."""

from typing import List

from ._search_context import SearchContext


class Realizations(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        return

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return

    def filter(self, **kwargs):
        sc = super().filter(**kwargs)
        uuids = sc.get_field_values("fmu.realization.uuid.keyword")
        return Realizations(self, uuids)

    @property
    def classes(self) -> List[str]:
        return ["realization"]

    @property
    async def classes_async(self) -> List[str]:
        return ["realization"]

    @property
    def realizationids(self) -> List[int]:
        return [self.get_object(uuid).realizationid for uuid in self._hits]

    @property
    async def realizationids_async(self) -> List[int]:
        return [
            (await self.get_object_async(uuid)).realizationid
            for uuid in self._hits
        ]
