"""Module for searchcontext for collection of realizations."""

from typing import List

from ._search_context import SearchContext


class Realizations(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        return

    def __len__(self):
        return len(self.uuids)

    async def length_async(self):
        return len(await self.uuids_async)

    @property
    def classes(self) -> List[str]:
        return ["realization"]

    @property
    async def classes_async(self) -> List[str]:
        return ["realization"]

    @property
    def realizationids(self) -> List[int]:
        return self.get_field_values("fmu.realization.id")

    @property
    async def realizationids_async(self) -> List[int]:
        return await self.get_field_values_async("fmu.realization.id")
