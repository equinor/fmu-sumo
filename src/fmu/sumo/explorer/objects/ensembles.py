"""Module for searchcontext for collection of ensembles."""

from typing import List

from ._search_context import SearchContext


class Ensembles(SearchContext):
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
        return ["ensemble"]

    @property
    async def classes_async(self) -> List[str]:
        return ["ensemble"]

    @property
    def ensemblenames(self) -> List[str]:
        return self.get_field_values("fmu.ensemble.name.keyword")

    @property
    async def ensemblenames_async(self) -> List[str]:
        return await self.get_field_values_async("fmu.ensemble.name.keyword")
