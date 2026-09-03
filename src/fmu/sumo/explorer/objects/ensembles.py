"""Module for searchcontext for collection of ensembles."""

from ._search_context import SearchContext


class Ensembles(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids

    @property
    def classes(self) -> list[str]:
        return ["ensemble"]

    @property
    async def classes_async(self) -> list[str]:
        return ["ensemble"]

    @property
    def ensemblenames(self) -> list[str]:
        return self.get_field_values("fmu.ensemble.name.keyword")

    @property
    async def ensemblenames_async(self) -> list[str]:
        return await self.get_field_values_async("fmu.ensemble.name.keyword")
