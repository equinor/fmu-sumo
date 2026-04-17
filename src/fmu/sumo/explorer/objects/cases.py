"""Module for searchcontext for collection of cases."""

from typing import List

from ._search_context import SearchContext


class Cases(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        return

    @property
    def classes(self) -> List[str]:
        return ["case"]

    @property
    async def classes_async(self) -> List[str]:
        return ["case"]

    @property
    def names(self):
        return self.get_field_values("fmu.case.name.keyword")
