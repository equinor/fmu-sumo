"""Module for searchcontext for collection of iterations."""

from typing import List

from ._search_context import SearchContext


class Iterations(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        return

    @property
    def classes(self) -> List[str]:
        return ["iteration"]

    @property
    async def classes_async(self) -> List[str]:
        return ["iteration"]

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return

    def filter(self, **kwargs):
        sc = super().filter(**kwargs)
        uuids = sc.get_field_values("fmu.iteration.uuid.keyword")
        return Iterations(sc, uuids)
