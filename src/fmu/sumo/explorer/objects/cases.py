""" Module for searchcontext for collection of cases. """

from fmu.sumo.explorer.objects._search_context import SearchContext

class Cases(SearchContext):
    def __init__(self, sc):
        super().__init__(sc._sumo, sc._must, sc._must_not)
        return

    def __len__(self):
        if self._length is None:
            if self._hits is None:
                self._hits = self._search_all(select=False)
                pass
            self._length = len(self._hits)
            pass
        return self._length

    async def length_async(self):
        if self._length is None:
            if self._hits is None:
                self._hits = self._search_all(select=False)
                pass
            self._length = len(self._hits)
            pass
        return self._length
    
    def _search_all(self, select=False):
        uuids = self._get_field_values("fmu.case.uuid.keyword")
        if select is False:
            return uuids
        # ELSE
        return SearchContext(must=[{"ids": {"values": uuids}}])._search_all(select=select)

    async def _search_all_async(self, select=False):
        uuids = await self._get_field_values_async("fmu.case.uuid.keyword")
        if select is False:
            return uuids
        # ELSE
        return await SearchContext(must=[{"ids": {"values": uuids}}])._search_all_async(select=select)

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return
    
