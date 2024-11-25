""" Module for searchcontext for collection of cases. """

from fmu.sumo.explorer.objects._search_context import SearchContext

class Cases(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"terms": {"fmu.case.uuid.keyword": uuids}}])
        self._hits = uuids
        return

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return

    def filter(self, **kwargs):
        sc = super().filter(**kwargs)
        uuids = sc.uuids
        return Cases(self, uuids)
    
