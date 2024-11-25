""" Module for searchcontext for collection of realizations. """

from typing import Dict, List
from fmu.sumo.explorer.objects._search_context import SearchContext

class Realizations(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"terms": {"fmu.realization.uuid.keyword": uuids}}])
        self._hits = uuids
        return

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return
    
    def get_object(self, uuid: str, select: List[str] = None) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """
        obj = self._cache.get(uuid)
        if obj is None:
            obj = self.get_realization_by_uuid(uuid)
            self._cache.put(uuid, obj)
            pass

        return obj

    async def get_object_async(
        self, uuid: str, select: List[str] = None
    ) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """

        obj = self._cache.get(uuid)
        if obj is None:
            obj = await self.get_realization_by_uuid_async(uuid)
            self._cache.put(uuid, obj)

        return obj

    def filter(self, **kwargs):
        sc = super().filter(**kwargs)
        uuids = sc.uuids
        return Realizations(self, uuids)
