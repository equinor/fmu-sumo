""" Module for searchcontext for collection of realizations. """

from typing import Dict, List
from fmu.sumo.explorer.objects._search_context import SearchContext

class Realizations(SearchContext):
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
        return self._get_field_values("fmu.realization.uuid.keyword")

    async def _search_all_async(self, select=False):
        return await self._get_field_values_async("fmu.realization.uuid.keyword")
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

