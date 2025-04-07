"""Module for searchcontext for collection of ensembles."""

from typing import Dict, List

from ._search_context import SearchContext

import json


class Ensembles(SearchContext):
    def __init__(self, sc, uuids):
        super().__init__(sc._sumo, must=[{"ids": {"values": uuids}}])
        self._hits = uuids
        return

    # def __str__(self) -> str:
    #     length = len(self)
    #     if length == 0:
    #         return "None"
    #     else:
    #         preview = [self[i].metadata for i in range(min(5, length))]
    #         return f"Data Preview:\n{json.dumps(preview, indent=4)}"

    # def __repr__(self) -> str:
    #     return(f"<{self.__class__.__name__} {len(self)} objects of type ensemble>")

    @property
    def classes(self) -> List[str]:
        return ['ensemble']

    @property
    async def classes_async(self) -> List[str]:
        return ['ensemble']

    def _maybe_prefetch(self, index):
        return

    async def _maybe_prefetch_async(self, index):
        return

    def get_object(self, uuid: str) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """
        obj = self._cache.get(uuid)
        if obj is None:
            obj = self.get_ensemble_by_uuid(uuid)
            self._cache.put(uuid, obj)
            pass

        return obj

    async def get_object_async(self, uuid: str) -> Dict:
        """Get metadata object by uuid

        Args:
            uuid (str): uuid of metadata object
            select (List[str]): list of metadata fields to return

        Returns:
            Dict: a metadata object
        """

        obj = self._cache.get(uuid)
        if obj is None:
            obj = await self.get_ensemble_by_uuid_async(uuid)
            self._cache.put(uuid, obj)

        return obj

    def filter(self, **kwargs):
        sc = SearchContext(self._sumo, must=[{"terms": {"fmu.ensemble.uuid.keyword": self._hits}}]).filter(**kwargs)
        uuids = sc.get_field_values("fmu.iteration.uuid.keyword")
        return Ensembles(sc, uuids)
