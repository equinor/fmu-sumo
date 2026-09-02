"""Module containing class for dictionary object"""

import json

from sumo.wrapper import SumoClient

from fmu.sumo.explorer.objects._child import Child


class Dictionary(Child):
    """Class representing a dictionary object in Sumo"""

    _parsed: dict | None

    def __init__(self, sumo: SumoClient, metadata: dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): dictionary metadata
        """
        self._parsed: dict | None = None

        super().__init__(sumo, metadata, blob)

    def parse(self) -> dict:
        parsed = (
            json.loads(self.blob.read().decode("utf-8"))
            if self._parsed is None
            else self._parsed
        )
        if self._parsed is None:
            self._parsed = parsed
        return parsed

    async def parse_async(self) -> dict:
        parsed = self._parsed = (
            json.loads((await self.blob_async).read().decode("utf-8"))
            if self._parsed is None
            else self._parsed
        )
        if self._parsed is None:
            self._parsed = parsed
        return parsed
