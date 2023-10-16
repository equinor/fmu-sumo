"""Module containing class for dictionary object"""
import json
from typing import Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child


class Dictionary(Child):
    """Class representig a dictionary object in Sumo"""

    _parsed: dict

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): dictionary metadata
        """
        self._parsed = None

        super().__init__(sumo, metadata)

    @property
    def blob(self) -> bytes:
        """Object blob"""
        if self._blob is None:
            res = self._sumo.get(f"/objects('{self.uuid}')/blob")
            self._blob = res.content

        return self._blob

    def parse(self) -> Dict:
        if self._parsed is None:
            self._parsed = json.loads(self.blob.decode("utf-8"))

        return self._parsed
