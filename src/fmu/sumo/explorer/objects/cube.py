"""Module containing class for cube object"""
import json
import openvds
from typing import Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child


class Cube(Child):
    """Class representig a seismic cube object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): cube metadata
        """
        super().__init__(sumo, metadata)
        self._url = None
        self._sas = None

    def _populate_url_and_sas(self):
        res = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")
        res = json.loads(res.decode("UTF-8"))
        self._url = res.get("baseuri") + self.uuid
        self._sas = res.get("auth")

    @property
    def url(self) -> str:
        if self._url is None:
            self._populate_url_and_sas()
        return self._url

    @property
    def sas(self) -> str:
        if self._sas is None:
            self._populate_url_and_sas()
        return self._sas

    @property
    def openvds_handle(self) -> openvds.core.VDS:
        if self._url is None or self._sas is None:
            self._populate_url_and_sas()
        url = "azureSAS" + self._url[5:] + "/"
        sas = "Suffix=?" + self._sas
        return openvds.open(url, sas)

    @property
    def timestamp(self) -> str:
        """Surface timestmap data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is None:
            return t0

        return None

    @property
    def interval(self) -> str:
        """Surface interval data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is not None:
            return (t0, t1)

        return None

