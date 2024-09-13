"""Module containing class for cube object"""

import json
from typing import Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child
import sys
import warnings


class Cube(Child):
    """Class representig a seismic cube object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): cube metadata
        """
        super().__init__(sumo, metadata, blob)
        self._url = None
        self._sas = None

    def _populate_url(self):
        res = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")
        try:
            res = res.json()
            self._url = res.get("baseuri") + self.uuid
            self._sas = res.get("auth")
        except Exception:
            self._url = res.text

    async def _populate_url_async(self):
        res = await self._sumo.get_async(
            f"/objects('{self.uuid}')/blob/authuri"
        )
        try:
            res = res.json()
            self._url = res.get("baseuri") + self.uuid
            self._sas = res.get("auth")
        except Exception:
            self._url = res.text

    @property
    def url(self) -> str:
        if self._url is None:
            self._populate_url()
        if self._sas is None:
            return self._url
        else:
            return self._url.split("?")[0] + "/"

    @property
    async def url_async(self) -> str:
        if self._url is None:
            await self._populate_url_async()
        if self._sas is None:
            return self._url
        else:
            return self._url.split("?")[0] + "/"

    @property
    def sas(self) -> str:
        if self._url is None:
            self._populate_url()
        if self._sas is None:
            return self._url.split("?")[1]
        else:
            return self._sas

    @property
    async def sas_async(self) -> str:
        if self._url is None:
            await self._populate_url_async()
        if self._sas is None:
            return self._url.split("?")[1]
        else:
            return self._sas

    @property
    def openvds_handle(self):
        try:
            import openvds
        except ModuleNotFoundError:
            raise RuntimeError("Unable to import openvds; probably not installed.")

        if self._url is None:
            self._populate_url()

        if self._sas is None:
            return openvds.open(self._url)
        else:
            url = "azureSAS" + self._url[5:] + "/"
            sas = "Suffix=?" + self._sas
            return openvds.open(url, sas)

    @property
    async def openvds_handle_async(self):
        try:
            import openvds
        except ModuleNotFoundError:
            raise RuntimeError("Unable to import openvds; probably not installed.")

        if self._url is None:
            await self._populate_url_async()

        if self._sas is None:
            return openvds.open(self._url)
        else:
            url = "azureSAS" + self._url[5:] + "/"
            sas = "Suffix=?" + self._sas
            return openvds.open(url, sas)
