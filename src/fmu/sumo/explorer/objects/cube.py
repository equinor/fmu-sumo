"""Module containing class for cube object"""

from typing import Dict, Tuple

from sumo.wrapper import SumoClient

from ._child import Child


class Cube(Child):
    """Class representig a seismic cube object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): cube metadata
        """
        super().__init__(sumo, metadata, blob)

    def _extract_auth(self, res) -> Tuple[str, str]:
        try:
            res = res.json()
            url = res.get("baseuri") + self.uuid
            sas = res.get("auth")
        except Exception:
            url, sas = res.text.split("?")
            pass
        return url, sas

    @property
    def auth(self) -> Tuple[str, str]:
        res = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")
        return self._extract_auth(res)

    @property
    async def auth_async(self) -> Tuple[str, str]:
        res = await self._sumo.get_async(
            f"/objects('{self.uuid}')/blob/authuri"
        )
        return self._extract_auth(res)

    @property
    def openvds_handle(self):
        try:
            import openvds
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import openvds; probably not installed."
            )

        url, sas = self.auth
        url = url.replace("https://", "azureSAS://") + "/"
        sas = "Suffix=?" + sas
        return openvds.open(url, sas)

    @property
    async def openvds_handle_async(self):
        try:
            import openvds
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import openvds; probably not installed."
            )

        url, sas = await self.auth_async
        url = url.replace("https://", "azureSAS://") + "/"
        sas = "Suffix=?" + sas
        return openvds.open(url, sas)
