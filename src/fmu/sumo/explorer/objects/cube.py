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
        Child.__init__(self, sumo, metadata)
        self._sumo = sumo

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

    @property
    def url_and_sas(self) -> Dict:
        res = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")
        res = json.loads(res.decode("UTF-8"))
        url = "azureSAS" + res.get("baseuri")[5:] + self.uuid + "/"
        sas = "Suffix=?" + res.get("auth")
        
        return {
            "url": url,
            "sas": sas
        }

    @property
    def openvds_handle(self) -> openvds.core.VDS:
        response = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")

        json_resp = json.loads(response.decode("UTF-8"))
        my_url = "azureSAS" + json_resp.get("baseuri")[5:] + self.uuid + "/"
        my_url_conn = "Suffix=?" + json_resp.get("auth")

        return openvds.open(my_url, my_url_conn)

    @property
    def oneseismic_handle(self):
        response = self._sumo.get(f"/objects('{self.uuid}')/blob/authuri")

        json_resp = json.loads(response.decode("UTF-8"))
        url = json_resp.get("baseuri") + self.uuid
        url = url.replace(":443", "")
        sas = "?" + json_resp.get("auth")

        return oneseismic.simple.open(url, sas)

