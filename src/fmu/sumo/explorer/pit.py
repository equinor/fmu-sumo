"""Module containing class for pit handling"""
from typing import Dict
from sumo.wrapper import SumoClient


class Pit:
    """Class for handling of pit"""

    def __init__(self, sumo: SumoClient, keep_alive: str) -> None:
        """Init

        Args:
            sumo (SumoClient): Activated sumo client
            keep_alive (str): how long to keep instance alive
        """
        self._sumo = sumo
        self._keep_alive = keep_alive
        self._pit_id = self.__get_pit_id(keep_alive)

    def __get_pit_id(self, keep_alive) -> str:
        res = self._sumo.post("/pit", params={"keep-alive": keep_alive})
        return res.json()["id"]

    def get_pit_object(self) -> Dict:
        """Get the pit object

        Returns:
            Dict: dict with id and info about how long to keep alive
        """
        return {"id": self._pit_id, "keep_alive": self._keep_alive}
