from sumo.wrapper import SumoClient
from typing import Dict


class Pit:
    def __init__(self, sumo: SumoClient, keep_alive: str) -> None:
        self._sumo = sumo
        self._keep_alive = keep_alive
        self._pit_id = self.__get_pit_id(keep_alive)

    def __get_pit_id(self, keep_alive) -> str:
        res = self._sumo.post("/pit", params={"keep-alive": keep_alive})
        return res.json()["id"]

    def get_pit_object(self) -> Dict:
        return {"id": self._pit_id, "keep_alive": self._keep_alive}
