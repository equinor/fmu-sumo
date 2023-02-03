from fmu.sumo.explorer.objects.document import Document
from sumo.wrapper import SumoClient
from io import BytesIO
from typing import Dict


class Child(Document):
    """Class for representing a child object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        super().__init__(metadata)
        self._sumo = sumo
        self._blob = None

    @property
    def name(self) -> str:
        return self._get_property(["data", "name"])

    @property
    def tagname(self) -> str:
        return self._get_property(["data", "tagname"])

    @property
    def context(self) -> str:
        return self._get_property(["fmu", "context", "stage"])

    @property
    def iteration(self) -> int:
        return self._get_property(["fmu", "iteration", "id"])

    @property
    def realization(self) -> int:
        return self._get_property(["fmu", "realization", "id"])

    @property
    def operation(self) -> str:
        return self._get_property(["fmu", "aggregation", "operation"])

    @property
    def stage(self) -> str:
        return self._get_property(["fmu", "context", "stage"])
    
    @property
    def format(self) -> str:
        return self._get_property(["data", "format"])

    @property
    def blob(self) -> BytesIO:
        if self._blob is None:
            res = self._sumo.get(f"/objects('{self.id}')/blob")
            self._blob = BytesIO(res)

        return self._blob
