"""module containing class for child object"""
from typing import Dict
from io import BytesIO
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document


class Child(Document):
    """Class representing a child object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(metadata)
        self._sumo = sumo
        self._blob = None

    @property
    def name(self) -> str:
        """Object name"""
        return self._get_property(["data", "name"])

    @property
    def tagname(self) -> str:
        """Object tagname"""
        return self._get_property(["data", "tagname"])

    @property
    def stratigraphic(self) -> str:
        """Object stratigraphic"""
        return self._get_property(["data", "stratigraphic"])

    @property
    def vertical_domain(self) -> str:
        """Object vertical_domain"""
        return self._get_property(["data", "vertical_domain"])

    @property
    def context(self) -> str:
        """Object context"""
        return self._get_property(["fmu", "context", "stage"])

    @property
    def iteration(self) -> int:
        """Object iteration"""
        return self._get_property(["fmu", "iteration", "name"])

    @property
    def realization(self) -> int:
        """Object realization"""
        return self._get_property(["fmu", "realization", "id"])

    @property
    def aggregation(self) -> str:
        """Object aggregation operation"""
        return self._get_property(["fmu", "aggregation", "operation"])

    @property
    def stage(self) -> str:
        """Object stage"""
        return self._get_property(["fmu", "context", "stage"])

    @property
    def format(self) -> str:
        """Object file format"""
        return self._get_property(["data", "format"])

    @property
    def relative_path(self) -> str:
        """Object relative file path"""
        return self._get_property(["file", "relative_path"])

    @property
    def blob(self) -> BytesIO:
        """Object blob"""
        if self._blob is None:
            res = self._sumo.get(f"/objects('{self.uuid}')/blob")
            self._blob = BytesIO(res.content)

        return self._blob

    @property
    async def blob_async(self) -> BytesIO:
        """Object blob"""
        if self._blob is None:
            res = await self._sumo.get_async(f"/objects('{self.uuid}')/blob")
            self._blob = BytesIO(res.content)

        return self._blob
