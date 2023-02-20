from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
from xtgeo import RegularSurface, surface_from_file
from typing import Dict


class Surface(Child):
    """Class for representing a surfac object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        super().__init__(sumo, metadata)

    @property
    def bbox(self) -> Dict:
        return self._get_property(["data", "bbox"])

    @property
    def spec(self) -> Dict:
        return self._get_property(["data", "spec"])

    @property
    def timestamp(self) -> str:
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is None:
            return t0

        return None

    @property
    def interval(self) -> str:
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is not None:
            return (t0, t1)

        return None

    def to_regular_surface(self) -> RegularSurface:
        """Get surface object as a RegularSurface

        Returns:
            A RegularSurface object
        """
        if self.format == "irap_binary":
            return surface_from_file(self.blob)
        else:
            raise Exception(f"Unknown format: {self.format}")
