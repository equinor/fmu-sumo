from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
from xtgeo import RegularSurface, surface_from_file
from typing import Dict


class Surface(Child):
    """Class for representing a surfac object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        super().__init__(sumo, metadata)

    def to_regular_surface(self) -> RegularSurface:
        """Get surface object as a RegularSurface

        Returns:
            A RegularSurface object
        """
        return surface_from_file(self.blob)
