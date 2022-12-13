from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
import xtgeo


class Surface(Child):
    """Class for representing a surfac object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: dict) -> None:
        super().__init__(sumo, metadata)

    def to_regular_surface(self) -> xtgeo.RegularSurface:
        """Get surface object as a RegularSurface
        
        Returns:
            A RegularSurface object
        """
        return xtgeo.surface_from_file(self.blob)
