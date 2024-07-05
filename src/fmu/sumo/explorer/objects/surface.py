"""Module containg class for surface"""

from typing import Dict
from xtgeo import RegularSurface, surface_from_file
from fmu.sumo.explorer.objects._child import Child


class Surface(Child):
    """Class representing a surface object in Sumo"""

    def to_regular_surface(self) -> RegularSurface:
        """Get surface object as a RegularSurface

        Returns:
            RegularSurface: A RegularSurface object
        """
        try:
            return surface_from_file(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    async def to_regular_surface_async(self) -> RegularSurface:
        """Get surface object as a RegularSurface

        Returns:
            RegularSurface: A RegularSurface object
        """
        try:
            return surface_from_file(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err
