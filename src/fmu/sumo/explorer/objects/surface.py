"""Module containg class for surface"""
from typing import Dict
from xtgeo import RegularSurface, surface_from_file
from fmu.sumo.explorer.objects._child import Child


class Surface(Child):
    """Class representing a surface object in Sumo"""

    @property
    def bbox(self) -> Dict:
        """Surface bbox data"""
        return self._get_property(["data", "bbox"])

    @property
    def spec(self) -> Dict:
        """Surface spec data"""
        return self._get_property(["data", "spec"])

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
