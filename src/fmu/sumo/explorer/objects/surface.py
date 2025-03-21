"""Module containg class for surface"""

from typing import Dict

from sumo.wrapper import SumoClient

from ._child import Child


class Surface(Child):
    """Class representing a surface object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): dictionary metadata
            blob: data object
        """
        super().__init__(sumo, metadata, blob)

    def to_regular_surface(self):
        """Get surface object as a RegularSurface

        Returns:
            RegularSurface: A RegularSurface object
        """
        try:
            from xtgeo import surface_from_file
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )

        try:
            return surface_from_file(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    async def to_regular_surface_async(self):
        """Get surface object as a RegularSurface

        Returns:
            RegularSurface: A RegularSurface object
        """
        try:
            from xtgeo import surface_from_file
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )

        try:
            return surface_from_file(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err
