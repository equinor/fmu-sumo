"""Module containing class for cpgrid_property"""

from typing import Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child

class CPGridProperty(Child):
    """Class representing a cpgrid_property object in Sumo."""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): dictionary metadata
            blob: data object
        """
        super().__init__(sumo, metadata, blob)

    def to_cpgrid_property(self):
        """Get cpgrid_property object as a GridProperty
        Returns:
            GridProperty: A GridProperty object
        """
        try:
            from xtgeo import gridproperty_from_file
        except ModuleNotFoundError:
            raise RuntimeError("Unable to import xtgeo; probably not installed.")
        try:
            return gridproperty_from_file(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    async def to_cpgrid_property_async(self):
        """Get cpgrid_property object as a GridProperty
        Returns:
            GridProperty: A GridProperty object
        """
        try:
            from xtgeo import gridproperty_from_file
        except ModuleNotFoundError:
            raise RuntimeError("Unable to import xtgeo; probably not installed.")

        try:
            return gridproperty_from_file(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err
