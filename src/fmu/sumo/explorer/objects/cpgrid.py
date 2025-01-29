"""Module containing class for cpgrid"""

from typing import Dict

from sumo.wrapper import SumoClient

from fmu.sumo.explorer.objects._child import Child
from fmu.sumo.explorer.objects._search_context import SearchContext


class CPGrid(Child):
    """Class representing a cpgrid object in Sumo."""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): dictionary metadata
            blob: data object
        """
        super().__init__(sumo, metadata, blob)

    def to_cpgrid(self):
        """Get cpgrid object as a Grid
        Returns:
            Grid: A Grid object
        """
        try:
            from xtgeo import grid_from_file
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )
        try:
            return grid_from_file(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    async def to_cpgrid_async(self):
        """Get cpgrid object as a Grid
        Returns:
            Grid: A Grid object
        """
        try:
            from xtgeo import grid_from_file
        except ModuleNotFoundError:
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )

        try:
            return grid_from_file(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    @property
    def grid_properties(self):
        """Get cpgrid_property instances that use this cpgrid instance.
        Returns:
            GridProperties: a SearchContext that holds the linked CPGridProperty instances.
        """
        sc = SearchContext(self._sumo)
        return sc.filter(
            complex={
                "bool": {
                    "must": [
                        {
                            "term": {
                                "data.geometry.relative_path.keyword": self._metadata[
                                    "file"
                                ]["relative_path"]
                            }
                        },
                        {
                            "term": {
                                "fmu.case.uuid.keyword": self._metadata["fmu"][
                                    "case"
                                ]["uuid"]
                            }
                        },
                    ]
                }
            }
        )
