"""Module containing class for cpgrid_property"""

from typing import Dict

from sumo.wrapper import SumoClient

from ._child import Child
from ._search_context import SearchContext


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
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )
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
            raise RuntimeError(
                "Unable to import xtgeo; probably not installed."
            )

        try:
            return gridproperty_from_file(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    @property
    def grid(self):
        """Get cpgrid object associated with this cpgrid_property instances.
        Returns:
            Grid: a Grid object (an instance of class CPGrid).
        """
        sc = SearchContext(self._sumo).grids.filter(
            uuid=self.caseuuid,
            iteration=self.iteration,
            realization=self.realization,
        )
        should = [
            {"term": {"data.name.keyword": self.tagname}},
        ]
        dgrp = (
            self.metadata.get("data", {})
            .get("geometry", {})
            .get("relative_path", None)
        )
        if dgrp is not None:
            should.append({"term": {"file.relative_path.keyword": dgrp}})
            pass
        sc = sc.filter(
            complex={
                "bool": {
                    "minimum_should_match": 1,
                    "should": should,
                }
            }
        )
        assert len(sc) == 1
        return sc[0]
