"""Module for (pseudo) ensemble class."""

from typing import Dict, Optional

from sumo.wrapper import SumoClient

from ._document import Document
from ._search_context import SearchContext


class Ensemble(Document, SearchContext):
    """Class for representing an ensemble in Sumo."""

    def __init__(
        self, sumo: SumoClient, metadata: Dict, blob: Optional[bytes] = None
    ):
        assert blob is None
        Document.__init__(self, metadata)
        SearchContext.__init__(
            self,
            sumo,
            must=[{"term": {"fmu.ensemble.uuid.keyword": self.uuid}}],
        )
        pass

    def __str__(self):
        return (
            f"<{self.__class__.__name__}: {self.name} {self.uuid}(uuid) "
            f"in case {self.casename} "
            f"in asset {self.asset}>"
        )

    def __repr__(self):
        return self.__str__()

    @property
    def field(self) -> str:
        """Case field"""
        return self.get_property("masterdata.smda.field[0].identifier")

    @property
    def asset(self) -> str:
        """Case asset"""
        return self.get_property("access.asset.name")

    @property
    def user(self) -> str:
        """Name of user who uploaded ensemble."""
        return self.get_property("fmu.case.user.id")

    @property
    def caseuuid(self) -> str:
        """FMU case uuid"""
        return self.get_property("fmu.case.uuid")

    @property
    def casename(self) -> str:
        """FMU case name"""
        return self.get_property("fmu.case.name")

    @property
    def ensembleuuid(self) -> str:
        """FMU ensemble uuid"""
        return self.get_property("fmu.ensemble.uuid")

    @property
    def ensemblename(self) -> str:
        """FMU ensemble name"""
        return self.get_property("fmu.ensemble.name")

    @property
    def name(self) -> str:
        """FMU ensemble name"""
        return self.get_property("fmu.ensemble.name")

    @property
    def uuid(self) -> str:
        """FMU ensemble uuid"""
        return self.get_property("fmu.ensemble.uuid")

    @property
    def reference_realizations(self):
        """Reference realizations in ensemble. If none, return
        realizations 0 and 1, if they exist."""
        sc = super().reference_realizations
        if len(sc) > 0:
            return sc
        else:
            return self.filter(realization=[0, 1]).realizations

    @property
    async def reference_realizations_async(self):
        """Reference realizations in ensemble. If none, return
        realizations 0 and 1, if they exist."""
        sc = await super().reference_realizations_async
        if await sc.length_async() > 0:
            return sc
        else:
            return self.filter(realization=[0, 1]).realizations
