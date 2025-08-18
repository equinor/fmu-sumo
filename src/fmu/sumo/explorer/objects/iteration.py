"""Module for (pseudo) iteration class."""

from typing import Dict, Optional

from sumo.wrapper import SumoClient

from ._document import Document
from ._search_context import SearchContext


class Iteration(Document, SearchContext):
    """Class for representing an iteration in Sumo."""

    def __init__(
        self, sumo: SumoClient, metadata: Dict, blob: Optional[bytes] = None
    ):
        assert blob is None
        Document.__init__(self, metadata)
        SearchContext.__init__(
            self,
            sumo,
            must=[{"term": {"fmu.iteration.uuid.keyword": self.uuid}}],
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
        """Name of user who uploaded iteration."""
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
    def iterationuuid(self) -> str:
        """FMU iteration uuid"""
        return self.get_property("fmu.iteration.uuid")

    @property
    def iterationname(self) -> str:
        """FMU iteration name"""
        return self.get_property("fmu.iteration.name")

    @property
    def name(self) -> str:
        """FMU iteration name"""
        return self.get_property("fmu.iteration.name")

    @property
    def uuid(self) -> str:
        """FMU iteration uuid"""
        return self.get_property("fmu.iteration.uuid")

    @property
    def reference_realizations(self):
        """Reference realizations in iteration. If none, return
        realizations 0 and 1, if they exist."""
        sc = super().reference_realizations
        if len(sc) > 0:
            return sc
        else:
            return self.filter(realization=[0, 1]).realizations

    @property
    async def reference_realizations_async(self):
        """Reference realizations in iteration. If none, return
        realizations 0 and 1, if they exist."""
        sc = await super().reference_realizations_async
        if await sc.length_async() > 0:
            return sc
        else:
            return self.filter(realization=[0, 1]).realizations
