"""Module for (pseudo) realization class."""

from typing import Dict, Optional

from sumo.wrapper import SumoClient

from ._document import Document
from ._search_context import SearchContext


class Realization(Document, SearchContext):
    """Class for representing a realization in Sumo."""

    def __init__(
        self, sumo: SumoClient, metadata: Dict, blob: Optional[bytes] = None
    ):
        assert blob is None
        Document.__init__(self, metadata)
        SearchContext.__init__(
            self,
            sumo,
            must=[{"term": {"fmu.realization.uuid.keyword": self.uuid}}],
        )
        pass

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}: {self.realizationid} {self.uuid}(uuid) "
            f"in iteration {self.iterationname} "
            f"in case {self.casename} "
            f"in asset {self.asset}>"
        )

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
    def realizationuuid(self) -> str:
        """FMU realization uuid"""
        return self.get_property("fmu.realization.uuid")

    @property
    def realizationname(self) -> str:
        """FMU realization name"""
        return self.get_property("fmu.realization.name")

    @property
    def realizationid(self) -> int:
        """FMU realization id"""
        return self.get_property("fmu.realization.id")
