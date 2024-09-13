""" Module for (pseudo) iteration class. """

from typing import Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document
from fmu.sumo.explorer.objects._search_context import SearchContext

_prop_desc = [
    ("name", "fmu.iteration.name", "FMU iteration name"),
    ("casename", "fmu.case.name", "FMU case name"),
    ("caseuuid", "fmu.case.uuid", "FMU case uuid"),
    ("user", "fmu.case.user.id", "Name of user who uploaded iteration."),
    ("asset", "access.asset.name", "Case asset"),
    ("field", "masterdata.smda.field[0].identifier", "Case field"),
]


class Iteration(Document, SearchContext):
    """Class for representing an iteration in Sumo."""

    def __init__(self, sumo: SumoClient, metadata: Dict):
        Document.__init__(self, metadata)
        SearchContext.__init__(self, sumo, must=[{"term": {"fmu.iteration.uuid.keyword": self.uuid}}])


Iteration.map_properties(Iteration, _prop_desc)
