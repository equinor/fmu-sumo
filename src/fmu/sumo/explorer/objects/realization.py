""" Module for (pseudo) realization class. """
from typing import Dict, List
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document
from fmu.sumo.explorer.objects._search_context import SearchContext

_prop_desc = [
    ("iterationname", "fmu.iteration.name", "FMU iteration name"),
    ("iterationuuid", "fmu.iteration.uuid", "FMU iteration uuid"),
    ("casename", "fmu.case.name", "FMU case name"),
    ("caseuuid", "fmu.case.uuid", "FMU case uuid"),
    ("user", "fmu.case.user.id", "Name of user who uploaded iteration."),
    ("asset", "access.asset.name", "Case asset"),
    ("field", "masterdata.smda.field[0].identifier", "Case field"),
]

class Realization(Document):
    """ Class for representing a realization in Sumo. """
    def __init__(self, sumo: SumoClient, metadata: Dict):
        super().__init__(metadata)
        self._sumo = sumo

    def searchcontext(self):
        return SearchContext(self._sumo, must=[{"term": {"fmu.realization.uuid.keyword": self.uuid}}])
        

Realization.map_properties(Realization, _prop_desc)        
    
    
