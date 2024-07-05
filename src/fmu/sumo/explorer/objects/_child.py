"""module containing class for child object"""
import re
from typing import Dict
from io import BytesIO
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document

_prop_desc = [("name", "data.name", "Object name"),
              ("dataname", "data.name", "Object name"),
              ("classname", "class.name", "Object class name"),
              ("casename", "fmu.case.name", "Object case name"),
              ("content", "data.content", "Content"),
              ("tagname", "data.tagname", "Object tagname"),
              ("stratigraphic", "data.stratigraphic", "Object stratigraphic"),
              ("vertical_domain", "data.vertical_domain", "Object vertical domain"),
              ("context", "fmu.context.stage", "Object context"),
              ("iteration", "fmu.iteration.name", "Object iteration"),
              ("realization", "fmu.realization.id", "Object realization"),
              ("aggregation", "fmu.aggregation.operation", "Object aggregation operation"),
              ("stage", "fmu.context.stage", "Object stage"),
              ("format", "data.format", "Object file format"),
              ("dataformat", "data.format", "Object file format"),
              ("relative_path", "file.relative_path", "Object relative file path")
              ]

class Child(Document):
    """Class representing a child object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(metadata)
        self._sumo = sumo
        self._blob = None

    @property
    def blob(self) -> BytesIO:
        """Object blob"""
        if self._blob is None:
            res = self._sumo.get(f"/objects('{self.uuid}')/blob")
            self._blob = BytesIO(res.content)

        return self._blob

    @property
    async def blob_async(self) -> BytesIO:
        """Object blob"""
        if self._blob is None:
            res = await self._sumo.get_async(f"/objects('{self.uuid}')/blob")
            self._blob = BytesIO(res.content)

        return self._blob

_path_split_rx=re.compile("\]\.|\.|\[")
def _splitpath(path):
    parts = _path_split_rx.split(path)
    return [int(x) if re.match("\d+", x) else x for x in parts]

def _makeprop(attribute):
    path = _splitpath(attribute)
    return lambda self: self._get_property(path)

for name, attribute, doc in _prop_desc:
    setattr(Child, name, property(_makeprop(attribute), None, None, doc))
