"""module containing class for child object"""

from typing import Dict
from io import BytesIO
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document import Document

_prop_desc = [
    ("name", "data.name", "Object name"),
    ("dataname", "data.name", "Object name"),
    ("classname", "class.name", "Object class name"),
    ("casename", "fmu.case.name", "Object case name"),
    ("content", "data.content", "Content"),
    ("tagname", "data.tagname", "Object tagname"),
    ("columns", "data.spec.columns", "Object table columns"),
    ("stratigraphic", "data.stratigraphic", "Object stratigraphic"),
    ("vertical_domain", "data.vertical_domain", "Object vertical domain"),
    ("context", "fmu.context.stage", "Object context"),
    ("iteration", "fmu.iteration.name", "Object iteration"),
    ("realization", "fmu.realization.id", "Object realization"),
    (
        "aggregation",
        "fmu.aggregation.operation",
        "Object aggregation operation",
    ),
    ("stage", "fmu.context.stage", "Object stage"),
    ("format", "data.format", "Object file format"),
    ("dataformat", "data.format", "Object file format"),
    ("relative_path", "file.relative_path", "Object relative file path"),
    ("bbox", "data.bbox", "Object boundary-box data"),
    ("spec", "data.spec", "Object spec data"),
]


class Child(Document):
    """Class representing a child object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(metadata)
        self._sumo = sumo
        self._blob = blob

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

    @property
    def timestamp(self) -> str:
        """Object timestmap data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is None:
            return t0

        return None

    @property
    def interval(self) -> str:
        """Object interval data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is not None:
            return (t0, t1)

        return None

    @property
    def template_path(self):
        return "/".join(["{realization}", "{iteration}"] +
                        self.relative_path.split("/")[2:])


Child.map_properties(Child, _prop_desc)
