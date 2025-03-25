"""module containing class for child object"""

from io import BytesIO
from typing import Dict, List, Tuple, Union

from sumo.wrapper import SumoClient

from ._document import Document


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
    def timestamp(self) -> Union[str, None]:
        """Object timestmap data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is None:
            return t0

        return None

    @property
    def interval(self) -> Union[str, Tuple[str, str], None]:
        """Object interval data"""
        t0 = self._get_property(["data", "time", "t0", "value"])
        t1 = self._get_property(["data", "time", "t1", "value"])

        if t0 is not None and t1 is not None:
            return (t0, t1)

        return None

    @property
    def template_path(self) -> str:
        return "/".join(
            ["{realization}", "{iteration}"]
            + self.relative_path.split("/")[2:]
        )

    @property
    def spec(self) -> Dict:
        """Object spec data"""
        return self.get_property("data.spec")

    @property
    def bbox(self) -> Dict:
        """Object boundary-box data"""
        return self.get_property("data.bbox")

    @property
    def relative_path(self) -> str:
        """Object relative file path"""
        return self.get_property("file.relative_path")

    @property
    def dataformat(self) -> str:
        """Object file format"""
        return self.get_property("data.format")

    @property
    def format(self) -> str:
        """Object file format"""
        return self.get_property("data.format")

    @property
    def stage(self) -> str:
        """Object stage"""
        return self.get_property("fmu.context.stage")

    @property
    def aggregation(self) -> str:
        """Object aggregation operation"""
        return self.get_property("fmu.aggregation.operation")

    @property
    def realization(self) -> str:
        """Object realization"""
        return self.get_property("fmu.realization.id")

    @property
    def iteration(self) -> str:
        """Object iteration"""
        return self.get_property("fmu.iteration.name")

    @property
    def context(self) -> str:
        """Object context"""
        return self.get_property("fmu.context.stage")

    @property
    def vertical_domain(self) -> str:
        """Object vertical domain"""
        return self.get_property("data.vertical_domain")

    @property
    def stratigraphic(self) -> str:
        """Object stratigraphic"""
        return self.get_property("data.stratigraphic")

    @property
    def columns(self) -> List[str]:
        """Object table columns"""
        return self.get_property("data.spec.columns")

    @property
    def tagname(self) -> str:
        """Object tagname"""
        return self.get_property("data.tagname")

    @property
    def content(self) -> str:
        """Content"""
        return self.get_property("data.content")

    @property
    def caseuuid(self) -> str:
        """Object case uuid"""
        return self.get_property("fmu.case.uuid")

    @property
    def casename(self) -> str:
        """Object case name"""
        return self.get_property("fmu.case.name")

    @property
    def classname(self) -> str:
        """Object class name"""
        return self.get_property("class.name")

    @property
    def dataname(self) -> str:
        """Object data name"""
        return self.get_property("data.name")

    @property
    def name(self) -> str:
        """Object data name"""
        return self.get_property("data.name")
