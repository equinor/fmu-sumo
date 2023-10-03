"""Module containing class for collection of cases"""
from typing import Union, List, Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._document_collection import DocumentCollection
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.pit import Pit

_CASE_FIELDS = [
    "_id",
    "fmu.case.name",
    "fmu.case.user.id",
    "_sumo.status",
    "access.asset",
    "masterdata.smda.field",
]


class CaseCollection(DocumentCollection):
    """A class for representing a collection of cases in Sumo"""

    def __init__(self, sumo: SumoClient, query: Dict = None, pit: Pit = None):
        """
        Args:
            sumo (SumoClient): connection to Sumo
            query (dict): elastic query object
            pit (Pit): point in time
        """
        super().__init__("case", sumo, query, _CASE_FIELDS, pit)

    @property
    def names(self) -> List[str]:
        """List of unique case names"""
        return self._get_field_values("fmu.case.name.keyword")

    @property
    async def names_async(self) -> List[str]:
        """List of unique case names"""
        return await self._get_field_values_async("fmu.case.name.keyword")

    @property
    def statuses(self) -> List[str]:
        """List of unique statuses"""
        return self._get_field_values("_sumo.status.keyword")

    @property
    async def statuses_async(self) -> List[str]:
        """List of unique statuses"""
        return await self._get_field_values_async("_sumo.status.keyword")

    @property
    def users(self) -> List[str]:
        """List of unique user names"""
        return self._get_field_values("fmu.case.user.id.keyword")

    @property
    async def users_async(self) -> List[str]:
        """List of unique user names"""
        return await self._get_field_values_async("fmu.case.user.id.keyword")

    @property
    def assets(self) -> List[str]:
        """List of unique asset names"""
        return self._get_field_values("access.asset.name.keyword")

    @property
    async def assets_async(self) -> List[str]:
        """List of unique asset names"""
        return await self._get_field_values_async("access.asset.name.keyword")

    @property
    def fields(self) -> List[str]:
        """List of unique field names"""
        return self._get_field_values(
            "masterdata.smda.field.identifier.keyword"
        )

    @property
    async def fields_async(self) -> List[str]:
        """List of unique field names"""
        return await self._get_field_values_async(
            "masterdata.smda.field.identifier.keyword"
        )

    def __getitem__(self, index: int) -> Case:
        doc = super().__getitem__(index)
        return Case(self._sumo, doc, self._pit)

    async def getitem_async(self, index: int) -> Case:
        doc = await super().getitem_async(index)
        return Case(self._sumo, doc)

    def filter(
        self,
        uuid: Union[str, List[str]] = None,
        name: Union[str, List[str]] = None,
        status: Union[str, List[str]] = None,
        user: Union[int, List[int]] = None,
        asset: Union[int, List[int]] = None,
        field: Union[str, List[str]] = None,
    ) -> "CaseCollection":
        """Filter cases

        Args:
            uuid (str or List[str]): case uuid
            name (str or List[str]): case name
            status (str or List[str]): case status
            user (str or List[str]): name of case owner
            asset (str or List[str]): asset
            field (str or List[str]): field

        Returns:
            CaseCollection: A filtered CaseCollection
        """
        must = self._utils.build_terms(
            {
                "_id": uuid,
                "fmu.case.name.keyword": name,
                "_sumo.status.keyword": status,
                "fmu.case.user.id.keyword": user,
                "access.asset.name.keyword": asset,
                "masterdata.smda.field.identifier.keyword": field,
            }
        )

        query = super()._add_filter({"bool": {"must": must}})
        return CaseCollection(self._sumo, query, self._pit)
