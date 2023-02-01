from fmu.sumo.explorer.objects.document_collection import DocumentCollection
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.case import Case
from typing import Union, List, Dict


class CaseCollection(DocumentCollection):
    """A class for representing a collection of cases in Sumo"""

    def __init__(self, sumo: SumoClient, filter: List[Dict] = None):
        super().__init__("case", sumo, filter)

    @property
    def names(self) -> List[str]:
        """List of unique case names"""
        return self._get_field_values("fmu.case.name.keyword")

    @property
    def statuses(self) -> List[str]:
        """List of unique statuses"""
        return self._get_field_values("_sumo.status.keyword")

    @property
    def users(self) -> List[str]:
        """List of unique user names"""
        return self._get_field_values("fmu.case.user.id.keyword")

    @property
    def assets(self) -> List[str]:
        """List of unique asset names"""
        return self._get_field_values("access.asset.name.keyword")

    @property
    def fields(self) -> List[str]:
        """List of unique field names"""
        return self._get_field_values("masterdata.smda.field.identifier.keyword")

    def __getitem__(self, index: int) -> Case:
        doc = super().__getitem__(index)
        return Case(self._sumo, doc)

    def filter(
        self,
        id: Union[str, List[str]] = None,
        name: Union[str, List[str]] = None,
        status: Union[str, List[str]] = None,
        user: Union[int, List[int]] = None,
        asset: Union[int, List[int]] = None,
        field: Union[str, List[str]] = None,
    ) -> "CaseCollection":
        """Filter cases

        Arguments:
            - id (str or List[str]): case id
            - name (str or List[str]): case name
            - status (str or List[str]): case status
            - user (str or List[str]): name of case owner
            - asset (str or List[str]): asset
            - field (str or List[str]): field

        Returns:
            A filtered CaseCollection
        """
        filter = {}

        if id is not None:
            filter["_id"] = id

        if name is not None:
            filter["fmu.case.name.keyword"] = name

        if status is not None:
            filter["_sumo.status.keyword"] = status

        if user is not None:
            filter["fmu.case.user.id.keyword"] = user

        if asset is not None:
            filter["access.asset.name.keyword"] = asset

        if field is not None:
            filter["masterdata.smda.field.identifier.keyword"] = field

        filter = super()._add_filter(filter)
        return CaseCollection(self._sumo, filter)
