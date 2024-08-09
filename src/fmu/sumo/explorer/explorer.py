"""Module containing class for exploring results from sumo"""

import warnings

from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._search_context import SearchContext
from fmu.sumo.explorer.objects.surface import Surface
from fmu.sumo.explorer.objects.polygons import Polygons
from fmu.sumo.explorer.objects.table import Table
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.objects.iteration import Iteration
from fmu.sumo.explorer.objects.realization import Realization

_CASE_FIELDS = {"include": [], "exclude": []}

_CHILD_FIELDS = {
    "include": [],
    "exclude": ["data.spec.columns", "fmu.realization.parameters"],
}


class Explorer(SearchContext):
    """Class for consuming FMU results from Sumo.
    The Sumo Explorer is a Python package for consuming FMU results stored
    in Sumo. It is FMU aware, and creates an abstraction on top of the
    Sumo API. The purpose of the package is to create an FMU-oriented
    Python interface towards FMU data in Sumo, and make it easy for FMU
    users in various contexts to use data stored in Sumo.

    Examples of use cases:
      - Applications (example: Webviz)
      - Scripts (example: Local post-processing functions)
      - Manual data browsing and visualization (example: A Jupyter Notebook)
    """

    def __init__(
        self,
        env: str = "prod",
        token: str = None,
        interactive: bool = True,
        keep_alive: str = None,
    ):
        """Initialize the Explorer class

        Args:
            env (str): Sumo environment
            token (str): authenticate with existing token
            interactive (bool): authenticate using interactive flow (browser)
            keep_alive (str): point in time lifespan (deprecated and ignored)
        """
        sumo = SumoClient(env, token=token, interactive=interactive)
        SearchContext.__init__(self, sumo)
        if keep_alive:
            warnings.warn(
                "The constructor argument 'keep_alive' to class 'Explorer' has been deprecated.",
                DeprecationWarning,
            )

    def get_permissions(self, asset: str = None):
        """Get permissions

        Args:
            asset (str): asset in Sumo

        Returns:
          dict: Dictionary of user permissions
        """
        res = self._sumo.get("/userpermissions").json()

        if asset is not None:
            if asset not in res:
                raise PermissionError(f"No permissions for asset: {asset}")

        return res

    async def get_permissions_async(self, asset: str = None):
        """Get permissions

        Args:
            asset (str): asset in Sumo

        Returns:
          dict: Dictionary of user permissions
        """
        res = await self._sumo.get_async("/userpermissions")
        res = res.json()

        if asset is not None:
            if asset not in res:
                raise PermissionError(f"No permissions for asset: {asset}")

        return res

    def _get_object_by_class_and_uuid(self, cls, uuid):
        obj = self.get_object(uuid)
        if obj["_source"]["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return self._to_sumo(obj)

    async def _get_object_by_class_and_uuid_async(self, cls, uuid):
        obj = self.get_object_async(uuid)
        if obj["_source"]["class"] != cls:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return self._to_sumo(obj)

    def get_case_by_uuid(self, uuid: str) -> Case:
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return self._get_object_by_class_and_uuid("case", uuid)

    async def get_case_by_uuid_async(self, uuid: str) -> Case:
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return await self._get_object_by_class_and_uuid_async("case", uuid)

    def _iteration_query(self, uuid):
        return {
            "query": {"term": {"fmu.iteration.uuid.keyword": {"value": uuid}}},
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                ],
            },
        }

    def get_iteration_by_uuid(self, uuid: str) -> Iteration:
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        res = self._sumo.post(
            "/search", json=self._iteration_query(uuid)
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return Iteration(self._sumo, obj)

    async def get_iteration_by_uuid_async(self, uuid: str) -> Iteration:
        """Get iteration object by uuid

        Args:
            uuid (str): iteration uuid

        Returns: iteration object
        """
        res = (
            await self._sumo.post("/search", json=self._iteration_query(uuid))
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return Iteration(self._sumo, obj)

    def _realization_query(self, uuid):
        return {
            "query": {
                "term": {"fmu.realization.uuid.keyword": {"value": uuid}}
            },
            "size": 1,
            "_source": {
                "includes": [
                    "$schema",
                    "source",
                    "version",
                    "access",
                    "masterdata",
                    "fmu.case",
                    "fmu.iteration",
                    "fmu.realization",
                ],
            },
        }

    def get_realization_by_uuid(self, uuid: str) -> Realization:
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        res = self._sumo.post(
            "/search", json=self._realization_query(uuid)
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return Realization(self._sumo, obj)

    async def get_realization_by_uuid_async(self, uuid: str) -> Realization:
        """Get realization object by uuid

        Args:
            uuid (str): realization uuid

        Returns: realization object
        """
        res = (
            await self._sumo.post(
                "/search", json=self._realization_query(uuid)
            )
        ).json()
        obj = res["hits"]["hits"][0]
        obj["_id"] = uuid
        return Realization(self._sumo, obj)

    def get_surface_by_uuid(self, uuid: str) -> Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = self.get_object(uuid, _CHILD_FIELDS)
        return Surface(self._sumo, metadata)

    async def get_surface_by_uuid_async(self, uuid: str) -> Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = await self.get_object_async(uuid, _CHILD_FIELDS)
        return Surface(self._sumo, metadata)

    def get_polygons_by_uuid(self, uuid: str) -> Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = self.get_object(uuid, _CHILD_FIELDS)
        return Polygons(self._sumo, metadata)

    async def get_polygons_by_uuid_async(self, uuid: str) -> Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = await self.get_object_async(uuid, _CHILD_FIELDS)
        return Polygons(self._sumo, metadata)

    def get_table_by_uuid(self, uuid: str) -> Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = self.get_object(uuid, _CHILD_FIELDS)
        return Table(self._sumo, metadata)

    async def get_table_by_uuid_async(self, uuid: str) -> Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = await self.get_object_async(uuid, _CHILD_FIELDS)
        return Table(self._sumo, metadata)
