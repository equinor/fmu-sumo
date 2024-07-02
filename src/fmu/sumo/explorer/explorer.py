"""Module containing class for exploring results from sumo"""
import warnings

from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._search_context import SearchContext
from fmu.sumo.explorer.objects.case_collection import (
    CaseCollection,
    _CASE_FIELDS,
)
from fmu.sumo.explorer.objects._child_collection import _CHILD_FIELDS
from fmu.sumo.explorer.objects.surface import Surface
from fmu.sumo.explorer.objects.polygons import Polygons
from fmu.sumo.explorer.objects.table import Table
from fmu.sumo.explorer.objects.case import Case


class Explorer:
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

        When iterating over large datasets, use the `keep_alive` argument
        to create a snapshot of the data to ensure consistency. The
        argument specifies the lifespan of the snapshot and every
        request to the Sumo API will extend the lifetime of the snapshot
        with the specified `keep_alive` value. The argument uses a format
        of a number followed by a unit indicator. Supported indicators are:
            - d (day)
            - h (hour)
            - m (minute)
            - s (second)
            - ms (milisecond)
            - micros (microsecond)
            - nanos (nanosecond)

        Examples: 1d, 2h, 15m, 30s

        Every request to Sumo will extend the lifespan of the snapshot
        by the time specified in `keep_alive`.

        Args:
            env (str): Sumo environment
            token (str): authenticate with existing token
            interactive (bool): authenticate using interactive flow (browser)
            keep_alive (str): point in time lifespan
        """
        self._sumo = SumoClient(env, token=token, interactive=interactive)
        self._sc = SearchContext(sumo=self._sumo)
        if keep_alive:
            warnings.warn("The constructor argument 'keep_alive' to class 'Explorer' has been deprecated.", DeprecationWarning)

    @property
    def cases(self):
        """Cases in Sumo"""
        return self._sc.filter(cls="case")

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
        objects = self._sc.filter(cls="case", uuid=uuid)
        if len(objects) == 0:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return objects[0]

    async def _get_object_by_class_and_uuid_async(self, cls, uuid):
        objects = self._sc.filter(cls="case", uuid=uuid)
        if await objects.length_async() == 0:
            raise Exception(f"Document of type {cls} not found: {uuid}")
        return objects[0]

    def get_case_by_uuid(self, uuid: str) -> Case:
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return _get_object_by_class_and_uuid("case", uuid)

    async def get_case_by_uuid_async(self, uuid: str) -> Case:
        """Get case object by uuid

        Args:
            uuid (str): case uuid

        Returns:
            Case: case object
        """
        return await self._get_object_by_class_and_uuid_async("case", uuid)

    def get_surface_by_uuid(self, uuid: str) -> Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = self._sc.get_object(uuid, _CHILD_FIELDS)
        return Surface(self._sumo, metadata)

    async def get_surface_by_uuid_async(self, uuid: str) -> Surface:
        """Get surface object by uuid

        Args:
            uuid (str): surface uuid

        Returns:
            Surface: surface object
        """
        metadata = await self._sc.get_object_async(uuid, _CHILD_FIELDS)
        return Surface(self._sumo, metadata)

    def get_polygons_by_uuid(self, uuid: str) -> Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = self._sc.get_object(uuid, _CHILD_FIELDS)
        return Polygons(self._sumo, metadata)

    async def get_polygons_by_uuid_async(self, uuid: str) -> Polygons:
        """Get polygons object by uuid

        Args:
            uuid (str): polygons uuid

        Returns:
            Polygons: polygons object
        """
        metadata = await self._sc.get_object_async(uuid, _CHILD_FIELDS)
        return Polygons(self._sumo, metadata)

    def get_table_by_uuid(self, uuid: str) -> Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = self._sc.get_object(uuid, _CHILD_FIELDS)
        return Table(self._sumo, metadata)

    async def get_table_by_uuid_async(self, uuid: str) -> Table:
        """Get table object by uuid

        Args:
            uuid (str): table uuid

        Returns:
            Table: table object
        """
        metadata = await self._sc.get_object_async(uuid, _CHILD_FIELDS)
        return Table(self._sumo, metadata)
