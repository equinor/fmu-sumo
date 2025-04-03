"""Module containing class for exploring results from sumo"""

import warnings
from typing import Optional

import httpx
from sumo.wrapper import SumoClient

from .objects._search_context import SearchContext
from .objects.cases import Cases


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
        token: Optional[str] = None,
        interactive: bool = True,
        keep_alive: Optional[str] = None,
        http_client=None,
        async_http_client=None,
    ):
        """Initialize the Explorer class

        Args:
            env (str): Sumo environment
            token (str): authenticate with existing token
            interactive (bool): authenticate using interactive flow (browser)
            keep_alive (str): point in time lifespan (deprecated and ignored)
        """
        sumo = SumoClient(
            env,
            token=token,
            interactive=interactive,
            timeout=httpx.Timeout(180.0),
            http_client=http_client,
            async_http_client=async_http_client,
        )
        SearchContext.__init__(self, sumo)
        if keep_alive:
            warnings.warn(
                "The constructor argument 'keep_alive' to class 'Explorer' has been deprecated.",
                DeprecationWarning,
            )

    @property
    def cases(self):
        uuids = self._context_for_class("case").uuids
        return Cases(self, uuids)

    def get_permissions(self, asset: Optional[str] = None):
        """Get permissions

        Args:
            asset (str): asset in Sumo

        Returns:
          dict: Dictionary of user permissions
        """
        res = self._sumo.get("/userpermissions").json()

        if asset is not None and asset not in res:
            raise PermissionError(f"No permissions for asset: {asset}")

        return res

    async def get_permissions_async(self, asset: Optional[str] = None):
        """Get permissions

        Args:
            asset (str): asset in Sumo

        Returns:
          dict: Dictionary of user permissions
        """
        res = await self._sumo.get_async("/userpermissions")
        res = res.json()

        if asset is not None and asset not in res:
            raise PermissionError(f"No permissions for asset: {asset}")

        return res
