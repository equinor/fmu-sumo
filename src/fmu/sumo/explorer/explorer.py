from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.case_collection import CaseCollection
from fmu.sumo.explorer.pit import Pit


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
        argument specifies the lifespan of the snapshot and uses a format of
        a number followed by a unit indicator. Supported indicators are:
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
        self._pit = Pit(self._sumo, keep_alive) if keep_alive else None

    @property
    def cases(self):
        """Cases in Sumo"""
        return CaseCollection(sumo=self._sumo, pit=self._pit)

    def get_permissions(self, asset: str = None):
        """Get permissions

        Args:
            asset (str): asset in Sumo

        Returns:
          dict: Dictionary of user permissions
        """
        res = self._sumo.get("/userpermissions")

        if asset is not None:
            if asset not in res:
                raise Exception(f"No permissions for asset: {asset}")
            else:
                return res[asset]

        return res
