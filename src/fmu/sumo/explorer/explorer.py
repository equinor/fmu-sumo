from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.case_collection import CaseCollection


class Explorer:
    """Class for consuming FMU results from Sumo.
    The Sumo Explorer is a Python package for consuming FMU results stored in Sumo. It
    is FMU aware, and creates an abstraction on top of the Sumo API. The purpose of the
    package is to create an FMU-oriented Python interface towards FMU data in Sumo, and
    make it easy for FMU users in various contexts to use data stored in Sumo. Examples
    of use cases:
      - Applications (example: Webviz)
      - Scripts (example: Local post-processing functions)
      - Manual data browsing and visualization (example: A Jupyter Notebook)
    """

    def __init__(self, env="prod", token=None, interactive=True):
        self._sumo = SumoClient(env, token=token, interactive=interactive)
        self._cases = CaseCollection(self._sumo)

    @property
    def cases(self):
        """Cases in Sumo"""
        return self._cases

    def get_permissions(self, asset: str = None):
        """Get permissions

        Arguments:
            - asset (str): asset in Sumo

        Returns:
          Dictionary of user permissions
        """
        res = self._sumo.get("/userpermissions")

        if asset is not None:
            if asset not in res:
                raise Exception(f"No permissions for asset: {asset}")
            else:
                return res[asset]

        return res
