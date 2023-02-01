from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
import pandas as pd


class Table(Child):
    """Class for representing a table object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: dict) -> None:
        super().__init__(sumo, metadata)

    def to_dataframe(self) -> pd.DataFrame:
        """Get table object as a DataFrame

        Returns:
            A DataFrame object
        """
        return pd.read_csv(self.blob)
