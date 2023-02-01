from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
import pandas as pd
from typing import Dict


class Table(Child):
    """Class for representing a table object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        super().__init__(sumo, metadata)

    def to_dataframe(self) -> pd.DataFrame:
        """Get table object as a DataFrame

        Returns:
            A DataFrame object
        """
        return pd.read_csv(self.blob)
