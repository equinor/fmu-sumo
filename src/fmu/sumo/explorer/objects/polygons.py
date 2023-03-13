"""Module containing class for polygons object"""
from typing import Dict
import pandas as pd
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child


class Polygons(Child):
    """Class representig a polygons object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): polygon metadata
        """
        Child.__init__(self, sumo, metadata)

    def to_dataframe(self) -> pd.DataFrame:
        """Get polygons object as a DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        try:
            return pd.read_csv(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err
