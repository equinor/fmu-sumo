"""Module containing class for polygons object"""
from typing import Dict
import pandas as pd
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child
from warnings import warn


class Polygons(Child):
    """Class representig a polygons object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata (dict): polygon metadata
        """
        super().__init__(sumo, metadata)

    def to_dataframe(self) -> pd.DataFrame:
        """Get polygons object as a DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        warn(
            ".to_dataframe() is deprecated, renamed to .to_pandas() ",
            DeprecationWarning,
            stacklevel=2,
        )

        return self.to_pandas()

    def to_pandas(self) -> pd.DataFrame:
        """Get polygons object as a DataFrame

        Returns:
            DataFrame: A DataFrame object
        """

        try:
            return pd.read_csv(self.blob)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err

    async def to_pandas_async(self) -> pd.DataFrame:
        """Get polygons object as a DataFrame

        Returns:
            DataFrame: A DataFrame object
        """

        try:
            return pd.read_csv(await self.blob_async)
        except TypeError as type_err:
            raise TypeError(f"Unknown format: {self.format}") from type_err
