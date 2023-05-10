"""module containing class for table"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as pf
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child


class Table(Child):
    """Class representing a table object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(sumo, metadata)
        self._dataframe = None
        self._arrowtable = None

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        if not self._dataframe:

            try:
                self._dataframe = pd.read_parquet(self.blob)

            except pa.lib.ArrowInvalid:
                try:
                    self._dataframe = pf.read_feather(self.blob)
                except pa.lib.ArrowInvalid:
                    try:
                        self._dataframe = pd.read_csv(self.blob)


                    except UnicodeDecodeError as ud_error:
                        raise TypeError("Come on, no way this is converting to pandas!!") from ud_error

        return self._dataframe

    @dataframe.setter
    def dataframe(self, frame: pd.DataFrame):
        self._dataframe = frame

    @property
    def arrowtable(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        if not self._arrowtable:
            try:
                self._arrowtable = pq.read_table(self.blob)
            except pa.lib.ArrowInvalid:
                try:
                    self._arrowtable = pf.read_table(self.blob)
                except pa.lib.ArrowInvalid:
                    self._arrowtable = pa.Table.from_pandas(
                        pd.read_csv(self.blob)
                    )
            except TypeError as type_err:
                raise OSError("Cannot read this") from type_err

        return self._arrowtable
