"""module containing class for table"""
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as pf
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child
from warnings import warn


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
        self._logger = logging.getLogger("__name__" + ".Table")

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        warn(
            ".dataframe is deprecated, renamed to .to_pandas",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.to_pandas

    @property
    def to_pandas(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """

        if self._dataframe is None:
            if self["data"]["format"] == "csv":
                worked = "csv"
                self._logger.debug("Treating blob as csv")
                try:
                    self._dataframe = pd.read_csv(self.blob)
                    worked = "csv"

                except UnicodeDecodeError as ud_e:
                    raise UnicodeDecodeError("Maybe not csv?") from ud_e
            else:
                try:
                    worked = "feather"
                    self._dataframe = pf.read_feather(self.blob)
                except pa.lib.ArrowInvalid:
                    try:
                        worked = "parquet"
                        self._dataframe = pd.read_parquet(self.blob)

                    except UnicodeDecodeError as ud_error:
                        raise TypeError(
                            "Come on, no way this is converting to pandas!!"
                        ) from ud_error

        self._logger.debug("Read blob as %s to return pandas", worked)
        return self._dataframe

    async def to_pandas_async(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """

        if self._dataframe is None:
            if self["data"]["format"] == "csv":
                worked = "csv"
                self._logger.debug("Treating blob as csv")
                try:
                    self._dataframe = pd.read_csv(await self.blob_async)
                    worked = "csv"

                except UnicodeDecodeError as ud_e:
                    raise UnicodeDecodeError("Maybe not csv?") from ud_e
            else:
                try:
                    worked = "feather"
                    self._dataframe = pf.read_feather(await self.blob_async)
                except pa.lib.ArrowInvalid:
                    try:
                        worked = "parquet"
                        self._dataframe = pd.read_parquet(await self.blob_async)

                    except UnicodeDecodeError as ud_error:
                        raise TypeError(
                            "Come on, no way this is converting to pandas!!"
                        ) from ud_error

        self._logger.debug("Read blob as %s to return pandas", worked)
        return self._dataframe

    @to_pandas.setter
    def to_pandas(self, frame: pd.DataFrame):
        self._dataframe = frame

    @property
    def arrowtable(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        warn(
            ".arrowtable is deprecated, renamed to .to_arrow",
            DeprecationWarning,
            stacklevel=2,
        )

        return self.to_arrow()

    def to_arrow(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        if self._arrowtable is None:
            if self["data"]["format"] == "arrow":
                try:
                    worked = "feather"
                    self._arrowtable = pf.read_table(self.blob)
                except pa.lib.ArrowInvalid:
                    worked = "parquet"
                    self._arrowtable = pq.read_table(self.blob)
            else:
                warn(
                    "Reading csv format into arrow, you will not get the full benefit of native arrow"
                )
                worked = "csv"
                try:
                    self._arrowtable = pa.Table.from_pandas(
                        pd.read_csv(self.blob)
                    )

                except TypeError as type_err:
                    raise OSError("Cannot read this into arrow") from type_err

            self._logger.debug("Read blob as %s to return arrow", worked)

        return self._arrowtable

    async def to_arrow_async(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        if self._arrowtable is None:
            if self["data"]["format"] == "arrow":
                try:
                    worked = "feather"
                    self._arrowtable = pf.read_table(await self.blob_async)
                except pa.lib.ArrowInvalid:
                    worked = "parquet"
                    self._arrowtable = pq.read_table(await self.blob_async)
            else:
                warn(
                    "Reading csv format into arrow, you will not get the full benefit of native arrow"
                )
                worked = "csv"
                try:
                    self._arrowtable = pa.Table.from_pandas(
                        pd.read_csv(await self.blob_async)
                    )

                except TypeError as type_err:
                    raise OSError("Cannot read this into arrow") from type_err

            self._logger.debug("Read blob as %s to return arrow", worked)

        return self._arrowtable
