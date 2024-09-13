"""module containing class for table"""

import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as pf
from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects._child import Child
from warnings import warn
from typing import Dict


class Table(Child):
    """Class representing a table object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict, blob=None) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(sumo, metadata, blob)
        self._dataframe = None
        self._arrowtable = None
        self._logger = logging.getLogger("__name__" + ".Table")

    def _get_blob(self):
        if self._blob is None:
            self._blob = self.blob
        return self._blob

    async def _get_blob_async(self):
        if self._blob is None:
            self._blob = await self.blob_async
        return self._blob

    def _read_table(self):
        return self._construct_table_from_blob(self._get_blob())

    async def _read_table_async(self):
        return self._construct_table_from_blob(await self._get_blob_async())

    def _construct_table_from_blob(self, blob):
        try:
            if self.dataformat == "csv":
                dataframe = pd.read_csv(blob)
            elif self.dataformat == "parquet":
                dataframe = pd.read_parquet(blob)
            elif self.dataformat == "arrow":
                dataframe = pf.read_feather(blob)
            else:
                raise TypeError(
                    f"Don't know how to convert a blob of format {self.dataformat} to a pandas table."
                )
        except Exception as ex0:
            try:
                dataframe = pd.read_csv(blob)
            except Exception as ex:
                try:
                    dataframe = pd.read_parquet(blob)
                except Exception as ex:
                    try:
                        dataframe = pf.read_feather(blob)
                    except Exception as ex:
                        raise TypeError(
                            f"Unable to convert a blob of format {self.dataformat} to pandas table; tried csv, parquet and feather."
                        )
                    pass
                pass
            pass
        return dataframe

    def to_pandas(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        if self._dataframe is None:
            self._dataframe = self._read_table()
        return self._dataframe

    async def to_pandas_async(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        if self._dataframe is None:
            self._dataframe = await self._read_table_async()
        return self._dataframe

    def _read_arrow(self):
        return self._construct_arrow_from_blob(self._get_blob())

    async def _read_arrow_async(self):
        return self._construct_arrow_from_blob(await self._get_blob_async())

    def _construct_arrow_from_blob(self, blob):
        try:
            if self.dataformat == "csv":
                arrowtable = pa.Table.from_pandas(pd.read_csv(blob))
            elif self.dataformat == "parquet":
                arrowtable = pq.read_table(blob)
            elif self.dataformat == "arrow":
                arrowtable = pf.read_table(blob)
            else:
                raise TypeError(
                    f"Don't know how to convert a blob of format {self.dataformat} to a pandas table."
                )
        except Exception as ex0:
            try:
                arrowtable = pa.Table.from_pandas(pd.read_csv(blob))
            except Exception as ex:
                try:
                    arrowtable = pq.read_table(blob)
                except Exception as ex:
                    try:
                        arrowtable = pf.read_table(selfblob)
                    except Exception as ex:
                        raise TypeError(
                            f"Unable to convert a blob of format {self.dataformat} to arrow; tried csv, parquet and feather."
                        )
                    pass
                pass
            pass
        return arrowtable

    def to_arrow(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        if self._arrowtable is None:
            self._arrowtable = self._read_arrow()
        return self._arrowtable

    async def to_arrow_async(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        if self._arrowtable is None:
            self._arrowtable = await self._read_arrow_async()
        return self._arrowtable
