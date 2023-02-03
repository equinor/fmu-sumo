from fmu.sumo.explorer.objects.child import Child
from sumo.wrapper import SumoClient
import pandas as pd
from typing import Dict
import pyarrow as pa
import pyarrow.parquet as pq


class Table(Child):
    """Class for representing a table object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        super().__init__(sumo, metadata)
        self._format = self._get_property(["data", "format"])

    def to_dataframe(self) -> pd.DataFrame:
        """Get table object as a DataFrame

        Returns:
            A DataFrame object
        """
        if self._format == "arrow":
            reader = pa.BufferReader(self.blob)
            table = pq.read_table(reader)
            return table.to_pandas()
        elif self._format == "csv":
            return pd.read_csv(self.blob)
        else:
            raise Exception(f"Unknown format: {self._format}")
