"""Classes for easy access to tabular data"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as pf
from fmu.sumo.explorer.objects._child import Child


class Table(Child):
    """Class representing a table object in Sumo"""

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        frame = None
        try:
            frame = pd.read_parquet(self.blob)
        except UnicodeDecodeError:
            frame = pd.read_csv(self.blob)
        return frame

    @property
    def arrowtable(self) -> pa.Table:
        """Return object as an arrow Table

        Returns:
            pa.Table: _description_
        """
        try:
            table = pq.read_table(self.blob)
        except pa.lib.ArrowInvalid:
        try:
            table = pf.read_table(self.blob)
        except pa.lib.ArrowInvalid:
            table = pa.Table.from_pandas(pd.read_csv(self.blob))
        except TypeError:
            print("This does not work")
        return table


class AggregatedTable:
    """Class for representing an aggregated table in Sumo"""

    def __init__(
        self,
        case,
        name,
        tag,
        iteration,
        aggregation="collection",
    ) -> None:
        """Init of aggregated table

        Args:
            case (Sumo.Case): given case object
            name (str): name of table
            tag (str): name of tag
            iteration (str): name of interation
            aggregation (str, optional): aggregation type. Defaults to "collection".
        """
        self._collection = case.tables.filter(
            name, tag, iteration, aggregation=aggregation
        )

    @property
    def columns(self):
        """Return column names

        Returns:
            list: the column names available
        """
        return self._collection.columns

    def __getitem__(self, col_name) -> pd.DataFrame:
        item = None
        try:
            item = self._collection.filter(column=col_name)[0]

        except IndexError as i_ex:
            raise IndexError(
                f"Column: '{col_name}' does not exist try again"
            ) from i_ex
        return item
