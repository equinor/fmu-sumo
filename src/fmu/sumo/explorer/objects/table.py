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
        self._dataframe = pd.DataFrame()
        self._arrowtable = pa.Table.from_pandas(self._dataframe)

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return object as a pandas DataFrame

        Returns:
            DataFrame: A DataFrame object
        """
        if self._dataframe.empty:
            try:
                self._dataframe = pd.read_parquet(self.blob)
            except UnicodeDecodeError:
                self._dataframe = pd.read_csv(self.blob)

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
        if len(self._arrowtable) == 0:
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
        self._case = case
        self._name = name
        self._tag = tag
        self._iteration = iteration
        self._aggregation = aggregation
        self._parameters = {}

    @property
    def columns(self):
        """Return column names

        Returns:
            list: the column names available
        """
        return self._collection.columns

    @property
    def parameters(self):
        """Return parameter set for iteration

        Returns:
            dict: parameters connected to iteration
        """
        if len(self._parameters) == 0:
            self._parameters = self._case.tables.filter(
                self._name,
                self._tag,
                self._iteration,
                aggregation=self._aggregation,
                column=self.columns[0],
                size=1,
            )[0]["fmu"]["iteration"]["parameters"]
        return self._parameters

    def __len__(self):
        return len(self._collection)

    def __getitem__(self, col_name) -> pd.DataFrame:
        item = None
        try:
            item = self._collection.filter(column=col_name)[0]

        except IndexError as i_ex:
            raise IndexError(
                f"Column: '{col_name}' does not exist try again"
            ) from i_ex
        return item
