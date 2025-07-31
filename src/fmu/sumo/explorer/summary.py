"""
Module for extracting summary data from Sumo in a (subsurface) user-friendly way.
Wrapper around the Sumo Explorer object.
"""

import re
from typing import Literal, Optional, Union

import pandas as pd
import pyarrow
import pyarrow.compute as pc

from fmu.ensemble.util.dates import unionize_smry_dates
from fmu.sumo.explorer import Explorer
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.objects.table import Table


def get_case(uuid: str) -> Case:
    """
    Get a case stored in Sumo.

    Args:
        uuid (str): case id in Sumo.

    Returns:
        Case: object representing a case in Sumo
    """

    exp = Explorer()
    case = exp.get_case_by_uuid(uuid)

    return case


class Summary:
    """
    Base class for grouping common functionality in the Realization and Ensemble
    classes
    """

    def __init__(
        self,
        case: Case,
        vectors: Optional[Union[str, list]] = None,
        name: Optional[str] = None,
        ensemble: Optional[str] = None,
    ) -> None:
        self.case = case
        self.vectors = vectors
        self.name = name
        self.ensemble = ensemble

        # Save time not having to access & iterate over
        # all columns in a summary file if a regex isn't present
        self._regex_in_vectors = False
        if self.vectors:
            self._regex_in_vectors = self._check_regex_in_vectors()

    def _check_regex_in_vectors(self):
        regex_in_vectors = False
        for v in self.vectors:
            if re.match(r".*\*.*", v):
                regex_in_vectors = True
                break

        return regex_in_vectors

    def _get_wildcard_vectors(self, table: Table):
        columns = table.column_names

        # Expanded vectors e.g. FOPT* -> FOPT, FOPTH
        new_vectors = []

        for v in self.vectors:
            # Only try to match regex is wildcard character "*" is present
            if re.match(r".*\*.*", v):
                for col in columns:
                    # {0, } to match e.g. both FOPT and FOPTH if regex is 'F*PT'
                    pattern = rf"{v}".replace("*", ".*.{0,}")
                    if re.match(pattern, col):
                        new_vectors.append(col)
            else:
                new_vectors.append(v)

        return new_vectors

    def _check_number_summary_files(self, tables):
        assert len(tables.names) > 0, (
            "No summary file found in the provided case."
        )
        assert len(tables.names) == 1, (
            f"More than one summary file found: {tables.names}. Specify the name of the summary file using Ensemble(..., summary_name='...')"
        )

    def _check_number_ensembles(self, tables):
        assert len(tables.ensembles) > 0, (
            "No summary file found in the provided case."
        )
        assert len(tables.ensembles) == 1, (
            f"More than one ensemble found: {[ens.name for ens in tables.ensembles]}. Specify the name of the ensemble file using Ensemble(..., summary_name='...')"
        )

    def _filter_time_index(self, table: pyarrow.Table, frequency: str) -> list:
        timestamps = pc.unique(table.select(["DATE"])[0])
        timestamps_series = timestamps.to_pandas().unique()
        dates = unionize_smry_dates(
            [timestamps_series], freq=frequency, normalize=True
        )
        time_index = pd.to_datetime(dates)

        return time_index

    def _check_frequency_allowed(
        self,
        frequency: Optional[
            Literal["daily", "weekly", "monthly", "yearly", "raw"]
        ] = None,
    ):
        allowed_frequencies = ("daily", "weekly", "monthly", "yearly", "raw")

        if frequency and frequency not in allowed_frequencies:
            raise ValueError(
                f"Invalid frequency: '{frequency}'. Allowed values are: {allowed_frequencies}"
            )

        return frequency


class Realization(Summary):
    """
    An object for downloading summary data for a single realisation from Sumo.

    Args:
        case (Case): fmu.sumo.explorer case
        real (int): realisation number
        vectors (str | list | None, optional): Summary vectors to download.
        Defaults to None (all).
        name (Optional[str], optional): Summary file metadata name in Sumo. Used to
        select one summary file if multiple runs are present for a single
        realisation. Defaults to None.
        ensemble (Optional[str], optional): name of the ensemble in Sumo (e.g. "pred", "iter-0")
    """

    def __init__(
        self,
        case: Case,
        real: int,
        vectors: str | list | None = None,
        name: Optional[str] = None,
        ensemble: Optional[str] = None,
    ) -> None:
        super().__init__(case, vectors, name, ensemble)

        self.real = real

        self._format_vectors()

        # Get tables object and check there is only data for only one summary file per realisation
        self.tables = self.case.tables.filter(
            tagname="summary",
            realization=self.real,
            name=self.name,
            ensemble=self.ensemble,
        )
        self._check_number_ensembles(self.tables)
        self._check_number_summary_files(self.tables)

    def _format_vectors(self):
        """
        Format vectors argument. If a string is given, add it to a list. Always
        keep DATE column as first vector. If vectors=None, nothing is done here.
        """
        if isinstance(self.vectors, str):
            self.vectors = [self.vectors]

        # Always keep DATE column
        if isinstance(self.vectors, list):
            _vectors = ["DATE"]
            _vectors.extend(self.vectors)
            self.vectors = _vectors

    def _reindex_dates(
        self, table: pyarrow.Table, time_index: list
    ) -> pyarrow.Table:
        df = table.to_pandas()

        # If duplicates are found, re-indexing will not work. An edge case when
        # duplicate days will be found is when the raw data has sub-daily
        # resolution and multiple timesteps/entries on the same day.
        if df["DATE"].dt.normalize().duplicated().any():
            time_delta = df["DATE"] - df["DATE"].shift()
            if (time_delta < pd.Timedelta(days=1)).any():
                raise ValueError(
                    "Duplicate dates found in summary data. "
                    "Reindexing is not supported when the raw data has multiple timesteps on the same day."
                )

        df_interpolated = (
            df.set_index("DATE")
            .reindex(time_index, level=0)
            .interpolate(method="linear")
            .dropna(axis=0, how="all")
            .reset_index(names="DATE")
        )

        table_interpolated = pyarrow.Table.from_pandas(
            df_interpolated, preserve_index=True
        )

        return table_interpolated

    def df(
        self,
        frequency: Optional[
            Literal["daily", "weekly", "monthly", "yearly", "raw"]
        ] = None,
    ) -> pd.DataFrame:
        """
        Get summary data as a pandas dataframe.

        Args:
            frequency (Optional[ Literal["daily", "weekly", "monthly", "yearly",
            "raw"] ], optional): Time frequency to reindex to. Defaults to None
            (which in turn defaults to "raw").

        Returns:
            pd.DataFrame: Summary data with requested vectors
        """

        frequency = self._check_frequency_allowed(frequency)

        table = self.tables[0].to_arrow()

        if self._regex_in_vectors:
            self.vectors = self._get_wildcard_vectors(table)

        # If vectors are given, filter the table to the vectors
        if self.vectors:
            table = table.select(self.vectors)

        if frequency:
            time_index = self._filter_time_index(table, frequency)
            table = self._reindex_dates(table, time_index)

        return table.to_pandas()


class Ensemble(Summary):
    """
    An object for downloading summary data for an ensemble of realisations from Sumo.

    Args:
        case (Case): fmu.sumo.explorer case
        ensemble: (str): name of the ensemble in Sumo (e.g. "pred", "iter-0")
        vectors (str | list | None, optional): Summary vectors to download.
        name (Optional[str], optional): Summary file metadata name in Sumo. Used
        to select one summary file if multiple runs are present for a single
        realisation. Defaults to None.
    """

    def __init__(
        self,
        case: Case,
        ensemble: str,
        vectors: str | list,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(case, vectors, name, ensemble)

        self.ensemble = ensemble

        self._format_vectors()

        self.tables = self.case.tables.filter(
            tagname="summary", name=self.name, ensemble=self.ensemble
        )
        self._check_number_summary_files(self.tables)

        if self._regex_in_vectors:
            _real = self.case.realizationids[0]
            tables = self.case.tables.filter(
                tagname="summary",
                ensemble=self.ensemble,
                realization=_real,
                name=self.name,
            )
            table = tables[0].to_arrow()
            self.vectors = self._get_wildcard_vectors(table)

    def _format_vectors(self):
        """
        Format vectors argument.
        If a string is given, add it to a list.
        """
        if isinstance(self.vectors, str):
            self.vectors = [self.vectors]

    def _aggregate_summary_data(self):
        """
        1. For each vector requested, aggregates (collects) summary data for all
           realisations for a single summary vector into a single table
        2. Adds these tables to a list

        Returns:
            tables (list[Table]): list of Sumo Table objects which each contain
            a single summary vector for an entire ensemble
        """

        tables = [
            self.tables.aggregation(operation="collection", column=v)
            for v in self.vectors
        ]

        return tables

    def _join_tables(self, tables: list[Table]) -> pd.DataFrame:
        """
        Combines individual summary vector tables into one table for an entire
        ensemble.

        Args:
            tables (list[Table]): list of Sumo Table objects, each with one
            summary vector for an entire ensemble

        Returns:
            pd.DataFrame: Combined summary data for an ensemble
        """
        for i, table_obj in enumerate(tables):
            if i == 0:
                table = table_obj.to_arrow()
            else:
                table = table.join(table_obj.to_arrow(), ["DATE", "REAL"])

        return table

    def _reindex_dates(
        self, table: pyarrow.Table, time_index: list
    ) -> pyarrow.Table:
        df = table.to_pandas()

        for i, real in enumerate(df.REAL.unique()):
            if i == 0:
                df_interpolated = pd.DataFrame()
            _df_ri = (
                df[df["REAL"] == real]
                .copy()
                .set_index("DATE")
                .reindex(time_index, level=0)
                .interpolate(method="linear")
                .reset_index(names="DATE")
            )

            df_interpolated = pd.concat((df_interpolated, _df_ri))

        table_interpolated = pyarrow.Table.from_pandas(
            df_interpolated, preserve_index=True
        )

        return table_interpolated

    def df(
        self,
        frequency: Optional[
            Literal["daily", "weekly", "monthly", "yearly", "raw"]
        ] = None,
    ) -> pd.DataFrame:
        """
        Get summary data as a pandas dataframe.

        Args:
            frequency (Optional[ Literal["daily", "weekly", "monthly", "yearly",
            "raw"] ], optional): Time frequency to reindex to. Defaults to None
            (which in turn defaults to "raw").

        Returns:
            pd.DataFrame: Summary data with requested vectors
        """
        frequency = self._check_frequency_allowed(frequency)

        tables = self._aggregate_summary_data()
        table = self._join_tables(tables)

        if frequency:
            time_index = self._filter_time_index(table, frequency)
            table = self._reindex_dates(table, time_index)

        df = table.to_pandas()

        return df
