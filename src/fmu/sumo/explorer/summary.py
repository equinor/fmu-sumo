"""
Module for extracting summary data from Sumo in a (subsurface) user-friendly way.
Wrapper around the Sumo Explorer object.
"""

import re
from typing import Literal, Optional, Union

import pandas as pd
import pyarrow

from fmu.ensemble.util.dates import unionize_smry_dates
from fmu.sumo.explorer import Explorer
from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.objects.table import Table

# Regexs for columns which will be interpolated if the time
# axis is reindexed. These are cumulatives, in-place volumes
# and pressures.
COLUMNS_INTERPOLATE = [
    r"F[A-Z]{1}(I|P)(P|T).*$",  # matches e.g. FOPT, FWIT, FOIP, FGIP
    r"WB(HP|P\d{1}):.*$",  # matches e.g. WBHP:*, WBP9:*
    r"FPR",
]
# Regexs for columns which will be back-filled if the time
# axis is reindexed. These are rates.
COLUMNS_BACKFILL = [
    r"W[A-Z]{1}(I|P)R:.*$",
    r"F[A-Z]{1}(I|P)R.*$",  # matches e.g. FOPR, FWIR, FGPRH, but not FPR
    r"FWCT.*$",
    r"WWCT:.*$",
]


def get_case(uuid: str, env: str = "prod") -> Case:
    """
    Get a case stored in Sumo.

    Args:
        uuid (str): case id in Sumo.

    Returns:
        Case: object representing a case in Sumo
    """

    exp = Explorer(env=env)
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

    def _expand_wildcard_vectors(self, table: pyarrow.Table) -> list[str]:
        """
        Get a list of all vectors. If wildcards are used in the
        input, these will be 'expanded' based on the columns in the summary
        data. For example: if F*PT is in the list of column_keys and the summary
        data contains FOPT, FGPT and FWPT, these three strings will be matched
        and returned in the new_vectors list.

        Args:
            table (pyarrow.Table): pyarrow table

        Returns:
            list[str]: list of vectors
        """
        expanded_vectors = []
        columns = table.column_names

        # Get expanded wilcard vectors e.g. FOPT* -> FOPT, FOPTH
        if self.vectors:
            for v in self.vectors:
                # Only try to match regex if wildcard character "*" is present
                if re.match(r".*\*.*", v):
                    for col in columns:
                        pattern = rf"{v}".replace("*", ".*")
                        if re.match(pattern, col):
                            expanded_vectors.append(col)
                else:
                    expanded_vectors.append(v)

        return expanded_vectors

    def _check_number_summary_files(self, tables):
        if len(tables.names) == 0:
            raise ValueError("No summary file found in the provided case.")

        if len(tables.names) > 1:
            raise ValueError(
                f"More than one summary file found: {tables.names}. Specify the name of the summary file using Ensemble(..., summary_name='...')"
            )

    def _check_number_ensembles(self, tables):
        if len(tables.ensembles) == 0:
            raise ValueError(
                "No ensemble with the requested name found in the provided case."
            )
        if len(tables.ensembles) > 1:
            raise ValueError(
                f"More than one ensemble found: {[ens.name for ens in tables.ensembles]}. Specify the name of the ensemble file using Ensemble(..., ensemble='...')"
            )

    def _filter_time_index(
        self, table: pyarrow.Table, frequency: str
    ) -> list[pd.Timestamp]:
        timestamps = table.select(["DATE"])[0]
        timestamps_series = timestamps.to_pandas()
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

    def _reindex_dates(
        self, df: pd.DataFrame, time_index: list[pd.Timestamp]
    ) -> pd.DataFrame:
        """

        If duplicates are found, re-indexing will not work. An edge case when
        duplicate days are found is when the raw data has sub-daily
        resolution and multiple timesteps/entries on the same day.

        This is put in its own function to allow for more detailed error
        messages if required.

        Args:
            df (pd.DataFrame): dataframe with DATE as index
            time_index (list[pd.Timestamp]): list of timestamps to reindex to

        Returns:
            pd.DataFrame: reindexed dataframe
        """

        df_reindexed = df.reindex(time_index, level=0)
        return df_reindexed

    def _join_original_reindexed_df(
        self, df: pd.DataFrame, df_reindexed: pd.DataFrame
    ) -> pd.DataFrame:
        """

        Join original dataframe and reindexed dataframe. Rows from the
        original dataframe are used to fill/interpolate the reindexed data.

        Args:
            df (pd.DataFrame): original dataframe
            df_reindexed (pd.DataFrame): reindexed dataframe

        Returns:
            pd.DataFrame: combined dataframe with both original and reindexed entries
        """

        df_combined = pd.concat(
            (
                df.reset_index(names="DATE"),
                df_reindexed.reset_index(names="DATE"),
            ),
            axis=0,
        )
        df_combined = df_combined.sort_values(by="DATE", ascending=True)
        df_combined = df_combined.set_index("DATE")

        return df_combined

    def _ensure_first_row_not_na(
        self, df: pd.DataFrame, df_combined: pd.DataFrame
    ) -> pd.DataFrame:
        """
        This function handles an edge case when the first row in the reindexed
        dataframe is missing values. This happens when reindexing introduces a
        date before the original dataframe's first date. For example, when
        reindexing to a monthly resolution and the first date in the original
        dataframe is 2018-01-02, the new first date in the reindexed dataframe
        will be 2018-01-01 and will contain NaN values. If this happens, this function
        will set the first row of the intepolated dataframe to the first row of
        the original dataframe while keeping the new date, so that the
        interpolation method has something to interpolate from.

        Args:
            df (pd.DataFrame): original dataframe
            df_combined (pd.DataFrame): combined dataframe with both original and reindexed entries

        Returns:
            pd.DataFrame: dataframe with values (not NaN values) in first row
        """

        if df_combined.iloc[0, :].isna().any():
            date_keep = df_combined.index[0]
            df_combined.iloc[0] = df.iloc[0].copy()
            new_index = df_combined.index.to_numpy()
            new_index[0] = date_keep
            df_combined.index = new_index

        return df_combined

    def _get_column_fill_methods(self, df: pd.DataFrame) -> dict:
        """

        When reindexing, rows with NaN values will be introduced into the
        reindexed dataframe. The method for imputing (filling missing values)
        at the new time frequency needs to be specified: interpolate or bfill.

        Two lists COLUMNS_INTERPOLATE and COLUMNS_BACKFILL contains regex
        patterns which are used to decide which method should be used to fill
        missing values for each of the columns (vectors) in the summary dataframe.

        If a vector cannot be matched by a pattern in either of these lists,
        reindexing with this vector is not supported.

        Args:
            df (pd.DataFrame): combined dataframe with both original and reindexed entries

        Returns:
            dict: map of column name (vector) to method to use to fill missing values
        """
        fill_methods = {}

        table = pyarrow.Table.from_pandas(df)

        # Get relevant vectors from user input, and check if these vectors can
        # be matched by a regex in the lists of allowed regex vectors.
        # NOTE DATE column will be found in this list.
        relevant_vectors = self.vectors if self.vectors else table.column_names

        for vector in relevant_vectors:
            method = None

            for pattern in COLUMNS_INTERPOLATE:
                if re.match(pattern, vector):
                    method = "interpolate"

            for pattern in COLUMNS_BACKFILL:
                if re.match(pattern, vector):
                    method = "fill"

            fill_methods[vector] = method

        # Remove date from this list. DATE is reindexed and doesn't need imputed
        # values.
        if "DATE" in fill_methods:
            del fill_methods["DATE"]

        return fill_methods

    def _impute_values_reindexed_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Impute values (fill missing values) in the reindexed dataframe.

        Args:
            df (pd.DataFrame): combined dataframe with both original and reindexed entries

        Returns:
            pd.DataFrame: combined dataframe with imputed values in place of NaN values
        """
        fill_methods = self._get_column_fill_methods(df)

        # Fill values depending on the data type
        for column in df.columns:
            fill_method = fill_methods.get(column)

            if fill_method == "fill":
                df[column] = df[column].bfill()
                # Fill last value which has nothing to be backfilled from
                df[column] = df[column].fillna(0)
            elif fill_method == "interpolate":
                df[column] = df[column].interpolate(method="time")
            elif not fill_method:
                df = df.drop(columns=column)
                print(
                    f"Reindexing with summary vector {column} is not supported. Removing this vector from the dataframe"
                )

        return df


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

    def _change_data_frequency(
        self, table: pyarrow.Table, time_index: list[pd.Timestamp]
    ) -> pyarrow.Table:
        """
        Change frequency of the data by reindexing to the requested time index
        and imputing values from the original data.

        Args:
            table (pyarrow.Table): table with DATE as column time_index
            (list[pd.Timestamp]): list of timestamps to reindex to

        Returns:
            pyarrow.Table: reindexed table with imputed values at the new frequency
        """

        df = table.to_pandas()  # DATE is a column
        df = df.set_index("DATE")  # DATE is index
        df_reindexed = self._reindex_dates(df, time_index)  # DATE is index

        df_combined = self._join_original_reindexed_df(df, df_reindexed)
        df_combined = self._ensure_first_row_not_na(df, df_combined)
        df_combined = self._impute_values_reindexed_df(df_combined)

        # Remove duplicates and only keep dates for new time index
        df_combined = df_combined[~df_combined.index.duplicated(keep="first")]
        df_imputed = df_combined.loc[
            df_combined.index.isin(df_reindexed.index)
        ].copy()

        table_imputed = pyarrow.Table.from_pandas(
            df_imputed, preserve_index=True
        )

        return table_imputed

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
            self.vectors = self._expand_wildcard_vectors(table)

        # If vectors are given, filter the table to the vectors
        if self.vectors:
            table = table.select(self.vectors)

        if frequency:
            time_index = self._filter_time_index(table, frequency)
            table = self._change_data_frequency(table, time_index)

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
            self.vectors = self._expand_wildcard_vectors(table)

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

    def _change_data_frequency(
        self, table: pyarrow.Table, time_index: list
    ) -> pyarrow.Table:
        df = table.to_pandas()  # DATE is a column

        df_ens_combined = pd.DataFrame()
        for i, real in enumerate(df.REAL.unique()):
            df_real = df[df["REAL"] == real].copy()

            # Drop REAL column so the data can be handled in the same way
            # as for the Realization class. This column would be dropped
            # during data imputation after reindexing anyway.
            df_real = df_real.drop(columns="REAL")

            """ From this point is the same as what's done in the same method in the Realization class """
            # TODO consider putting this code in its own method in the Summar class
            df_real = df_real.set_index("DATE")  # DATE is index
            df_reindexed = self._reindex_dates(
                df_real, time_index
            )  # DATE is index
            df_combined = self._join_original_reindexed_df(
                df_real, df_reindexed
            )
            df_combined = self._ensure_first_row_not_na(df_real, df_combined)
            df_combined = self._impute_values_reindexed_df(df_combined)

            # Remove duplicates and only keep dates for new time index
            df_combined = df_combined[
                ~df_combined.index.duplicated(keep="first")
            ]
            df_imputed = df_combined.loc[
                df_combined.index.isin(df_reindexed.index)
            ].copy()
            """ Up to this point """

            # Add realisation number back to dataframe
            df_imputed.loc[:, "REAL"] = real
            # Add imputed data for this realisation to the "ensemble" dataframe
            df_ens_combined = pd.concat((df_ens_combined, df_imputed))

        table_ens_combined = pyarrow.Table.from_pandas(
            df_ens_combined, preserve_index=True
        )

        return table_ens_combined

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
            table = self._change_data_frequency(table, time_index)

        df = table.to_pandas()

        return df
