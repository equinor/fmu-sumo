class Metrics:
    def __init__(self, search_context):
        self._search_context = search_context
        return

    def _aggregate(self, op, **kwargs):
        aggs = {"agg": {op: {k: v for k, v in kwargs.items() if v is not None}}}
        qdoc = {"query": self._search_context._query,
                "aggs": aggs,
                "size": 0}
        res = self._search_context._sumo.post("/search", json=qdoc).json()
        return res["aggregations"]["agg"]

    def min(self, field):
        """Find the minimum value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The minimum value.

        """
        return self._aggregate("min", field=field)["value"]

    def max(self, field):
        """Find the maximum value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The maximum value.

        """
        return self._aggregate("max", field=field)["value"]

    def avg(self, field):
        """Find the average value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The average value.

        """
        return self._aggregate("avg", field=field)["value"]

    def sum(self, field):
        """Find the sumo of all values for the specified property across
        the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The sum of all values.

        """
        return self._aggregate("sum", field=field)["value"]

    def value_count(self, field):
        """Find the count of values for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The total number of values.

        """
        return self._aggregate("value_count", field=field)["value"]

    def cardinality(self, field):
        """Find the count of distinct values for the specified property
        across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The number of distinct values.

        Note: The value returned is approximate.

        """
        return self._aggregate("cardinality", field=field)["value"]

    def stats(self, field):
        """Compute a basic set of statistics of the values for the specified
        property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return self._aggregate("stats", field=field)

    def extended_stats(self, field):
        """Compute an extended set of statistics of the values for the
        specified property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return self._aggregate("extended_stats", field=field)

    def percentiles(self, field, percents=None):
        """Find the values at specific percentiles for the specified
        property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.
            - percents ([number]): list of percent values. If omitted, uses
              a default set of values.

        Returns:
            A dictionary of percentiles.

        """
        return self._aggregate("percentiles", field=field,
                               percents=percents)["values"]
