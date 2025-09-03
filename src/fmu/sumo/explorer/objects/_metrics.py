class Metrics:
    def __init__(self, search_context):
        self._search_context = search_context
        return

    def _aggregate(self, op, **kwargs):
        aggs = {
            "agg": {op: {k: v for k, v in kwargs.items() if v is not None}}
        }
        qdoc = {"query": self._search_context._query, "aggs": aggs, "size": 0}
        res = self._search_context._sumo.post("/search", json=qdoc).json()
        return res["aggregations"]["agg"]

    async def _aggregate_async(self, op, **kwargs):
        aggs = {
            "agg": {op: {k: v for k, v in kwargs.items() if v is not None}}
        }
        qdoc = {"query": self._search_context._query, "aggs": aggs, "size": 0}
        res = (
            await self._search_context._sumo.post_async("/search", json=qdoc)
        ).json()
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

    async def min_async(self, field):
        """Find the minimum value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The minimum value.

        """
        return (await self._aggregate_async("min", field=field))["value"]

    def max(self, field):
        """Find the maximum value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The maximum value.

        """
        return self._aggregate("max", field=field)["value"]

    async def max_async(self, field):
        """Find the maximum value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The maximum value.

        """
        return (await self._aggregate_async("max", field=field))["value"]

    def avg(self, field):
        """Find the average value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The average value.

        """
        return self._aggregate("avg", field=field)["value"]

    async def avg_async(self, field):
        """Find the average value for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The average value.

        """
        return (await self._aggregate_async("avg", field=field))["value"]

    def sum(self, field):
        """Find the sumo of all values for the specified property across
        the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The sum of all values.

        """
        return self._aggregate("sum", field=field)["value"]

    async def sum_async(self, field):
        """Find the sumo of all values for the specified property across
        the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The sum of all values.

        """
        return (await self._aggregate_async("sum", field=field))["value"]

    def value_count(self, field):
        """Find the count of values for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The total number of values.

        """
        return self._aggregate("value_count", field=field)["value"]

    async def value_count_async(self, field):
        """Find the count of values for the specified property across the
        current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The total number of values.

        """
        return (await self._aggregate_async("value_count", field=field))[
            "value"
        ]

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

    async def cardinality_async(self, field):
        """Find the count of distinct values for the specified property
        across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            The number of distinct values.

        Note: The value returned is approximate.

        """
        return (await self._aggregate_async("cardinality", field=field))[
            "value"
        ]

    def stats(self, field):
        """Compute a basic set of statistics of the values for the specified
        property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return self._aggregate("stats", field=field)

    async def stats_async(self, field):
        """Compute a basic set of statistics of the values for the specified
        property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return await self._aggregate_async("stats", field=field)

    def extended_stats(self, field):
        """Compute an extended set of statistics of the values for the
        specified property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return self._aggregate("extended_stats", field=field)

    async def extended_stats_async(self, field):
        """Compute an extended set of statistics of the values for the
        specified property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            A dictionary of statistical metrics.

        """
        return await self._aggregate_async("extended_stats", field=field)

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
        return self._aggregate("percentiles", field=field, percents=percents)[
            "values"
        ]

    async def percentiles_async(self, field, percents=None):
        """Find the values at specific percentiles for the specified
        property across the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.
            - percents ([number]): list of percent values. If omitted, uses
              a default set of values.

        Returns:
            A dictionary of percentiles.

        """
        return (
            await self._aggregate_async(
                "percentiles", field=field, percents=percents
            )
        )["values"]

    def _fnv1a_script(self, field):
        return {
            "init_script": """
                state.h = state.count = state.total = 0L;
            """,
            "map_script": f"""
                state.total++;
                if (doc['{field}'].size() == 0) return;
                def s = doc.get('{field}').value;
                long h = -3750763034362895579L;
                for (int i = 0; i < s.length(); i++) {{
                    h ^= (long) s.charAt(i);
                    h *= 1099511628211L;
                }}
                state.h ^= h;
                state.count++;
            """,
            "combine_script": """
                return state;
            """,
            "reduce_script": """
                long h = 0, c = 0, t = 0;
                for (st in states) {
                    h ^= st.h; c += st.count; t += st.total
                }
                return ['checksum': Long.toHexString(h), 'docs_in_checksum': c, 'docs_total': t];
            """,
        }

    def fnv1a(self, field):
        """Compute the 64-bit FNV-1a checksum for field over the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            - a dict with the keys "docs_all", "docs_seen" and "xor_fnv64_hex".
        """
        return self._aggregate("scripted_metric", **self._fnv1a_script(field))

    async def fnv1a_async(self, field):
        """Compute the 64-bit FNV-1a checksum for field over the current set of objects.

        Arguments:
            - field (str): the name of a property in the metadata.

        Returns:
            - a dict with the keys "docs_all", "docs_seen" and "xor_fnv64_hex".
        """
        return await self._aggregate_async(
            "scripted_metric", **self._fnv1a_script(field)
        )
