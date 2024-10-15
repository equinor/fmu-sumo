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
        return self._aggregate("min", field=field)

    def max(self, field):
        return self._aggregate("max", field=field)

    def avg(self, field):
        return self._aggregate("avg", field=field)

    def sum(self, field):
        return self._aggregate("sum", field=field)

    def value_count(self, field):
        return self._aggregate("value_count", field=field)

    def cardinality(self, field):
        return self._aggregate("cardinality", field=field)

    def stats(self, field):
        return self._aggregate("stats", field=field)

    def extended_stats(self, field):
        return self._aggregate("extended_stats", field=field)

    def percentiles(self, field, percents=None):
        if percents is None:
            return self._aggregate("percentiles", field=field)
        else:
            return self._aggregate("percentiles", field=field,
                                   percents=percents)

    

        
