try:
    from collections import Sequence
except ImportError:
    from collections.abc import Sequence

from fmu.sumo.explorer._case import Case

class CaseCollection(Sequence):
    def __init__(self, sumo_client, query, result_count):
        self.sumo = sumo_client
        self.query = query
        self.result_count = result_count

    def __len__(self):
        return self.result_count

    def __getitem__(self, key):
        start = key
        size = 1

        if type(key) is slice:
            start = key.start
            size = key.stop - key.start

        result = self.sumo.get("/search", **{
            "query": self.query,
            "sort": "tracklog.datetime:desc",
            "from": start,
            "size": size
        })

        cases = list(map(
            lambda c: Case(self.sumo, c),
            result["hits"]["hits"]
        ))

        if size == 1:
            return cases[0]
        
        return cases
