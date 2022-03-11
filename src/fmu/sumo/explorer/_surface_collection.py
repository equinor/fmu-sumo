try:
    from collections import Sequence
except ImportError:
    from collections.abc import Sequence

from io import BytesIO
import zipfile
from fmu.sumo.explorer._object import Surface

class SurfaceCollection(Sequence):
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

        query = self.query.copy()
        query["from"] = start
        query["size"] = size
        query["sort"] = [{"tracklog.datetime": "desc"}]

        result = self.sumo.post("/debug-search", json=query)

        cases = list(map(
            lambda c: Surface(self.sumo, c),
            result.json()["hits"]["hits"]
        ))

        if size == 1:
            return cases[0]
        
        return cases

    def aggregate(self, operations):
        multiple_operations = False
        operation_list = operations

        if type(operations) is list:
            if len(operations) > 1:
                multiple_operations = True
        else:
            operation_list = [operations]

        query = self.query.copy()
        query["size"] = self.result_count

        result = self.sumo.post("/debug-search", json=query)

        object_ids = list(map(
            lambda s : s["_id"],
            result.json()["hits"]["hits"]
        ))

        response = self.sumo.post("/aggregate", json={
            "operation": operation_list,
            "object_ids": object_ids
        })

        if multiple_operations:
            result = {}

            with zipfile.ZipFile(BytesIO(response.content), "r") as zip_obj:
                for file in zip_obj.namelist():
                    result[file] = zip_obj.read(file)

                return result

        return response.content