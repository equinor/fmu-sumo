try:
    from collections import Sequence
except ImportError:
    from collections.abc import Sequence

from io import BytesIO
import zipfile
from fmu.sumo.explorer._object import Object

class ObjectCollection(Sequence):
    def __init__(self, sumo_client, query, result_count, object_type):
        self.sumo = sumo_client
        self.result_count = result_count
        self.object_type = object_type
        self.search_after = None

        self.query = {**query, "size": 500, "sort":[{"tracklog.datetime": "desc"}]}
        self.objects = self.__next_batch__()


    def __next_batch__(self):
        query = self.query.copy()

        if self.search_after:
            query["search_after"] = self.search_after

        result = self.sumo.post("/search", json=query)
        objects = result.json()["hits"]["hits"]
        self.search_after = objects[-1]["sort"]

        return list(map(lambda c: Object(self.sumo, c), objects))


    def __len__(self):
        return self.result_count

    
    def __getitem__(self, key):
        start = key
        stop = None

        if type(key) is slice:
            start = key.start
            stop = key.stop

        if (stop or start) > (len(self.objects) - 1):
            self.objects += self.__next_batch__()
            return self.__getitem__(key)
        else:
            return self.objects[start:stop] if stop else self.objects[start]


    def aggregate(self, operations):
        if self.object_type != "surface":
            raise Exception(f"Can't aggregate object type: {self.object_type}")

        multiple_operations = False
        operation_list = operations

        if type(operations) is list:
            if len(operations) > 1:
                multiple_operations = True
        else:
            operation_list = [operations]

        query = {**self.query, "size": self.result_count}
        result = self.sumo.post("/search", json=query)

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