try:
    from collections import Sequence
except ImportError:
    from collections.abc import Sequence

from io import BytesIO
import zipfile


class DocumentCollection(Sequence):
    def __init__(
        self, 
        sumo_client, 
        query, 
        result_count, 
        sort,
        mapper_function,
        initial_batch=None,
        search_after=None,
    ):
        if initial_batch and not search_after:
            raise Exception("search_after argument is required when initializing ObjectCollection with initial_batch")

        self.DEFAULT_BATCH_SIZE = 500
        self.sumo = sumo_client
        self.result_count = result_count
        self.search_after = search_after
        self.mapper_function = mapper_function

        self.query = {
            **query, 
            "size": self.DEFAULT_BATCH_SIZE, 
            "sort": sort
        }

        self.documents = mapper_function(initial_batch) if initial_batch else self.__next_batch__()


    def __next_batch__(self):
        query = self.query.copy()

        if self.search_after:
            query["search_after"] = self.search_after

        result = self.sumo.post("/search", json=query)
        documents = result.json()["hits"]["hits"]
        self.search_after = documents[-1]["sort"]

        return self.mapper_function(documents)


    def __len__(self):
        return self.result_count

    
    def __getitem__(self, key):
        start = key
        stop = None

        if type(key) is slice:
            start = key.start
            stop = key.stop

        if (stop or start) > (len(self.documents) - 1):
            print(f"Documents length: {len(self.documents)}. Index out of range: {(stop or start)}. Fetching next batch!")

            self.documents += self.__next_batch__()
            return self.__getitem__(key)
        else:
            return self.documents[start:stop] if stop else self.documents[start]


    def aggregate(self, operations):
        if self.documents[0].object_type != "surface":
            raise Exception(f"Can't aggregate: {self.documents[0].object_type}")

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