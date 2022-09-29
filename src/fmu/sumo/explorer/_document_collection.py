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
        mapper_function=None
    ):
        self.sumo = sumo_client
        self.result_count = None
        self.search_after = None
        self.mapper_function = mapper_function
        self.query = self.__validate_query__(query)

        self.documents = self.__next_batch__()


    def __validate_query__(self, query):
        required_keys = ["size", "sort"]
        query_keys = query.keys()

        for key in required_keys:
            if key not in query_keys:
                raise Exception(f"Query is missing required key: {key}")

        return query


    def __next_batch__(self):
        query = self.query.copy()

        if self.search_after:
            query["search_after"] = self.search_after

        result = self.sumo.post("/search", json=query).json()
        documents = result["hits"]["hits"]

        if(len(documents) > 0):
            self.search_after = documents[-1]["sort"]

            if not self.result_count:
                self.result_count = result["hits"]["total"]["value"]

            return self.mapper_function(documents) if self.mapper_function else documents
        else:
            self.result_count = 0
            return []


    def __len__(self):
        return self.result_count

    
    def __getitem__(self, key):
        start = key
        stop = None

        if type(key) is slice:
            start = key.start
            stop = key.stop

        if (stop or start) >= self.result_count:
            raise IndexError("list index out of range")

        if (stop or start) > (len(self.documents) - 1):
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