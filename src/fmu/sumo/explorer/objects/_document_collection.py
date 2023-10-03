"""Module containing class for collection of documents"""
from typing import List, Dict
from sumo.wrapper import SumoClient
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer.pit import Pit


class DocumentCollection:
    """Class for representing a collection of documents in Sumo"""

    def __init__(
        self,
        doc_type: str,
        sumo: SumoClient,
        query: Dict = None,
        select: List[str] = None,
        pit: Pit = None,
    ):
        self._utils = Utils(sumo)
        self._type = doc_type
        self._sumo = sumo
        self._query = self._init_query(doc_type, query)

        self._pit = pit
        self._new_pit_id = None
        self._after = None
        self._curr_index = 0
        self._len = None
        self._items = []
        self._field_values = {}
        self._select = select

    def __iter__(self):
        self._curr_index = 0
        return self

    def __next__(self):
        if self._curr_index < self.__len__():
            res = self.__getitem__(self._curr_index)
            self._curr_index += 1
            return res
        else:
            raise StopIteration

    def __aiter__(self):
        self._curr_index = 0
        return self

    async def __anext__(self):
        if self._curr_index < await self.length_async():
            res = await self.getitem_async(self._curr_index)
            self._curr_index += 1
            return res
        else:
            raise StopAsyncIteration

    def __len__(self) -> int:
        """Get size of document collection

        Returns:
            Document collection size
        """
        if self._len is None:
            self._next_batch()

        return self._len

    async def length_async(self) -> int:
        """Get size of document collection. Async equivalent to 'len(collection)'

        Returns:
            Document collection size
        """
        if self._len is None:
            await self._next_batch_async()

        return self._len

    def __getitem__(self, index: int) -> Dict:
        """Get document

        Arguments:
            - index (int): index

        Returns:
            A document at a given index
        """
        if index > self.__len__():
            raise IndexError

        while len(self._items) <= index:
            hits_size = self._next_batch()

            if hits_size == 0:
                raise IndexError

        return self._items[index]

    async def getitem_async(self, index: int) -> Dict:
        """Get document. Async equivalent to 'collection[index]'

        Arguments:
            - index (int): index

        Returns:
            A document at a given index
        """
        if index > await self.length_async():
            raise IndexError

        while len(self._items) <= index:
            hits_size = await self._next_batch_async()

            if hits_size == 0:
                raise IndexError

        return self._items[index]

    def _get_field_values(
        self, field: str, query: Dict = None, key_as_string: bool = False
    ) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            bucket_query = self._utils.extend_query_object(self._query, query)
            key = "key_as_string" if key_as_string is True else "key"
            buckets = self._utils.get_buckets(field, bucket_query)
            self._field_values[field] = list(
                map(lambda bucket: bucket[key], buckets)
            )

        return self._field_values[field]

    async def _get_field_values_async(
        self, field: str, query: Dict = None, key_as_string: bool = False
    ) -> List:
        """Get List of unique values for a given field

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            bucket_query = self._utils.extend_query_object(self._query, query)
            key = "key_as_string" if key_as_string is True else "key"
            buckets = await self._utils.get_buckets_async(field, bucket_query)
            self._field_values[field] = list(
                map(lambda bucket: bucket[key], buckets)
            )

        return self._field_values[field]

    def _next_batch(self) -> List[Dict]:
        """Get next batch of documents

        Returns:
            The next batch of documents
        """
        query = {
            "query": self._query,
            "sort": [{"_doc": {"order": "desc"}}],
            "size": 500,
        }

        if self._select:
            query["_source"] = self._select

        if self._len is None:
            query["track_total_hits"] = True

        if self._after is not None:
            query["search_after"] = self._after

        if self._pit is not None:
            query["pit"] = self._pit.get_pit_object(self._new_pit_id)

        res = self._sumo.post("/search", json=query).json()
        hits = res["hits"]

        if self._pit is not None:
            self._new_pit_id = res["pit_id"]

        if self._len is None:
            self._len = hits["total"]["value"]

        if len(hits["hits"]) > 0:
            self._after = hits["hits"][-1]["sort"]
            self._items.extend(hits["hits"])

        return len(hits["hits"])

    async def _next_batch_async(self) -> List[Dict]:
        """Get next batch of documents

        Returns:
            The next batch of documents
        """
        query = {
            "query": self._query,
            "sort": [{"_doc": {"order": "desc"}}],
            "size": 500,
        }

        if self._select:
            query["_source"] = self._select

        if self._len is None:
            query["track_total_hits"] = True

        if self._after is not None:
            query["search_after"] = self._after

        if self._pit is not None:
            query["pit"] = self._pit.get_pit_object(self._new_pit_id)

        res = await self._sumo.post_async("/search", json=query)
        data = res.json()
        hits = data["hits"]

        if self._pit is not None:
            self._new_pit_id = data["pit_id"]

        if self._len is None:
            self._len = hits["total"]["value"]

        if len(hits["hits"]) > 0:
            self._after = hits["hits"][-1]["sort"]
            self._items.extend(hits["hits"])

        return len(hits["hits"])

    def _init_query(self, doc_type: str, query: Dict = None) -> Dict:
        """Initialize base filter for document collection

        Arguments:
            - type (str): object type
            - filters (List[Dict]): a List of filters

        Returns:
            Document collection base filters
        """
        class_filter = {
            "bool": {"must": [{"term": {"class.keyword": doc_type}}]}
        }

        if query is not None:
            return self._utils.extend_query_object(class_filter, query)

        return class_filter

    def _add_filter(self, query: Dict) -> Dict:
        """Add filter to DocumentCollection base filter

        Argmuments:
            - user_filter (Dict[str, List]): new filters

        Returns:
            Filter object containing base filters and new filters
        """

        return self._utils.extend_query_object(self._query, query)
