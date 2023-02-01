from sumo.wrapper import SumoClient
from fmu.sumo.explorer.utils import Utils
from typing import List, Dict


class DocumentCollection:
    """Class for representing a collection of documents in Sumo"""

    def __init__(
        self,
        type: str,
        sumo: SumoClient,
        initial_filter: List[Dict] = None,
    ):
        self._utils = Utils(sumo)
        self._type = type
        self._sumo = sumo
        self._after = None
        self._curr_index = 0
        self._len = None
        self._items = []
        self._field_values = {}
        self._base_filter = self._init_base_filter(type, initial_filter)

    def __len__(self) -> int:
        """Get size of document collection

        Returns:
            Document collection size
        """
        if self._len is None:
            query = {"query": {"bool": {"must": self._base_filter}}, "size": 0}
            res = self._sumo.post("/search", json=query)
            self._len = res.json()["hits"]["total"]["value"]

        return self._len

    def __getitem__(self, index: int) -> Dict:
        """Get document

        Arguments:
            - index (int): index

        Returns:
            A document at a given index
        """
        if index >= self.__len__():
            raise IndexError

        if len(self._items) <= index:
            while len(self._items) <= index:
                next_batch = self._next_batch()

                if len(next_batch) > 0:
                    self._items.extend(next_batch)
                else:
                    raise IndexError

        return self._items[index]

    def _get_field_values(self, field: str) -> List:
        """Get List of unique values for a given field in the document collection

        Arguments:
            - field (str): a metadata field

        Returns:
            A List of unique values for the given field
        """
        if field not in self._field_values:
            self._field_values[field] = self._utils.get_buckets(
                field, self._base_filter
            )

        return self._field_values[field]

    def _next_batch(self) -> List[Dict]:
        """Get next batch of documents

        Returns:
            The next batch of documents
        """
        query = {
            "query": {"bool": {"must": self._base_filter}},
            "sort": [{"_sumo.timestamp": {"order": "desc"}}],
            "size": 50,
        }

        if self._after is not None:
            query["search_after"] = self._after

        res = self._sumo.post("/search", json=query)
        hits = res.json()["hits"]["hits"]

        if len(hits) > 0:
            self._after = hits[-1]["sort"]

        return hits

    def _init_base_filter(self, type: str, initial_filter: Dict = None) -> Dict:
        """Initialize base filter for document collection

        Arguments:
            - type (str): object type
            - filters (List[Dict]): a List of filters

        Returns:
            Document collection base filters
        """
        if initial_filter is not None:
            return initial_filter

        return [{"term": {"class.keyword": type}}]

    def _List_wrap(self, item) -> List:
        """Wrap item to List

        Arguments:
            - item (any): item to wrap

        Returns:
            Item wrapped in List
        """
        if type(item) == list:
            return item
        else:
            return [item]

    def _add_filter(self, user_filter: Dict[str, List]):
        """Add filter to DocumentCollection base filter

        Argmuments:
            - user_filter (Dict[str, List]): new filters

        Returns:
            Filter object containing base filters and new filters
        """
        new_filter = self._base_filter.copy()

        for field in user_filter:
            new_filter.append({"terms": {field: self._List_wrap(user_filter[field])}})

        return new_filter
