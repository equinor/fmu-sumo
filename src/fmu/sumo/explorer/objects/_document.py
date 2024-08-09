"""Contains class for one document"""

import re
from typing import List, Dict

_path_split_rx = re.compile(r"\]\.|\.|\[")


def _splitpath(path):
    parts = _path_split_rx.split(path)
    return [int(x) if re.match(r"\d+", x) else x for x in parts]


def _makeprop(attribute):
    path = _splitpath(attribute)
    return lambda self: self._get_property(path)


class Document:
    """Class for representing a document in Sumo"""

    def __init__(self, metadata: Dict) -> None:
        self._uuid = metadata["_id"]
        self._metadata = metadata["_source"]

    @property
    def uuid(self):
        """Return uuid

        Returns:
            str: the uuid of the case
        """
        return self._uuid

    @property
    def metadata(self):
        """Return metadata for document

        Returns:
            dict: the metadata
        """
        return self._metadata

    def _get_property(self, path: List[str]):
        curr = self._metadata

        for key in path:
            if (isinstance(curr, list) and key < len(curr)) or key in curr:
                curr = curr[key]
            else:
                return None

        return curr

    def __getitem__(self, key: str):
        return self._metadata[key]

    @staticmethod
    def map_properties(cls, propmap):
        for name, attribute, doc in propmap:
            setattr(cls, name, property(_makeprop(attribute), None, None, doc))
