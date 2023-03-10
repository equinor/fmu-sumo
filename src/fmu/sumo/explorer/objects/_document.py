"""Contains class for one document"""
from typing import List, Dict


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
        curr = self._metadata.copy()

        for key in path:
            if key in curr:
                curr = curr[key]
            else:
                return None

        return curr

    def __getitem__(self, key: str):
        return self._metadata[key]
