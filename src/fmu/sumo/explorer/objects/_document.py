"""Contains class for one document"""

import json
import re
from typing import Any, Dict, List, Union

_path_split_rx = re.compile(r"\]\.|\.|\[")


def _splitpath(path):
    parts = _path_split_rx.split(path)
    return [int(x) if re.match(r"\d+", x) else x for x in parts]


class Document:
    """Class for representing a document in Sumo"""

    def __init__(self, metadata: Dict) -> None:
        self._uuid = metadata["_id"]
        self._metadata = metadata["_source"]

    def __str__(self):
        return f"{json.dumps(self.metadata, indent=4)}"

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}: {self.name} {self.uuid}(uuid) "
            f"in asset {self.asset}>"
        )

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

    def _get_property(self, path: List[Union[str, int]]):
        curr = self._metadata

        for key in path:
            if (
                isinstance(curr, list)
                and isinstance(key, int)
                and key < len(curr)
            ) or key in curr:
                curr = curr[key]
            else:
                return None

        return curr

    def get_property(self, path: str) -> Any:
        return self._get_property(_splitpath(path))

    def __getitem__(self, key: str):
        return self._metadata[key]

    @property
    def template_path(self) -> str:
        return ""

    @property
    def name(self) -> str:
        return "Should not happen"

    @property
    def asset(self) -> str:
        return self.get_property("access.asset.name")
