class Document:
    """Class for representing a document in Sumo"""

    def __init__(self, metadata: dict) -> None:
        self._id = metadata["_id"]
        self._metadata = metadata["_source"]

    @property
    def id(self):
        return self._id

    def _get_property(self, path: list[str]):
        curr = self._metadata

        for key in path:
            if key in curr:
                curr = curr[key]
            else:
                return None

    def __getitem__(self, key: str):
        return self._metadata[key]
