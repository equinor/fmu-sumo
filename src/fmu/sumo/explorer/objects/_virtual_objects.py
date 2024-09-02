"""Module containing class for virtual objects.

Virtual objects are objects that exist conceptually, but not as specific
data objects in Sumo, such as Realization and Iteration (ensembles)."""

from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.surface_collection import SurfaceCollection
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer.pit import Pit

# from fmu.sumo.explorer.objects.case import Case


class Iteration:
    """Representation of the virtual Iteration object."""

    def __init__(
        self,
        sumo: SumoClient,
        uuid: str,
        case=None,
        name: str = None,
        pit: Pit = None,
    ) -> None:

        self._sumo = sumo
        self._case = case
        self._uuid = uuid
        self._name = name
        self._utils = Utils(sumo)
        self._realizations = None
        self._pit = pit

    def __repr__(self):
        return str(self)

    def __str__(self):
        """String representation of this instance."""
        return f"{self.name} ({self.uuid})"

    @property
    def uuid(self):
        """The fmu.iteration.uuid for this iteration."""
        return self._uuid

    @property
    def name(self):
        """The fmu.iteration.name for this iteration."""
        if self._name is None:
            pass  # TODO get name
        return self._name

    @property
    def case(self):
        """Case instance representing the case that this iter belongs to."""
        if self._case is None:
            pass  # TODO get_case
        return self._case

    @property
    def realizations(self):
        """The realizations of this iteration."""
        if self._realizations is None:
            self._realizations = self._get_realizations()
        return self._realizations

    def _get_realizations(self):
        """Get all realizations of this iteration/ensemble."""

        must = [
            {
                "term": {
                    "_sumo.parent_object.keyword": self._case.uuid,
                }
            },
            {
                "term": {
                    "fmu.iteration.uuid.keyword": self.uuid,
                },
            },
        ]

        print("query realizations")

        query = {
            "query": {
                "match": {
                    "fmu.iteration.uuid": "447992a4-b9c4-8619-6992-5d7d65b73309"
                }
            },
            "aggs": {
                "id": {
                    "terms": {
                        "field": "fmu.realization.id",
                        "size": 1000,  # usually (!) less than 1000
                    },
                    "aggs": {
                        "uuid": {
                            "terms": {
                                "field": "fmu.realization.uuid.keyword",
                                "size": 10,  # should be only one
                            }
                        },
                        "name": {
                            "terms": {
                                "field": "fmu.realization.name.keyword",
                                "size": 10,  # should be only one
                            }
                        },
                    },
                }
            },
            "size": 0,
        }

        res = self._sumo.post("/search", json=query)
        buckets = res.json()["aggregations"]["id"]["buckets"]

        realizations = {}

        for bucket in buckets:
            _realization = Realization(
                sumo=self._sumo,
                id=bucket["key"],
                name=bucket["name"]["buckets"][0]["key"],
                uuid=bucket["uuid"]["buckets"][0]["key"],
                case=self.case,
            )
            realizations[_realization.id] = _realization

        return realizations


class Realization:
    """Representation of the virtual Realization object."""

    def __init__(
        self,
        sumo: SumoClient,
        uuid: str,
        id: str = None,
        name: str = None,
        pit: Pit = None,
        case=None,
    ) -> None:
        self._sumo = sumo
        self._uuid = uuid
        self._id = id
        self._name = name
        self._pit = pit
        self._case = case

    def __repr__(self):
        return str(self)

    def __str__(self):
        """String representation of this instance."""
        return f"Realization {self.id} ({self.uuid})"

    @property
    def uuid(self):
        """The fmu.realization.uuid for this realization."""
        return self._uuid

    @property
    def id(self):
        """The fmu.realization.id for this realization."""
        return self._id

    @property
    def case(self):
        """Case instance representing the case this realization belongs to."""
        # needed for the class-specific methods, e.g. .surfaces
        if self._case is None:
            pass  # TODO get_case
        return self._case

    @property
    def name(self):
        """The fmu.realization.name for this realization."""
        if self._name is None:
            pass  # TODO get_name
        return self._name

    @property
    def surfaces(self) -> SurfaceCollection:
        """List of surfaces under this realization."""
        query = {"match": {"fmu.realization.uuid": self.uuid}}
        return SurfaceCollection(
            self._sumo, self._uuid, pit=self._pit, query=query
        )
