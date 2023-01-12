"""Functions for interrogating specific case in sumo"""
from typing import List, Dict
import deprecation
from fmu.sumo.explorer._utils import Utils, TimeData, Property, ObjectType
from fmu.sumo.explorer._utils import get_object_ids
from fmu.sumo.explorer._document_collection import DocumentCollection
from fmu.sumo.explorer._child_object import ChildObject


class Case:
    """Private class for the fmu.sumo.explorer module."""

    def __init__(self, sumo_client, meta_data):
        """Initialize a Case object."""
        self.sumo = sumo_client
        self.meta_data = meta_data
        self.utils = Utils()

        source = self.meta_data["_source"]

        self._sumo_id = self.meta_data["_id"]
        self.fmu_id = source["fmu"]["case"]["uuid"]
        self._name = source["fmu"]["case"]["name"]
        self.field_name = source["masterdata"]["smda"]["field"][0]["identifier"]
        self.status = source["_sumo"]["status"]
        self.user = source["fmu"]["case"]["user"]["id"]
        self.object_type = "case"

    @property
    def sumo_id(self) -> str:
        """Returns attribute _sumo_id"""
        return self._sumo_id

    @property
    def name(self) -> str:
        """returns name attribute"""
        return self._name

    def get_object_types(self) -> Dict[str, int]:
        """Getting count of object types for case"""
        result = self.sumo.get(
            "/search",
            query=f"_sumo.parent_object:{self.sumo_id}",
            buckets=["class.keyword"],
        )

        buckets = result["aggregations"]["class.keyword"]["buckets"]

        return self.utils.map_buckets(buckets)

    def get_summary_object_ids(self, **kwargs) -> Dict[str, str]:
        """Gets blob_ids for summary data aggregated per vector
        args kwargs (dict): various keyword arguments
        returns dictionary with vector name as key, value object id
        """
        return get_object_ids(self, data_type="table", content="timeseries", **kwargs)

    def get_object_ids(
        self, name, tag, data_type="surface", content="depth", iteration=0, **kwargs
    ) -> Dict[str, str]:
        """Gets blob ids for most datatypes.

        * For summary data use get_summary_blob_ids

        args:
            name (str): name of data object
            tag (str): what type of tag, more or less the same as representation
                   in rms
            data_type (str): what type
            content (str): what type of content depth, time, timeseries etc
            iteration (int): the iteration to get data for
            kwargs (dict): keyword arguements.

        """
        return get_object_ids(
            self,
            name=name,
            tag=tag,
            content=content,
            data_type=data_type,
            iteration=iteration,
            **kwargs,
        )

    def get_iterations(self):
        """Getting iterations connected to case"""
        elastic_query = {
            "query": {"query_string": {"query": f"_sumo.parent_object:{self.sumo_id}"}},
            "size": 0,
            "aggs": {
                "iteration_ids": {
                    "terms": {"field": "fmu.iteration.id", "size": 100},
                    "aggs": {
                        "iteration_names": {
                            "terms": {
                                "field": "fmu.iteration.name.keyword",
                                "size": 100,
                            }
                        }
                    },
                }
            },
        }

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"]["iteration_ids"]["buckets"]
        iterations = list(
            map(
                lambda b: {
                    "id": b["key"],
                    "name": b["iteration_names"]["buckets"][0]["key"],
                    "doc_count": b["doc_count"],
                },
                buckets,
            )
        )

        return iterations

    @deprecation.deprecated(
        details="Use get_object_property_values to retrieve list of unique"
        + "values for a property"
    )
    def get_realizations(self, iteration_id) -> List[dict]:
        """Getting realizations for case"""
        elastic_query = {
            "query": {
                "query_string": {
                    "query": f"_sumo.parent_object:{self.sumo_id} "
                    + f"AND fmu.iteration.id:{iteration_id}"
                }
            },
            "size": 0,
            "aggs": {
                "realization_ids": {
                    "terms": {"field": "fmu.realization.id", "size": 1000},
                    "aggs": {
                        "realization_names": {
                            "terms": {
                                "field": "fmu.realization.name.keyword",
                                "size": 1000,
                            }
                        }
                    },
                }
            },
        }

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"]["realization_ids"]["buckets"]
        realizations = list(
            map(
                lambda b: {
                    "id": b["key"],
                    "name": b["realization_names"]["buckets"][0]["key"],
                    "doc_count": b["doc_count"],
                },
                buckets,
            )
        )

        return realizations

    @deprecation.deprecated(
        details="Use get_object_property_values to retrieve list of unique values for a property"
    )
    def get_object_tag_names(
        self, object_type, iteration_id=None, realization_id=None, aggregation=None
    ):
        return self.get_object_property_values(
            "tag_name",
            object_type,
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation),
        )

    @deprecation.deprecated(
        details="Use get_object_property_values to retrieve list of unique values for a property"
    )
    def get_object_names(
        self,
        object_type,
        tag_name=None,
        iteration_id=None,
        realization_id=None,
        aggregation=None,
    ):
        return self.get_object_property_values(
            "object_name",
            object_type,
            tag_names=self._list_wrap(tag_name),
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation),
        )

    @deprecation.deprecated(
        details="Use get_object_property_values to retrieve list of unique values for a property"
    )
    def get_object_time_intervals(
        self,
        object_type,
        object_name=None,
        tag_name=None,
        iteration_id=None,
        realization_id=None,
        aggregation=None,
    ):
        return self.get_object_property_values(
            "time_interval",
            object_type,
            object_names=self._list_wrap(object_name),
            tag_names=self._list_wrap(tag_name),
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation),
        )

    @deprecation.deprecated(
        details="Use get_object_property_values to retrieve list of unique values for a property"
    )
    def get_object_aggregations(
        self,
        object_type,
        object_name=None,
        tag_name=None,
        iteration_id=None,
    ):
        return self.get_object_property_values(
            "aggregation",
            object_type,
            object_names=self._list_wrap(object_name),
            tag_names=self._list_wrap(tag_name),
            iteration_ids=self._list_wrap(iteration_id),
        )

    def _list_wrap(self, value):
        """Don't know what this one is doing"""
        return [value] if value is not None else []

    def get_object_property_values(
        self,
        prop: Property,
        object_type: ObjectType,
        object_names: List[str] = (),
        tag_names: List[str] = (),
        time_intervals: List[str] = (),
        iteration_ids: List[str] = (),
        realization_ids: List[int] = (),
        aggregations: List[int] = (),
        include_time_data: TimeData = None,
    ):
        """
        Get a dictionary of unique values for a given property in case child objects.

        Arguments:
            `property`: tag_name | time_interval | time_type | aggregation | object_name | iteration_id | realization_id (Property)
            `object_type`: surface | polygons | table (ObjectType)
            `object_names`: list of object names (strings)
            `tag_names`: list of tag names (strings)
            `time_intervals`: list of time intervals (strings)
            `iteration_ids`: list of iteration ids (integers)
            `realization_ids`: list of realizatio ids (intergers)
            `aggregations`: list of aggregation operations (strings)
            `include_time_data`: ALL | NO_TIMEDATA | ONLY_TIMEDATA (TimeData)

        Returns:
            Dictionary of unique values and number of objects
        """

        accepted_properties = {
            "tag_name": "tag_name",
            "time_interval": "time_interval",
            "time_type": "time_type",
            "aggregation": "fmu.aggregation.operation.keyword",
            "object_name": "data.name.keyword",
            "iteration_id": "fmu.iteration.id",
            "realization_id": "fmu.realization.id",
        }

        if prop not in accepted_properties:
            raise Exception(
                f"Invalid field: {property}. Accepted fields: {accepted_properties.keys()}"
            )

        terms = {"_sumo.parent_object.keyword": [self.sumo_id]}

        if iteration_ids:
            terms["fmu.iteration.id"] = iteration_ids

        if realization_ids:
            terms["fmu.realization.id"] = realization_ids

        if tag_names:
            terms["tag_name"] = tag_names

        if object_names:
            terms["data.name.keyword"] = object_names

        if time_intervals:
            terms["time_interval"] = time_intervals

        if aggregations:
            terms["fmu.aggregation.operation"] = aggregations

        agg_field = accepted_properties[prop]

        elastic_query = self.utils.create_elastic_query(
            object_type=object_type,
            terms=terms,
            aggregate_field=agg_field,
            include_time_data=include_time_data,
        )

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"][agg_field]["buckets"]

        return self.utils.map_buckets(buckets)

    def get_objects(
        self,
        object_type: ObjectType,
        object_names: List[str] = (),
        tag_names: List[str] = (),
        time_intervals: List[str] = (),
        iteration_ids: List[int] = (),
        realization_ids: List[int] = (),
        aggregations: List[str] = (),
        include_time_data: TimeData = None,
        stages: List[str] = (),
        terms: Dict[str, List[str]] = {},
    ):
        """
        Search for child objects in a case.

        Arguments:
            `object_type`: surface | polygons | table (ObjectType)
            `object_names`: list of object names (strings)
            `tag_names`: list of tag names (strings)
            `time_intervals`: list of time intervals (strings)
            `iteration_ids`: list of iteration ids (integers)
            `realization_ids`: list of realization ids (integers)
            `aggregations`: list of aggregation operations (strings)
            `include_time_data`: ALL | NO_TIMEDATA | ONLY_TIMEDATA (TimeData)
            `stages`: list of stages (strings)
            `terms`: map of str to list of str, for additional filtering

        Returns:
            `DocumentCollection` used for retrieving search results
        """

        terms = terms.copy()

        terms["_sumo.parent_object.keyword"] = [self.sumo_id]
        fields_exists = []

        if iteration_ids:
            terms["fmu.iteration.id"] = iteration_ids

        if realization_ids:
            terms["fmu.realization.id"] = realization_ids

        if tag_names:
            terms["tag_name"] = tag_names

        if object_names:
            terms["data.name.keyword"] = object_names

        if time_intervals:
            terms["time_interval"] = time_intervals

        if aggregations:
            terms["fmu.aggregation.operation.keyword"] = aggregations

        if stages:
            terms["fmu.context.stage.keyword"] = stages

        query = self.utils.create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            terms=terms,
            size=20,
            sort=[{"tracklog.datetime": "desc"}],
            include_time_data=include_time_data,
        )

        return DocumentCollection(
            self.sumo,
            query,
            lambda d: list(map(lambda c: ChildObject(self.sumo, c), d)),
        )
