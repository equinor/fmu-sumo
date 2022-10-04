from typing import List
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer._document_collection import DocumentCollection
from fmu.sumo.explorer._child_object import ChildObject
import deprecation

OBJECT_TYPES = {
    'surface': '.gri',
    'polygons': '.csv',
    'table': '.csv'
}

class Case:
    def __init__(self, sumo_client, meta_data):
        self.sumo = sumo_client
        self.meta_data = meta_data
        self.utils = Utils()

        source = self.meta_data["_source"]

        self.sumo_id = self.meta_data["_id"]
        self.fmu_id = source["fmu"]["case"]["uuid"]
        self.case_name = source["fmu"]["case"]["name"]
        self.field_name = source["masterdata"]["smda"]["field"][0]["identifier"]
        self.status = source["_sumo"]["status"]
        self.user = source["fmu"]["case"]["user"]["id"]
        self.object_type = "case"


    def get_object_types(self):
        result = self.sumo.get("/search",
            query=f"_sumo.parent_object:{self.sumo_id}",
            buckets=["class.keyword"]
        )

        buckets = result["aggregations"]["class.keyword"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_iterations(self):
        elastic_query = {
            "query": {
                "query_string": {
                    "query": f"_sumo.parent_object:{self.sumo_id}"
                }
            },
            "size": 0,
            "aggs": {
                "iteration_ids": {
                    "terms": {
                        "field": "fmu.iteration.id",
                        "size": 100
                    },
                    "aggs": {
                        "iteration_names": {
                            "terms": {
                                "field": "fmu.iteration.name.keyword",
                                "size": 100
                            }
                        }
                    }
                }
            }
        }

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"]["iteration_ids"]["buckets"]
        iterations = list(map(lambda b: {'id': b['key'], 'name': b['iteration_names']['buckets'][0]['key'], 'doc_count': b['doc_count']}, buckets))
        
        return iterations


    @deprecation.deprecated(details="Use get_object_property_values to retrieve list of unique values for a property")
    def get_realizations(self, iteration_id):
        elastic_query = {
            "query": {
                "query_string": {
                    "query": f"_sumo.parent_object:{self.sumo_id} AND fmu.iteration.id:{iteration_id}"
                }
            },
            "size": 0,
            "aggs": {
                "realization_ids": {
                    "terms": {
                        "field": "fmu.realization.id",
                        "size": 1000
                    },
                    "aggs": {
                        "realization_names": {
                            "terms": {
                                "field": "fmu.realization.name.keyword",
                                "size": 1000
                            }
                        }
                    }
                }
            }
        }

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"]["realization_ids"]["buckets"]
        realizations = list(map(lambda b: {'id': b['key'], 'name': b['realization_names']['buckets'][0]['key'], 'doc_count': b['doc_count']}, buckets))
        
        return realizations


    @deprecation.deprecated(details="Use get_object_property_values to retrieve list of unique values for a property")
    def get_object_tag_names(
        self, 
        object_type,
        iteration_id=None, 
        realization_id=None,
        aggregation=None
    ):
        return self.get_object_property_values(
            "tag_name",
            object_type,
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation)
        )


    @deprecation.deprecated(details="Use get_object_property_values to retrieve list of unique values for a property")
    def get_object_names(
        self, 
        object_type,
        tag_name=None,
        iteration_id=None, 
        realization_id=None, 
        aggregation=None
    ):
        return self.get_object_property_values(
            "object_name",
            object_type,
            tag_names=self._list_wrap(tag_name),
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation)
        )


    @deprecation.deprecated(details="Use get_object_property_values to retrieve list of unique values for a property")
    def get_object_time_intervals(
        self,
        object_type,
        object_name=None, 
        tag_name=None,
        iteration_id=None, 
        realization_id=None,
        aggregation=None
    ):
        return self.get_object_property_values(
            "time_interval",
            object_type,
            object_names=self._list_wrap(object_name),
            tag_names=self._list_wrap(tag_name),
            iteration_ids=self._list_wrap(iteration_id),
            realization_ids=self._list_wrap(realization_id),
            aggregations=self._list_wrap(aggregation)
        )


    @deprecation.deprecated(details="Use get_object_property_values to retrieve list of unique values for a property")
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
            iteration_ids=self._list_wrap(iteration_id)
        )


    def _list_wrap(self, value):
        return [value] if value is not None else []

    
    def get_object_property_values(
        self,
        property: str,
        object_type: str,
        object_names: List[str]=[],
        tag_names: List[str]=[],
        time_intervals: List[str]=[],
        iteration_ids: List[str]=[],
        realization_ids: List[int]=[],
        aggregations: List[int]=[]
    ):
        """
            Get a dictionary of unique values for a given property in case child objects.

            Arguments:
                `property`: tag_name | time_interval | aggregation | object_name | iteration_id | realization_id
                `object_type`: surface | polygons | table
                `object_names`: list of object names (strings)
                `tag_names`: list of tag names (strings)
                `time_intervals`: list of time intervals (strings)
                `iteration_ids`: list of iteration ids (integers)
                `realization_ids`: list of realizatio ids (intergers)
                `aggregations`: list of aggregation operations (strings)

            Returns:
                Dictionary of unique values and number of objects
        """

        accepted_properties = {
            "tag_name": "tag_name",
            "time_interval": "time_interval",
            "aggregation": "fmu.aggregation.operation.keyword",
            "object_name": "data.name.keyword",
            "iteration_id": "fmu.iteration.id",
            "realization_id": "fmu.realization.id"
        }

        if property not in accepted_properties.keys():
            raise Exception(f"Invalid field: {property}. Accepted fields: {accepted_properties.keys()}")

        terms = {
            "_sumo.parent_object.keyword": [self.sumo_id]
        }

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

        agg_field = accepted_properties[property]

        elastic_query = self.utils.create_elastic_query(
            object_type=object_type,
            terms=terms,
            aggregate_field=agg_field
        )

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"][agg_field]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_objects(
        self,
        object_type: str,
        object_names: List[str]=[],
        tag_names: List[str]=[],
        time_intervals: List[str]=[],
        iteration_ids: List[int]=[],
        realization_ids: List[int]=[],
        aggregations: List[str]=[]
    ):
        """
            Search for child objects in a case.

            Arguments:
                `object_type`: surface | polygons | table
                `object_names`: list of object names (strings)
                `tag_names`: list of tag names (strings)
                `time_intervals`: list of time intervals (strings)
                `iteration_ids`: list of iteration ids (integers)
                `realization_ids`: list of realizatio ids (intergers)
                `aggregations`: list of aggregation operations (strings)

            Returns:
                `DocumentCollection` used for retrieving search results
        """

        terms = {
            "_sumo.parent_object.keyword": [self.sumo_id]
        }
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
            terms["fmu.aggregation.operation"] = aggregations
        else:
            fields_exists.append("fmu.realization.id")

        query = self.utils.create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            terms=terms,
            size=20,
            sort=[{"tracklog.datetime": "desc"}]
        )

        return DocumentCollection(
            self.sumo, 
            query,
            lambda d: list(map(lambda c: ChildObject(self.sumo, c), d))
        )