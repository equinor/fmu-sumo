from sumo.wrapper import SumoClient
from fmu.sumo.explorer._case import Case
from fmu.sumo.explorer._utils import Utils, TimeData, ObjectType
from fmu.sumo.explorer._document_collection import DocumentCollection
from typing import List
from fmu.sumo.explorer._child_object import ChildObject


class Explorer:
    def __init__(self, env, token=None, interactive=True):
        self.utils = Utils()
        self.sumo = SumoClient(
            env=env, 
            token=token,
            interactive=interactive
        )
    

    def get_fields(self):
        result = self.sumo.get("/search", 
            size=0, 
            buckets=['masterdata.smda.field.identifier.keyword'], 
            query="class:case",
            bucketsize=100
        )

        buckets = result["aggregations"]["masterdata.smda.field.identifier.keyword"]["buckets"]
        fields = self.utils.map_buckets(buckets)

        return fields


    def get_users(self):
        result = self.sumo.get("/search", 
            size=0, 
            buckets=['fmu.case.user.id.keyword'], 
            query="class:case",
            bucketsize=500
        )

        buckets = result["aggregations"]["fmu.case.user.id.keyword"]["buckets"]
        users = self.utils.map_buckets(buckets)

        return users


    def get_status(self):
        result = self.sumo.get("/searchroot",
            size=0,
            buckets=["_sumo.status.keyword"]
        )

        buckets = result["aggregations"]["_sumo.status.keyword"]["buckets"]
        status = self.utils.map_buckets(buckets)

        return status


    def get_case_by_id(self, sumo_id):
        result = self.sumo.get("/searchroot", query=f"_id:{sumo_id}")
        hits = result["hits"]["hits"]

        if len(hits) < 1:
            return None

        return Case(self.sumo, hits[0])


    def get_cases(
        self, 
        status=None, 
        fields=None, 
        users=None
    ):
        query_string = "class:case"

        if status:
            status_query = " OR ".join(status)
            query_string += f" _sumo.status:({status_query})"

        if fields:
            field_query = " OR ".join(fields)
            query_string += f" masterdata.smda.field.identifier:({field_query})"

        if users:
            user_query = " OR ".join(users)
            query_string += f" fmu.case.user.id:({user_query})"

        elastic_query = {
            "query": {
                "query_string": {
                    "query": query_string,
                    "default_operator": "AND"
                }
            },
            "sort": [{"tracklog.datetime": "desc"}],
            "size": 500
        }

        return DocumentCollection(
            self.sumo, 
            elastic_query, 
            lambda d: list(map(lambda c: Case(self.sumo, c), d)),
        )
        
    def get_objects(
        self,
        object_type: ObjectType,
        case_ids: List[str]=[],
        object_names: List[str]=[],
        tag_names: List[str]=[],
        time_intervals: List[str]=[],
        iteration_ids: List[int]=[],
        realization_ids: List[int]=[],
        aggregations: List[str]=[],
        include_time_data: TimeData = None
    ):
        """
            Search for child objects in a case.

            Arguments:
                `object_type`: surface | polygons | table (ObjectType)
                `object_names`: list of object names (strings)
                `tag_names`: list of tag names (strings)
                `time_intervals`: list of time intervals (strings)
                `iteration_ids`: list of iteration ids (integers)
                `realization_ids`: list of realizatio ids (intergers)
                `aggregations`: list of aggregation operations (strings)

            Returns:
                `DocumentCollection` used for retrieving search results
        """

        terms = {}
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

        if case_ids:
            terms["_sumo.parent_object.keyword"] = case_ids

        if aggregations:
            terms["fmu.aggregation.operation"] = aggregations
        else:
            fields_exists.append("fmu.realization.id")

        query = self.utils.create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            terms=terms,
            size=20,
            sort=[{"tracklog.datetime": "desc"}],
            include_time_data=include_time_data
        )

        return DocumentCollection(
            self.sumo, 
            query,
            lambda d: list(map(lambda c: ChildObject(self.sumo, c), d))
        )

    def get(self, path, **params):
        return self.sumo.get(path, **params)


    def post(self, path, json=None, blob=None):
        return self.sumo.post(path, json=json, blob=blob)


    def put(self, path, json=None, blob=None):
        return self.sumo.put(path, json=json, blob=blob)


    def delete(self, path):
        return self.sumo.delete(path)
