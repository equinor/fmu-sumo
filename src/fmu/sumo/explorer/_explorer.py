"""Functionality for exploring results from sumo"""
from typing import List
from sumo.wrapper import SumoClient
from fmu.sumo.explorer._case import Case
from fmu.sumo.explorer._utils import Utils, TimeData, ObjectType
from fmu.sumo.explorer._document_collection import DocumentCollection
from fmu.sumo.explorer._child_object import ChildObject


class Explorer:
    """Class for exploring sumo"""
    def __init__(self, env, token=None, interactive=True):
        self._env = env
        self.utils = Utils()
        self.sumo = SumoClient(
            env=env,
            token=token,
            interactive=interactive
        )

    @property
    def env(self):
        """Returning the _env attribute
        """
        return self._env

    def get_fields(self):
        """Returns the fields with stored results in given sumo environment"""
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
        """Returns users that have stored results in given sumo environment"""
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
        """Returns the status of the different cases
           i.e. whether case is to be kept, scratched or to be deleted
        """
        result = self.sumo.get("/searchroot",
            size=0,
            buckets=["_sumo.status.keyword"]
        )

        buckets = result["aggregations"]["_sumo.status.keyword"]["buckets"]
        status = self.utils.map_buckets(buckets)

        return status

    def get_case_by_name(self, name):
        """Fetches case from case name
        args:
            name (str): name of case

        """
        query = f"class:case AND fmu.case.name:{name}"
        result = self.sumo.get("/search", select=["fmu.case"],
                               sort=["_doc:desc"],
                               query=query)
        hits = result["hits"]["hits"]
        if len(hits) > 1:
            warnings.warn(name, TooManyCasesWarning)

        sumo_id = hits[0]["_id"]
        return self.get_case_by_id(sumo_id)

    def get_case_by_id(self, sumo_id):
        """Returns Case class with given id
        args:
        sumo_id (str): sumo id for case to extract
        returns Case : Case given the sumo_id
        """
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
        """Returns all cases in given sumo environment
        args:
        status (str or None): filter on status (i.e. whether they will be kept or not)
        fields (list or None): filter on what fields that own the data
        users (list or None): filter on what users that have generated the data
        """
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
        case_ids: List[str]=(),
        object_names: List[str]=(),
        tag_names: List[str]=(),
        time_intervals: List[str]=(),
        iteration_ids: List[int]=(),
        realization_ids: List[int]=(),
        aggregations: List[str]=(),
        include_time_data: TimeData = None
    ): # noqa
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
        """Performing the get operation to sumo
        path (str): path to endpoint
        params (dict): get parameters
        """
        return self.sumo.get(path, **params)


    def post(self, path, json=None, blob=None):
        """Performing the post operation to sumo
        path (str): path to endpoint
        json (dict): metadata
        blob (?): the object to post
        params (dict): get parameters
        """
        return self.sumo.post(path, json=json, blob=blob)


    def put(self, path, json=None, blob=None):
        """Performing the put operation to sumo
        path (str): path to endpoint
        json (dict): metadata
        blob (?): the object to post
        params (dict): get parameters
        """
        return self.sumo.put(path, json=json, blob=blob)


    def delete(self, path):
        """Performing the delete operation to sumo
        path (str): path to endpoint
        """
        return self.sumo.delete(path)
