from sumo.wrapper import SumoClient
from fmu.sumo.explorer._case import Case
from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer._document_collection import DocumentCollection


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
        

    def get(self, path, **params):
        return self.sumo.get(path, **params)


    def post(self, path, json=None, blob=None):
        return self.sumo.post(path, json=json, blob=blob)


    def put(self, path, json=None, blob=None):
        return self.sumo.put(path, json=json, blob=blob)


    def delete(self, path):
        return self.sumo.delete(path)
