from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer._document_collection import DocumentCollection
from fmu.sumo.explorer._child_object import ChildObject

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


    def _create_elastic_query(
            self, 
            object_type='surface',
            size=0, 
            sort=None,
            fields_match=[], 
            fields_exists=[],
            aggregate_field=None
    ):
        if object_type not in list(OBJECT_TYPES.keys()):
            raise Exception(f"Invalid object_type: {object_type}. Accepted object_types: {OBJECT_TYPES.keys()}")

        elastic_query = {
            "size": size, 
            "runtime_mappings": {
                "time_start": {
                    "type": "keyword",
                    "script": {
                        "lang": "painless", 
                        "source": """
                            def time = params['_source']['data']['time'];
                            
                            if(time != null && time.length > 1) {
                                emit(params['_source']['data']['time'][1]['value'].splitOnToken('T')[0]); 
                            } else {
                                emit('NULL');
                            }
                        """
                    }
                },
                "time_end": {
                    "type": "keyword",
                    "script": {
                        "lang": "painless", 
                        "source": """
                            def time = params['_source']['data']['time'];
                            
                            if(time != null && time.length > 0) {
                                emit(params['_source']['data']['time'][0]['value'].splitOnToken('T')[0]); 
                            } else {
                                emit('NULL');
                            }
                        """
                    }
                },
                "time_interval": {
                    "type": "keyword",
                    "script": {
                        "lang": "painless", 
                        "source": """
                            def time = params['_source']['data']['time'];
                            
                            if(time != null) {
                                if(time.length > 1) {
                                String start = params['_source']['data']['time'][1]['value'].splitOnToken('T')[0];
                                String end = params['_source']['data']['time'][0]['value'].splitOnToken('T')[0];
                                
                                emit(start + ' - ' + end);
                                } else if(time.length > 0) {
                                emit(params['_source']['data']['time'][0]['value'].splitOnToken('T')[0]);
                                }
                            }else {
                                emit('NULL');
                            }
                        """
                    }
                },
                "tag_name": {
                    "type": "keyword",
                    "script": {
                        "source": f"""
                            String[] split_path = doc['file.relative_path.keyword'].value.splitOnToken('/');
                            String file_name = split_path[split_path.length - 1];
                            String surface_content = file_name.splitOnToken('--')[1].replace('{OBJECT_TYPES[object_type]}', '');
                            emit(surface_content);
                        """
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {"match": {"class": object_type}}
                    ]
                }
            },
            "fields": ["tag_name", "time_start", "time_end", "time_interval"]
        }

        if sort:
            elastic_query["sort"] = sort

        for field in fields_match:
            elastic_query["query"]["bool"]["must"].append({
                "match": { field: fields_match[field]}
            })

        for field in fields_exists:
            elastic_query["query"]["bool"]["must"].append({
                "exists": { "field": field}
            })

        if aggregate_field:
            elastic_query["aggs"] = {
                aggregate_field: {
                    "terms": {
                        "field": aggregate_field,
                        "size": 300
                    }
                }
            }

        return elastic_query


    def get_object_types(self):
        result = self.sumo.get("/search",
            query=f"_sumo.parent_object:{self.sumo_id}",
            buckets=["class.keyword"]
        )

        buckets = result["aggregations"]["class.keyword"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_iterations(self):
        result = self.sumo.get("/search",
            query=f"_sumo.parent_object:{self.sumo_id}",
			buckets=["fmu.iteration.id"]
        )

        buckets = result["aggregations"]["fmu.iteration.id"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_realizations(self, iteration_id):
        result = self.sumo.get("/search",
            query=f"_sumo.parent_object:{self.sumo_id} AND fmu.iteration.id:{iteration_id}",
            buckets=["fmu.realization.id"],
            bucketsize=1000
        )

        buckets = result["aggregations"]["fmu.realization.id"]["buckets"]

        return self.utils.map_buckets(sorted(buckets, key=lambda b : b["key"]))


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
            iteration_id=iteration_id,
            realization_id=realization_id,
            aggregation=aggregation
        )


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
            tag_name=tag_name,
            iteration_id=iteration_id,
            realization_id=realization_id,
            aggregation=aggregation
        )


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
            object_name=object_name,
            tag_name=tag_name,
            iteration_id=iteration_id,
            realization_id=realization_id,
            aggregation=aggregation
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
            object_name=object_name,
            tag_name=tag_name,
            iteration_id=iteration_id
        )


    def get_object_property_values(
        self,
        property,
        object_type,
        object_name=None,
        tag_name=None,
        time_interval=None,
        iteration_id=None,
        realization_id=None,
        aggregation=None
    ):
        accepted_properties = {
            "tag_name": "tag_name",
            "time_interval": "time_interval",
            "aggregation": "fmu.aggregation.operation.keyword",
            "object_name": "data.name.keyword",
            "iteration_id": "fmu.iteration.id",
            "realization_id": "fmu.realization.id"
        }

        if property not in accepted_properties.keys():
            raise Exception(f"Invalid property: {property}. Accepted properties: {accepted_properties.keys()}")

        fields_match = {"_sumo.parent_object": self.sumo_id}

        if iteration_id is not None:
            fields_match["fmu.iteration.id"] = iteration_id

        if realization_id is not None:
            fields_match["fmu.realization.id"] = realization_id

        if tag_name:
            fields_match["tag_name"] = tag_name

        if object_name:
            fields_match["data.name.keyword"] = object_name

        if time_interval:
            fields_match["time_interval"] = time_interval

        if aggregation:
            fields_match["fmu.aggregation.operation"] = aggregation

        agg_field = accepted_properties[property]

        elastic_query = self._create_elastic_query(
            object_type=object_type,
            fields_match=fields_match,
            aggregate_field=agg_field
        )

        result = self.sumo.post("/search", json=elastic_query)
        buckets = result.json()["aggregations"][agg_field]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_objects(
        self,
        object_type,
        object_name=None,
        tag_name=None,
        time_interval=None,
        iteration_id=None,
        realization_id=None,
        aggregation=None
    ):
        fields_match = {"_sumo.parent_object": self.sumo_id}
        fields_exists = []

        if iteration_id is not None:
            fields_match["fmu.iteration.id"] = iteration_id

        if realization_id is not None:
            fields_match["fmu.realization.id"] = realization_id

        if tag_name:
            fields_match["tag_name"] = tag_name

        if object_name:
            fields_match["data.name.keyword"] = object_name

        if time_interval:
            fields_match["time_interval"] = time_interval

        if aggregation:
            fields_match["fmu.aggregation.operation"] = aggregation
        else:
            fields_exists.append("fmu.realization.id")

        sort = [{"tracklog.datetime": "desc"}]

        query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            fields_match=fields_match,
            size=20,
            sort=sort
        )

        result = self.sumo.post("/search", json=query).json()
        count = result["hits"]["total"]["value"]
        documents = result["hits"]["hits"]
        search_after = documents[-1]["sort"]

        return DocumentCollection(
            self.sumo, 
            query, 
            count, 
            sort,
            lambda d: list(map(lambda c: ChildObject(self.sumo, c), d)),
            initial_batch=documents,
            search_after=search_after
        )