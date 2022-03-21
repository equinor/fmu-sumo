from fmu.sumo.explorer._utils import Utils
from fmu.sumo.explorer._object_collection import SurfaceCollection, ObjectCollection

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


    def _create_elastic_query(
            self, 
            object_type='surface',
            size=0, 
            fields_match=[], 
            fields_exists=[],
            aggregate_field=None
    ):
        if object_type not in list(OBJECT_TYPES.keys()):
            raise Exception(f"Invalid object_type: {object_type}")

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
                        "field": aggregate_field
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
        fields_match = {"_sumo.parent_object": self.sumo_id}
        fields_exists = []

        if iteration_id is not None:
            fields_match["fmu.iteration.id"] = iteration_id

        if realization_id is not None:
            fields_match["fmu.realization.id"] = realization_id

        if aggregation:
            fields_match["fmu.aggregation.operation"] = aggregation
        else:
            fields_exists.append("fmu.realization.id")

        elastic_query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            fields_match=fields_match,
            aggregate_field="tag_name",
        )

        result = self.sumo.post("/debug-search", json=elastic_query)
        buckets = result.json()["aggregations"]["tag_name"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_object_names(
        self, 
        object_type,
        tag_name=None,
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

        if aggregation:
            fields_match["fmu.aggregation.operation"] = aggregation
        else:
            fields_exists.append("fmu.realization.id")

        elastic_query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            fields_match=fields_match,
            aggregate_field="data.name.keyword",
        )

        result = self.sumo.post("/debug-search", json=elastic_query)
        buckets = result.json()["aggregations"]["data.name.keyword"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_object_time_intervals(
        self,
        object_type,
        object_name=None, 
        tag_name=None,
        iteration_id=None, 
        realization_id=None,
        aggregation=None
    ):
        fields_match = {"_sumo.parent_object": self.sumo_id}
        fields_exists = []

        if object_name:
            fields_match["data.name.keyword"] = object_name

        if iteration_id is not None:
            fields_match["fmu.iteration.id"] = iteration_id

        if realization_id is not None:
            fields_match["fmu.realization.id"] = realization_id

        if tag_name:
            fields_match["tag_name"] = tag_name

        if aggregation:
            fields_match["fmu.aggregation.operation"] = aggregation
        else:
            fields_exists.append("fmu.realization.id")

        elastic_query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            fields_match=fields_match,
            aggregate_field="time_interval",
        )

        result = self.sumo.post("/debug-search", json=elastic_query)
        buckets = result.json()["aggregations"]["time_interval"]["buckets"]

        return self.utils.map_buckets(buckets)


    def get_object_aggregations(
        self, 
        object_type,
        object_name=None, 
        tag_name=None,
        iteration_id=None, 
    ):
        fields_match = { "_sumo.parent_object": self.sumo_id }

        if object_name:
            fields_match["data.name.keyword"] = object_name

        if iteration_id is not None:
            fields_match["fmu.iteration.id"] = iteration_id

        if tag_name:
            fields_match["tag_name"] = tag_name

        elastic_query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=["fmu.aggregation.operation"],
            fields_match=fields_match,
            aggregate_field="fmu.aggregation.operation.keyword",
        )

        result = self.sumo.post("/debug-search", json=elastic_query)
        buckets = result.json()["aggregations"]["fmu.aggregation.operation.keyword"]["buckets"]

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

        query = self._create_elastic_query(
            object_type=object_type,
            fields_exists=fields_exists,
            fields_match=fields_match,
            size=0
        )

        result = self.sumo.post("/debug-search", json=query)
        count = result.json()["hits"]["total"]["value"]

        if object_type == "surface":
            return SurfaceCollection(self.sumo, query, count)

        return ObjectCollection(self.sumo, query, count)