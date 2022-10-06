from enum import Enum

class ObjectType(str, Enum):
    SURFACE = "surface"
    POLYGONS = "polyons"
    TABLE = "table"
    
class Property(str, Enum):
    TAG_NAME = "tag_name"
    TIME_INTERVAL = "time_interval"
    TIME_TYPE = "time_type"
    AGGREGATION = "aggregation"
    OBJECT_NAME = "object_name"
    ITERATION_ID = "iteration_id"
    REALIZATION_ID = "realization_id"

class TimeData(str, Enum):
    ALL = "ALL"
    TIMESTAMP = "TIMESTAMP"
    TIME_INTERVAL = "TIME_INTERVAL"
    NONE = "NONE"


OBJECT_TYPES = {
    'surface': '.gri',
    'polygons': '.csv',
    'table': '.csv'
}

class Utils:
    def map_buckets(self, buckets):
        mapped = {}
        buckets_sorted = sorted(buckets, key=lambda b: b['key'])

        for bucket in buckets_sorted:
            mapped[bucket["key"]] = bucket["doc_count"]

        return mapped


    def create_elastic_query(
            self, 
            object_type='surface',
            size=0, 
            sort=None,
            terms={}, 
            fields_exists=[],
            aggregate_field=None,
            include_time_data=None
    ):
        if object_type not in list(OBJECT_TYPES.keys()):
            raise Exception(f"Invalid object_type: {object_type}. Accepted object_types: {OBJECT_TYPES.keys()}")

        elastic_query = {
            "size": size, 
            "runtime_mappings": {
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
                                emit('NONE');
                            }
                        """
                    }
                },
                "time_type": {
                    "type": "keyword",
                    "script": {
                        "lang": "painless",
                        "source": """
                            def time = params['_source']['data']['time'];
            
                            if(time != null) {
                                if(time.length == 0) {
                                    emit("NONE");
                                } else if(time.length == 1) {
                                    emit("TIMESTAMP");
                                } else if (time.length == 2) {
                                    emit("TIME_INTERVAL");
                                } else {
                                    emit("UNKNOWN");
                                }
                            } else {
                                emit("NONE");
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
                            String[] split_file_name = file_name.splitOnToken('--');

                            if(split_file_name.length == 1) {{
                                emit('NONE');
                            }} else {{
                                String surface_content = split_file_name[1].replace('{OBJECT_TYPES[object_type]}', '');
                                emit(surface_content);
                            }}
                        """
                    }
                }
            },
            "query": {
                "bool": {}
            },
            "fields": ["tag_name", "time_interval"]
        }

        must = [{"match": {"class": object_type}}]
        must_not = []

        if sort:
            elastic_query["sort"] = sort

        for field in terms:
            must.append({
                "terms": {field: terms[field]}
            })

        for field in fields_exists:
            must.append({
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

        if aggregate_field in ["tag_name", "time_interval"]:
            must_not.append({
                "term": {aggregate_field: "NONE"}
            })

        if include_time_data is not None:
            if include_time_data == TimeData.ALL:
                must.append({
                    "terms": {"time_type": ["TIMESTAMP", "TIME_INTERVAL"]}
                })
            elif include_time_data == TimeData.TIMESTAMP:
                must.append({
                    "term": {"time_type": "TIMESTAMP"}
                })
            elif include_time_data == TimeData.TIME_INTERVAL:
                must.append({
                    "term": {"time_type": "TIME_INTERVAL"}
                })
            elif include_time_data == TimeData.NONE:
                must_not.append({
                    "terms": {"time_type": ["TIMESTAMP", "TIME_INTERVAL"]}
                })
            else:
                raise ValueError(f"Invalid value for include_time_data: {include_time_data}")

        if len(must) > 0:
            elastic_query["query"]["bool"]["must"] = must

        if len(must_not) > 0:
            elastic_query["query"]["bool"]["must_not"] = must_not

        return elastic_query