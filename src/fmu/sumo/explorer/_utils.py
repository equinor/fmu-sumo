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

        for field in terms:
            elastic_query["query"]["bool"]["must"].append({
                "terms": {field: terms[field]}
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