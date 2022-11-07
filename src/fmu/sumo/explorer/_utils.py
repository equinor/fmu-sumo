"""Utilities for explorer"""
import logging
import warnings
from abc import ABCMeta
from enum import Enum


OBJECT_TYPES = {
    'surface': '.gri',
    'polygons': '.csv',
    'table': '.csv'
}


class ObjectType(str, Enum):
    """Class containing object type"""
    SURFACE = "surface"
    POLYGONS = "polyons"
    TABLE = "table"


class Property(str, Enum):
    """Class containing property types"""
    TAG_NAME = "tag_name"
    TIME_INTERVAL = "time_interval"
    TIME_TYPE = "time_type"
    AGGREGATION = "aggregation"
    OBJECT_NAME = "object_name"
    ITERATION_ID = "iteration_id"
    REALIZATION_ID = "realization_id"


class TimeData(str, Enum):
    """Class containing TimeData types"""
    ALL = "ALL"
    TIMESTAMP = "TIMESTAMP"
    TIME_INTERVAL = "TIME_INTERVAL"
    NONE = "NONE"


class WarnTemplate(Warning):
    """template for custom templates"""
    __metaclass__ = ABCMeta

    def __init__(self, message):
        """Init"""
        self._message = message

    def __str__(self):
        return self._message


class TooManyCasesWarning(WarnTemplate):

    """Warning about too many cases"""


class TooLowSizeWarning(Warning):

    """Warning about request for too small size"""


def init_logging(name, loglevel=None):
    """Init of logger instance
    args:
    name (str): name that will appear when logger is activated
    loglevel (None or string): the log level
    returns logger (logging.Logger)
    """
    dateformat = '%m/%d/%Y %I:%M:%S'
    mess_format = "%(name)s %(levelname)s: %(message)s"

    if loglevel is None:
        logger = logging.getLogger(name)
        logger.addHandler(logging.NullHandler())
    else:
        # Allow use both lower and upper case with upper
        logging.basicConfig(level=loglevel.upper(), format=mess_format,
                            datefmt=dateformat)
        logger = logging.getLogger(name)
    return logger


def return_case_sumo_id(case_name, query_results):
    """
    args:
    query_results (dict): elastic search results
    returns sumo_id (str): the sumo id
    """
    sumo_id = None
    hits = return_hits(query_results)

    if len(hits) > 1:
        message = (
            f"Several cases called {case_name} ({len(hits)}, returning first " +
            "match, this might not be the case you wanted!! Having several " +
            "cases with the same name is unwise, and strongly discouraged."
        )
        warnings.warn(message, TooManyCasesWarning)

    try:
        sumo_id = hits[0]["_id"]
    except IndexError:
        warnings.warn(f"No hits for case {case_name}")
    return sumo_id


def return_hits(query_results):
    """takes query results gives basic info, and returns hits
    args:
        query_results (dict): elastic search results
    returns (hits): return data from search, stripped of uneccesaries
    """
    logger = init_logging(__name__ + ".return_hits")
    logger.debug(query_results)
    total_count = query_results["hits"]["total"]["value"]
    logger.debug(total_count)
    hits = query_results["hits"]["hits"]
    logger.debug("hits: %s", len(hits))
    logger.debug(hits)
    return_count = len(hits)
    if return_count < total_count:
        message = (
            "Your query returned less than the total number of hits\n" +
            f"({return_count} vs {total_count}). You might wanna rerun \n" +
            f"the query with size set to {total_count}"
        )
        warnings.warn(message, TooLowSizeWarning)
    return hits


def get_vector_name(source):
    """Gets name of vector from query results
    source (dict): results from elastic search query _source
    """

    name = [
        col for col in source["data"]["spec"]["columns"] if "REAL" not in col
    ][-1]
    return name


def deal_w_tag(kwargs):
    """Works on what to do with tag
    kwargs (dict): dictionary"""
    # This function is here just because of an inability to use content.tagname
    # in elastic search. Remove when this is solved
    logger = init_logging(__name__ + ".deal_w_tag")
    logger.debug(kwargs)
    name_of_tags = "tagname"
    tagname = kwargs.get(name_of_tags, None)
    standard_get_name = True
    if tagname is not None:
        del kwargs[name_of_tags]
    if (kwargs["data_type"] == "table" and
        kwargs["content"] == "timeseries"):
        standard_get_name = False
    return tagname, standard_get_name


def perform_query(case, **kwargs):
    """Performs an elastic search
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    """
    logger = init_logging(__name__ + ".get_object_blobs")
    logger.debug("Calling function with %s", kwargs)
    convert = {"data_type": "class", "content": "data.content",
               "name": "data.name", "tag": "data.tagname",
               "iteration": "fmu.iteration.id"}

    size = kwargs.get("size", 10)
    try:
        del kwargs["size"]
    except KeyError:
        logger.debug("Size is predefined")

    query = f"_sumo.parent_object:{case.sumo_id}"
    for key, value in kwargs.items():
        if key == "tag":
            # Somehow the tagname doesn't work
            continue
        query += f" AND {convert[key]}:{value}"
    logger.debug("sending query: %s", query)
    results = return_hits(case.sumo.get("/search", query=query, size=size))
    return results


def get_object_blob_ids(case, **kwargs):
    """Makes dictionary pointing to blob files
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    return blob_dict (dict): dictionary of blobs, key is name
                             value is blob path
    """
    logger = init_logging(__name__ + ".get_object_blobs")
    logger.debug(kwargs)
    tagname, standard_get_name = deal_w_tag(kwargs)

    results = perform_query(case, **kwargs)
    logger.debug(len(results))
    blob_ids = {}
    for result in results:
        source = result["_source"]
        if tagname is not None:
            if source["data"]["tagname"] != tagname:
                continue
        if standard_get_name:
            name = str(source["fmu"]["realization"]["id"])
        else:
            name = get_vector_name(source)

        blob_ids[name] = result["_id"]
    logger.info("returning %s blob ids", len(blob_ids.keys()))
    return blob_ids


class Utils:
    """Utility class for explorer"""

    def map_buckets(self, buckets):
        """Mapping count of docs to name
        args:
            buckets (list): from elastic search query
        returns: mapped (dict): key is bucket_name, value is count
        """
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
            terms=None,
            fields_exists=None,
            aggregate_field=None,
            include_time_data=None
    ):
        """Creates elastic query"""
        if object_type not in list(OBJECT_TYPES):
            raise Exception(f"Invalid object_type: {object_type}. Accepted object_types: {OBJECT_TYPES}")

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
                "exists": {"field": field}
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
