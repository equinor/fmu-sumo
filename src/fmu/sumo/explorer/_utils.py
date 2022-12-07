"""Utilities for explorer"""
import logging
import warnings
from abc import ABCMeta
from enum import Enum
from io import BytesIO
import pyarrow as pa
from xtgeo import surface_from_file


# This is set as a global variables in case another userfriendly name
# is introduced
AGG_NAME = "aggregation"

OBJECT_TYPES = {"surface": ".gri", "polygons": ".csv", "table": ".csv"}


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
    AGGREGATION = AGG_NAME
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


class SumoGetObjectError(Exception):

    """Exception to raise when not able to get get object"""


def init_logging(name, loglevel=None):
    """Init of logger instance
    args:
    name (str): name that will appear when logger is activated
    loglevel (None or string): the log level
    returns logger (logging.Logger)
    """
    dateformat = "%m/%d/%Y %I:%M:%S"
    mess_format = "%(name)s %(levelname)s: %(message)s"

    if loglevel is None:
        logger = logging.getLogger(name)
        logger.addHandler(logging.NullHandler())
    else:
        # Allow use both lower and upper case with upper
        logging.basicConfig(
            level=loglevel.upper(), format=mess_format, datefmt=dateformat
        )
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
            f"Several cases called {case_name} ({len(hits)}, returning first "
            + "match, this might not be the case you wanted!! Having several "
            + "cases with the same name is unwise, and strongly discouraged."
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
            "Your query returned less than the total number of hits\n"
            + f"({return_count} vs {total_count}). You might wanna rerun \n"
            + f"the query with size set to {total_count}"
        )
        warnings.warn(message, TooLowSizeWarning)
    return hits


def get_vector_name(source):
    """Gets name of vector from query results
    source (dict): results from elastic search query _source
    """

    name = [col for col in source["data"]["spec"]["columns"] if "REAL" not in col][-1]
    return name


def choose_naming_convention(kwargs):
    """Figures out how to name keys in functions dealing with object_ids
    kwargs (dict): dictionary
    returns name_per_real (bool):"""
    name_per_real = True
    if kwargs["data_type"] == "table" and kwargs["content"] == "timeseries":
        name_per_real = False
    return name_per_real


def perform_query(case, **kwargs):
    """Performs an elastic search
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    """
    logger = init_logging(__name__ + ".perform_query")
    logger.debug("Calling function with %s", kwargs)
    convert = {
        "data_type": "class",
        "content": "data.content",
        "name": "data.name",
        "tag": "data.tagname",
        "iteration": "fmu.iteration.id",
        AGG_NAME: "fmu.aggregation.operation",
    }

    size = kwargs.get("size", 1000)
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


def get_aggregated_object_ids(case, **kwargs):
    """Makes dictionary for aggregated object object ids
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    return object_ids (dict): dictionary of objects, key is name
                             value dict where key is aggregation operation, value is
                             object id
    """
    logger = init_logging(__name__ + ".get_aggregated_object_ids")
    results = perform_query(case, **kwargs)
    object_ids = {}
    count = 0
    for result in results:
        source = result["_source"]
        name = source["data"]["name"]
        operation = source["fmu"]["aggregation"]["operation"]
        object_ids[name] = object_ids.get(name, {})
        object_ids[name][operation] = result["_id"]
        count += 1
    logger.info("returning %s object ids", count)
    return object_ids


def get_real_object_ids(case, **kwargs):
    """Makes dictionary pointing to object ids
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    return object_ids (dict): dictionary of objects, key is name
                             value is object id
    """
    logger = init_logging(__name__ + ".get_real_object_ids")
    results = perform_query(case, **kwargs)
    name_per_real = choose_naming_convention(kwargs)
    logger.debug("%s results", len(results))
    object_ids = {}
    for result in results:
        source = result["_source"]

        if name_per_real:
            try:
                name = str(source["fmu"]["realization"]["id"])
            except KeyError:
                logger.debug("could not find realization")
        else:
            name = get_vector_name(source)

        object_ids[name] = result["_id"]
    logger.info("returning %s object ids", len(object_ids.keys()))
    return object_ids


def get_object_ids(case, **kwargs):
    """Makes dictionary pointing to object ids
    args:
    case (explorer.Case): case to explore
    kwargs (dict): keword argument
    return object_ids (dict): dictionary of objects, key is name
                             value is object id
    """
    logger = init_logging(__name__ + ".get_object_ids")

    logger.debug(kwargs)
    try:
        if kwargs[AGG_NAME] == "all":
            kwargs[AGG_NAME] = "*"
    except KeyError:
        logger.debug("No aggregations in query")

    if AGG_NAME in kwargs:
        object_ids = get_aggregated_object_ids(case, **kwargs)
    else:
        object_ids = get_real_object_ids(case, **kwargs)

    return object_ids


def get_object(object_id, exp):
    """Fetches object for blob store
    args:
    object_id (str): uuid string
    exp (fmu.sumo.Explorer): the explorer to find object with
    returns obj: the object
    raises SumoGetObjectError: if cannot retrieve object
    """
    object_query = f"/objects('{object_id}')/blob"
    try:
        obj = exp.sumo.get(object_query)
    except Exception as exc:
        raise SumoGetObjectError("No results with query {object_query}") from exc
    return obj


def read_arrow(object_id, exp):
    """Reads arrow from sumo object
    args:
    object_id (str): id of the object
    returns: table (pa.Table)
    """
    with pa.ipc.open_file(get_object(object_id, exp)) as reader:
        table = reader.read_table()
    return table


def get_vector_data(object_ids, vector_name, exp):

    """Reads from dictionary of object id's
    args:
    object_ids (dict): dictionary with key vector name, and value object id
    vector_name (str): name of vector
    exp (fmu.sumo.Explorer): the explorer to find data with
    returns frame (pd.DataFrame): the fetched data
    """
    object_id = object_ids[vector_name]
    frame = read_arrow(object_id, exp).to_pandas()
    return frame


def get_surface_object(surf_id, exp):
    """Fetches a surface as xtgeo object from blob store
    args:
     surf_id (str): object id for surface
     exp (fmu.sumo.Explorer): the explorer to find data with
     returns surface (xtgeo.surface): the extracted object
    """
    surface = surface_from_file(BytesIO(get_object(surf_id, exp)))
    return surface


def get_surface_from_real(object_ids, real_nr, exp):
    """Fetches a surface as xtgeo object from blob store
    args:
    object_ids (dict): key is real, value is object id
    real_nr (int): realization nr
    exp (fmu.sumo.Explorer): the explorer to find data with
    """
    try:
        surf_id = object_ids[str(real_nr)]
    except KeyError as key_err:
        raise KeyError("Maybe this is an aggregation?") from key_err
    return get_surface_object(surf_id, exp)


def get_aggregated_surface(aggregated_ids, name, agg_type, exp):
    """Fetches a surface as xtgeo object from blob store
    args:
    aggregated_ids (dict): key is name, value is dict, where key is agg_type, value is object id
    exp (fmu.sumo.Explorer): the explorer to find data with
    """
    surf_id = aggregated_ids[name][agg_type]
    return get_surface_object(surf_id, exp)


def get_surface(object_ids, exp, **kwargs):
    """Fetches a surface from blob store, either aggregation or per realization
    args:
    kwargs (dict): input depending on whether you want aggregation results or
                   individual real
                   for real: real_nr=real_nr
                   for aggregation: name=surface name, and agg_type
    """
    keywords = kwargs.copy()
    name = kwargs.get("name", None)
    agg_type = kwargs.get("aggregation", None)
    real_nr = kwargs.get("real_nr", None)
    if real_nr is not None:

        surf = get_surface_from_real(object_ids, real_nr, exp)

    elif (name is not None) and (agg_type is not None):
        surf = get_aggregated_surface(object_ids, name, agg_type, exp)
    else:
        raise SumoGetObjectError(
            f"Cannot get object from dict {object_ids} either real_nr or"
            + " combination of name, and agg_type needs to be defined\n"
            + f"Your call {keywords}"
        )
    return surf


class Utils:
    """Utility class for explorer"""

    def map_buckets(self, buckets):
        """Mapping count of docs to name
        args:
            buckets (list): from elastic search query
        returns: mapped (dict): key is bucket_name, value is count
        """
        mapped = {}
        buckets_sorted = sorted(buckets, key=lambda b: b["key"])

        for bucket in buckets_sorted:
            mapped[bucket["key"]] = bucket["doc_count"]

        return mapped

    def create_elastic_query(
        self,
        object_type="surface",
        size=0,
        sort=None,
        terms=None,
        fields_exists=None,
        aggregate_field=None,
        include_time_data=None,
    ):
        """Creates elastic query"""
        if object_type not in list(OBJECT_TYPES):
            raise Exception(
                f"Invalid object_type: {object_type}. Accepted object_types: {OBJECT_TYPES}"
            )

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
                        """,
                    },
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
                        """,
                    },
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
                    },
                },
            },
            "query": {"bool": {}},
            "fields": ["tag_name", "time_interval"],
        }

        must = [{"match": {"class": object_type}}]
        must_not = []

        if sort:
            elastic_query["sort"] = sort

        for field in terms:
            must.append({"terms": {field: terms[field]}})

        for field in fields_exists:
            must.append({"exists": {"field": field}})

        if aggregate_field:
            elastic_query["aggs"] = {
                aggregate_field: {"terms": {"field": aggregate_field, "size": 300}}
            }

        if aggregate_field in ["tag_name", "time_interval"]:
            must_not.append({"term": {aggregate_field: "NONE"}})

        if include_time_data is not None:
            if include_time_data == TimeData.ALL:
                must.append({"terms": {"time_type": ["TIMESTAMP", "TIME_INTERVAL"]}})
            elif include_time_data == TimeData.TIMESTAMP:
                must.append({"term": {"time_type": "TIMESTAMP"}})
            elif include_time_data == TimeData.TIME_INTERVAL:
                must.append({"term": {"time_type": "TIME_INTERVAL"}})
            elif include_time_data == TimeData.NONE:
                must_not.append(
                    {"terms": {"time_type": ["TIMESTAMP", "TIME_INTERVAL"]}}
                )
            else:
                raise ValueError(
                    f"Invalid value for include_time_data: {include_time_data}"
                )

        if len(must) > 0:
            elastic_query["query"]["bool"]["must"] = must

        if len(must_not) > 0:
            elastic_query["query"]["bool"]["must_not"] = must_not

        return elastic_query
