"""
Complex(ish) filters for use with fmu-sumo Explorer.
"""


class Filters:
    # Filter that matches 4d-seismic objects.
    seismic4d = {
        "bool": {
            "must": [
                {"term": {"data.content.keyword": "seismic"}},
                {"exists": {"field": "data.time.t0.label"}},
                {"exists": {"field": "data.time.t1.label"}},
            ]
        }
    }

    # Filter that matches aggregations
    aggregations = {"exists": {"field": "fmu.aggregation.operation"}}

    # Filter that matches observations
    observations = {
        "bool": {
            "must_not": [
                {"exists": {"field": "fmu.iteration.name.keyword"}},
                {"exists": {"field": "fmu.realization.id"}},
            ]
        }
    }

    # Filter that matches realizations
    realizations = {"exists": {"field": "fmu.realization.id"}}
