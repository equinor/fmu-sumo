"""
Complex(ish) filters for use with fmu-sumo Explorer.
"""

from typing import ClassVar


class Filters:
    # Filter that matches 4d-seismic objects.
    seismic4d: ClassVar = {
        "bool": {
            "must": [
                {"term": {"data.content.keyword": "seismic"}},
                {"exists": {"field": "data.time.t0.label"}},
                {"exists": {"field": "data.time.t1.label"}},
            ]
        }
    }

    # Filter that matches aggregations
    aggregations: ClassVar = {"exists": {"field": "fmu.aggregation.operation"}}

    # Filter that matches observations
    observations: ClassVar = {
        "bool": {
            "must_not": [
                {"exists": {"field": "fmu.ensemble.name.keyword"}},
                {"exists": {"field": "fmu.realization.id"}},
            ]
        }
    }

    # Filter that matches realizations
    realizations: ClassVar = {"exists": {"field": "fmu.realization.id"}}
