from enum import Enum


class TimeType(Enum):
    """An Enum representing diffent time types in Sumo

    Used when creating a TimeFilter object.
    """

    TIMESTAMP = 0
    INTERVAL = 1
    ALL = 2
    NONE = 3


class TimeFilter:
    """Class representing a time filter

    A TimeFilter object can be used when doing time filtering on case objects.
    """

    def __init__(
        self,
        type: TimeType,
        start: str = None,
        end: str = None,
        overlap: bool = False,
        exact: bool = False,
    ) -> None:
        """Initialize TimeFilter

        Args:
            type (TimeType): time type (TIMESTAMP, INTERVAL, ALL, NONE)
            start (str): start of range
            end (str): end of range
            overlap (bool): include overlapping intervals
            exact (bool): include only exact matches

        Examples:

            Get surfaces with timestamps::

                time = TimeFilter(type=TimeType.TIMESTAMP)

                case.surfaces.filter(time=time)

            Get surfaces whith timestamp in range::

                time = TimeFilter(
                    type=TimeType.TIMESTAMP,
                    start="2018-01-01",
                    end="2022-01-01"
                )

                case.surfaces.filter(time=time)

            Get surfaces with intervals::

                time = TimeFilter(type=TimeType.INTERVAL)

                case.surfaces.filter(time=time)

            Get surfaces with intervals in range::

                time = TimeFilter(
                    type=TimeType.INTERVAL,
                    start="2018-01-01",
                    end="2022-01-01"
                )

                case.surfaces.filter(time=time)

            Get surfaces where intervals overlap::

                time = TimeFIlter(
                    type=TimeType.INTERVAL,
                    start="2018-01-01",
                    end="2022-01-01",
                    overlap=True
                )

                case.surfaces.filter(time=time)

            Get surfaces with exact interval match::

                time = TimeFilter(
                    type=TimeType.INTERVAL,
                    start="2018-01-01",
                    end="2022-01-01",
                    exact=True
                )

                case.surfaces.filter(time=time)
        """
        self.type = type
        self.start = start
        self.end = end
        self.overlap = overlap
        self.exact = exact

    def _get_range_filter(self, key, start=None, end=None):
        if not start and not end:
            return None

        filter = {"range": {key: {}}}

        if start:
            filter["range"][key]["gte"] = start

        if end:
            filter["range"][key]["lte"] = end

        return filter

    def _get_query(self):
        must = []
        must_not = []
        should = []
        minimum_should_match = None
        t0_filter = self._get_range_filter(
            "data.time.t0.value", self.start, self.end
        )
        t1_filter = self._get_range_filter(
            "data.time.t1.value", self.start, self.end
        )

        if self.type == TimeType.TIMESTAMP:
            must.append({"exists": {"field": "data.time.t0"}})
            must_not.append({"exists": {"field": "data.time.t1"}})

            if t0_filter:
                if self.exact:
                    must.append({"term": {"data.time.t0.value": self.start}})
                else:
                    must.append(t0_filter)
        elif self.type == TimeType.INTERVAL:
            must.append({"exists": {"field": "data.time.t0"}})
            must.append({"exists": {"field": "data.time.t1"}})

            if t0_filter and t1_filter:
                if self.exact:
                    must.append({"term": {"data.time.t0.value": self.start}})
                    must.append({"term": {"data.time.t1.value": self.end}})
                else:
                    if self.overlap:
                        should.append(t0_filter)
                        should.append(t1_filter)
                        minimum_should_match = 1
                    else:
                        must.append(t0_filter)
                        must.append(t1_filter)
        elif self.type == TimeType.ALL:
            must.append({"exists": {"field": "data.time"}})

            if t0_filter and t1_filter:
                minimum_should_match = 1

                if self.exact:
                    should.append({"term": {"data.time.t0.value": self.start}})
                    should.append(
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "data.time.t0.value": self.start
                                        }
                                    },
                                    {"term": {"data.time.t1.value": self.end}},
                                ]
                            }
                        }
                    )
                else:
                    if self.overlap:
                        should.append(t0_filter)
                        should.append(t1_filter)
                    else:
                        should.append(t0_filter)
                        should.append(
                            {"bool": {"must": [t0_filter, t1_filter]}}
                        )
        elif self.type == TimeType.NONE:
            must_not.append({"exists": {"field": "data.time"}})
        else:
            raise Exception(f"Invalid TimeType: {self.type}")

        query = {"bool": {}}

        if len(must) > 0:
            query["bool"]["must"] = must

        if len(must_not) > 0:
            query["bool"]["must_not"] = must_not

        if len(should) > 0:
            query["bool"]["should"] = should

        if minimum_should_match is not None:
            query["bool"]["minimum_should_match"] = minimum_should_match

        return query
