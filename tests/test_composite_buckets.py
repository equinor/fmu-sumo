"""Unit tests for the composite buckets query helpers."""

from context import add_path

add_path()

# pylint: disable=wrong-import-position
from fmu.sumo.explorer.objects._search_context import (  # noqa: E402
    _build_composite_buckets_query,
    _extract_composite_buckets,
)


def test_build_composite_buckets_query_without_sub_aggs():
    query = {"bool": {"must": []}}
    sources = [
        {"name": {"terms": {"field": "data.name.keyword"}}},
        {"t0": {"terms": {"field": "data.time.t0.value", "missing_bucket": True}}},
    ]

    built = _build_composite_buckets_query(query, sources, None, 1000)

    assert built["size"] == 0
    assert built["query"] is query
    composite = built["aggs"]["composite"]
    assert composite["composite"]["size"] == 1000
    assert composite["composite"]["sources"] == sources
    assert "aggs" not in composite


def test_build_composite_buckets_query_with_sub_aggs():
    query = {"bool": {"must": []}}
    sources = [{"name": {"terms": {"field": "data.name.keyword"}}}]
    sub_aggs = {
        "value_min": {"min": {"field": "data.spec.value_statistics.min"}},
        "value_max": {"max": {"field": "data.spec.value_statistics.max"}},
    }

    built = _build_composite_buckets_query(query, sources, sub_aggs, 500)

    composite = built["aggs"]["composite"]
    assert composite["composite"]["sources"] == sources
    assert composite["aggs"] == sub_aggs


def test_extract_composite_buckets_returns_full_buckets_and_after_key():
    res = {
        "aggregations": {
            "composite": {
                "after_key": {"name": "PORO", "t0": None},
                "buckets": [
                    {
                        "key": {"name": "PORO", "t0": None},
                        "doc_count": 3,
                        "value_min": {"value": 0.1},
                        "value_max": {"value": 0.4},
                    },
                ],
            }
        }
    }

    buckets, after_key = _extract_composite_buckets(res)

    assert after_key == {"name": "PORO", "t0": None}
    assert buckets[0]["doc_count"] == 3
    assert buckets[0]["value_min"]["value"] == 0.1
    assert buckets[0]["key"]["name"] == "PORO"


def test_extract_composite_buckets_handles_missing_after_key():
    res = {"aggregations": {"composite": {"buckets": []}}}

    buckets, after_key = _extract_composite_buckets(res)

    assert buckets == []
    assert after_key is None
