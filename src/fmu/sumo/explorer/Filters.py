
# Filter that matches 4d-seismic objects.

seismic4d = {
    "bool": {
        "must": [
            {
                "term": {
                    "data.content.keyword": "seismic"
                }
            },
            {
                "term": {
                    "data.time.t0.label.keyword": "base"
                }
            },
            {
                "term": {
                    "data.time.t1.label.keyword": "monitor"
                }
            }
        ]
    }
}
