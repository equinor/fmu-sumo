from time import time
from fmu.sumo.explorer import Explorer

sumo = Explorer(env="test", write_back=True)

""" my_case = sumo.get_case_by_id("0005976b-6e0c-68a4-63bc-fa6e5c74e111")


surface_names = my_case.get_surface_names(
    iteration_id=0,
    #realization_id=0,
    aggregation="MEAN"
)

print(surface_names)

tag_names = my_case.get_surface_tag_names(
    surface_name="VIKING GP. Top",
    iteration_id=0,
    #realization_id=0,
    aggregation="MEAN"
)

print(tag_names)

aggregations = my_case.get_surface_aggregations(
    surface_name="VIKING GP. Top",
    tag_name="structural_model",
    iteration_id=0,
)

print(aggregations)

surfaces = my_case.get_surfaces(
    surface_name="VIKING GP. Top",
    tag_name="structural_model",
    iteration_id=0,
    #realization_id=0,
    aggregation="mean"
)

print(len(surfaces)) """


""" my_case = sumo.get_case_by_id("69852e8a-b230-d341-e0de-bf61b1308f2b")

time_intervals = my_case.get_surface_time_intervals(
 
)

print(time_intervals) """

""" surfaces = my_case.get_surfaces(
    surface_name="Draupne Fm. 1 JS Top",
    tag_name="depth_depth_conversion",
    iteration_id=2,
    realization_id=26
) """


res = sumo.post("/debug-search", json={
    "size": 1, 
    "runtime_mappings": {
        "time_start": {
            "type": "keyword",
            "script": {
                "source": "emit(doc['data.time'][1]['value'])"
            }
        },
        "time_end": {
            "type": "keyword",
            "script": {
                "source": "emit(doc['data.time'][0]['value'])"
            }
        },
        "surface_content": {
            "type": "keyword",
            "script": {
                "source": """
                    String[] split_path = doc['file.relative_path.keyword'].value.splitOnToken('/');
                    String file_name = split_path[split_path.length - 1];
                    String surface_content = file_name.splitOnToken('--')[1].replace('.gri', '');
                
                    emit(surface_content);
                """
            }
        },
    },
    "query": {
        "bool": {
            "must": [
                {"match": {"class": "surface"}}
            ]
        }
    },
    "fields": ["surface_content"]
})


print(res.json()["hits"]["hits"][0])