from time import time

from numpy import real
from fmu.sumo.explorer import Explorer

sumo = Explorer(env="dev", write_back=True)

my_case = sumo.get_case_by_id("81a57a32-37e7-06bc-924e-6710ba6e59b0")

# Get tag names
tag_names = my_case.get_surface_tag_names(
    iteration_id=0,
    realization_id=0,
    #aggregation="MEAN"
)

#print(tag_names)

# Get surface names
surface_names = my_case.get_surface_names(
    tag_name="amplitude_full_max",
    iteration_id=0, 
    realization_id=0,
    #aggregation="MEAN"
)

#print(surface_names)

# Get aggregation operations
aggregations = my_case.get_surface_aggregations(
    surface_name="draupne_fm_1",
    tag_name="amplitude_full_max",
    iteration_id=0,
)

#print(aggregations)

# Get surface time spans
timespans = my_case.get_surface_time_spans(
    surface_name="draupne_fm_1",
    tag_name="amplitude_full_max",
    iteration_id=0,
    realization_id=0,
    #aggregation="MEAN"
)

print(timespans)


# Current pattern
my_case.get_surface_tag_names()
my_case.get_surface_names()
my_case.get_surface_aggregations()
my_case.get_surface_time_intervals()
my_case.get_surfaces()

my_case.get_polygon_tag_names()
my_case.get_polygon_names()
my_case.get_polygons()

# Alternative pattern
my_case.surfaces.tag_names()
my_case.surfaces.names()
my_case.surfaces.aggregations()
my_case.surfaces.time_intervals()
my_case.surfaces.get()

my_case.polygons.tag_names()
my_case.polygons.names()
my_case.polygons.get()