from fmu.sumo.explorer import Explorer

sumo = Explorer("dev")

case = sumo.get_case_by_id("81a57a32-37e7-06bc-924e-6710ba6e59b0")

surface_names = case.get_object_names("surface", iteration_id=0, realization_id=0)

print(surface_names)