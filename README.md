[![Documentation Status](https://readthedocs.org/projects/fmu-sumo/badge/?version=latest)](https://fmu-sumo.readthedocs.io/en/latest/?badge=latest)


# fmu-sumo
This package is intended for interaction with Sumo within the FMU (Fast Model Update(TM)) ecosystem.


## Explorer

> :warning: OpenVDS does not publish builds for MacOS. You can still use the Explorer without OpenVDS, but some Cube methods will not work.

Explore and retrieve data from Sumo.

```python
from fmu.sumo.explorer import Explorer

sumo = Explorer()
```

#### Example: Find cases
```python
# List of all available cases
cases = sumo.cases

# Get filter values
cases.statuses
cases.users
cases.fields

# Apply filters
cases = cases.filter(status=["keep", "offical"], user="peesv", field="Drogon")

for case in cases:
    print(case.id)
    print(case.name)

# select case
case = cases[0]
```

#### Example: Retrieve case objects
Get objects within a case through `case.[CONTEXT].[OBJECT_TYPE]`.

##### Realized data
```python
# All realized surface objects in case
surfs = case.realization.surfaces

# Get filter values
surfs.names
surfs.tagnames
surfs.iterations

# Apply filters
surfs = surfs.filter(name="surface_name", tagname="surface_tagname", iteration=0)

# Get surface
surf = surfs[0]

# Metadata
surf.id
surf.name
surf.tagname
surf.iteration
surf.realization

# Binary
surf.blob

# Get xtgeo.RegularSurface
reg = surf.to_regular_surface()
reg.quickplot()
```

##### Aggregated data
```python
# All aggregated surfaces in case
surfs = case.aggregation.surfaces

# Get filter values
surfs.names
surfs.tagnames
surfs.iterations
surfs.operations

# Apply filters
surfs = surfs.filter(name="surface_name", tagname="surface_tagname", iteration=0, operation="mean")

# Get surface
surf = surfs[0]

```

##### Observed data
```python
# All observed surfaces in case
surfs = case.observation.surfaces

# Get filter values
surfs.names
surfs.tagnames

# Apply filters
surfs = surfs.filter(name="surface_name", tagname="surface_tagname")

# Get surfaces
surf = surfs[0]
```

