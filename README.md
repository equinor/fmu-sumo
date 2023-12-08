[![Documentation Status](https://readthedocs.org/projects/fmu-sumo/badge/?version=latest)](https://fmu-sumo.readthedocs.io/en/latest/?badge=latest)


# fmu-sumo
`fmu.sumo` is a Python package intended for interaction with FMU results stored in Sumo.<br />
`fmu.sumo.explorer` is a Python API for consuming FMU results from Sumo.

_Note! Access to Sumo is required. For Equinor users, apply through `AccessIT``._

## Installation
```
$ pip install fmu-sumo
```
... or for the latest development version:
```
$ git clone git@github.com:equinor/fmu-sumo.git
$ cd fmu-sumo
$ pip install .
```
:warning: OpenVDS does not publish builds for MacOS. You can still use the Explorer without OpenVDS, but some Cube methods will not work.

#### For development

```
$ git clone <your fork>
$ cd fmu-sumo
$ pip install .[dev]
```

#### Run tests
```
$ pytest tests/
```

## Documentation and guidelines
:link: [fmu-sumo documentation](https://fmu-sumo.readthedocs.io/en/latest/)

## Example usage
```python
from fmu.sumo.explorer import Explorer

# Connect
sumo = Explorer()

# Apply filters
cases = cases.filter(status=["keep", "offical"], user="peesv", field="DROGON")

for case in cases:
    print(f"{case.name} ({case.uuid})")

# Select case
case = cases[0]

# Alternatively, get specific case via case.uuid
case = sumo.get_case_by_uuid(<uuid>)
```

## Contribute
[Contribution guidelines](./CONTRIBUTING.md)

## License
Apache 2.0