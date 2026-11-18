# fmu-sumo-explorer

[![Documentation Status](https://readthedocs.org/projects/fmu-sumo/badge/?version=latest)](https://fmu-sumo.readthedocs.io/en/latest/?badge=latest)


## Installation

```bash
python -m pip install fmu-sumo-explorer
```

The old PyPI name, `fmu-sumo`, is still available as a compatibility package.
The Python import path remains `fmu.sumo.explorer`.

If you already have an older `fmu-sumo` release that contains the library,
uninstall it before installing either name. Installing the new distribution
alongside the old one, or upgrading the old name in place, can leave files
missing because both implementations use the same import path.

```bash
python -m pip uninstall fmu-sumo
python -m pip install fmu-sumo-explorer
```

If another dependency still requires `fmu-sumo`, use
`python -m pip install fmu-sumo` in the second step instead.

## Documentation and guidelines
:link: [Sumo Explorer documentation](https://fmu-sumo.readthedocs.io/en/latest/)


## Contribute
[Contribution guidelines](./CONTRIBUTING.md)
