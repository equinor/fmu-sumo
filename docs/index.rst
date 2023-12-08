fmu-sumo
########

Sumo is a solution for indexing, storing and supplying results produced by FMU workflows
and associated processes (post-processing). See the `Sumo front-end for FMU <https://fmu-sumo.app.radix.equinor.com>`_
for additional information and documentation on Sumo.

``fmu-sumo`` is a Python library for interaction with FMU results stored in Sumo. It contains
multiple modules:

* **Explorer** (This module) for *reading* data from Sumo in the FMU context.
* `Uploader <https://github.com/equinor/fmu-sumo-uploader>`_ for *writing* data to Sumo during FMU runs.
* `Sim2sumo <https://github.com/equinor/fmu-sumo-sim2sumo>`_ for making reservoir simulator (Eclipse, OPM) results available through Sumo.

.. toctree::
    :maxdepth: 2
    :hidden:

    self
    explorer