fmu-sumo
########

Sumo is developed for handling storage of data produced in and by FMU workflows
with the intention of usage outside the realization context. Scope for Sumo in this context
are the data that are currently left behind on /scratch after the ERT run is finished.

Sumo provides indexing, storage, API and data management capabilities for FMU results. Further,
Sumo provides a data-backend for services for e.g post-processing.

For documentation of Sumo in the FMU context, see the `Sumo front-end for FMU <https://fmu-sumo.app.radix.equinor.com>`_

``fmu-sumo`` is a Python library for interaction with Sumo in the FMU context. It contains
multiple modules:

* **Explorer** (This module) for *reading* data from Sumo in the FMU context.
* `Uploader <https://github.com/equinor/fmu-sumo-uploader>`_ for *writing* data to Sumo during FMU runs.
* `Sim2sumo <https://github.com/equinor/fmu-sumo-sim2sumo>`_ for making reservoir simulator (Eclipse, OPM) results available through Sumo.

.. toctree::
    :maxdepth: 2
    :hidden:

    self
    explorer