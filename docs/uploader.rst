Sumo Uploader
############

The ``fmu.sumo.uploader`` is a python plugin for FMU for uploading data to Sumo.


API reference
-------------

- `API reference <apiref/fmu.sumo.uploader.html>`_

Usage and examples
----------

Assumptions:

- An FMU case is being ran.
- fmu-dataio is being used to export data with rich metadata.

.. note::

  **fmu-dataio** is the FMU plugin for exporting data out of FMU workflows with rich metadata.
  For more information and instructions/documentation, please see the
  `fmu-dataio documentation <https://fmu-dataio.readthedocs.io/en/latest/>`_

- User has necessary accesses

.. note::

  Within Equinor, apply for Sumo on your respective asset in AccessIT. With Komodo activated,
  run ``sumo_login`` from the Equinor unix command line to confirm/establish the local access.

- Case metadata is being generated for this case, and the case is registered on Sumo.

.. note::

  Use the ``--sumo`` argument flag to the `create_case_metadata` job in an ERT hook workflow,
  see `fmu-dataio documentation <https://fmu-dataio.readthedocs.io/en/latest/>`_ for more
  details.


As a FORWARD_MODEL in ERT
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    FORWARD_MODEL SUMO_UPLOAD(<SEARCHPATH>="share/results/maps/*.gri")


Example above uploads all surfaces dumped to ``share/results/maps``. You can add as many
instances of ``<SUMO_UPLOAD>`` FORWARD_JOB in your ERT workflow as you want. It is,
however, recommended that you rather use broad search strings and few instances.

Using ``<SUMO_UPLOAD>`` in the *realization context* is the primary usage. All data exported
out of FMU workflows with the intention of being used outside the realization context should
be exported. E.g. if exported data is to be used for other purposes than feeding other
FORWARD_JOBS in the same workflow.

As a workflow job in ERT:
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    -- call            -- case root directory    -- absolute path to files
    WF_SUMO_UPLOAD     <SCRATCH>/<USER>/<CASE>   "/abs/path/mycase/share/results/maps/*.gri"


Example above show how ``SUMO_UPLOAD`` can be used as an ERT workflow job, e.g. if doing
local post-processing. This feature is mainly added for legacy reasons. Generally, it
would be recommended to upload from realizations, and post-process using Sumo as data
source.