Sumo Explorer
#############

The ``fmu.sumo.explorer`` is a python package for reading data from Sumo in the FMU context.


Api Reference 
-------------

- `API reference <apiref/fmu.sumo.explorer.html>`_

.. warning::
    OpenVDS does not publish builds for MacOS. You can still use the Explorer without OpenVDS,
    but some Cube methods will not work.

Usage and examples
------------------

Initializing an Explorer object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We establish a connection to Sumo by initializing an Explorer object.
This object will handle authentication and can be used to retrieve cases and case data.

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer()


Authentication
^^^^^^^^^^^^^^^
If you have not used the `Explorer` before and no access token is found in your system, a login form will open in your web browser.
It is also possible to provide the `Explorer` with an existing token to use for authentication, in this case you will not be prompted to login.

.. code-block:: 

    from fmu.sumo.explorer import Explorer 

    USER_TOKEN="123456789"
    sumo = Explorer(token=USER_TOKEN)

This assumes the `Explorer` is being used within a system which handles authentication and queries Sumo on a users behalf.

Finding a case
^^^^^^^^^^^^^^
The `Explorer` has a property called `cases` which represents all cases you have access to in Sumo:

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases 

The `cases` property is a `CaseCollection` object and acts as a list of cases.
We can use the `filter` method to apply filters to the case collection which will return a new filtered `CaseCollection` instance:

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases
    cases = cases.filter(user="peesv")

In this example we're getting all the cases belonging to user `peesv`.

The resulting `CaseCollection` is iterable:

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases
    cases = cases.filter(user="peesv")

    for case in cases:
        print(case.uuid)
        print(case.name)
        print(case.status)

We can use the filter method to filter on the following properties:

* uuid
* name
* status
* user
* asset
* field

Example: finding all official cases uploaded by `peesv` in Drogon: 

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases
    cases = cases.filter(
        user="peesv",
        status="official",
        asset="Drogon"
    )


The `CaseCollection` has properties which lets us find available filter values.

Example: finding assets 

.. code-block:: 

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases
    cases = cases.filter(
        user="peesv",
        status="official"
    )

    assets = cases.assets

The `CaseCollection.assets` property gives us a list of unique values for the asset property in our list of cases. 
We can now use this information to apply an asset filter:

.. code-block:: 

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    cases = sumo.cases
    cases = cases.filter(
        user="peesv",
        status="official"
    )

    assets = cases.assets

    cases = cases.filter(
        asset=assets[0]
    )

We can retrieve list of unique values for the following properties:

* names 
* statuses
* users 
* assets 
* fields

You can also use a case `uuid` to get a `Case` object:

.. code-block:: 

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    my_case = sumo.get_case_by_uuid("1234567")


Browsing data in a case
^^^^^^^^^^^^^^^^^^^^^^^
The `Case` object has properties for accessing different data types:

* surfaces
* polygons
* tables 

Example: get case surfaces 

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    surfaces = case.surfaces

The `SurfaceCollection` object has a filter method and properties for getting filter values, similar to `CaseCollection`:

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    surfaces = case.surfaces.filter(iteration="iter-0")

    names = surfaces.names 

    surfaces = surfaces.filter(
        name=names[0]
    )

    tagnames = surfaces.tagnames 

    surfaces = surfaces.filter(
        tagname=tagnames[0]
    )

    stratigraphic = surfaces.filter(stratigraphic = "false")
    vertical_domain = surfaces.filter(vertical_domain = "depth")


The `SurfaceCollection.filter` method takes the following parameters:

* uuid
* name 
* tagname 
* iteration 
* realization 
* aggregation
* stage 
* time
* stratigraphic
* vertical_domain

All parameters support a single value, a list of values or a `boolean` value.

Example: get aggregated surfaces 

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    # get mean aggregated surfaces
    surfaces = case.surfaces.filter(aggregation="mean")

    # get min, max and mean aggregated surfaces 
    surfaces = case.surfaces.filter(aggregation=["min", "max", "mean"])

    # get all aggregated surfaces
    surfaces = case.surfaces.filter(aggregation=True)

    # get names of aggregated surfaces 
    names = surfaces.names

We can get list of filter values for the following properties:

* names
* tagnames 
* iterations 
* realizations
* aggregations 
* stages 
* timestamps
* intervals
* stratigraphic
* vertical_domain


Once we have a `Surface` object we can get surface metadata using properties:

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    surface = case.surfaces[0]

    print(surfaces.uuid)
    print(surfaces.name)
    print(surfaces.tagname)
    print(surface.stratigraphic)
    print(surface.vertical_domain)

We can get the surface binary data as a `BytesIO` object using the `blob` property. 
The `to_regular_surface` method returns the surface as a `xtgeo.RegularSurface` object.

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    surface = case.surfaces[0]

    # get blob
    blob = surface.blob 

    # get xtgeo.RegularSurface
    reg_surf = surface.to_regular_surface() 

    reg_surf.quickplot()


If we know the `uuid` of the surface we want to work with we can get it directly from the `Explorer` object: 

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    surface = sumo.get_surface_by_uuid("1234567")

    print(surface.name)


Time filtering
^^^^^^^^^^^^^^
The `TimeFilter` class lets us construct time filters to be used in the `SurfaceCollection.filter` method:

Example: get surfaces with timestamp in a specific range

.. code-block::

    from fmu.sumo.explorer import Explorer, TimeFilter, TimeType

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    time = TimeFilter(
        type=TimeType.TIMESTAMP,
        start="2018-01-01",
        end="2022-01-01"
    )

    surfaces = case.surfaces.filter(time=time)


Example: get surfaces with exact interval 

.. code-block::

    from fmu.sumo.explorer import Explorer, TimeFilter, TimeType

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    time = TimeFilter(
        type=TimeType.INTERVAL,
        start="2018-01-01",
        end="2022-01-01",
        exact=True
    )

    surfaces = case.surfaces.filter(time=time)


Time filters can also be used to get all surfaces that has a specific type of time data.

.. code-block::

    from fmu.sumo.explorer import Explorer, TimeFilter, TimeType

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    # get surfaces with timestamps
    time = TimeFilter(type=TimeType.TIMESTAMP)

    surfaces = case.surfaces.filter(time=time)

    # get surfaces with intervals
    time = TimeFilter(type=TimeType.INTERVAL)

    surfaces = case.surfaces.filter(time=time)

    # get surfaces with any time data
    time = TimeFilter(type=TimeType.ALL)

    surfaces = case.surfaces.filter(time=time)

    # get surfaces without time data
    time = TimeFilter(type=TimeType.NONE)

    surfaces = case.surfaces.filter(time=time)



Performing aggregations
^^^^^^^^^^^^^^^^^^^^^^^
The `SurfaceCollection` class can be used to do on-demand surface aggregations.

.. code-block::

    from fmu.sumo.explorer import Explorer 

    sumo = Explorer() 

    case = sumo.get_case_by_uuid("1234567")

    surfaces = case.surfaces.filter(
        stage="realization",
        iteration="iter-0",
        name="Valysar Fm.",
        tagname="FACIES_Fraction_Channel"
        stratigraphic="false"
        vertical_domain="depth"
    )

    mean = surfaces.mean()
    min = surfaces.min()
    max = surfaces.max() 
    p10 = surfaces.p10()

    p10.quickplot()

In this example we perform aggregations on all realized instance of the surface `Valysar Fm. (FACIES_Fraction_Channel)` in iteration 0.
The aggregation methods return `xtgeo.RegularSurface` objects.