"""Top-level package for fmu.sumo.explorer"""

try:
    from ._version import version

    __version__ = version
except ImportError:
    __version__ = "0.0.0"


from fmu.sumo.explorer.explorer import Explorer
from fmu.sumo.explorer.timefilter import TimeType, TimeFilter
from fmu.sumo.explorer.objects.table_aggregated import AggregatedTable
