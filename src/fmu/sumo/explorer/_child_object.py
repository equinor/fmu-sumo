"""Contains child object classes"""
# Uncomment if SurfaceObject id is matured
# from fmu.sumo.explorer._utils import get_surface


class ChildObject:

    """Object containing some easy metadata, and object"""

    def __init__(self, sumo_client, meta_data):
        """Init
        args:
        sumo_client (SumoClient): client to do queries with
        meta_data (dict): metadata for the object
        """

        self.sumo = sumo_client
        self.__blob = None
        self.__png = None

        source = meta_data["_source"]
        fields = meta_data["fields"]

        self.tag_name = fields["tag_name"][0]
        self.sumo_id = meta_data["_id"]
        self.name = source["data"]["name"]
        self.iteration_id = source["fmu"]["iteration"]["id"]
        self.relative_path = source["file"]["relative_path"]
        self.meta_data = source
        self.object_type = source["class"]

        if "realization" in source["fmu"]:
            self.realization_id = source["fmu"]["realization"]["id"]
        else:
            self.realization_id = None

        if "aggregation" in source["fmu"]:
            self.aggregation = source["fmu"]["aggregation"]["operation"]
        else:
            self.aggregation = None

    @property
    def blob(self):
        """Returns the __blob attribute"""
        if self.__blob is None:
            self.__blob = self.__get_blob()

        return self.__blob

    def __get_blob(self):
        """Gets the object from blob store"""
        blob = self.sumo.get(f"/objects('{self.sumo_id}')/blob")
        return blob

    @property
    def png(self):
        """returns the __png attribute"""
        if self.__png is None:
            self.__png = self.__get_png()

        return self.__png

    def __get_png(self):
        """Gets the object as png"""
        png = self.sumo.get(f"/objects('{self.sumo_id}')/blob", encoder="png")
        return png


# Commented out, immature. But idea should be persued further
# class SurfaceObjects:
#
#     """Container for a set of objects"""
#
#     def __init__(object_ids, case):
#         """Init of objects
#         args:
#         object_ids (dict): key is real, value is object id
#         case fmu.sumo.Ex
#         """
#         self._object_ids = object_ids
#         self._sumo = case
#
#     @property
#     def object_ids(self):
#         """Returns _object_ids attribute"""
#         return self._object_ids
#
#     @property
#     def sumo(self):
#         """Returns _sumo attribute"""
#         return self._sumo
#
#     def get_surface(self, **kwargs):
#         """Returns xtgeo surface
#         args:
#         kwargs (dict): dictionary of input
#
#         """
#         return get_surface(self.object_ids, self.sumo, **kwargs)
