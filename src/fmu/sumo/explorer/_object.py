class Object:
    def __init__(self, sumo_client, meta_data):
        self.sumo = sumo_client
        self.__blob = None

        source = meta_data["_source"]

        self.sumo_id = meta_data["_id"]
        self.name = source["data"]["name"]
        self.iteration_id = source["fmu"]["iteration"]["id"]
        self.relative_path = source["file"]["relative_path"]
        self.meta_data = source

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
        if self.__blob is None:
            self.__blob = self.__get_blob()

        return self.__blob

    def __get_blob(self):
        blob = self.sumo.get(f"/objects('{self.sumo_id}')/blob")
        return blob


class Surface(Object):
    def __init__(self, sumo_client, meta_data):
        Object.__init__(self, sumo_client, meta_data)
        
        fields = meta_data["fields"]
        self.tag_name = fields["surface_content"][0]