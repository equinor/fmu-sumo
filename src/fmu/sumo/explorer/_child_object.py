class ChildObject:
    def __init__(self, sumo_client, meta_data):
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
        if self.__blob is None:
            self.__blob = self.__get_blob()

        return self.__blob

    def __get_blob(self):
        blob = self.sumo.get(f"/objects('{self.sumo_id}')/blob")
        return blob

    @property
    def png(self):
        if self.__png is None:
            self.__png = self.__get_png()

        return self.__png

    def __get_png(self):
        png = self.sumo.get(f"/objects('{self.sumo_id}')/blob", encoder="png")
        return png