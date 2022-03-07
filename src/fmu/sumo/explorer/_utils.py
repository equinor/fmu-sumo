class Utils:
    def map_buckets(self, buckets):
        mapped = {}

        for bucket in buckets:
            mapped[bucket["key"]] = bucket["doc_count"]

        return mapped