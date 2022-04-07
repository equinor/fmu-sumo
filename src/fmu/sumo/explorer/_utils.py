class Utils:
    def map_buckets(self, buckets):
        mapped = {}
        buckets_sorted = sorted(buckets, key=lambda b: b['key'])

        for bucket in buckets_sorted:
            mapped[bucket["key"]] = bucket["doc_count"]

        return mapped