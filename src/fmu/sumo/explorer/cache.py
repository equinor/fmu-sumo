from collections import deque
from threading import Lock

# Based on https://llego.dev/posts/implement-lru-cache-python/


class LRUCache:
    def __init__(self, capacity):
        self.cache = {}
        self.capacity = capacity
        self.access = deque()
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            if key not in self.cache:
                return None
            else:
                self.access.remove(key)
                self.access.append(key)
                return self.cache[key]

    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                self.access.remove(key)
            elif len(self.cache) == self.capacity:
                oldest = self.access.popleft()
                del self.cache[oldest]
        self.cache[key] = value
        self.access.append(key)

    def has(self, key):
        return key in self.cache

    def clear(self):
        with self.lock:
            self.cache.clear()
            self.access.clear()
