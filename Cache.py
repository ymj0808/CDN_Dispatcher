from CircularDoubeLinkedList import *


class LRUCache():
    def __init__(self, capacity, ip, hash_key, index):
        self.capacity = capacity   # capacity of cache
        self.size = capacity       # available size of cache
        self.ip = ip
        self.hash_key = hash_key
        self.index = index
        self.hit_bytes = 0
        self.hit_counts = 0
        self.req_bytes = 0
        self.req_counts = 0
        self.map = {}
        self.list = CircularDoubeLinkedList()

    def evict(self):
        node = self.list.head_node()
        self.list.remove(node)
        del self.map[node.object.id]

        self.size += node.object.size
        return node

    def request(self, id, size):
        self.req_counts += 1
        self.req_bytes += size

        if size > self.capacity:
            return False

        if id in self.map.keys():
            node = self.map[id]
            self.list.remove(node)
            self.list.append(node)
            self.hit_counts += 1
            self.hit_bytes += size
            return True
        else:
            while size > self.size:
                self.evict()
            object = CacheObject(id, size)
            node = Node(object)
            self.map[id] = node
            self.list.append(node)
            self.size -= size
            return False

    def print(self):
        hit_rate = 0
        byte_hit_rate = 0
        try:
            hit_rate = self.hit_counts / self.req_counts
            byte_hit_rate = self.hit_bytes / self.req_bytes
        finally:
            print("cache", self.index, "hit rate: %.4f" % hit_rate, "byte hit rate: %.4f" % byte_hit_rate)





