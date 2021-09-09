from Cache import LRUCache
HASH_RING = 65535

class Vnode(object):
    def __init__(self, cache_index, hash_key):
        self.cache_index = cache_index
        self.hash_key = hash_key

    cache_index = -1
    hash_key = 0


class Cluster():

    ##########  statistic for RL agent?  ############
    vnode_hit_counts = []
    vnode_hit_bytes = []
    vnode_req_counts = []
    vnode_req_bytes = []

    cache_hit_counts = []
    cache_hit_bytes = []
    cache_req_counts = []
    cache_req_bytes = []
    ##################################################

    def __init__(self, cache_num, vnode_num, capacity):
        self.caches = []
        self.vnodes = []

        self.hit_bytes = 0
        self.hit_counts = 0
        self.req_bytes = 0
        self.req_counts = 0

        base_ip = "192.168.1."
        for i in range(cache_num):
            tmp_ip = base_ip + str(i+1)
            tmp_hash_key = abs(hash(tmp_ip)) % HASH_RING
            tmp_cache = LRUCache(capacity, tmp_ip, tmp_hash_key, i)
            self.caches.append(tmp_cache)

            for j in range(vnode_num):
                tmp_v_ip = tmp_ip + ":" + str(j+1)
                tmp_v_hash_key = abs(hash(tmp_v_ip)) % HASH_RING
                tmp_vnode = Vnode(i, tmp_v_hash_key)
                self.vnodes.append(tmp_vnode)

        self.vnodes.sort(key=lambda vnode: vnode.hash_key)

        self.vnode_to_cache = []
        for v in self.vnodes:
            self.vnode_to_cache.append(v.cache_index)



        for i in range(cache_num):
            self.cache_hit_counts.append(0)
            self.cache_hit_bytes.append(0)
            self.cache_req_counts.append(0)
            self.cache_req_bytes.append(0)

        for i in range(cache_num*vnode_num):
            self.vnode_hit_counts.append(0)
            self.vnode_hit_bytes.append(0)
            self.vnode_req_counts.append(0)
            self.vnode_req_bytes.append(0)

    def request(self, id, size):
        id = str(id)

        self.req_bytes += size
        self.req_counts += 1

        vnode_index = self.find_vnode_index(id)
        cache_index = self.vnode_to_cache[vnode_index]

        self.cache_req_counts[cache_index] += 1
        self.cache_req_bytes[cache_index] += size
        self.vnode_req_counts[cache_index] += 1
        self.vnode_req_bytes[cache_index] += size

        if self.caches[cache_index].request(id, size):
            self.hit_bytes += size
            self.hit_counts += 1

            self.cache_hit_counts[cache_index] += 1
            self.cache_hit_bytes[cache_index] += size
            self.vnode_hit_counts[cache_index] += 1
            self.vnode_hit_bytes[cache_index] += size

    def find_vnode_index(self, id):
        hash_key = abs(hash(id)) % HASH_RING
        low = 0
        high = len(self.vnodes) - 1
        if hash_key > self.vnodes[high].hash_key:
            return 0

        while low < high:
            mid = (low + high) // 2
            if hash_key == self.vnodes[mid].hash_key:
                return mid
            if hash_key > self.vnodes[mid].hash_key:
                low = mid + 1
            else:
                high = mid - 1

        if hash_key < self.vnodes[low].hash_key:
            return low
        elif low == len(self.vnodes) - 1:
            return 0
        else:
            return low+1

    def print(self):
        hit_rate = 0
        byte_hit_rate = 0
        try:
            hit_rate = self.hit_counts / self.req_counts
            byte_hit_rate = self.hit_bytes / self.req_bytes
        finally:
            print("Cluster   hit rate %.4f" % hit_rate, "   byte hit rate: %.4f" % byte_hit_rate)


def main():
    cluster = Cluster(1, 40, 1000000000)  #cache vnode capacity
    trace = open("trace")
    for line in trace.readlines():
        seq = line.split(' ')
        id = seq[0]
        size = int(seq[1][:-1])
        cluster.request(id, size)
    cluster.print()
    for c in cluster.caches:
        c.print()


if __name__ == '__main__':
    main()

