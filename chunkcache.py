import threading

class chunknode(object):

    def __init__(self, data):
        self.data = data
        self.prev = None
        self.next = None

max_chunks = 10240

class chunkcache(object):

    def __init__(self):
        self.table = {}
        self.count = 0
        self.head = chunknode(None)
        self.tail = head
        self.lock = threading.Lock()

    def read(self, keys):
        self.lock.acquire()
        chunks = {}
        for key in keys:
            node = self.table.get(key)
            if node is not None:
                chunks[key] = node.data
                if not node is self.head:
                    if node is self.tail:
                        node.prev.next = None
                        self.tail = node.prev
                        node.next = self.head
                        node.prev = None
                        self.head.prev = node
                        self.head = node
                    else:
                        node.prev.next = node.next
                        node.next.prev = node.prev
                        node.next = self.head
                        self.head.prev = node
                        self.head = node
        self.lock.release()
        return chunks

    def write(self, chunks):
        self.lock.acquire()
        for key in chunks.keys():
            if self.table.get(key) is None:
                node = chunknode(chunks[key])
                self.table[key] = node
                node.next = self.head
                self.head.prev = node
                self.head = node
                if self.count < max_chunks:
                    self.count += 1
                else:
                    tmp = self.tail.prev
                    tmp.next = None
                    del self.tail
                    self.tail = tmp
        self.lock.release()



