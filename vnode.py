import s3operator
import global_variables

class vnode(object):
    
    def __init__(self, attributes, hashs):
        # attributes: 
        #   size                int
        #   base_rev            int
        #   rev                 int
        #   modifier            dict
        #   modified_time       int             (Unix ticks)
        self.attributes = attributes

        #   hashs               list of string  (sha256)
        self.hashs = hashs

    def set_attribute(self, name, attr):
        self.attributes[name] = attr

    def get_attribute(self, name):
        attr = self.attributes[name]
        return attr

    def get_metadata(self):
        return self.attributes['size'], self.attributes['modified_time'], self.attributes['modifier']

    def get_hashlist(self):
        return self.hashs

    def read(self):
        cached = global_variables._chunkcache.read(self.hashs)
        args = []
        for hash in self.hashs:
            if cached.get(hash) is None:
                args.append({'operation': 'r', 'key': hash})
        threads = []
        for arg in args:
            threads.append(s3operator.operator(arg))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        downloaded = {}
        for arg in args:
            downloaded[arg['key']] = arg['data']
        global_variables._chunkcache.write(downloaded)
        cached.update(downloaded)
        data = b''
        boundaries = []
        for hash in hashs:
            boundaries.append(len(data))
            data += cached[hash]
        return data, boundaries

def from_dict(dict):
    return vnode(dict['attributes'], 
                 dict['hashs'])

