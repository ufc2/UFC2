import vnode
import ocdc
import operation
import utils
import config
import time

class versions(object):
    
    def __init__(self):
        self.rev = -1
        self.vector = []

    def get_rev_metadata(self, rev = None):
        if rev is None:
            rev = self.rev
        return self.vector[rev].get_metadata()

    def get_hashlist(self, rev = None):
        if rev is None:
            rev = self.rev
        return self.vector[rev].get_hashlist()

    def add_vnode(self, node, ignore = False):
        conflict = False
        if node.get_attribute('base_rev') < self.rev:
            conflict = True
        self.rev += 1
        node.set_attribute('rev', self.rev)
        self.vector.append(node)
        if not ignore and conflict:
            (hashs, datas) = self.resolve_conflict()
            return hashs, datas
        return None, None

    def resolve_conflict(self):
        base = self.find_same_base(self.rev, self.rev - 1)
        v0, boundaries = read(base)
        v1, _ = read(self.rev - 1)
        v2, _ = read(self.rev)
        user1 = self.vector[self.rev - 1].get_attribute('modifier')
        user2 = self.vector[self.rev].get_attribute('modifier')
        user1 = user1['firstname'] + user1['lastname']
        user2 = user2['firstname'] + user2['lastname']
        ops = operation.conflict_resolution(v0, v1, user1, v2, user2)
        v12 = operation.apply_sequence(v0, ops)
        chunker = ocdc.ocdc(ops, boundaries)
        chunker.mark()
        chunker.recalculate(v12)
        hashs = []
        datas = []
        for i in range(len(chunker.boundaries)):
            start = chunker.boundaries[i]
            if i == len(chunker.boundaries) - 1:
                end = len(v12)
            else:
                end = chunker.boundaries[i + 1]
            data = v12[start:end]
            datas.append(data)
            hashs.append(utils.sha256(data))
        node = vnode.vnode({}, hashs)
        node.set_attribute('size', len(v12))
        node.set_attribute('base_rev', base)
        node.set_attribute('modifier', config.server_user)
        node.set_attribute('modified_time', int(time.time() * 1000))
        self.add_vnode(node, True)
        return hashs, datas

    def find_same_base(self, v1, v2):
        if v1 == v2:
            return v1
        elif v1 < v2:
            return self.find_same_base(v1, self.vector[v2].get_attribute('base_rev'))
        else:
            return self.find_same_base(v2, self.vector[v1].get_attribute('base_rev'))

    def read(self, rev = None):
        if rev is None:
            rev = self.rev
        data, boundaries = self.vector[rev].read()
        return data, boundaries

    def to_dict(self):
        dict = {}
        dict['rev'] = self.rev
        dict['vector'] = []
        for _vnode in self.vector:
            dict['vector'].append(_vnode.__dict__)
        return dict

def from_dict(dict):
    _versions = versions()
    _versions.rev = dict.get('rev')
    vector = dict.get('vector')
    for v_dict in vector:
        _versions.vector.append(vnode.from_dict(v_dict))
    return _versions