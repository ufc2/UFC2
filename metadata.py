import pickle
import json
import os
import copy
import config
import versions
import global_variables
import s3operator

class metadata(object):

    def __init__(self, attributes, path, _versions = None):
        # attributes: 
        #   tag                 string      ('file', 'folder')
        #   name                string
        #   fullpath            string
        #   size                int
        #   rev                 int         (start from 0)
        #   creation_time       int         (Unix ticks)
        #   modified_time       int         (Unix ticks)
        #   modifier            dict        (include user_id, email, firstname, lastname)
        #   owner               dict        (include user_id, email, firstname, lastname)
        #   shared_users        list of dict
        #   is_shared           bool
        #   is_deleted          bool
        self.attributes = attributes
        if _versions is None:
            if self.attributes['tag'] == 'file':
                self.versions = versions.versions()
            else:
                self.versions = None
        else:
            self.versions = _versions
        self.path = path
        self.dirty = True

    def set_attribute(self, name, attr):
        self.attributes[name] = attr
        self.dirty = True

    def get_attribute(self, name):
        attr = self.attributes[name]
        return attr

    def get_metadata(self):
        return self.attributes

    def get_metadata_by_version(self, rev):
        attr = copy.deepcopy(self.attributes)
        attr['rev'] = rev
        (size, modified_time, modifier) = self.versions.get_rev_metadata(rev)
        attr['size'] = size
        attr['modified_time'] = modified_time
        attr['modifier'] = modifier
        return json.dumps(attr)

    def get_hashlist(self, rev = None):
        if self.attributes['tag'] == 'folder':
            return None
        return self.versions.get_hashlist(rev)

    def add_vnode(self, node):
        hashs, datas = self.versions.add_vnode(node)
        if hashs is not None:
            to_update = {}
            for i in range(len(hashs)):
                if not global_variables._hashcache.search(hashs[i]):
                    global_variables._hashcache.insert(hashs[i])
                    to_update[hashs[i]] = datas[i]
            global_variables._chunkcache.write(to_update)
            for hash in to_update.keys():
                s3operator.operator({'operation': 'w', 'key': hash, 'data': to_update[hash]}).start()
        self.set_attribute('rev', self.versions.rev)
        (size, modified_time, modifier) = self.versions.get_rev_metadata()
        self.set_attribute('size', size)
        self.set_attribute('modified_time', modified_time)
        self.set_attribute('modifier', modifier)
        self.dirty = True

    def read(self, rev = None):
        data, boundaries = self.versions.read(rev)
        return data, boundaries

    def to_dict(self):
        dict = {}
        dict['attributes'] = self.attributes
        if self.versions is not None:
            dict['versions'] = self.versions.to_dict()
        return dict

    def write_back(self):
        if self.dirty:
            fp = open(self.path, 'wb')
            pickle.dump(self.to_dict(), fp)
            fp.close()
            self.dirty = False

def load(path):
    if(os.path.exists(path)):
        fp = open(path, 'rb')
        dict = pickle.load(fp)
        fp.close()
        _versions = None if dict.get('versions') is None else versions.from_dict(dict['versions'])
        meta = metadata(dict['attributes'], path, _versions)
        meta.dirty = False
        return meta
    else:
        return None

def create(dict):
    tag = dict['attributes']['tag']
    if tag == 'file':
        open(dict['path'], 'wb').close()
    else:
        os.makedirs(os.path.dirname(dict['path']))
    return metadata(dict['attributes'], dict['path'])