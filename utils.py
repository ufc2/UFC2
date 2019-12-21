import hashlib
import config
import os
import threading

def sha256(data):
    sha = hashlib.sha256()
    sha.update(data)
    return sha.hexdigest()

def get_hashs(data, boundaries):
    hashs = []
    for i in range(len(boundaries) - 1):
        hashs.append(sha256(data[boundaries[i]:boundaries[i+1]]))
    hashs.append(sha256(data[boundaries[i]:]))
    return hashs

def bytes2str(b):
    return ''.join(list(map(chr, b)))

def str2bytes(s):
    return bytes(list(map(ord, s)))

class id_generator(object):

    def __init__(self, first):
        self.global_id = first
        self.lock = threading.Lock()

    def next(self):
        self.lock.acquire()
        self.global_id += 1
        id = self.global_id
        self.lock.release()
        return id

# user id generator
user_id_generator = id_generator(0)
def get_new_user_id():
   return user_id_generator.next()

# util methods
def create_user_namespace(user_id):
    namespace = config.efs_file_root + config.path_join + 'user_' + str(user_id)
    if not os.path.exists(namespace):
        os.mkdir(namespace)
    return namespace

def get_true_path(user, path):
    user_root = config.efs_file_root + config.path_join + 'user_' + str(user['user_id'])
    pos = path.find(config.path_join, 1)
    if pos != -1:
        linkpath = user_root + path[:path.find(config.path_join, 1)] + '.li'
    else:
        linkpath = user_root + path + '.li'
    if os.path.exists(linkpath):
        fp = open(path, 'r')
        per = fp.readline()
        fp.close()
        if pos != -1:
            ret = per + path[path.find(config.path_join, 1):]
        else:
            ret = per
    else:
        ret = user_root + path
    return ret
