import sys
import json
import os
import math
import uuid
import utils
import vnode
import cdc
import config
import GRPCServer_pb2
import GRPCServer_pb2_grpc
import time
import global_variables
import s3operator


class GRPCServer_impl(GRPCServer_pb2_grpc.GRPCServerServicer):

    def Register(self, request, context):
        user = global_variables._user_session.register(request.Email, request.Password, request.FirstName, request.LastName)
        if user is None:
            return GRPCServer_pb2.RegisterResult(Succeed = False, Error = 1)
        else:
            namespace = utils.create_user_namespace(user['user_id'])
            return GRPCServer_pb2.RegisterResult(Succeed = True, Error = 0)

    def Login(self, request, context):
        login_result = global_variables._user_session.login(request.Email, request.Password)
        if login_result['code'] == 0:
            return GRPCServer_pb2.LoginResult(
                Succeed = True, 
                SessionId = login_result['session_id'],
                FirstName = login_result['firstname'],
                LastName =  login_result['lastname'],
                Error = 0)
        else:
            return GRPCServer_pb2.LoginResult(
                Succeed = False, 
                SessionId = None,
                FirstName = None,
                LastName = None,
                Error = login_result['code'])

    def Logout(self, request, context):
        session_id = request.SessionId
        print ('user ' + session_id + ' logout')
        global_variables._user_session.logout(session_id)
        return GRPCServer_pb2.StringResponse(PayLoad = '')

    def Share(self, request, context):
        jstr = json.dumps({ 'error': 'share not developed'})
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def CreateFolder(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path)
        modified_time = request.ModifiedTime
        paths_to_create = []
        while not os.path.exists(truepath):
            paths_to_create.append(truepath)
            truepath = os.path.dirname(truepath)
        paths_to_create.reverse()
        for path in paths_to_create:
            os.mkdir(path)
            mnode = global_variables._metadata_cache.get_usable_node(path + config.path_join + '.metadata')
            mnode.acquire_lock()
            attrs = {}
            attrs['tag'] = 'folder'
            attrs['name'] = os.path.basename(path)
            fullpath = path[len(config.efs_file_root) + 1:]
            fullpath = fullpath[fullpath.find(config.path_join):]
            attrs['fullpath'] = fullpath
            attrs['creation_time'] = modified_time
            attrs['owner'] = user
            attrs['shared_users'] = [user]
            attrs['is_shared'] = False
            attrs['is_deleted'] = False
            arguments = {}
            arguments['attributes'] = attrs
            arguments['path'] = path + config.path_join + '.metadata'
            mnode.create(arguments)
            mnode.release_lock()
        truepath = truepath + config.path_join + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        jstr = json.dumps(mnode.obj.get_metadata())
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def Rename(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        path = request.OldPath
        newpath = request.Path
        user_root = config.efs_file_root + config.path_join + 'user_' + str(user['user_id'])
        pos = path.find(config.path_join, 1)
        if pos != -1:
            linkpath = user_root + path[:path.find(config.path_join, 1)] + '.li'
            if os.path.exists(linkpath):
                fp = open(path, 'r')
                per = fp.readline()
                fp.close()
                if pos != -1:
                    truepath = per + path[path.find(config.path_join, 1):]
                else:
                    truepath = per
            else:
                truepath = user_root + path
        else:
            linkpath = user_root + path + '.li'
            if os.path.exists(linkpath):
                os.rename(linkpath, user_root + newpath + '.li')
                truepath = None
            else:
                truepath = user_root + path
        if truepath is not None:
            if os.isdir(truepath):
                truepath = truepath + config.path_join + '.metadata'
            else:
                truepath = truepath + '.metadata'
            mnode = global_variables._metadata_cache.get_usable_node(truepath)
            mnode.acquire_lock()
            if mnode.empty or not mnode.obj.path == truepath:
                mnode.load(truepath)
            mnode.obj.set_attribute('name', os.path.basename(newpath))
            mnode.obj.set_attribute('fullpath', os.path.basename(newpath))
            mnode.release_lock()
            if os.isdir(truepath):
                truepath = truepath[:-10]
                os.rename(truepath, os.path.dirname(truepath) + config.path_join + os.path.basename(newpath))
            else:
                os.rename(truepath, os.path.dirname(truepath) + config.path_join + os.path.basename(newpath) + '.metadata')
        truepath = utils.get_true_path(user, request.Path) 
        if os.isdir(truepath):
            truepath = truepath + config.path_join + '.metadata'
        else:
            truepath = truepath + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        jstr = json.dumps(mnode.obj.get_metadata())
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def Delete(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path) 
        if os.isdir(truepath):
            truepath = truepath + config.path_join + '.metadata'
        else:
            truepath = truepath + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        mnode.obj.set_attribute('is_deleted', True)
        jstr = json.dumps(mnode.obj.get_metadata())
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def Upload(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path) + '.metadata'
        is_new = not global_variables._metadata_cache.is_in_cache(truepath) and not os.path.exists(truepath)
        hashs = ''
        if not is_new:
            mnode = global_variables._metadata_cache.get_usable_node(truepath)
            mnode.acquire_lock()
            if mnode.empty or not mnode.obj.path == truepath:
                mnode.load(truepath)
            base_rev = request.BaseRev
            if base_rev == '':
                base_rev = mnode.obj.get_attribute('rev')
            base_rev = int(base_rev)
            base_hashs = mnode.obj.get_hashlist(base_rev)
            mnode.release_lock()
        else:
            base_hashs = []
        for i in range(0, len(request.Hashs) // 64):
            _hash = request.Hashs[i*64:i*64+64]
            if not _hash in base_hashs and not global_variables._hash_cache.search(_hash):
                hashs = hashs + _hash
        return GRPCServer_pb2.StringResponse(PayLoad = hashs)

    def UploadBlock(self, request_iterator, context):
        indexs = []
        hashs = []
        offsets = []
        datas = []
        _offset = 0
        for request in request_iterator:
            session_id = request.SessionId
            path = request.Path
            modified_time = request.ModifiedTime
            base_rev = request.BaseRev
            indexs.append(request.Index)
            hashs.append(request.Hash)
            datas.append(request.Content)
            offsets.append(_offset)
            _offset += request.Size
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, path) + '.metadata'
        is_new = not global_variables._metadata_cache.is_in_cache(truepath) and not os.path.exists(truepath)
        to_update = {}
        for i in range(len(hashs)):
            if not global_variables._hashcache.search(hashs[i]):
                global_variables._hashcache.insert(hashs[i])
                to_update[hashs[i]] = datas[i]
        global_variables._chunkcache.write(to_update)
        for hash in to_update.keys():
            s3operator.operator({'operation': 'w', 'key': hash, 'data': to_update[hash]}).start()
        if is_new:
            mnode = global_variables._metadata_cache.get_usable_node(truepath)
            mnode.acquire_lock()
            attrs = {}
            attrs['tag'] = 'file'
            attrs['name'] = os.path.basename(path)
            attrs['fullpath'] = path
            attrs['creation_time'] = modified_time
            attrs['owner'] = user
            attrs['shared_users'] = [user]
            attrs['is_shared'] = False
            attrs['is_deleted'] = False
            arguments = {}
            arguments['attributes'] = attrs
            arguments['path'] = truepath
            mnode.create(arguments)
            _nodeattrs = {}
            _nodeattrs['size'] = _offset
            _nodeattrs['base_rev'] = -1
            _nodeattrs['modifier'] = user
            _nodeattrs['modified_time'] = modified_time
            _node = vnode.vnode(_nodeattrs, hashs)
            mnode.obj.add_vnode(_node)
            retstr = json.dumps(mnode.obj.get_metadata())
            mnode.release_lock()
        else:
            mnode = global_variables._metadata_cache.get_usable_node(truepath)
            mnode.acquire_lock()
            if mnode.empty or not mnode.obj.path == truepath:
                mnode.load(truepath)
            if mnode.obj.get_attribute('is_deleted'):
                mnode.obj.set_attribute('is_deleted', False)
            if base_rev == '':
                base_rev = mnode.obj.get_attribute('rev')
            base_rev = int(base_rev)
            _node = vnode.vnode({}, hashs)
            _node.set_attribute('size', _offset)
            _node.set_attribute('base_rev', base_rev)
            _node.set_attribute('modifier', user)
            _node.set_attribute('modified_time', modified_time)
            mnode.obj.add_vnode(_node)
            retstr = json.dumps(mnode.obj.get_metadata())
            mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = retstr)

    def Download(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path) + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        hashs = mnode.obj.get_hashlist()
        rev = mnode.obj.get_attribute('rev')
        jstr = json.dumps({ 'rev': rev, 'hashs': hashs })
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def DownloadBlock(self, request_iterator, context):
        hashs = []
        for request in request_iterator:
            session_id = request.SessionId
            path = request.Path
            base_rev = int(request.BaseRev)
            hashs.append(request.Hash)
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, path) + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        data, boundaries = mnode.obj.read(base_rev)
        boundaries.append(len(data))
        hashs_base = mnode.obj.get_hashlist(base_rev)
        blocks = []
        for hash in hashs:
            i = hashs_base.index(hash)
            blockdata = data[boundaries[i]:boundaries[i+1]]
            blocks.append({ 'hash': hash, 'data': utils.bytes2str(blockdata) })
        jstr = json.dumps({ 'blocks': blocks })
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def GetMetadata(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path) 
        if os.isdir(truepath):
            truepath = truepath + config.path_join + '.metadata'
        else:
            truepath = truepath + '.metadata'
        mnode = global_variables._metadata_cache.get_usable_node(truepath)
        mnode.acquire_lock()
        if mnode.empty or not mnode.obj.path == truepath:
            mnode.load(truepath)
        jstr = json.dumps(mnode.obj.get_metadata())
        mnode.release_lock()
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)

    def ListFolder(self, request, context):
        session_id = request.SessionId
        user = global_variables._user_session.get_user_by_session(session_id)
        truepath = utils.get_true_path(user, request.Path)
        metadatas = []
        for child in os.listdir(truepath):
            if child == '.metadata':
                continue
            elif child.endswith('.metadata'):
                meta_path = config.path_join.join([truepath, child])
            else:
                meta_path = config.path_join.join([truepath, child, '.metadata'])
            mnode = global_variables._metadata_cache.get_usable_node(meta_path)
            mnode.acquire_lock()
            if mnode.empty or not mnode.obj.path == meta_path:
                mnode.load(meta_path)
            metadatas.append(mnode.obj.get_metadata())
            mnode.release_lock()
        jstr = json.dumps({ 'entries': metadatas })
        return GRPCServer_pb2.StringResponse(PayLoad = jstr)
