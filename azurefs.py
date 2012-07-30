#!/usr/bin/env python
"""
A FUSE wrapper for locally mounting Azure blob storage

Ahmet Alp Balkan <ahmetalpbalkan at gmail.com>
"""

import logging
from sys import argv, exit, maxint
from stat import S_IFDIR, S_IFREG
from errno import *
from os import getuid
import time
from datetime import datetime

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from winazurestorage import *

if __name__ == '__main__':
    log = logging.getLogger()
    ch = logging.StreamHandler()
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class AzureFS(LoggingMixIn, Operations):
    'Azure Blob Storage filesystem'

    blobs = None

    containers = {} # <cname, dict(stat:dict, files:None|dict<fname, stat>)

    fds = {} # <fd, (path, bytes, dirty)>
    fd = 0


    def __init__(self, account, key):
        self.blobs = BlobStorage(CLOUD_BLOB_HOST, account, key)
        self.rebuild_container_list()


    def rebuild_container_list(self):
        cmap = {}
        cnames = set() 
        for c in self.blobs.list_containers():
            cstat = dict(st_mode = (S_IFDIR | 0755), st_uid = getuid(),
                               st_mtime = int(time.mktime(c[2])), st_size = 0)
            cname = c[0]
            cmap['/'+cname] = dict(stat=cstat, files=None)
            cnames.add(cname)

        cmap['/'] = dict(files = {},
                         stat = dict(st_mode = (S_IFDIR), st_uid=getuid(),
                                     st_size = 0, st_mtime=int(time.time()))
                         )

        self.containers = cmap   # destroys fs tree cache but resistant to misses

    def _parse_path(self, path): # returns </dir, file(=None)>
        if path.count('/') > 1: #file
            return str(path[:path.rfind('/')]), str(path[path.rfind('/')+1:])
        else: #dir
            pos = path.rfind('/',1)
            if pos == -1:
                return path, None
            else:
                return str(path[:pos]), None

    def parse_container(self, path):
        base_container = path[1:] #/abc/def/g --> abc
        if base_container.find('/') > -1:
            base_container = base_container[:base_container.find('/')]
        return str(base_container)

    def _get_dir(self, path, contents_required=False):
        if not self.containers:
            self.rebuild_container_list()

        if path in self.containers and not (contents_required and self.containers[path]['files'] is None):
            return self.containers[path]

        cname = self.parse_container(path)

        if '/'+cname not in self.containers:
            raise FuseOSError(ENOENT)
        else:
            if self.containers['/'+cname]['files'] is None:    # retrieve contents
                log.info("------> CONTENTS NOT FOUND: %s" % cname)

                blobs = self.blobs.list_blobs(cname)

                dirstat = dict(st_mode= (S_IFDIR|0755), st_size=0, st_uid=getuid(),
                               st_mtime=time.time())  

                if self.containers['/'+cname]['files'] is None:
                    self.containers['/'+cname]['files'] = dict()

                for f in blobs:
                    blob_name = f[0]

                    node = dict(st_mode=(S_IFREG | 0644), st_size=f[3],
                                         st_mtime=int(time.mktime(f[2])), 
                                         st_uid=getuid())

                    if blob_name.find('/') == -1:       # file just under container
                        self.containers['/'+cname]['files'][blob_name] = node

            return self.containers['/'+cname]
        return None


    def _get_file(self, path):
        d,f = self._parse_path(path)
        
        dir = self._get_dir(d, True)
        if dir is not None and f in dir['files']:
            return dir['files'][f] 

    def getattr(self, path, fh=None):
        d,f = self._parse_path(path)

        if f is None:
            dir = self._get_dir(d)
            return dir['stat']
        else:
            file = self._get_file(path)

            if file:
                return file 

        raise FuseOSError(ENOENT)

    # FUSE
    def mkdir(self, path, mode):
        if path.count('/') <= 1: # create on root
            name = path[1:]

            if not 3 <= len(name) <= 63:
                log.error("Container names can be 3 through 63 chars long.")
                raise FuseOSError(ENAMETOOLONG)
            if name != name.lower():
                log.error("Container names cannot contain uppercase characters.")
                raise FuseOSError(EACCES)
            if name.count('--') > 0:
                log.error('Container names cannot contain consecutive dashes (-).')
                raise FuseOSError(EAGAIN)
            #TODO handle all "-"s must be preceded by letter or numbers
            #TODO starts with only letter or number, can contain letter, nr, dash

            resp = self.blobs.create_container(name)

            if 200 <= resp < 300:
                self.rebuild_container_list()
                log.info("CONTAINER %s CREATED" % name)
            elif resp == 409:
                log.error("REST ERROR HTTP %d" % resp)
                raise FuseOSError(EEXIST)
            else:
                raise FuseOSError(EACCES)
                log.error("REST ERROR HTTP %d" % resp)
        else:
            raise FuseOSError(ENOSYS) #TODO support 2nd+ level mkdirs

    def rmdir(self, path):
        if path.count('/') == 1:
            c_name = path[1:]
            resp = self.blobs.delete_container(c_name)

            if 200 <= resp < 300:
                if path in self.containers:
                    del self.containers[path]
            else:
                if resp == 404:
                    log.info("Container %s not found." % c_name)
                    raise FuseOSError(ENOENT)
                raise FuseOSError(EACCES)
        else:
            raise FuseOSError(ENOSYS)  #TODO support 2nd+ level mkdirs




    def create(self, path, mode):
        node  = dict(st_mode = (S_IFREG | mode), st_size=0, st_nlink=1,
                     st_uid = getuid(), st_mtime = time.time())
        d,f = self._parse_path(path)

        if not f: 
            log.error("Cannot create files on root level: /")
            raise FuseOSError(ENOSYS)

        dir = self._get_dir(d, True)
        if not dir:
            raise FuseOSError(EIO)
        dir['files'][f] = node

        return self.open(path, data='') # reusing handler provider

    
    def open(self, path, flags=0, data=None):
        if data == None:                # download contents
            c_name = self.parse_container(path)
            f_name = path[path.find('/',1)+1:]

            try:
                data = self.blobs.get_blob(c_name, f_name)
            except URLError, e:
                if e.code == 404: 
                    dir = self._get_dir('/'+c_name, True)
                    if f_name in dir['files']:
                        del dir['files'][f_name]
                    raise FuseOSError(ENOENT)
                elif e.code == 403: raise FUSEOSError(EPERM)
                else: 
                    log.error("Read blob failed HTTP %d" % e.code)
                    raise FuseOSError(EAGAIN)

        self.fd += 1
        self.fds[self.fd] = (path, data, False)


        return self.fd

    def flush(self, path, fh=None): 
        if not fh:
            raise FuseOSError(EIO)
        else:
            if fh not in self.fds:
                raise FuseOSError(EIO)
            path = self.fds[fh][0]
            data = self.fds[fh][1]
            dirty = self.fds[fh][2]
            
            if not dirty:
                return 0 # avoid redundant write
            
            if data is None: data = ''

            d,f = self._parse_path(path)
            c_name = self.parse_container(path)

            resp = self.blobs.put_blob(c_name, f, data)

            if 200 <= resp < 300:
                self.fds[fh] = (path, data, False) # mark as not dirty
                
                dir = self._get_dir(d, True)
                if not dir or f not in dir['files']:
                    raise FuseOSError(EIO)

                # update local data
                dir['files'][f]['st_size'] = len(data)
                dir['files'][f]['st_mtime'] = time.time()

                return 0
            else:
                log.error("Flush error HTTP %d" % resp)
                raise FuseOSError(EAGAIN)

    def release(self, path, fh=None):
        if fh is not None and fh in self.fds:
            del self.fds[fh]

    def truncate(self, path, length, fh=None):
        return 0 # assume done, no need

    def write(self, path, data, offset, fh=None):
        if not fh or fh not in self.fds:
            raise FuseOSError(ENOENT)
        else:
            d = self.fds[fh][1] 
            if d is None: d = ""
            self.fds[fh] = (self.fds[fh][0], d[:offset] + data, True)
            return len(data)

    def unlink(self, path):
        c_name = self.parse_container(path)
        d,f = self._parse_path(path)

        resp = self.blobs.delete_blob(c_name, f)
        log.info("UNLINK REST RESPONSE %d" % resp)
        if 200 <= resp < 300:
            dir = self._get_dir(path, True)
            if dir and f in dir['files']:
                del dir['files'][f]
            return 0
        elif resp == 404:
            raise FuseOSError(ENOENT)
        else:
            log.error("Error occurred %d" % resp)
            raise FuseOSError(EAGAIN)

    def readdir(self, path, fh):
        if path == '/':
            return ['.', '..'] + [x[1:] for x in self.containers.keys() if x != '/']

        dir = self._get_dir(path, True)
        if not dir:
            raise FuseOSError(ENOENT)
        return ['.', '..'] + dir['files'].keys()

    def read(self, path, size, offset, fh):
        if not fh or fh not in self.fds:
            raise FuseOSError(ENOENT)

        f_name = path[path.find('/',1)+1:]
        c_name = path[1:path.find('/',1)]

        try:
            data = self.blobs.get_blob(c_name, f_name)
            self.fds[fh] = (self.fds[fh][0], data, False)
            return data[offset:offset+size]
        except URLError, e:
            if e.code == 404: 
                raise FuseOSError(ENOENT)
            elif e.code == 403: raise FUSEOSError(EPERM)
            else: 
                log.error("Read blob failed HTTP %d" % e.code)
                raise FuseOSError(EAGAIN)
        data = self.fds[fh][1]
        if data is None: data = ""
        return data[offset:offset + size]

    def statfs(self, path):
        return dict(f_bsize=1024, f_blocks=1, f_bavail=maxint)

    def rename(self, old, new):
        """Three stage move operation because Azure do not have 
        move or rename call. """
        od,of = self._parse_path(old)

        if of is None: # move dir
            raise FuseOSError(ENOSYS)

        files = self._list_container_blobs(old)
        if of not in files:
            raise FuseOSError(ENOENT)

        src = files[of]

        if src['st_mode'] & S_IFREG <= 0: # move dir
            raise FuseOSError(ENOSYS)

        fh = self.open(old, 0)
        data = self.read(old, src['st_size'], 0, fh)

        self.flush(old, fh)

        fh = self.create(new, 0644)
        if new < 0:
            raise FuseOSError(EIO)
        self.write(new, data, 0, fh)
        res = self.flush(new, fh)

        if res == 0:
            self.unlink(old)

    def symlink(self, target, source):
        raise FuseOSError(ENOSYS)

    def getxattr(self, path, name, position=0):
        return ''

    def chmod(self, path, mode): pass
    def chown(self, path, uid, gid): pass


if __name__ == '__main__':
    if len(argv) < 4:
        print('Usage: %s <mount_directory> <account> <secret_key>' % argv[0])
        exit(1)
    fuse = FUSE(AzureFS(argv[2], argv[3]), argv[1], debug=True)

