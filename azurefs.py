#!/usr/bin/env python
"""
A FUSE wrapper for locally mounting Azure blob storage

Ahmet Alp Balkan <ahmetalpbalkan at gmail.com>
"""

import logging
import time

from sys import argv, exit, maxint
from stat import S_IFDIR, S_IFREG
from errno import *
from os import getuid
from datetime import datetime
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from azure.storage import BlobService


TIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'

if not hasattr(__builtins__, 'bytes'):
    bytes = str

if __name__ == '__main__':
    log = logging.getLogger()
    ch = logging.StreamHandler()
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)

class AzureFS(LoggingMixIn, Operations):
    """Azure Blob Storage filesystem"""

    blobs = None
    containers = dict()  # <cname, dict(stat:dict,
                                    #files:None|dict<fname, stat>)
    fds = dict()  # <fd, (path, bytes, dirty)>
    fd = 0


    def __init__(self, account, key):
        self.blobs = BlobService(account, key)
        self.rebuild_container_list()

    def convert_to_epoch(self, date):
        """Converts Tue, 31 Jul 2012 07:17:34 GMT format to epoch"""
        return int(time.mktime(time.strptime(date, TIME_FORMAT)))

    def rebuild_container_list(self):
        cmap = dict()
        cnames = set()
        for c in self.blobs.list_containers():
            date = c.properties.last_modified
            cstat = dict(st_mode=(S_IFDIR | 0755), st_uid=getuid(), st_size=0,
                         st_mtime=self.convert_to_epoch(date))
            cname = c.name
            cmap['/' + cname] = dict(stat=cstat, files=None)
            cnames.add(cname)

        cmap['/'] = dict(files={},
                         stat=dict(st_mode=(S_IFDIR | 0755),
                                     st_uid=getuid(), st_size=0,
                                     st_mtime=int(time.time())))

        self.containers = cmap   # destroys fs tree cache resistant to misses

    def _parse_path(self, path):    # returns </dir, file(=None)>
        if path.count('/') > 1:     # file
            return str(path[:path.rfind('/')]), str(path[path.rfind('/') + 1:])
        else:                       # dir
            pos = path.rfind('/', 1)
            if pos == -1:
                return path, None
            else:
                return str(path[:pos]), None

    def parse_container(self, path):
        base_container = path[1:]   # /abc/def/g --> abc
        if base_container.find('/') > -1:
            base_container = base_container[:base_container.find('/')]
        return str(base_container)

    def _get_dir(self, path, contents_required=False):
        if not self.containers:
            self.rebuild_container_list()

        if path in self.containers and not (contents_required and \
                self.containers[path]['files'] is None):
            return self.containers[path]

        cname = self.parse_container(path)

        if '/' + cname not in self.containers:
            raise FuseOSError(ENOENT)
        else:
            if self.containers['/' + cname]['files'] is None:
                # fetch contents of container
                log.info("------> CONTENTS NOT FOUND: %s" % cname)

                blobs = self.blobs.list_blobs(cname)

                dirstat = dict(st_mode=(S_IFDIR | 0755), st_size=0,
                               st_uid=getuid(), st_mtime=time.time())

                if self.containers['/' + cname]['files'] is None:
                    self.containers['/' + cname]['files'] = dict()

                for f in blobs:
                    blob_name = f.name
                    blob_date = f.properties.last_modified
                    blob_size = long(f.properties.content_length)

                    node = dict(st_mode=(S_IFREG | 0644), st_size=blob_size,
                                st_mtime=self.convert_to_epoch(blob_date),
                                st_uid=getuid())

                    if blob_name.find('/') == -1:  # file just under container
                        self.containers['/' + cname]['files'][blob_name] = node

            return self.containers['/' + cname]
        return None

    def _get_file(self, path):
        d, f = self._parse_path(path)
        dir = self._get_dir(d, True)
        if dir is not None and f in dir['files']:
            return dir['files'][f]

    def getattr(self, path, fh=None):
        d, f = self._parse_path(path)

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
        if path.count('/') <= 1:    # create on root
            name = path[1:]

            if not 3 <= len(name) <= 63:
                log.error("Container names can be 3 through 63 chars long.")
                raise FuseOSError(ENAMETOOLONG)
            if name is not name.lower():
                log.error("Container names cannot contain uppercase \
                        characters.")
                raise FuseOSError(EACCES)
            if name.count('--') > 0:
                log.error('Container names cannot contain consecutive \
                        dashes (-).')
                raise FuseOSError(EAGAIN)
            #TODO handle all "-"s must be preceded by letter or numbers
            #TODO starts with only letter or number, can contain letter, nr,'-'

            resp = self.blobs.create_container(name)

            if resp:
                self.rebuild_container_list()
                log.info("CONTAINER %s CREATED" % name)
            else:
                raise FuseOSError(EACCES)
                log.error("Invalid container name or container already \
                        exists.")
        else:
            raise FuseOSError(ENOSYS)  # TODO support 2nd+ level mkdirs

    def rmdir(self, path):
        if path.count('/') == 1:
            c_name = path[1:]
            resp = self.blobs.delete_container(c_name)

            if resp:
                if path in self.containers:
                    del self.containers[path]
            else:
                raise FuseOSError(EACCES)
        else:
            raise FuseOSError(ENOSYS)  # TODO support 2nd+ level mkdirs

    def create(self, path, mode):
        node = dict(st_mode=(S_IFREG | mode), st_size=0, st_nlink=1,
                     st_uid=getuid(), st_mtime=time.time())
        d, f = self._parse_path(path)

        if not f:
            log.error("Cannot create files on root level: /")
            raise FuseOSError(ENOSYS)

        dir = self._get_dir(d, True)
        if not dir:
            raise FuseOSError(EIO)
        dir['files'][f] = node

        return self.open(path, data='')     # reusing handler provider

    def open(self, path, flags=0, data=None):
        if data == None:                    # download contents
            c_name = self.parse_container(path)
            f_name = path[path.find('/', 1) + 1:]

            try:
                data = self.blobs.get_blob(c_name, f_name)
            except URLError as e:
                if e.code == 404:
                    dir = self._get_dir('/' + c_name, True)
                    if f_name in dir['files']:
                        del dir['files'][f_name]
                    raise FuseOSError(ENOENT)
                elif e.code == 403:
                    raise FUSEOSError(EPERM)
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
                return 0     # avoid redundant write

            d, f = self._parse_path(path)
            c_name = self.parse_container(path)

            if data is None:
                data = ''

            if len(data) >= 1 << 26:   # 64mb
                log.error("Files larger than 64 MB are not supported \
                        currently.")
                raise FuseOSError(EFBIG)

            try: 
                self.blobs.put_blob(c_name, f, data, 'BlockBlob')

                dir = self._get_dir(d, True)
                if not dir or f not in dir['files']:
                    raise FuseOSError(EIO)

                # update local data
                dir['files'][f]['st_size'] = len(data)
                dir['files'][f]['st_mtime'] = time.time()
                return 0
            except Exception as e:
                log.error("Flush error HTTP %d" % e)
                raise FuseOSError(EAGAIN)

                self.fds[fh] = (path, data, False)  # mark as not dirty

    def release(self, path, fh=None):
        if fh is not None and fh in self.fds:
            del self.fds[fh]

    def truncate(self, path, length, fh=None):
        return 0     # assume done, no need

    def write(self, path, data, offset, fh=None):
        if not fh or fh not in self.fds:
            raise FuseOSError(ENOENT)
        else:
            d = self.fds[fh][1]
            if d is None:
                d = ""
            self.fds[fh] = (self.fds[fh][0], d[:offset] + data, True)
            return len(data)

    def unlink(self, path):
        c_name = self.parse_container(path)
        d, f = self._parse_path(path)

        try: 
            self.blobs.delete_blob(c_name, f)

            _dir = self._get_dir(path, True)
            if _dir and f in _dir['files']:
                del _dir['files'][f]
            return 0
        except WindowsAzureMissingResourceError:
            raise FuseOSError(ENOENT)
        except Exception as e:
            raise FuseOSError(EAGAIN)

    def readdir(self, path, fh):
        if path == '/':
            return ['.', '..'] + [x[1:] for x in self.containers.keys() \
                    if x is not '/']

        dir = self._get_dir(path, True)
        if not dir:
            raise FuseOSError(ENOENT)
        return ['.', '..'] + dir['files'].keys()

    def read(self, path, size, offset, fh):
        if not fh or fh not in self.fds:
            raise FuseOSError(ENOENT)

        f_name = path[path.find('/', 1) + 1:]
        c_name = path[1:path.find('/', 1)]

        try:
            data = self.blobs.get_blob(c_name, f_name)
            self.fds[fh] = (self.fds[fh][0], data, False)
            return data[offset:offset + size]
        except URLError, e:
            if e.code == 404:
                raise FuseOSError(ENOENT)
            elif e.code == 403:
                raise FUSEOSError(EPERM)
            else:
                log.error("Read blob failed HTTP %d" % e.code)
                raise FuseOSError(EAGAIN)
        data = self.fds[fh][1]
        if data is None:
            data = ""
        return data[offset:offset + size]

    def statfs(self, path):
        return dict(f_bsize=1024, f_blocks=1, f_bavail=maxint)

    def rename(self, old, new):
        """Three stage move operation because Azure do not have
        move or rename call. """
        od, of = self._parse_path(old)

        if of is None:   # move dir
            raise FuseOSError(ENOSYS)

        files = self._list_container_blobs(old)
        if of not in files:
            raise FuseOSError(ENOENT)

        src = files[of]

        if src['st_mode'] & S_IFREG <= 0:   # move dir
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

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass


if __name__ == '__main__':
    if len(argv) < 4:
        print('Usage: %s <mount_directory> <account> <secret_key>' % argv[0])
        exit(1)
    fuse = FUSE(AzureFS(argv[2], argv[3]), argv[1], debug=True,
            nothreads=False, foreground=True)
