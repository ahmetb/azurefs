import logging
from sys import argv, exit

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from winazurestorage import *

if __name__ == '__main__':
    log = logging.getLogger()
    ch = logging.StreamHandler()
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)

blobs = None

class AzureFS(LoggingMixIn, Operations):
    'Azure Blob Storage filesystem'

    def __init__(self, account, key):
        blobs = BlobStorage(CLOUD_BLOB_HOST, account, key)
        containers = blobs.list_containers()
        for i in containers: print(i)

    def mkdir(self, path, mode):
        log.info('Create directory %s' % path)

    def getattr(self, path, fh=None):
        log.info('Requesting attributes for %s' % path)
        return None


if __name__ == '__main__':
    if len(argv) < 4:
        print('Usage: %s <mount_directory> <account> <secret_key>' % argv[0])
        exit(1)
    fuse = FUSE(AzureFS(argv[2], argv[3]), argv[1], debug=True)

