import logging
from sys import argv, exit

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class AzureFS(LoggingMixIn, Operations):
    'Azure Blob Storage filesystem'

    def __init__(self):
        pass
        

if __name__ == '__main__':
    if len(argv) < 2:
        print('Usage: %s <mount directory>' % argv[0])
        exit(1)
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(AzureFS(), argv[1], debug=True)

