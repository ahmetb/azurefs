from winazurestorage import *

if __name__=='__main__':
    account = 'ollaa'
    key = u'sPxSZjJAIxonBHG3CAIgMhZOaR+aVBcd1EbpVOblZ7dHBWDhj4T9Jev09rfR+bPqn+GTKzl8baI4UxCqlf4D6Q=='
    blobs = BlobStorage(CLOUD_BLOB_HOST, account, key)
    containers = blobs.list_containers()
    for i in containers: print(i)
