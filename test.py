from winazurestorage import *
if __name__ == '__main__':
    blobs = BlobStorage(CLOUD_BLOB_HOST, 'azurefs', 'A1vfWiFf7Tqx/XgD2nzdc9AiovtHGpDUSMTo0qhCvey+jdWOacg+bxHM2YBxbIgUTnRsQmqw4zF61L0JhoB6hw==')

    f=open('dosya', 'r')
    buf=f.read()
    resp = blobs.put_blob('test', 't.txt', buf)
    if resp != 201:
        print 'error'
    print 'ok'
