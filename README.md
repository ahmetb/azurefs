AzureFS
=======

A FUSE wrapper for **Windows Azure Blob Storage**. It provides basic
functionality to mount Azure Blob Storage as a local filesystem to
your computer.

> **NOTE:** This project is still under development and should not be
considered for production use.

### Current significant limitations

* Single-level file hierarchy (/container/file)
* Untested multithreading support
* Files larger than 64 MB not supported (requires page blobs)
* Untested Mac OS X support



### Licensing
AzureFS, copyright 2012 Ahmet Alp Balkan. Licensed under Apache License
Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0.html

This project is not affiliated with Windows Azure(TM) and
not supported by Microsoft Corporation (C).

WinAzureStorage, copyright Sriram Krishnan and Steve Marx.
Project available on https://github.com/sriramk/winazurestorage

FusePy, licensed under New BSD License. Project available on
https://github.com/terencehonles/fusepy


### Authors
Ahmet Alp Balkan <ahmetalpbalkan at gmail.com>

