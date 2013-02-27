AzureFS
=======

A FUSE wrapper for **Windows Azure Blob Storage**. It provides basic
functionality to mount Azure Blob Storage as a local filesystem to
your computer.

### Introduction & Aim

There is no user-friendly interface to manage files on Azure cloud 
storage. Sometimes we need to:

* **list** files under a container
* **transfer** a bunch of local files to the cloud
* **remove** files matching a specific name pattern
* **rename** files on the cloud
* **move** files accross containers

AzureFS is a **command-line interface** where you can mount your Windows
Azure storage account as a folder on your computer and accomplish
such everyday tasks practically using UNIX commands like `ls`, `mkdir`, `cp` etc.

It is neither rock-solid nor should be used except for manual tasks. 
Your programs should communicate Windows Azure Storage service via
its REST API.

### Installation

This project requires `fusepy`:

```
git clone https://github.com/terencehonles/fusepy.git
cd fusepy
sudo python setup.py install
```

Install `azure-sdk-for-python`. Run `sudo easy_install azure` or `sudo pip install azure` or

```
git clone https://github.com/WindowsAzure/azure-sdk-for-python.git
cd azure-sdk-for-python/src
sudo python setup.py install
```

Install `libfuse2`, `fuse-utils` and `libfuse-dev` dependencies.
On Debian/Ubuntu:

```
apt-get install libfuse2 fuse-utils libfuse-dev
```

(Optional: run `sudo chmod 777 /etc/fuse.conf`)

### Usage

1. Create a folder for your mount point e.g. `mkdir /home/john/azure_folder`

2. Navigate to `azurefs` folder you cloned from this repo

3. Run `python azurefs.py <MOUNT_POINT> <YOUR_STORAGE_ACCOUNT> <STORAGE_SECRET_KEY>`

4. Do not shutdown this process, in some other tab, navigate to your mount
point e.g. `azure_folder`.

5. To try something out, create a container by `mkdir mycontainer`, and create
a file e.g. `date >> date.txt`.

6. You are ready to go. When you're done, simply hit `Ctrl-C` to unmount.

### Tutorial

Here's a neat blog post explains the project, highly recommended read:

> http://ahmetalpbalkan.com/blog/introducing-azurefs/ 

### Current significant limitations

* Single-level file hierarchy (/container/file), no nested dirs.
* Untested Mac OS X support
* No support for files on root level ($root container)* 
* Freezes GUI environments e.g. standard Ubuntu; works fine on Ubuntu Server
* Couldn't make use of [delete container](http://msdn.microsoft.com/en-us/library/windowsazure/dd179408.aspx) REST API call due to UNIX VFS interface. 
Therefore if you attempt to `rm -rf` a container with 1000s of files, you'll wait a 
lot. Instead, do it programmatically.

### Licensing

**AzureFS**, copyright 2012 **Ahmet Alp Balkan**. Licensed under Apache
License Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0.html

This project is neither affiliated with Windows Azure(TM) nor
supported by Microsoft Corporation (C). Use it at your own risk.

#### Dependency Licenses

**Azure SDK for Python**, copyright Microsoft Corporation (C). Licensed
under Apache License Version 2.0
Project available on https://github.com/WindowsAzure/azure-sdk-for-python

**fusepy**, licensed under New BSD License. Project available on
https://github.com/terencehonles/fusepy

### Author(s)

Ahmet Alp Balkan 'ahmetalpbalkan at gmail.com'

