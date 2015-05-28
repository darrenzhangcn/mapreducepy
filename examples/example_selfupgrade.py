#!/usr/bin/env python
# coding=utf-8

import mapreduce
import os

# This mapreducepy job upgrades one worker machine, if version of Worker was detected as
# different one from JobTracker's
class mapreducejob:
    # datasource contains the JobTracker's version number
    datasource = {0: mapreduce.VERSION}

    # Running dependancy environment for worker machines (optional). Can be a file, a tar.gz package or a folder.
    # THe content will be sent from JobTracker to Worker (in folder "mapreduce/depenv"), as the running
    # environment of mapfn and reducefn.
    depenv = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapreduce.py')

    def mapfn(k, v):
        # note VERSION is from mapreduce.py at Worker side, v is the version transferred from server side
        if VERSION == v:
            yield k, 'latest'
            return

        # upgrade the mapreduce.py source code
        import os, stat
        curfile = os.path.realpath(__file__)
        curdir = os.path.dirname(curfile)
        sfname = os.path.join(os.path.join(curdir, DEPENV_FOLDER_NAME), 'mapreduce.py')
        try:
            if not os.access(curfile, os.W_OK):
                os.chmod(curfile, stat.S_IWUSR)
            tf = open(curfile, 'wb')
            sf = open(sfname, 'rb')
            tf.write(sf.read())
            sf.close()
            tf.close()
        except Exception, e:
            yield k, 'failed:%s' % str(e)
        else:
            yield k, 'updated.src:%s tar:%s' % (sfname, curfile)
