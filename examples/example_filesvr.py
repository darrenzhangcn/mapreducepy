#!/usr/bin/env python
# coding=utf-8

# This example mapreducepy job is for the infamous Word Counting task with MapReduce,
# and there's centralised file repository to store results.
class mapreducejob:
    # Data source (mandatory). Can be any dict object, the keys/values will be passed
    # to "mapfn" function
    datasource = {0: "This is the first phrase",
                  1: "This is the second phrase and the end",}

    # Centralised file repository for JobTracker and all Workers (optional).
    # "filesvr" can be shared folder on Windows, MAC OSX (SMB) or Linux (Samba),
    # and is visitable to Windows, MAC or Linux machines, except for this case:
    #   - if "filesvr" is from MAC OSX (SMB), it's not visitable to Linux machines
    #     due to incompatibility issues.
    filesvr = {'ip': '192.168.160.8',
               'sharedir': 'AnalysisResults',
               'localdir': '/Users/myname/Documents/AnalysisResults',
               'usr': 'myname',
               'pwd': 'changeme',}

    # Map function (mandatory). It's static method of map function code to be executed
    # on worker machines. Note that because this function code will be executed in other
    # environment alone, the variables outside of this function is invisible, for example
    # all the modules need to be imported inside this function.
    # @k - key. The key in "datasource" sent from server.
    # @v - value, The value in "datasource" sent from server.
    # @filesvr - FileServer object representing file repository.
    # @return - return a list of (key, value)
    @staticmethod
    def mapfn(k, v, filesvr):
        for w in v.split():
            yield w, 1
        import time
        filesvr.mount()
        print("Root dir of file server: %s" % filesvr.rootdir())
        time.sleep(5)
        filesvr.umount()

    # Reduce function (optional). It's static method of reduce function code to be executed
    # on worker machines. Note that because this function code will be executed in other
    # environment alone, the variables outside of this function is invisible, for example
    # all the modules need to be imported inside this function.
    # @k - key. The key returned by "mapfn".
    # @vs - values, A list of values returned from "mapfn" of all the worker machines.
    # @filesvr - FileServer object representing file repository.
    # @return - return a list of (key, value), "value" is the result of reducing for "vs"
    @staticmethod
    def reducefn(k, vs, filesvr):
        result = sum(vs)
        import time
        time.sleep(5)
        return result

    # Function (optional) run on JobTracker before mapreduce job, usually for environment setup.
    @staticmethod
    def svr_prejobfn():
        print('called function svr_prejobfn')

    # Function (optional) run on JobTracker after mapreduce job, usually for data post-job process.
    # @results - the results returned by reduce function of a mapreduce job. Eg.
    #            {key1: result1, key2: result2, ...}
    @staticmethod
    def svr_postjobfn(results):
        print('called function svr_postjobfn. results=%s' % str(results))