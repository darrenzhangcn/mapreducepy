#!/usr/bin/env python
# coding=utf-8

# This example mapreducepy job is for the infamous Word Counting task with MapReduce
class mapreducejob:
    # Data source (mandatory). Can be any dict object, the keys/values will be passed
    # to "mapfn" function
    datasource = {0: "This is the first phrase",
                  1: "This is the second phrase and the end",}

    # Map function (mandatory). It's static method of map function code to be executed
    # on worker machines. Note that because this function code will be executed in other
    # environment alone, the variables outside of this function is invisible, for example
    # all the modules need to be imported inside this function.
    # @k - key. The key in "datasource" sent from server.
    # @v - value, The value in "datasource" sent from server.
    # @return - return a list of (key, value)
    @staticmethod
    def mapfn(k, v):
        for w in v.split():
            yield w, 1
        import time
        time.sleep(5)

    # Reduce function (optional). It's static method of reduce function code to be executed
    # on worker machines. Note that because this function code will be executed in other
    # environment alone, the variables outside of this function is invisible, for example
    # all the modules need to be imported inside this function.
    # @k - key. The key returned by "mapfn".
    # @vs - values, A list of values returned from "mapfn" of all the worker machines.
    # @return - return a list of (key, value), "value" is the result of reducing for "vs"
    @staticmethod
    def reducefn(k, vs):
        result = sum(vs)
        import time
        time.sleep(5)
        return result
