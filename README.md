# mapreducepy

A light-weight mapreduce framework for Python. Easy to run a mapreduce job
on multiple worker machines simultaneously. To create and run your first
mapreduce job, just follow the 3 steps:
    1. edit a python file with a class "mapreducejob" in it,
       "./examples/example_basic.py" is an example.
    2. run job server by shell command:
          python mapreduce.py create -p changeme -I "path/to/the/python/file"
    3. run job client on worker machines (could be the same machine as server)
       by shell command:
          python mapreduce.py do -S 127.0.0.1 -p changeme

Usage: mapreduce.py [commands] [options]

Commands:
  create        	create a mapreduce job on the server
  do            	get tasks from server to do

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -I, --impl  		for server only, the py file contains job
                        implementation (class mapreducejob)
  -S, --svrip		for client only, ip address of mapreduce job server
  -P, --port		port of mapreduce job server
  -p, --password	password for mapreduce job clients and server
  -v, --verbose         print info messages
  -V, --loud            print debug messages
  -d, --duration	for client only, duration for client to request tasks
                        from server, in hours and float value is acceptable

Examples:
"./examples/example_basic.py":
    A basic example job to get started, classic word counting job with mapreducepy.

"./examples/example_filesvr.py":
    An example job to demo the usage of centralised file repository.

"./examples/example_selfupgrade.py"
    An example mapreducepy job to upgrade the worker machines.