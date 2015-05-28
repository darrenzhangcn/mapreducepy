#!/usr/bin/env python


################################################################################
# Copyright (c) 2010 Michael Fairley
# Copyright (c) 2015 Darren Zhang
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
################################################################################

import asynchat
import asyncore
import cPickle as pickle
import hashlib
import hmac
import logging
import marshal
import optparse
import os
import random
import socket
import sys
import types
import tarfile
import shutil
import stat
import time
from datetime import datetime, timedelta

VERSION = "0.1.11"

DEFAULT_PORT = 11235
TEMP_SERVER_DEPENV_TAR_NAME = r'svr_%d_depenv.tar.gz' # %d is the port num of server
DEPENV_FOLDER_NAME = 'depenv'
WORKING_FOLDER_NAME = 'wkdir'
CLIENT_LOGNAME = 'clienttask.log'
SERVER_LOGNAME = 'servertask.log'

# The work flow of authentication between client/server, implemented in Protocol class:
# 1. Client try to connect server
# 2. Server call send_challenge(), send 'challenge'
# 3. Client call respond_to_challenge(), send 'auth'
# 4. Server call verify_auth()
# 5. Client call send_challenge() in respond_to_challenge()->post_auth_init()
# 6. Server call respond_to_challenge()
# 7. Client call verify_auth()
# 8. Done auth
class Protocol(asynchat.async_chat):
    RECV_BUFFER_FILE_NAME = 'tmp_clirecvbuf.bin'

    # the format of data to transfer
    PICKLE = 'p'
    MARSHAL = 'm'
    STRING = 's'
    FILEOBJ = 'f'

    # the data producer for sending a file via async_chat class
    class FileProducer:
        DATA_SIZE_PER_READ = 4096

        # data is a file path name or opened file object
        def __init__(self, data):
            if isinstance(data, types.StringTypes) and os.path.isfile(data):
                self.file = open(data, 'rb')
            elif isinstance(data, types.FileType) and not data.closed:
                self.file = data
            else:
                logging.error("unrecognized data (%s) or can not opened file, discard data" % data)
                self.file = None

        def more(self):
            return self.file.read(Protocol.FileProducer.DATA_SIZE_PER_READ) if self.file else ''


    def __init__(self, conn=None, map=None):
        if conn:
            asynchat.async_chat.__init__(self, conn, map=map)
        else:
            asynchat.async_chat.__init__(self, map=map)

        self.set_terminator("\n")
        self.buffer = []
        self.auth = None
        self.mid_command = None
        self.command_datafmt = None
        self.recvbuffile = None # the buffer file object for storing received data when data format is fileobject
        self.peer = 'unknown'


    def collect_incoming_data(self, data):
        if self.mid_command and self.command_datafmt == Protocol.FILEOBJ:
            self.recvbuffile.write(data)
        else:
            self.buffer.append(data)


    # send a command via socket. if data is None then just send the 'command' out.
    # the protocol of 'data':
    #   1. if it's a file object then the whole content of the file is data to send,
    #   2. if it's a string then it's the raw data to send
    #   3. if it's function or code then marshal it and send
    #   4. if it's any other things, try to pickle it and send, if failed print error and return
    # format of command: "$cmd$:$lengthofdata$:$dataformat(pickled/marshal/string/fileobj)$\n$data$".
    # So if simple command without data, it's "$cmd$::\n"
    def send_command(self, command, data=None):
        if not ":" in command:
            command += ":"

        if data:
            # if data is file object then push data via fileproducer
            # if data is string then push directly
            # if others pickle and push
            if isinstance(data, types.FileType) and not data.closed:
                command += (str(os.path.getsize(data.name)) + ':' + Protocol.FILEOBJ)
                self.push(command + "\n")

                producer = Protocol.FileProducer(data)
                self.push_with_producer(producer)
                logging.debug("%s <- %s\n... %s" % (self.peer, command, data.name[:256]))

            elif isinstance(data, types.StringTypes):
                command += (str(len(data)) + ':' + Protocol.STRING)
                self.push(command + "\n" + data)
                logging.debug("%s <- %s\n... %s" % (self.peer, command, data[:256]))

            elif isinstance(data, (types.CodeType, types.FunctionType)):
                mdata = marshal.dumps(data)
                command += (str(len(mdata)) + ':' + Protocol.MARSHAL)
                self.push(command + "\n" + mdata)
                logging.debug("%s <- %s\n..." % (self.peer, command))

            else:
                try:
                    pdata = pickle.dumps(data)
                except Exception:
                    logging.error("data is not picklable. data:%s" % str(data))
                    return
                else:
                    command += (str(len(pdata)) + ':' + Protocol.PICKLE)
                    self.push(command + "\n" + pdata)
                    logging.debug("%s <- %s\n... %s" % (self.peer, command, str(data)[:256]))

        else:
            self.push(command + ":\n")
            logging.debug("%s <- %s:" % (self.peer, command))


    def found_terminator(self):
        if not self.mid_command:
            logging.debug("%s -> %s" % (self.peer, ''.join(self.buffer)))
            command, length, dformat = (''.join(self.buffer)).split(":", 2)

            if length:
                self.set_terminator(int(length))
                self.mid_command = command
                if dformat not in (Protocol.FILEOBJ, Protocol.PICKLE, Protocol.STRING, Protocol.MARSHAL):
                    logging.error("unrecognized data format:%s" % dformat)
                    dformat = None
                self.command_datafmt = dformat

                # for file obj format, create the file buffer handle
                if dformat == Protocol.FILEOBJ:
                    fn = os.path.join(os.path.dirname(os.path.realpath(__file__)), Protocol.RECV_BUFFER_FILE_NAME)
                    try:
                        if os.path.isfile(fn):
                            os.remove(fn)
                    except Exception:
                        logging.error("existing filebuf.bin can't be cleared")
                    else:
                        if self.recvbuffile:
                            logging.error("recv buffer file already opened when receive command")
                            self.recvbuffile.close()
                            self.recvbuffile = None
                        self.recvbuffile = open(fn, 'ab')

            else: # no data follows command
                if self.auth == "Done":
                    self.process_command(command)
                else:
                    self.process_unauthed_command(command)

        else: # Read the following data segment from the previous command
            # restore data
            if self.command_datafmt == Protocol.FILEOBJ:
                fn = self.recvbuffile.name
                self.recvbuffile.close()
                self.recvbuffile = open(fn, 'rb')
                data = self.recvbuffile
                logging.debug("->... %s" % fn[:256])
            elif self.command_datafmt == Protocol.PICKLE:
                data = pickle.loads(''.join(self.buffer))
                logging.debug("->... %s" % str(data)[:256])
            elif self.command_datafmt == Protocol.MARSHAL:
                data = marshal.loads(''.join(self.buffer))
                logging.debug("->...")
            elif self.command_datafmt == Protocol.STRING:
                data = ''.join(self.buffer)
                logging.debug("->... %s" % str(data)[:256])
            else:
                logging.warning("unrecognized data format, use raw format")
                data = ''.join(self.buffer)

            self.set_terminator("\n")
            command = self.mid_command
            self.mid_command = None
            self.command_datafmt = None
            self.recvbuffile = None

            if self.auth == "Done":
                self.process_command(command, data)
            else:
                self.process_unauthed_command(command, data)

            # close and delete temp buffer file after used
            if isinstance(data, types.FileType):
                fn = data.name
                if not data.closed:
                    data.close()
                try:
                    if os.path.isfile(fn):
                        os.remove(fn)
                except Exception:
                    logging.warning('delete file buffer failed')

        self.buffer = []


    def send_challenge(self):
        if self.peer == 'unknown':
            self.peer = str(self.socket.getpeername())

        self.auth = os.urandom(20).encode("hex")
        data = '&'.join([self.auth, VERSION]) # command data contains auth key and local mapreducepy version
        self.send_command("challenge", data)


    def respond_to_challenge(self, command, data):
        if self.peer == 'unknown':
            self.peer = str(self.socket.getpeername())

        key, ver = data.split('&')
        if ver != VERSION:
            logging.warning("remote version (%s) does not match local version (%s)" % (ver, VERSION))

        mac = hmac.new(self.password, key, hashlib.sha1)
        self.send_command("auth", mac.digest().encode("hex"))
        self.post_auth_init()


    def verify_auth(self, command, data):
        mac = hmac.new(self.password, self.auth, hashlib.sha1)
        if data == mac.digest().encode("hex"):
            self.auth = "Done"
            logging.info("Authenticated other end " + str(self.socket.getpeername()))
        else:
            self.handle_close()


    def process_command(self, command, data=None):
        commands = {
            'challenge': self.respond_to_challenge,
            'disconnect': lambda x, y: self.handle_close(),
            }

        if command in commands:
            commands[command](command, data)
        else:
            logging.critical("Unknown command received: %s" % (command,))
            self.handle_close()


    def process_unauthed_command(self, command, data=None):
        commands = {
            'challenge': self.respond_to_challenge,
            'auth': self.verify_auth,
            'disconnect': lambda x, y: self.handle_close(),
            }

        if command in commands:
            commands[command](command, data)
        else:
            logging.critical("Unknown unauthed command received: %s" % (command,))
            self.handle_close()



class Client(Protocol):
    def __init__(self, duration = None):
        Protocol.__init__(self)
        self.mapfn = self.reducefn = self.collectfn = self.filesvr = None
        self.closetime = datetime.now() + timedelta(seconds=duration) if duration else datetime.max


    def conn(self, server, port):
        while datetime.now() < self.closetime:
            self.svrconnected = False

            try:
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
                self.connect((server, port))
                asyncore.loop()

            except socket.error, e:
                logging.debug("Socket err (%s) in connection with (%s, %s), try again later...", str(e), server, port)
                time.sleep(30)

            except Exception, e:
                logging.exception("Exception: %s", str(e))
                break

            else:
                if not self.svrconnected:
                    logging.debug("Not able to connect server (%s, %s), try again later...", server, port)
                    time.sleep(30)
                else:
                    logging.info("===Tasks for one server job done===")
                    break


    def handle_connect(self):
        self.svrconnected = True


    def handle_close(self):
        self.close()


    def handle_error(self):
        logging.debug("Socket error occurred. closing socket")
        self.close_when_done()


    def set_mapfn(self, command, mapfn):
        self.mapfn = types.FunctionType(mapfn, globals(), 'mapfn')


    def set_collectfn(self, command, collectfn):
        self.collectfn = types.FunctionType(collectfn, globals(), 'collectfn')


    def set_reducefn(self, command, reducefn):
        self.reducefn = types.FunctionType(reducefn, globals(), 'reducefn')


    def set_filesvr(self, command, filesvr):
        self.filesvr = FileServer(filesvr)


    def call_mapfn(self, command, data):
        logging.info("Mapping %s" % str(data[0]))
        results = {}
        err = ""

        c = self.mapfn.func_code.co_argcount
        if self.filesvr and c < 3:
            logging.warning('have file server but mapfn do not have enough args')
        elif not self.filesvr and c >= 3:
            logging.warning("no file server but mapfn takes 3 args")

        # call map func
        try:
            if c == 3:
                for k, v in self.mapfn(data[0], data[1], self.filesvr):
                    if k not in results:
                        results[k] = []
                    results[k].append(v)
            elif c == 2:
                for k, v in self.mapfn(data[0], data[1]):
                    if k not in results:
                        results[k] = []
                    results[k].append(v)
            else:
                err += "arg count of mapfn not valid."
        except Exception, e:
            err += 'mapfn exception:' + str(e)

        if err != '':
            logging.error("call mapfn error:%s" % err)
            self.send_command('disconnect', err)
            return

        # collect results from map func if specified
        if self.collectfn:
            try:
                for k in results:
                    results[k] = [self.collectfn(k, results[k])]
            except Exception, e:
                err += 'collectfn exception:' + str(e)

        if err == '':
            self.send_command('mapdone', (data[0], results))
        else:
            logging.error("call collectfn error:%s" % err)
            self.send_command('disconnect', err)


    def call_reducefn(self, command, data):
        logging.info("Reducing %s" % str(data[0]))
        err = ''

        c = self.reducefn.func_code.co_argcount
        if self.filesvr and c < 3:
            logging.warning('have file server but reducefn do not have enough args')
        elif not self.filesvr and c >= 3:
            logging.warning("no file server but reducefn takes 3 args")

        # call reduce func
        results = ''
        try:
            if c == 3:
                results = self.reducefn(data[0], data[1], self.filesvr)
            elif c == 2:
                results = self.reducefn(data[0], data[1])
            else:
                err += "arg count of reducefn not valid"
        except Exception, e:
            err += 'reducefn exception:' + str(e)

        if err == '':
            self.send_command('reducedone', (data[0], results))
        else:
            logging.error("call reducefn error:%s" % err)
            self.send_command('disconnect', err)


    # data could be a string (depenv tar file path name if file server is local machine) or opened file object
    # of local copied depenv tar file
    def setup_depenv(self, command, data):
        curdir = os.path.dirname(os.path.realpath(__file__))

        # get path name of the tar file
        if isinstance(data, types.FileType):
            tarname = data.name

            # close file handle if data is file obj
            if not data.closed:
                data.close()
        else:
            # if local machine then data is the path name of tar file
            tarname = data
            logging.info("File server (mapreduce server) is the same machine. tarname:%s" % tarname)

        def onerror(func, path, exc_info):
            # if read-only files make them writable first, otherwise rmtree will fail
            import stat, os
            if not os.access(path, os.W_OK):
                os.chmod(path, stat.S_IWUSR)
                func(path)
            else:
                if exc_info[0]:
                    logging.debug(str(exc_info))
                raise

        # clear depenv dir and wkdir
        depenvdir = os.path.join(curdir, DEPENV_FOLDER_NAME)
        wkdir = os.path.join(curdir, WORKING_FOLDER_NAME)
        try:
            shutil.rmtree(depenvdir, onerror=onerror)
        except Exception, e:
            logging.warning("delete depenv folder (%s) failed, ignore. err:%s" % (depenvdir, str(e)))
        try:
            shutil.rmtree(wkdir, onerror=onerror)
        except Exception, e:
            logging.warning("delete wkdir (%s) failed, ignore. err:%s" % (wkdir, str(e)))

        # extract tar file to depenv
        try:
            tar = tarfile.open(tarname, 'r:*')
            tar.extractall(depenvdir)
            tar.close()
        except Exception, e:
            logging.warning("extrac tar file %s failed. err:%s" % (tarname, str(e)))

        # add depenv dir to system path
        sys.path.append(depenvdir)


    def process_command(self, command, data=None):
        if datetime.now() >= self.closetime:
            info = "Closed as timeout, command (%s) discarded" % command
            self.send_command('disconnect', info)
            self.close_when_done()
            logging.info(info)
            return

        commands = {
            'mapfn': self.set_mapfn,
            'collectfn': self.set_collectfn,
            'reducefn': self.set_reducefn,
            'map': self.call_mapfn,
            'reduce': self.call_reducefn,
            'filesvr': self.set_filesvr,
            'depenv': self.setup_depenv,
            }

        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)


    def post_auth_init(self):
        if not self.auth:
            self.send_challenge()



class Server(asyncore.dispatcher, object):
    def __init__(self):
        self.socket_map = {}
        asyncore.dispatcher.__init__(self, map=self.socket_map)
        self.mapfn = None
        self.reducefn = None
        self.collectfn = None
        self.datasource = None
        self.filesvr = None
        self.depenv = None
        self.svr_prejobfn = None
        self.svr_postjobfn = None
        self.password = None
        self._channels = []
        self._port = 0

    def run_server(self, password="", port=DEFAULT_PORT):
        logging.basicConfig(level=logging.DEBUG)

        # verify arguments
        if self.datasource is None or self.mapfn is None:
            logging.error("datasource or mapfn in mapreducejob should be specified")
            return
        if self.filesvr and self.mapfn and self.mapfn.func_code.co_argcount != 3:
            logging.error("the arg count (%d) of mapfn should be 3 if file server specified" %
                          self.mapfn.func_code.co_argcount)
            return
        if self.filesvr and self.reducefn and self.reducefn.func_code.co_argcount != 3:
            logging.error("the arg count (%d) of reducefn should be 3 if file server specified" %
                          self.reducefn.func_code.co_argcount)
            return

        # clear tmp file
        tarname = os.path.join(os.path.dirname(os.path.realpath(__file__)), TEMP_SERVER_DEPENV_TAR_NAME % port)
        if os.path.isfile(tarname):
            try:
                os.remove(tarname)
            except Exception:
                logging.warning("del old depenv tar file %s failed, assume this is the latest depenv" % tarname)
                pass

        # call prejob func
        if self.svr_prejobfn:
            self.svr_prejobfn()

        # server running
        self.password = password
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(("", port))
        self._port = port
        self.listen(8) # 8 is the queue size
        self.taskmanager = TaskManager(self.datasource, self)
        try:
            asyncore.loop(map=self.socket_map)
        except:
            self.close()
            raise

        # call postjob func
        if self.svr_postjobfn:
            self.svr_postjobfn(self.taskmanager.results)

        return self.taskmanager.results


    def handle_accept(self):
        conn, addr = self.accept()
        sc = ServerChannel(conn, self.socket_map, self)
        sc.password = self.password
        self._channels.append(sc)


    def handle_close(self):
        for c in self._channels:
            c.close_when_done()
            logging.debug("Closing client %s" % c.peer)
        self.close()


    def set_datasource(self, ds):
        self._datasource = ds


    def get_datasource(self):
        return self._datasource


    datasource = property(get_datasource, set_datasource)


    def set_depenv(self, de):
        self._depenv = de


    def get_depenv(self):
        return self._depenv


    depenv = property(get_depenv, set_depenv)


    def set_filesvr(self, fs):
        self._filesvr = fs


    def get_filesvr(self):
        return self._filesvr


    filesvr = property(get_filesvr, set_filesvr)


    def port(self):
        return self._port


class ServerChannel(Protocol):
    def __init__(self, conn, map, server):
        Protocol.__init__(self, conn, map=map)
        self.server = server

        self.start_auth()


    def handle_close(self):
        self.close()
        logging.info("Client %s connection closed" % self.peer)


    def start_auth(self):
        self.send_challenge()


    def start_new_task(self):
        command, data = self.server.taskmanager.next_task()
        if command is None:
            return
        self.send_command(command, data)


    def map_done(self, command, data):
        self.server.taskmanager.map_done(data)
        self.start_new_task()


    def reduce_done(self, command, data):
        self.server.taskmanager.reduce_done(data)
        self.start_new_task()


    def process_command(self, command, data=None):
        commands = {
            'mapdone': self.map_done,
            'reducedone': self.reduce_done,
            }

        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)


    def post_auth_init(self):
        if self.server.mapfn:
            self.send_command('mapfn', self.server.mapfn.func_code)

        if self.server.reducefn:
            self.send_command('reducefn', self.server.reducefn.func_code)

        if self.server.collectfn:
            self.send_command('collectfn', self.server.collectfn.func_code)

        if self.server.filesvr:
            self.send_command('filesvr', self.server.filesvr)

        if self.server.depenv:
            envtar = self.envtar(self.server.depenv) # full path name of tar file
            if envtar:
                logging.info("peer address is:%s" % self.peer)
                peerip = self.socket.getpeername()[0]
                localip = socket.gethostbyname_ex(socket.gethostname())
                if peerip in '127.0.0.1, localhost' + str(localip):
                    # if remote is local machine, send the local pathname directly
                    # one of the reasons is that if send a big file between client/server on same machine,
                    # they seems stuck, need future investigation but anyway we don't have to do that
                    self.send_command('depenv', envtar)
                else: # if remote is another machine, send all contents of tar file via socket
                    self.send_command('depenv', open(envtar, 'rb'))

        self.start_new_task()


    # return the full path name of the dependency env tar file
    def envtar(self, depenv):
        try:
            istar = tarfile.is_tarfile(depenv)
        except Exception:
            istar = False

        if os.path.isdir(depenv) or (os.path.isfile(depenv) and not istar):
            curdir = os.path.dirname(os.path.realpath(__file__))
            tarname = os.path.join(curdir, TEMP_SERVER_DEPENV_TAR_NAME % self.server.port())

            # compress the dir or file to a tar file
            if not os.path.isfile(tarname):
                tar = tarfile.open(tarname, 'w:gz')
                def filterfunc(i): # remove readonly att of all files to make client's life easier
                    i.mode = stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC
                    return i
                altname = r'/' if os.path.isdir(depenv) else os.path.join(r'/', os.path.basename(depenv))
                tar.add(depenv, altname, filter=filterfunc)
                tar.close()
            return tarname

        elif istar:
            return depenv
        else:
            logging.warning("Dependency environment for worker not valid, running without env setup")
            return None


class TaskManager:
    START = 0
    MAPPING = 1
    REDUCING = 2
    FINISHED = 3

    def __init__(self, datasource, server):
        self.datasource = datasource
        self.server = server
        self.state = TaskManager.START


    def next_task(self):
        if self.state == TaskManager.START:
            self.map_iter = iter(self.datasource)
            self.working_maps = {} # the list of datasource items under working in MAP step by workers
            self.map_results = {} # the list of results for MAP step of datasource items
            self.state = TaskManager.MAPPING

        if self.state == TaskManager.MAPPING:
            try:
                map_key = self.map_iter.next()
                map_item = map_key, self.datasource[map_key]
                self.working_maps[map_item[0]] = map_item[1]
                return 'map', map_item

            except StopIteration:
                if len(self.working_maps) > 0:
                    key = random.choice(self.working_maps.keys())
                    return 'map', (key, self.working_maps[key])

                if self.server.reducefn is None:
                    # if not specified reduce function then assume only mapping needed
                    self.results = self.map_results
                    self.state = TaskManager.FINISHED
                else:
                    self.state = TaskManager.REDUCING
                    self.reduce_iter = self.map_results.iteritems()
                    self.working_reduces = {}
                    self.results = {}

        if self.state == TaskManager.REDUCING:
            try:
                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = reduce_item[1]
                return 'reduce', reduce_item

            except StopIteration:
                if len(self.working_reduces) > 0:
                    key = random.choice(self.working_reduces.keys())
                    return 'reduce', (key, self.working_reduces[key])
                self.state = TaskManager.FINISHED

        if self.state == TaskManager.FINISHED:
            logging.info("Job already done. Results=%s" % str(self.results))
            self.server.handle_close()
            return 'disconnect', 'Finished!'


    def map_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_maps:
            logging.debug("out-of-date mapdone received, discard")
            return

        for (key, values) in data[1].iteritems():
            if key not in self.map_results:
                self.map_results[key] = []
            self.map_results[key].extend(values)

        del self.working_maps[data[0]]


    def reduce_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_reduces:
            logging.debug("out-of-date reducedone received, discard")
            return

        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]


# class for remote file server
class FileServer:
    # filesvr - {'ip': '192.168.1.1', 'sharedir': 'fileRepo',
    #           'localdir': '/path/to/my/local/dir', # the absolute dir of shared dir in file server
    #           'usr': 'myname', 'pwd': 'changeme',}
    def __init__(self, filesvr):
        self._filesvr = filesvr
        self._mounted = False
        self._localwkdir = os.path.join(os.path.dirname(os.path.realpath(__file__)), WORKING_FOLDER_NAME)
        self._islocal = None
        import platform
        self._plat = platform.system()
        if self._plat.lower() == 'windows':
            self._mountdir = 'X:\\'
        else:
            self._mountdir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../mount/filesvr')

    def mount(self):
        if self.islocal():
            # this worker client is the same machine as file server, no need to mount
            self._mounted = True
            return

        self.umount()

        if self._plat.lower() == 'windows':
            os.system('NET USE %s \\\\%s\\%s %s /USER:%s' %
                      (self._mountdir.rstrip('\\'), self._filesvr['ip'], self._filesvr['sharedir'],
                      self._filesvr['pwd'], self._filesvr['usr']))
        elif self._plat.lower() == 'darwin': # MAC OSX
            if not os.path.isdir(self._mountdir):
                os.makedirs(self._mountdir)

            # 'mount -t smbfs //myname:changeme@192.168.1.1/fileRepo /path/to/my/local/dir'
            remotedir = os.path.join('//%s:%s@%s/' % (self._filesvr['usr'], self._filesvr['pwd'], self._filesvr['ip']),
                                     self._filesvr['sharedir'])
            os.system('mount -t smbfs %s %s' % (remotedir, self._mountdir))
        else:
            logging.error("unsupported platform")

        self._mounted = True


    def umount(self):
        if self.islocal():
            # this worker client is the same machine as file server, no need to umount
            self._mounted = False
            return

        mountdir = self._mountdir

        if self._plat.lower() == 'windows':
            if os.path.isdir(mountdir):
                os.system('NET USE %s /DEL /Y' % mountdir.rstrip('\\'))
        elif self._plat.lower() in ('darwin', 'linux'):
            if os.path.isdir(mountdir):
                if os.path.ismount(mountdir):
                    os.system('umount %s' % mountdir)
                try:
                    os.rmdir(mountdir)
                except Exception, e:
                    logging.error("mount dir can't be removed, maybe not empty. msg:%s" % str(e))
            elif os.path.isfile(mountdir):
                os.remove(mountdir)
        else:
            logging.error("unsupported platform")

        self._mounted = False


    def rootdir(self):
        if self.islocal():
            # this worker client is the same machine as file server, use local dir
            return self._filesvr['localdir']

        # file server is a remote machine
        if self._mounted:
            return self._mountdir
        else:
            # if remote machine not mounted (or mounted failed) then return local wkdir
            logging.warning("try to get root dir before remote file server mounted successfully, use local work dir")
            return self._localwkdir

    def islocal(self):
        if self._islocal is None:
            host, alilist, localiplist = socket.gethostbyname_ex(socket.gethostname())
            logging.info('FileServer.islocal(): local ip list:%s' % str(localiplist))
            self._islocal = self._filesvr['ip'] in localiplist
        return self._islocal


    # copy a file (localsrcfile) to target file or directory (tardirinsvrroot) under root dir of file server
    def copy2svr(self, localsrcfile, tarinsvrroot):
        if not os.path.isfile(localsrcfile):
            logging.warning("FileServer.copy2svr takes arg 1 (%s) as not a file" % localsrcfile)
            return
        if tarinsvrroot[:1] in ('/', '\\'):
            tarinsvrroot = tarinsvrroot[1:]

        self.mount()
        tar = os.path.join(self.rootdir(), tarinsvrroot)
        try:
            if not os.path.isdir(os.path.dirname(tar)):
                os.makedirs(os.path.dirname(tar))
            shutil.copy2(localsrcfile, tar)
        except Exception, e:
            logging.warning("FileServer.copy2svr failed. copy from %s to %s, err:%s" %
                            (localsrcfile, tarinsvrroot, str(e)))
        self.umount()


# create logging, only called once for the module
# filename - the file to log to, no file stream if None
# loglevel - the logging level
def create_logging(filename=None, loglevel=logging.DEBUG):
    fmt = '[%(asctime)s]%(levelname)s:%(message)s'
    datefmt = '%m-%d %H:%M:%S'

    if filename:
        try:
            os.remove(filename)
        except Exception:
            pass
        logging.basicConfig(filename=filename, filemode='w', format=fmt, datefmt=datefmt, level=loglevel)

        # add stream handler
        console = logging.StreamHandler()
        console.setLevel(loglevel)
        console.setFormatter(logging.Formatter(fmt, datefmt))
        logging.getLogger().addHandler(console)
    else:
        logging.basicConfig(format=fmt, level=loglevel)


def run_client_once(svrip, port, password, duration):
    if duration < 1:
        logging.info("time (%s) not enough for a run, exit." % duration)
        return

    # class for duplicate console print to file
    class Out(object):
        def __init__(self, outfile):
            self.outfile = outfile

        def write(self, s):
            sys.__stdout__.write(s)
            with open(self.outfile, 'a') as f:
                f.write(s)

    # duplicate print log to file
    logfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), CLIENT_LOGNAME)
    tmplog = logfile + '.tmp'
    oldstdout = sys.stdout
    sys.stdout = Out(tmplog)

    # run
    begintime = datetime.now().strftime("%Y%m%d-%H%M%S")
    client = Client(duration)
    client.password = password
    client.conn(svrip, port)
    endtime = datetime.now().strftime("%Y%m%d-%H%M%S")

    # change stdout back and merge tmp log to main log
    sys.stdout = oldstdout
    try:
        f = open(tmplog)
        with open(logfile, 'a') as mf:
            mf.write('===mapreduce: below is log from STDOUT===\n')
            for l in f:
                mf.write(l)
        os.remove(tmplog)
    except Exception:
        pass

    # copy local main log file to file server if exist
    if client.filesvr:
        ip = socket.gethostbyname(socket.gethostname())
        client.filesvr.copy2svr(logfile, 'mapreduceworkerlogs/mapreduceworker_%s_%s_%s.log' % (begintime, endtime, ip))


def run_client(options):
    durinsec = int(options.duration * 3600)
    closetime = datetime.now() + timedelta(seconds=durinsec)

    while datetime.now() < closetime:
        run_client_once(options.svrip, options.port, options.password, (closetime-datetime.now()).total_seconds())
        time.sleep(60)


# impl -  the abs path of the implementation python source file
def run_server(impl, password, port):
    if not os.path.isfile(impl) or impl.split('.')[-1].lower() != 'py':
        logging.error("implementation (%s) is not a valid python source file" % impl)
        return

    sys.path.append(os.path.dirname(impl))
    m = __import__(os.path.basename(impl).split('.')[0], globals(), locals(), [], -1)
    if not hasattr(m, 'mapreducejob'):
        logging.error('implementation %s does not have mapreducejob' % impl)
        return
    jobclass = m.mapreducejob

    s = Server()
    s.datasource = jobclass.datasource if hasattr(jobclass, 'datasource') else None
    s.mapfn = jobclass.mapfn if hasattr(jobclass, 'mapfn') else None
    s.reducefn = jobclass.reducefn if hasattr(jobclass, 'reducefn') else None
    s.depenv = jobclass.depenv if hasattr(jobclass, 'depenv') else None
    s.filesvr = jobclass.filesvr if hasattr(jobclass, 'filesvr') else None
    s.svr_prejobfn = jobclass.svr_prejobfn if hasattr(jobclass, 'svr_prejobfn') else None
    s.svr_postjobfn = jobclass.svr_postjobfn if hasattr(jobclass, 'svr_postjobfn') else None

    results = s.run_server(password=password, port=port)
    logging.info("Results = %s" % str(results)[:256])


if __name__ == '__main__':
    desc = "A light-weight mapreduce framework for Python. Simple to execute a mapreduce "\
            "job on multiple worker machines simultaneously. Please refer to readme.txt for more details."
    cmdtxt = "Commands: [create]:create a job on the server.   [do]:get tasks from server to do."
    parser = optparse.OptionParser(usage="%prog [commands] [options]", description=desc,
                                   epilog=cmdtxt, version="%%prog %s"%VERSION)
    parser.add_option("-I", "--impl", dest="impl", default="",
                      help="for server only, the py file contains job implementation (class mapreducejob)")
    parser.add_option("-S", "--svrip", dest="svrip", default="",
                      help="for client only, ip address of mapreduce job server")
    parser.add_option("-P", "--port", dest="port", type="int", default=DEFAULT_PORT,
                      help="port of mapreduce job server")
    parser.add_option("-p", "--password", dest="password", default="",
                      help="password for mapreduce job clients and server")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="print info messages")
    parser.add_option("-V", "--loud", dest="loud", action="store_true", help="print debug messages")
    parser.add_option("-d", "--duration", dest="duration", type='float', default="7",
                      help="for client only, duration for client to request tasks from server, "
                           "in hours and float value is acceptable")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        sys.exit(0)

    # get log level
    loglevel = logging.INFO if options.verbose else logging.INFO
    loglevel = logging.DEBUG if options.loud else loglevel

    cmd = str(args[0])
    curdir = os.path.dirname(os.path.realpath(__file__))
    if cmd.lower() == 'create':
        create_logging(os.path.join(curdir, SERVER_LOGNAME), loglevel)
        run_server(os.path.join(os.getcwd(), options.impl), options.password, options.port)
    elif cmd.lower() == 'do':
        create_logging(os.path.join(curdir, CLIENT_LOGNAME), loglevel)
        run_client(options)
    else:
        logging.error("Unrecognized command: %s" % cmd)
        sys.exit(-1)

