"""A pure-python signlefile load balancer.
This file can be used as a python module or as a standalone python-script.
Designed for 2.7.
"""

#Contributors: (add your name and the changes you made here, everyone should have the right to be mentioned for his/her erforts)
#    -bennr01: first version; initial commit.
#If you want to contribute, just send a pull request. Please try to follow the rules if doing so:
#   -keep it pure-python
#   -keep it single-file
#   -only use the standard lybrary
#   -keep it as much compatible as possible(linux+windows+mac+..., CPython+Pypy+Jython,...)
#   -add your name to the start of this file
#   -only allow output in debug-mode
#   -(optional) add your name to the Contributors-Section
#   -dont remove Contributors (except if they say they have done something they didnt, however keep them if their work was later removed)
#these rules dont apply for forks or other branches, however please keep the Contributors-list.
#also: sorry for my bad english, but 'python -m pdb bennr01' doesnt seem to work for me xD

import socket,select,errno

class LoadBalancer(object):
    """A load balancer."""
    sendbuffer=4096
    maxbuffersize=2**16
    def __init__(self,host="0.0.0.0",port=80,targets=[],fallback=None,check_time=None,debug=False,family=socket.AF_INET,typ=socket.SOCK_STREAM):
        """Arguments:
host (str): hostname to bind to. Default: "0.0.0.0"
port (int): port to bind to. Default: 80
targets (list of tuples of lenght 2): a list of tupples of (host,port) to spread data to. Default: []
fallback (tuple of length 2 or None): a tuple of (host,port) to send relay connections to if targets-list is empty. Default: None
check_time (int, long or float): passed as timeout to select.select. The Server can only update its targets-list every check_time seconds. Default: None
debug (bool): the debug flag. Default: False

The LoadBalancer binds and strts listening on __init__.
To run the LoadBalancer, call LoadBalancer.mainloop().
"""
        assert isinstance(host,str),ValueError("Expected string as host-argument")
        assert isinstance(port,int),ValueError("Expected int as port-argument")
        assert isinstance(targets,list),ValueError("Expected list as targets-arguments")
        assert isinstance(fallback,tuple) or (fallback is None),ValueError("Expected tuple or None as fallback-argument")
        assert isinstance(check_time,int) or isinstance(check_time,long) or isinstance(check_time,float) or (check_time is None),ValueError("Expected int,long or float as check_time-argument")
        assert check_time>=0 or (check_time is None),ValueError("Expected positive integer as check_time-argument")
        assert isinstance(debug,bool),ValueError("Expected bool as debug-argument")
        for addr in targets:
            assert len(addr)==2,ValueError("Each address in targets needs to be a tuple of length 2")
            assert isinstance(addr[0],str),ValueError("Each address in targets needs to have a str at index 0")
            assert isinstance(addr[1],int),ValueError("Each address in targets needs to have a int at index 1")
            assert addr[1]>0,ValueError("Each address in targets needs to have a integer larger 0 as port")
        self.host=host
        self.port=port
        self.targets=targets
        self.fallback=fallback
        self.check_time=check_time
        self.debug=debug
        self.family=family
        self.type=typ
        self.o2i={}#map clients to servers
        self.i2o={}#map servers to clients
        self.s2b={}#map sockets to buffer
        self.o2t={}#map clients to targets
        self.t2n={}#map targets to number of connections
        self.running=False
        self.bind_and_listen()
    def bind_and_listen(self):
        """create and binds the listening socket and starts listening. This is automatically called on __init__""" 
        if self.debug:
            print "binding..."
        self.listen_s=socket.socket(self.family,self.type,0)
        self.listen_s.bind((self.host,self.port))
        if self.debug:
            print "bound to: {host}:{port}".format(host=self.host,port=self.port)
        try:
            self.listen_s.listen(3)
        except:
            #some systems doesnt support backlogs larger 1
            self.listen_s.listen(1)
    def mainloop(self):
        """enters the mainloop. The LoadBalancer will now serve incomming requests."""
        if self.debug:
            print "entering mainloop."
        self.running=True
        try:
            while self.running:
                checkread=checkex=[self.listen_s]+self.i2o.keys()+self.o2i.keys()
                checkwrite=filter(None,[s for s in self.s2b.keys() if len(self.s2b[s])>0])
                toread,towrite,exceptional=select.select(checkread,checkwrite,checkex,self.check_time)
                if self.debug:
                    print "data avaible on: ",toread
                    print "buffer free on:  ",towrite
                    print "errors on:       ",exceptional
                if len(toread)==0 and len(towrite)==0 and len(exceptional)==0:
                    continue
                for s in exceptional:
                    #exceptional sockets
                    if s is self.listen_s:
                        #error in balancer-listening-socket
                        if self.debug:
                            print "error in listening socket!"
                        self.running=False
                        #raise exception, finally-clause will disconnect all sockets
                        raise RuntimeError("Error in listening socket!")
                    elif s in self.o2i:
                        #error in client-balancer connection
                        if self.debug:
                            print "error in client connection: closing socket."
                        self.close_s(s)
                    else:
                        #error in balancer-server connection
                        #setup aliases so they are less confusing
                        s2c=s
                        c2s=self.i2o[s2c]
                        #first, check wether fallback-server is possible
                        if c2s in self.o2t:
                            old_peer=self.o2t[c2s]
                            if old_peer==self.fallback or (self.fallback is None):
                                old_peer=None
                        else:
                            old_peer=None
                        if old_peer is None:
                            #fallback-server not possible, closing socket
                            if self.debug:
                                print "fallback not possible, closing s"
                            self.close_s(c2s)
                        else:
                            #reconnect s to fallback server
                            if self.debug:
                                print "error in connection to normal server, instead connecting to fallback-server."""
                            #close old socket. we cant call self.close_s here, as it would close the correctly working side of the client to
                            if self.debug:
                                print "unregistering old peer"
                            try:
                                s2c.shutdown(socket.SHUT_RDWR)
                            except:
                                pass
                            try:
                                s2c.close()
                            except:
                                pass
                            if s2c in self.s2b:
                                old_buff=self.s2b[s2c]
                                if self.debug:
                                    print "redirecting {n} bytes to fallback server".format(n=len(old_buff))
                                del self.s2b[s2c]
                            else:
                                old_buff=""
                            if s2c in self.i2o:
                                del self.i2o[s2c]
                            if c2s in self.o2t:
                                t=self.o2t[c2s]
                                if t in self.t2n:
                                    self.t2n[t]=max(0,self.t2n[t]-1)
                                self.o2t[c2s]=self.fallback
                            #now reconnect
                            peer=socket.socket(self.family,self.type,0)
                            peer.setblocking(0)
                            self.s2b[peer]=old_buff
                            self.o2i[c2s]=peer
                            self.i2o[peer]=c2s
                            self.o2t[c2s]=self.fallback
                            try:
                                err=peer.connect_ex(self.fallback)
                            except Exception as e:
                                try:
                                    err=e.args[0]
                                except:
                                    err=None
                                if err==errno.EWOULDBLOCK or err==errno.WSAEWOULDBLOCK:
                                    pass
                                else:
                                    if self.debug:
                                        print "error during connect to fallback:",e
                                    self.close_s(c2s)
                                    continue
                            if err==errno.EINPROGRESS or err==errno.WSAEWOULDBLOCK:
                                pass
                            else:
                                if self.debug:
                                    print "error during connet to fallback:",errno.errorcode[err]
                                self.close_s(c2s)
                for s in towrite:
                    #write data from buffer to socket
                    if s not in self.s2b:
                        continue
                    if len(self.s2b[s])<self.sendbuffer:
                        tosend=self.s2b[s]
                    else:
                        tosend=self.s2b[s][:self.sendbuffer]
                    if self.debug:
                        print "sending {n} bytes (left: {t} bytes)".format(n=len(tosend),t=len(self.s2b[s])-len(tosend))
                    try:
                        sent=s.send(tosend)
                    except socket.error as e:
                        if self.debug:
                            print "error writing buffer:",e
                        self.close_s(s)
                        continue
                    if self.debug:
                        print "sent {n} bytes.".format(n=sent)
                    if sent>=len(self.s2b[s]):
                        self.s2b[s]=""
                    self.s2b[s]=self.s2b[s][sent:]
                for s in toread:
                    #receive data and accept connections
                    if s is self.listen_s:
                        if self.debug:
                            print "got request"
                        #handle connects
                        #first select target, so taht we are able to not accept a connection if no connection is aviable
                        if len(self.targets)==0:
                            #target is fallback server
                            if self.debug:
                                print "using fallback server"
                            target=self.fallback
                        else:
                            #get target with least connections
                            c=9999999
                            target=None
                            for ta in targets:
                                if ta not in self.t2n:
                                    self.t2n[ta]=0
                                n=self.t2n[ta]
                                if n<c:
                                    c=n
                                    target=ta
                        if target is None:
                            #either no targets are aviable and fallback isnt defined or all target have a enourmos load
                            if self.debug:
                                print "cannot find a target!"
                            continue
                        if target in self.t2n:
                            self.t2n[target]+=1
                        else:
                            self.t2n[target]=1
                        if self.debug:
                            print "target is:",target
                        new_s,addr=self.listen_s.accept()
                        new_s.setblocking(0)
                        peer=socket.socket(self.family,self.type,0)
                        peer.setblocking(0)
                        self.s2b[new_s]=""
                        self.s2b[peer]=""
                        self.o2i[new_s]=peer
                        self.i2o[peer]=new_s
                        self.o2t[new_s]=target
                        try:
                            err=peer.connect_ex(target)
                        except Exception as e:
                            try:
                                err=e.args[0]
                            except:
                                err=None
                            if err==errno.EWOULDBLOCK or err==errno.WSAEWOULDBLOCK:
                                pass
                            else:
                                if self.debug:
                                    print "error during connect to target:",e
                                self.close_s(new_s)
                                continue
                        if err==errno.EINPROGRESS or err==errno.WSAEWOULDBLOCK:
                            pass
                        else:
                            if self.debug:
                                print "error during connet to target:",errno.errorcode[err]
                            self.close_s(new_s)
                    else:
                        if s in self.i2o:
                            p=self.i2o[s]
                        elif s in self.o2i:
                            p=self.o2i[s]
                        else:
                            if self.debug:
                                print "a socket has no peer registered, closing it."
                            self.close_s(s)
                            continue
                        try:
                            peerbufferlength=len(self.s2b[p])
                        except KeyError:
                            peerbufferlength=0
                            self.s2b[p]=""
                        if peerbufferlength<self.maxbuffersize:
                            maxread=self.maxbuffersize-peerbufferlength
                            try:
                                data=s.recv(maxread)
                            except socket.error as e:
                                err=e.args[0]
                                if err==errno.EAGAIN or err==errno.EWOULDBLOCK:
                                    continue
                                else:
                                    if self.debug:
                                        print "error while receiving:",e
                                    self.close_s(s)
                                    continue
                            if self.debug:
                                print "reveived {n} bytes.".format(n=len(data))
                            if len(data)==0:
                                if self.debug:
                                    print "connection closed."
                                self.close_s(s)
                            else:
                                self.s2b[p]+=data
                        else:
                            continue
        finally:
            #close all open connections
            if self.debug:
                print "closing all connections..."
            try:
                self.listen_s.close()
            except:
                pass
            for s in self.o2i.keys()+self.i2o.keys():
                self.close_s(s)
    def close_s(self,s):
        """closes the socket and its peer."""
        try:
            s.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            s.close()
        except:
            pass
        if s in self.s2b:
            del self.s2b[s]
        if s in self.o2t:
            t=self.o2t[s]
            del self.o2t[s]
            if t in self.t2n:
                self.t2n[t]=max(self.t2n[t]-1,0)
        if s in self.o2i:
            p=self.o2i[s]
            del self.o2i[s]
            self.close_s(p)
        if s in self.i2o:
            p=self.i2o[s]
            del self.i2o[s]
            self.close_s(p)
    def add_target(self,addr):
        """adds a target to the targets-list."""
        assert isinstance(addr,tuple),ValueError("Expected a tuple as addr-argument")
        assert len(addr)==2,ValueError("Expected a tuple of length 2 as addr-argument")
        assert isinstance(addr[0],str),ValueError("Expected addr-argument to have a string at index 0")
        assert isinstance(addr[1],str),ValueError("Expected addr-argument to have a integer at index 1")
        if addr in self.targets:
            raise ValueError("Target already registered!")
        else:
            self.targets.append(addr)
    def remove_target(self,addr):
        """removes a target from the targets-list"""
        assert isinstance(addr,tuple),ValueError("Expected a tuple as addr-argument")
        assert len(addr)==2,ValueError("Expected a tuple of length 2 as addr-argument")
        assert isinstance(addr[0],str),ValueError("Expected addr-argument to have a string at index 0")
        assert isinstance(addr[1],str),ValueError("Expected addr-argument to have a integer at index 1")
        if addr in self.targets:
            self.targets.remove(addr)
        else:
            raise ValueError("Target not found!")
    def stop(self):
        """stops the server."""
        if not self.running:
            raise RuntimeError("Server not running!")
        self.running=False
        self.check_time=0

def load_from_file(path,ignore_errors=False):
    """opens the file at path, reads it and return a list of targets. Each line needs to be a signle target in the format host:port.
        Lines starting with a '#' are ignored. if ignore_errors is nonzero(defualt: False), invalid lines are ignored!"""
    targets=[]
    with open(path,"rU") as f:
        for line in f:
            try:
                if line.startswith("#"):
                    continue
                if line.count(":")!=1:
                    raise SyntaxError("Error in line '{l}': expected exactly one ':'!".format(l=line))
                host,port=line.split(":")
                try:
                    port=int(port)
                except ValueError:
                    raise SyntaxError("Error in line '{l}': cannot convert port to int!".format(l=line))
                if len(host)==0:
                    raise SyntaxError("Error in line '{l}': invalid host format!".format(l=line))
                targets.append((host,port))
            except SyntaxError:
                if not ignore_errors:
                    raise
    return targets

if __name__=="__main__":
    #single-file code
    import argparse,os,sys#import here to remove overhead in import
    parser=argparse.ArgumentParser(description="A Load-Balancer")
    parser.add_argument("host",help="host/ip to bind to")
    parser.add_argument("port",type=int,help="port to bind to")
    parser.add_argument("-d",action="store_true",dest="debug",help="enable debug-mode")
    parser.add_argument("-p",action="store",dest="path",help="load target list from file",required=False,default=None)
    parser.add_argument("-f",action="store",dest="fallback",help="target to relay connections to if no server are aviable",required=False,default=None)
    parser.add_argument("-t",action="store",dest="targets",help="targets to spread connections to",required=False,default=None,nargs="+")
    ns=parser.parse_args()
    if ns.targets is None:
        targets=[]
    else:
        targets=[]
        for ta in ns.targets:
            if ta.count(":")!=1:
                print "SyntaxError in command-line target-list: expected exactly one ':'!"
                sys.exit(1)
            host,port=ta.split(":")
            if len(host)==0:
                print "SyntaxError in command-line target-list: invalid host!"
                sys.exit(1)
            try:
                port=int(port)
                if port<=0:
                    raise ValueError
            except ValueError:
                print "SyntaxError in command-line target-list: invalid port!"
                sys.exit(1)
            targets.append((host,port))
    if ns.fallback is not None:
        if ns.fallback.count(":")!=1:
            print "SyntaxError in fallback-argument: expected exactly one ':'!"
            sys.exit(1)
        host,port=ns.fallback.split(":")
        if len(host)==0:
            print "SyntaxError in fallback-argument: invalid host!"
            sys.exit(1)
        try:
            port=int(port)
            if port<=0:
                raise ValueError
        except ValueError:
            print "SyntaxError in fallback-argument: invalid port!"
            sys.exit(1)
        fallback=(host,port)
    else:
        fallback=None
    if ns.path is None:
        pass
    elif not os.path.isfile(ns.path):
        print "Error: File not found!"
        sys.exit(1)
    else:
        targets+=load_from_file(ns.path,False)
    if len(targets)==0:
        print "Error: no targets found!"
        sys.exit(1)
    lb=LoadBalancer(ns.host,ns.port,targets,fallback,None,ns.debug)
    try:
        lb.mainloop()
    except (KeyboardInterrupt,SystemExit) as e:
        pass
    finally:
        try:
            lb.stop()
        except RuntimeError as e:
            pass

    
