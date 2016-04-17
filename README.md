# load_balancer.py - a pure-python single-file load balancer
load_balancer.py is a pure-python (runs everywhere python can run) single-file (you just need to copy a single file, no setup required) load balancer/spreader (i dont know where the diference is, correct me if i am wrong). It does not have any dependencies other than the standard library.
#Features
-tested with CPython 2.7.10 and Pypy 4.0.1 (probaly wont work with Cpython 3.X)
-should work with any Python interpreter providing socket, select.select and errno and using the 2.7.X syntax
-should be platform independent (however currently only tested on windows, tell me if you find any problems)
-command-line interface
-module interface
-silent until debug is set to True on __init__ or in command-line
-no dependencies
-open-source
-seemlessly auto-reconnects to fallback-server on connection failure
-buffers messages up to 65536 bytes on server. This allows the server-network to dispatch data quickly (internal network can deliever data to the balancer very quickly) while using the network-speed of the balancer ideal.
-asynchronous. No Threads are used, internal loop only executes when required. This keeps the resource usage low and the performance high.
-no disk usage (except python loading this script). The disk is avaible for other programs.
-works with both UDP and TCP (actually any socket type, however the commandline-interface always use TCP. Use the module-interface for other types)
-works with any address type (however the commandline-interface always use IPv4. Use the module interface for other families)
#Usage (Command line)
python load_balancer.py [-h] [-d] [-p PATH] [-f FALLBACK] [-t TARGETS [TARGETS ...]] host port

Arguments:
-h: print help and exit
-d: enable debug mode (print messages)
-p PATH: load a list of servers to spread load to from PATH. Each line must consist of HOST:PORT. Lines starting with "#" are ignored.
-f FALLBACK: when a connection to a server fails (or closes) or no targets are defined, reconnect the client seemlessly to fallback server. FALLBACK must consist of HOST:PORT. Connections are closed when fallback fails (including no fallback-server configured)
-t PATH PATH PATH...: add target-servers per command. Each parameter must consist of HOST:PORT. When used with the -p option, both are used.
host: host to bind to
port: port to bind to

#Usage (module)
You only need to import load_balancer and are able to use the LoadBalancer class. Call .mainloop() to start the balancer.
For more informations, take a look at the source code or use help(load_balancer) in the interpreter after importing it.

#Credits
See source-code

#Contribute
See source-code
