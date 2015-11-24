#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import random
import subprocess as sub
import re
import shlex
try:
    # python 3
    from urllib.parse import quote_plus
except ImportError:
    # python 2
    from urllib import quote_plus
try:
    # python 3
    import configparser
except ImportError:
    # python 2
    import ConfigParser as configparser

def is_windows():
    return sys.platform.startswith('win')

def identity(x):
    return x

def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = sub.Popen(command,stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split(os.linesep)
    return lines[0]

def init_storm_env():
    global CLUSTER_CONF_DIR
    ini_file = os.path.join(CLUSTER_CONF_DIR, 'storm_env.ini')
    if not os.path.isfile(ini_file):
        return
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(ini_file)
    options = config.options('environment')
    for option in options:
        value = config.get('environment', option)
        os.environ[option] = value

normclasspath = cygpath if sys.platform == 'cygwin' else identity
STORM_DIR = os.sep.join(os.path.realpath( __file__ ).split(os.sep)[:-2])
USER_CONF_DIR = os.path.expanduser("~" + os.sep + ".storm")
STORM_CONF_DIR = os.getenv('STORM_CONF_DIR', None)

if STORM_CONF_DIR == None:
    CLUSTER_CONF_DIR = os.path.join(STORM_DIR, "conf")
else:
    CLUSTER_CONF_DIR = STORM_CONF_DIR

if (not os.path.isfile(os.path.join(USER_CONF_DIR, "storm.yaml"))):
    USER_CONF_DIR = CLUSTER_CONF_DIR

STORM_LIB_DIR = os.path.join(STORM_DIR, "lib")
STORM_BIN_DIR = os.path.join(STORM_DIR, "bin")
STORM_LOG4J2_CONF_DIR = os.path.join(STORM_DIR, "log4j2")
STORM_SUPERVISOR_LOG_FILE = os.getenv('STORM_SUPERVISOR_LOG_FILE', "supervisor.log")

init_storm_env()

CONFIG_OPTS = []
CONFFILE = ""
JAR_JVM_OPTS = shlex.split(os.getenv('STORM_JAR_JVM_OPTS', ''))
JAVA_HOME = os.getenv('JAVA_HOME', None)
JAVA_CMD = 'java' if not JAVA_HOME else os.path.join(JAVA_HOME, 'bin', 'java')
if JAVA_HOME and not os.path.exists(JAVA_CMD):
    print "ERROR:  JAVA_HOME is invalid.  Could not find bin/java at %s." % JAVA_HOME
    sys.exit(1)
STORM_EXT_CLASSPATH = os.getenv('STORM_EXT_CLASSPATH', None)
STORM_EXT_CLASSPATH_DAEMON = os.getenv('STORM_EXT_CLASSPATH_DAEMON', None)

def get_config_opts():
    global CONFIG_OPTS
    return "-Dstorm.options=" + ','.join(map(quote_plus,CONFIG_OPTS))

if not os.path.exists(STORM_LIB_DIR):
    print("******************************************")
    print("The storm client can only be run from within a release. You appear to be trying to run the client from a checkout of Storm's source code.")
    print("\nYou can download a Storm release at http://storm-project.net/downloads.html")
    print("******************************************")
    sys.exit(1)

def get_jars_full(adir):
    files = []
    if os.path.isdir(adir):
        files = os.listdir(adir)
    elif os.path.exists(adir):
        files = [aidr]

    ret = []
    for f in files:
        if f.endswith(".jar"):
            ret.append(os.path.join(adir, f))
    return ret

def get_classpath(extrajars, daemon=True):
    ret = get_jars_full(STORM_DIR)
    ret.extend(get_jars_full(STORM_DIR + "/lib"))
    ret.extend(get_jars_full(STORM_DIR + "/extlib"))
    if daemon:
        ret.extend(get_jars_full(STORM_DIR + "/extlib-daemon"))
    if STORM_EXT_CLASSPATH != None:
        for path in STORM_EXT_CLASSPATH.split(os.pathsep):
            ret.extend(get_jars_full(path))
    if daemon and STORM_EXT_CLASSPATH_DAEMON != None:
        for path in STORM_EXT_CLASSPATH_DAEMON.split(os.pathsep):
            ret.extend(get_jars_full(path))
    ret.extend(extrajars)
    return normclasspath(os.pathsep.join(ret))

def confvalue(name, extrapaths, daemon=True):
    global CONFFILE
    command = [
        JAVA_CMD, "-client", get_config_opts(), "-Dstorm.conf.file=" + CONFFILE,
        "-cp", get_classpath(extrapaths, daemon), "backtype.storm.command.config_value", name
    ]
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    # python 3
    if not isinstance(output, str):
        output = output.decode('utf-8')
    lines = output.split(os.linesep)
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    return ""

def print_localconfvalue(name):
    """Syntax: [storm localconfvalue conf-name]

    Prints out the value for conf-name in the local Storm configs.
    The local Storm configs are the ones in ~/.storm/storm.yaml merged
    in with the configs in defaults.yaml.
    """
    print(name + ": " + confvalue(name, [USER_CONF_DIR]))

def print_remoteconfvalue(name):
    """Syntax: [storm remoteconfvalue conf-name]

    Prints out the value for conf-name in the cluster's Storm configs.
    The cluster's Storm configs are the ones in $STORM-PATH/conf/storm.yaml
    merged in with the configs in defaults.yaml.

    This command must be run on a cluster machine.
    """
    print(name + ": " + confvalue(name, [CLUSTER_CONF_DIR]))

def parse_args(string):
    r"""Takes a string of whitespace-separated tokens and parses it into a list.
    Whitespace inside tokens may be quoted with single quotes, double quotes or
    backslash (similar to command-line arguments in bash).

    >>> parse_args(r'''"a a" 'b b' c\ c "d'd" 'e"e' 'f\'f' "g\"g" "i""i" 'j''j' k" "k l' l' mm n\\n''')
    ['a a', 'b b', 'c c', "d'd", 'e"e', "f'f", 'g"g', 'ii', 'jj', 'k k', 'l l', 'mm', r'n\n']
    """
    re_split = re.compile(r'''((?:
        [^\s"'\\] |
        "(?: [^"\\] | \\.)*" |
        '(?: [^'\\] | \\.)*' |
        \\.
    )+)''', re.VERBOSE)
    args = re_split.split(string)[1::2]
    args = [re.compile(r'"((?:[^"\\]|\\.)*)"').sub('\\1', x) for x in args]
    args = [re.compile(r"'((?:[^'\\]|\\.)*)'").sub('\\1', x) for x in args]
    return [re.compile(r'\\(.)').sub('\\1', x) for x in args]

def exec_storm_class(klass, jvmtype="-server", jvmopts=[], extrajars=[], args=[], fork=False, daemon=True, daemonName=""):
    global CONFFILE
    storm_log_dir = confvalue("storm.log.dir",[CLUSTER_CONF_DIR])
    if(storm_log_dir == None or storm_log_dir == "nil"):
        storm_log_dir = os.path.join(STORM_DIR, "logs")
    all_args = [
        JAVA_CMD, jvmtype,
        "-Ddaemon.name=" + daemonName,
        get_config_opts(),
        "-Dstorm.home=" + STORM_DIR,
        "-Dstorm.log.dir=" + storm_log_dir,
        "-Djava.library.path=" + confvalue("java.library.path", extrajars, daemon),
        "-Dstorm.conf.file=" + CONFFILE,
        "-cp", get_classpath(extrajars, daemon),
    ] + jvmopts + [klass] + list(args)
    print("Running: " + " ".join(all_args))
    if fork:
        os.spawnvp(os.P_WAIT, JAVA_CMD, all_args)
    elif is_windows():
        # handling whitespaces in JAVA_CMD
        sub.call(all_args)
    else:
        os.execvp(JAVA_CMD, all_args)

def jar(jarfile, klass, *args):
    """Syntax: [storm jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments.
    The storm jars and configs in ~/.storm are put on the classpath.
    The process is configured so that StormSubmitter
    (http://storm.apache.org/apidocs/backtype/storm/StormSubmitter.html)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    exec_storm_class(
        klass,
        jvmtype="-client",
        extrajars=[jarfile, USER_CONF_DIR, STORM_BIN_DIR],
        args=args,
        daemon=False,
        jvmopts=JAR_JVM_OPTS + ["-Dstorm.jar=" + jarfile])

def kill(*args):
    """Syntax: [storm kill topology-name [-w wait-time-secs]]

    Kills the topology with the name topology-name. Storm will
    first deactivate the topology's spouts for the duration of
    the topology's message timeout to allow all messages currently
    being processed to finish processing. Storm will then shutdown
    the workers and clean up their state. You can override the length
    of time Storm waits between deactivation and shutdown with the -w flag.
    """
    if not args:
        print_usage(command="kill")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.kill_topology",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])


def upload_credentials(*args):
    """Syntax: [storm upload_credentials topology-name [credkey credvalue]*]

    Uploads a new set of credentials to a running topology
    """
    if not args:
        print_usage(command="upload_credentials")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.upload_credentials",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def heartbeats(*args):
    """Syntax: [storm heartbeats [cmd]]

    list PATH - lists heartbeats nodes under PATH currently in the ClusterState.
    get  PATH - Get the heartbeat data at PATH
    """
    exec_storm_class(
        "backtype.storm.command.heartbeats",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])
    
def activate(*args):
    """Syntax: [storm activate topology-name]

    Activates the specified topology's spouts.
    """
    if not args:
        print_usage(command="activate")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.activate",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def set_log_level(*args):
    """
    Dynamically change topology log levels

    Syntax: [storm set_log_level -l [logger name]=[log level][:optional timeout] -r [logger name]
    where log level is one of:
        ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
    and timeout is integer seconds.

    e.g.
        ./bin/storm set_log_level -l ROOT=DEBUG:30

        Set the root logger's level to DEBUG for 30 seconds

        ./bin/storm set_log_level -l com.myapp=WARN

        Set the com.myapp logger's level to WARN for 30 seconds

        ./bin/storm set_log_level -l com.myapp=WARN -l com.myOtherLogger=ERROR:123

        Set the com.myapp logger's level to WARN indifinitely, and com.myOtherLogger
        to ERROR for 123 seconds

        ./bin/storm set_log_level -r com.myOtherLogger

        Clears settings, resetting back to the original level
    """
    exec_storm_class(
        "backtype.storm.command.set_log_level",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def listtopos(*args):
    """Syntax: [storm list]

    List the running topologies and their statuses.
    """
    exec_storm_class(
        "backtype.storm.command.list",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def deactivate(*args):
    """Syntax: [storm deactivate topology-name]

    Deactivates the specified topology's spouts.
    """
    if not args:
        print_usage(command="deactivate")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.deactivate",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def rebalance(*args):
    """Syntax: [storm rebalance topology-name [-w wait-time-secs] [-n new-num-workers] [-e component=parallelism]*]

    Sometimes you may wish to spread out where the workers for a topology
    are running. For example, let's say you have a 10 node cluster running
    4 workers per node, and then let's say you add another 10 nodes to
    the cluster. You may wish to have Storm spread out the workers for the
    running topology so that each node runs 2 workers. One way to do this
    is to kill the topology and resubmit it, but Storm provides a "rebalance"
    command that provides an easier way to do this.

    Rebalance will first deactivate the topology for the duration of the
    message timeout (overridable with the -w flag) and then redistribute
    the workers evenly around the cluster. The topology will then return to
    its previous state of activation (so a deactivated topology will still
    be deactivated and an activated topology will go back to being activated).

    The rebalance command can also be used to change the parallelism of a running topology.
    Use the -n and -e switches to change the number of workers or number of executors of a component
    respectively.
    """
    if not args:
        print_usage(command="rebalance")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.rebalance",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def get_errors(*args):
    """Syntax: [storm get-errors topology-name]

    Get the latest error from the running topology. The returned result contains
    the key value pairs for component-name and component-error for the components in error.
    The result is returned in json format.
    """
    if not args:
        print_usage(command="get_errors")
        sys.exit(2)
    exec_storm_class(
        "backtype.storm.command.get_errors",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR, "bin")])

def healthcheck(*args):
    """Syntax: [storm node-health-check]

    Run health checks on the local supervisor.
    """
    exec_storm_class(
        "backtype.storm.command.healthcheck",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR, "bin")])

def kill_workers(*args):
    """Syntax: [storm kill_workers]

    Kill the workers running on this supervisor. This command should be run
    on a supervisor node. If the cluster is running in secure mode, then user needs
    to have admin rights on the node to be able to successfully kill all workers.
    """
    exec_storm_class(
        "backtype.storm.command.kill_workers",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR, "bin")])

def shell(resourcesdir, command, *args):
    tmpjarpath = "stormshell" + str(random.randint(0, 10000000)) + ".jar"
    os.system("jar cf %s %s" % (tmpjarpath, resourcesdir))
    runnerargs = [tmpjarpath, command]
    runnerargs.extend(args)
    exec_storm_class(
        "backtype.storm.command.shell_submission",
        args=runnerargs,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR],
        fork=True)
    os.system("rm " + tmpjarpath)

def repl():
    """Syntax: [storm repl]

    Opens up a Clojure REPL with the storm jars and configuration
    on the classpath. Useful for debugging.
    """
    cppaths = [CLUSTER_CONF_DIR]
    exec_storm_class("clojure.main", jvmtype="-client", extrajars=cppaths)

def get_log4j2_conf_dir():
    cppaths = [CLUSTER_CONF_DIR]
    storm_log4j2_conf_dir = confvalue("storm.log4j2.conf.dir", cppaths)
    if(storm_log4j2_conf_dir == None or storm_log4j2_conf_dir == "nil"):
        storm_log4j2_conf_dir = STORM_LOG4J2_CONF_DIR
    elif(not os.path.isabs(storm_log4j2_conf_dir)):
        storm_log4j2_conf_dir = os.path.join(STORM_DIR, storm_log4j2_conf_dir)
    return storm_log4j2_conf_dir

def nimbus(klass="backtype.storm.daemon.nimbus"):
    """Syntax: [storm nimbus]

    Launches the nimbus daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("nimbus.childopts", cppaths)) + [
        "-Dlogfile.name=nimbus.log",
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml"),
    ]
    exec_storm_class(
        klass,
        jvmtype="-server",
        daemonName="nimbus",
        extrajars=cppaths,
        jvmopts=jvmopts)

def pacemaker(klass="org.apache.storm.pacemaker.pacemaker"):
    """Syntax: [storm pacemaker]

    Launches the Pacemaker daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("pacemaker.childopts", cppaths)) + [
        "-Dlogfile.name=pacemaker.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml"),
    ]
    exec_storm_class(
        klass,
        jvmtype="-server",
        daemonName="pacemaker",
        extrajars=cppaths,
        jvmopts=jvmopts)

def supervisor(klass="backtype.storm.daemon.supervisor"):
    """Syntax: [storm supervisor]

    Launches the supervisor daemon. This command should be run
    under supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("supervisor.childopts", cppaths)) + [
        "-Dlogfile.name=" + STORM_SUPERVISOR_LOG_FILE,
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml"),
    ]
    exec_storm_class(
        klass,
        jvmtype="-server",
        daemonName="supervisor",
        extrajars=cppaths,
        jvmopts=jvmopts)

def ui():
    """Syntax: [storm ui]

    Launches the UI daemon. The UI provides a web interface for a Storm
    cluster and shows detailed stats about running topologies. This command
    should be run under supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("ui.childopts", cppaths)) + [
        "-Dlogfile.name=ui.log",
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml")
    ]
    exec_storm_class(
        "backtype.storm.ui.core",
        jvmtype="-server",
        daemonName="ui",
        jvmopts=jvmopts,
        extrajars=[STORM_DIR, CLUSTER_CONF_DIR])

def logviewer():
    """Syntax: [storm logviewer]

    Launches the log viewer daemon. It provides a web interface for viewing
    storm log files. This command should be run under supervision with a
    tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("logviewer.childopts", cppaths)) + [
        "-Dlogfile.name=logviewer.log",
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml")
    ]
    exec_storm_class(
        "backtype.storm.daemon.logviewer",
        jvmtype="-server",
        daemonName="logviewer",
        jvmopts=jvmopts,
        extrajars=[STORM_DIR, CLUSTER_CONF_DIR])

def drpc():
    """Syntax: [storm drpc]

    Launches a DRPC daemon. This command should be run under supervision
    with a tool like daemontools or monit.

    See Distributed RPC for more information.
    (http://storm.apache.org/documentation/Distributed-RPC)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("drpc.childopts", cppaths)) + [
        "-Dlogfile.name=drpc.log",
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(), "cluster.xml")
    ]
    exec_storm_class(
        "backtype.storm.daemon.drpc",
        jvmtype="-server",
        daemonName="drpc",
        jvmopts=jvmopts,
        extrajars=[CLUSTER_CONF_DIR])

def dev_zookeeper():
    """Syntax: [storm dev-zookeeper]

    Launches a fresh Zookeeper server using "dev.zookeeper.path" as its local dir and
    "storm.zookeeper.port" as its port. This is only intended for development/testing, the
    Zookeeper instance launched is not configured to be used in production.
    """
    cppaths = [CLUSTER_CONF_DIR]
    exec_storm_class(
        "backtype.storm.command.dev_zookeeper",
        jvmtype="-server",
        extrajars=[CLUSTER_CONF_DIR])

def version():
  """Syntax: [storm version]

  Prints the version number of this Storm release.
  """
  cppaths = [CLUSTER_CONF_DIR]
  exec_storm_class(
       "backtype.storm.utils.VersionInfo",
       jvmtype="-client",
       extrajars=[CLUSTER_CONF_DIR])

def print_classpath():
    """Syntax: [storm classpath]

    Prints the classpath used by the storm client when running commands.
    """
    print(get_classpath([]))

def monitor(*args):
    """Syntax: [storm monitor topology-name [-i interval-secs] [-m component-id] [-s stream-id] [-w [emitted | transferred]]]

    Monitor given topology's throughput interactively.
    One can specify poll-interval, component-id, stream-id, watch-item[emitted | transferred]
    By default,
        poll-interval is 4 seconds;
        all component-ids will be list;
        stream-id is 'default';
        watch-item is 'emitted';
    """
    exec_storm_class(
        "backtype.storm.command.monitor",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])


def print_commands():
    """Print all client commands and link to documentation"""
    print("Commands:\n\t" +  "\n\t".join(sorted(COMMANDS.keys())))
    print("\nHelp: \n\thelp \n\thelp <command>")
    print("\nDocumentation for the storm client can be found at http://storm.apache.org/documentation/Command-line-client.html\n")
    print("Configs can be overridden using one or more -c flags, e.g. \"storm list -c nimbus.host=nimbus.mycompany.com\"\n")

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if command in COMMANDS:
            print(COMMANDS[command].__doc__ or
                  "No documentation provided for <%s>" % command)
        else:
           print("<%s> is not a valid command" % command)
    else:
        print_commands()

def unknown_command(*args):
    print("Unknown command: [storm %s]" % ' '.join(sys.argv[1:]))
    print_usage()
    sys.exit(254)

COMMANDS = {"jar": jar, "kill": kill, "shell": shell, "nimbus": nimbus, "ui": ui, "logviewer": logviewer,
            "drpc": drpc, "supervisor": supervisor, "localconfvalue": print_localconfvalue,
            "remoteconfvalue": print_remoteconfvalue, "repl": repl, "classpath": print_classpath,
            "activate": activate, "deactivate": deactivate, "rebalance": rebalance, "help": print_usage,
            "list": listtopos, "dev-zookeeper": dev_zookeeper, "version": version, "monitor": monitor,
            "upload-credentials": upload_credentials, "pacemaker": pacemaker, "heartbeats": heartbeats,
            "get-errors": get_errors, "set_log_level": set_log_level, "kill_workers": kill_workers,
            "node-health-check": healthcheck}

def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)

def parse_config_opts(args):
  curr = args[:]
  curr.reverse()
  config_list = []
  args_list = []

  while len(curr) > 0:
    token = curr.pop()
    if token == "-c":
      config_list.append(curr.pop())
    elif token == "--config":
      global CONFFILE
      CONFFILE = curr.pop()
    else:
      args_list.append(token)

  return config_list, args_list

def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    global CONFIG_OPTS
    config_list, args = parse_config_opts(sys.argv[1:])
    parse_config(config_list)
    COMMAND = args[0]
    ARGS = args[1:]
    (COMMANDS.get(COMMAND, unknown_command))(*ARGS)

if __name__ == "__main__":
    main()
