#!/bin/bash
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

JDKPATH=$JAVA_HOME
BINPATH="/usr/bin"
USER=`whoami`

#SETTINGS=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home/jre/lib/jfr/profile.jfc
SETTINGS=profile

platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    platform='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
    platform='darwin'
elif [[ "$unamestr" == 'FreeBSD' ]]; then
    platform='freebsd'
fi

if [[ $platform == 'linux' ]]; then
    BINPATH="$JDKPATH/bin"
elif [[ $platform == 'darwin' ]]; then
    BINPATH="/usr/bin"
fi

function start_record {
    # start_record pid
    already_recording=false
    for rid in `get_recording_ids $1`; do
        already_recording=true
        break;
    done
    if [ "$already_recording" = false ]; then
        $BINPATH/jcmd $1 JFR.start settings=${SETTINGS}
    fi
}

function dump_record {
    for rid in `get_recording_ids $1`; do
        FILENAME=recording-$1-${rid}-${NOW}.jfr
        $BINPATH/jcmd $1 JFR.dump recording=$rid filename="$2/${FILENAME}"
    done
}

function jstack_record {
    FILENAME=jstack-$1-${NOW}.txt
    $BINPATH/jstack $1 > "$2/${FILENAME}" 2>&1
}

function jmap_record {
    FILENAME=recording-$1-${NOW}.bin
    $BINPATH/jmap -dump:format=b,file="$2/${FILENAME}" $1
}

function stop_record {
    for rid in `get_recording_ids $1`; do
        FILENAME=recording-$1-${rid}-${NOW}.jfr
        $BINPATH/jcmd $1 JFR.dump recording=$rid filename="$2/${FILENAME}"
        $BINPATH/jcmd $1 JFR.stop recording=$rid
    done
}

function get_recording_ids {
    $BINPATH/jcmd $1 JFR.check | perl -n -e '/recording=([0-9]+)/ && print "$1 "'
}

function usage_and_quit {
    echo "Usage: $0 pid start [profile_settings]"
    echo "       $0 pid dump target_dir"
    echo "       $0 pid stop target_dir"
    echo "       $0 pid jstack target_dir"
    echo "       $0 pid jmap target_dir"
    echo "       $0 pid kill"
    exit -1
}

# Before using this script: make sure FlightRecorder is enabled

if [ "$#" -le 1 ]; then
    echo "Wrong number of arguments.."
    usage_and_quit

fi
# call this script with the process pid, example: "./flight PID start" or "./flight PID stop"
PID="$1"
CMD="$2"

if /bin/ps -p $PID > /dev/null
then
    if [[ $platform == 'linux' ]]; then
        USER=`/bin/ps -ouser --noheader $PID`
    elif [[ $platform == 'darwin' ]]; then
        USER=`/bin/ps -ouser $PID`
    fi
else
    echo "No such pid running: $PID"
    usage_and_quit
fi

if [ "$CMD" != "start" ] && [ "$CMD" != "kill" ]; then
    if [[ $3 ]] && [[ -d $3 ]]
    then
        TARGETDIR="$3"
        mkdir -p ${TARGETDIR}
    else
        echo "Missing target directory"
        usage_and_quit
    fi
fi

NOW=`date +'%Y%m%d-%H%M%S'`
if [ "$CMD" = "" ]; then
    usage_and_quit
elif [ "$CMD" = "kill" ]; then
    echo "Killing process with pid: $PID"
    kill -9 ${PID}
elif [ "$CMD" = "start" ]; then
    if [[ $3 ]]
    then
        SETTINGS=$3
    fi
    start_record ${PID}
elif [ "$CMD" = "stop" ]; then
    echo "Capturing dump before stopping in dir $TARGETDIR"
    stop_record ${PID} ${TARGETDIR}
elif [ "$CMD" = "jstack" ]; then
    echo "Capturing dump in dir $TARGETDIR"
    jstack_record ${PID} ${TARGETDIR}
elif [ "$CMD" = "jmap" ]; then
    echo "Capturing dump in dir $TARGETDIR"
    jmap_record ${PID} ${TARGETDIR}
elif [ "$CMD" = "dump" ]; then
    echo "Capturing dump in dir $TARGETDIR"
    dump_record ${PID} ${TARGETDIR}
else
    usage_and_quit
fi


