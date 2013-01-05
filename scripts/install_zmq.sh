#!/bin/bash

if [ -z "$JAVA_HOME" ]; then
	if [ -d /usr/libexec/java_home/include ]; then
		JAVA_HOME=/usr/libexec/java_home
	elif [ -d /usr/lib/jvm/java/include ]; then
		JAVA_HOME=/usr/lib/jvm/java
	elif [ -d /usr/lib/jvm/default-java/include ]; then
		JAVA_HOME=/usr/lib/jvm/default-java
	fi
fi

if [ ! -d "$JAVA_HOME/include" ]; then
    echo "
Looks like you're missing your 'include' directory. If you're using Mac OS X, You'll need to install the Java dev package.

- Navigate to http://goo.gl/D8lI
- Click the Java tab on the right
- Install the appropriate version and try again.
"
    exit -1;
fi

export JAVA_HOME

#
# If ROOT_INSTALL != 1, then dependencies are installed to
# <topdir>/tmp
#
ROOT_INSTALL=${ROOT_INSTALL:-1}

TOPDIR="$PWD"
WORKDIR="$TOPDIR/tmp"

mkdir -p $WORKDIR/src

#install zeromq
cd $WORKDIR/src && \
    wget -q http://download.zeromq.org/zeromq-2.1.7.tar.gz && \
    tar -xzf zeromq-2.1.7.tar.gz && \
    cd zeromq-2.1.7 || exit 1

if [ "$ROOT_INSTALL" == "1" ]; then
	./configure && make && sudo make install
else
	./configure --prefix $WORKDIR/zeromq-2.1.7 && \
	    make && make install
fi

if [ $? -ne 0 ]; then
	echo "Failed to build zeromq"
	exit 1
fi

#install jzmq (both native and into local maven cache)
cd $WORKDIR/src && \
    git clone -q https://github.com/nathanmarz/jzmq.git && \
    cd jzmq && ./autogen.sh || exit 1

if [ "$ROOT_INSTALL" == "1" ]; then
	./configure && make && sudo make install
else
	./configure --prefix $WORKDIR/jzmq --with-zeromq=$WORKDIR/zeromq-2.1.7 && \
	    make && make install
fi

if [ $? -ne 0 ]; then
	echo "Failed to build jzmq"
	exit 1
fi
