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

#install zeromq
wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
tar -xzf zeromq-2.1.7.tar.gz
cd zeromq-2.1.7
./configure
make
sudo make install

cd ../

#install jzmq (both native and into local maven cache)
git clone https://github.com/nathanmarz/jzmq.git
cd jzmq
./autogen.sh
./configure
make
sudo make install
