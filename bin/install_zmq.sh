#!/bin/bash
export JAVA_HOME=${JAVA_HOME:/usr/libexec/java_home} 

zeromq="zeromq-2.1.7"

if [ ! -d "$JAVA_HOME/include" ]; then
    echo "
Looks like you're missing your 'include' directory. If you're using Mac OS X, You'll need to install the Java dev package.

- Navigate to http://goo.gl/D8lI
- Click the Java tab on the right
- Install the appropriate version and try again.
"
    exit -1;
fi
if [ ! -f ${zeromq}.tar.gz ]; then
  downloader="wget"
  if [ "`which wget`" == "" ]; then
    if [ "`which curl`" == "" ]; then
      echo "
Neither wget nor curl found; please install one of these programs.
"
      exit -1;
    else
      downloader="curl -O"
    fi
  fi
  ${downloader} http://download.zeromq.org/${zeromq}.tar.gz
  if [ ! -f ${zeromq}.tar.gz ]; then
    echo "
A problem occurred while downloading ${zeromq}.
"
    exit -1;
  fi
fi
if [ ! -d $zeromq ]; then
  tar -xzf ${zeromq}.tar.gz
fi
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
