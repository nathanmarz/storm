---
layout: documentation
---
The native dependencies are only needed on actual Storm clusters. When running Storm in local mode, Storm uses a pure Java messaging system so that you don't need to install native dependencies on your development machine.

Installing ZeroMQ and JZMQ is usually straightforward. Sometimes, however, people run into issues with autoconf and get strange errors. If you run into any issues, please email the [Storm mailing list](http://groups.google.com/group/storm-user) or come get help in the #storm-user room on freenode. 

Storm has been tested with ZeroMQ 2.1.7, and this is the recommended ZeroMQ release that you install. You can download a ZeroMQ release [here](http://download.zeromq.org/). Installing ZeroMQ should look something like this:

```
wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
tar -xzf zeromq-2.1.7.tar.gz
cd zeromq-2.1.7
./configure
make
sudo make install
```

JZMQ is the Java bindings for ZeroMQ. JZMQ doesn't have any releases (we're working with them on that), so there is risk of a regression if you always install from the master branch. To prevent a regression from happening, you should instead install from [this fork](http://github.com/nathanmarz/jzmq) which is tested to work with Storm. Installing JZMQ should look something like this:

```
#install jzmq
git clone https://github.com/nathanmarz/jzmq.git
cd jzmq
./autogen.sh
./configure
make
sudo make install
```

To get the JZMQ build to work, you may need to do one or all of the following:

1. Set JAVA_HOME environment variable appropriately
2. Install Java dev package (more info [here](http://codeslinger.posterous.com/getting-zeromq-and-jzmq-running-on-mac-os-x) for Mac OSX users)
3. Upgrade autoconf on your machine
4. Follow the instructions in [this blog post](http://blog.pmorelli.com/getting-zeromq-and-jzmq-running-on-mac-os-x)

If you run into any errors when running `./configure`, [this thread](http://stackoverflow.com/questions/3522248/how-do-i-compile-jzmq-for-zeromq-on-osx) may provide a solution.
