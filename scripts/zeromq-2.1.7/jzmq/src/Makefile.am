jarfile = zmq.jar
jardir = $(datadir)/java

JZMQ_JAVA_FILES = \
	org/zeromq/ZMQ.java \
	org/zeromq/ZMQException.java \
	org/zeromq/ZMQQueue.java \
	org/zeromq/ZMQForwarder.java \
	org/zeromq/ZMQStreamer.java

JZMQ_CPP_FILES = \
	ZMQ.cpp \
	Context.cpp \
	Socket.cpp \
	Poller.cpp \
	util.cpp

JZMQ_H_FILES = \
	org_zeromq_ZMQ.h \
	org_zeromq_ZMQ_Error.h \
	org_zeromq_ZMQ_Context.h \
	org_zeromq_ZMQ_Socket.h \
	org_zeromq_ZMQ_Poller.h

JZMQ_HPP_FILES = \
	util.hpp

JZMQ_CLASS_FILES = \
	org/zeromq/ZMQ.class \
	org/zeromq/ZMQ$$Context.class \
	org/zeromq/ZMQ$$Socket.class \
	org/zeromq/ZMQ$$Poller.class \
	org/zeromq/ZMQ$$Error.class \
	org/zeromq/ZMQException.class \
	org/zeromq/ZMQQueue.class \
	org/zeromq/ZMQForwarder.class \
	org/zeromq/ZMQStreamer.class

$(jarfile): $(dist_noinst_JAVA)
	$(JAR) cf $(JARFLAGS) $@ org/zeromq/*.class

jar_DATA = $(jarfile)

dist_noinst_JAVA = $(JZMQ_JAVA_FILES)

lib_LTLIBRARIES = libjzmq.la
libjzmq_la_SOURCES = $(JZMQ_CPP_FILES)

nodist_libjzmq_la_SOURCES = \
	$(JZMQ_H_FILES) \
	$(JZMQ_HPP_FILES)

libjzmq_la_CXXFLAGS = -Wall
libjzmq_la_LDFLAGS = -version-info @JLTVER@

BUILT_SOURCES = \
	$(JZMQ_H_FILES) \
	$(JZMQ_CLASS_FILES)

CLEANFILES = \
	$(JZMQ_H_FILES) \
	$(JZMQ_CLASS_FILES) \
	$(jarfile)

$(JZMQ_H_FILES): org/zeromq/ZMQ.class
	$(CLASSPATH_ENV) $(JAVAH) -jni -classpath . org.zeromq.ZMQ

./org/zeromq/ZMQ.class: classdist_noinst.stamp

$(srcdir)/ZMQ.cpp: \
	$(JZMQ_H_FILES) \
	$(JZMQ_HPP_FILES)

$(srcdir)/Context.cpp: \
	org_zeromq_ZMQ_Context.h \
	$(JZMQ_HPP_FILES)

$(srcdir)/Socket.cpp: \
	org_zeromq_ZMQ_Socket.h \
	$(JZMQ_HPP_FILES)

$(srcdir)/Poller.cpp: \
	org_zeromq_ZMQ_Poller.h \
	$(JZMQ_HPP_FILES)

$(srcdir)/util.cpp: \
	$(JZMQ_HPP_FILES)

dist-hook:
	-rm $(distdir)/*.h
