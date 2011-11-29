AM_JAVACFLAGS=-classpath $(top_builddir)/src

jarfile = zmq-perf.jar

JZMQ_PERF_JAVA_FILES = \
	local_lat.java \
	local_thr.java \
	remote_lat.java \
	remote_thr.java

JZMQ_PERF_CLASS_FILES = \
	local_lat.class \
	local_thr.class \
	remote_lat.class \
	remote_thr.class

$(jarfile): $(dist_noinst_JAVA)
	$(JAR) cf $(JARFLAGS) $@ *.class

dist_noinst_DATA = $(jarfile)

dist_noinst_JAVA = $(JZMQ_PERF_JAVA_FILES)

BUILT_SOURCES = \
	$(JZMQ_PERF_JAVA_FILES)

CLEANFILES = \
	$(JZMQ_PERF_CLASS_FILES) \
	$(jarfile)
