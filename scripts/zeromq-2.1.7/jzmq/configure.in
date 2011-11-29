#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
# AC_PREREQ(2.61)
#
# Change the version number below after doing a public release.
# The version in git should reflect the *next* version planned.
# Version must be MAJOR.MINOR.PATCH otherwise things will break.
#
AC_INIT([jzmq],[2.1.0],[zeromq-dev@lists.zeromq.org])

AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR(config)
AM_CONFIG_HEADER(src/config.hpp)
AM_INIT_AUTOMAKE(tar-ustar)

#
# Libtool -version-info (ABI version)
#
# Currently 0.0.0 ("unstable"). Don't change this unless you 
# know exactly what you're doing and have read and understand
# http://www.gnu.org/software/libtool/manual/html_node/Updating-version-info.html
#
# libjzmq -version-info
JLTVER="0:0:0"
AC_SUBST(JLTVER)

# Checks for programs.
AC_PROG_LIBTOOL
# AC_PROG_SED
AC_PROG_AWK
AM_PROG_CC_C_O
AC_PROG_CXX
AC_LANG(C++)

# Set default CPPFLAGS for reentrant code
CPPFLAGS="-D_REENTRANT -D_THREAD_SAFE $CPPFLAGS"

# Check for zeromq library
zeromq_prefix=detect
AC_ARG_WITH([zeromq], 
    [AS_HELP_STRING([--with-zeromq=PREFIX], 
        [build with ZeroMQ library installed in PREFIX [default=autodetect]])],
    [case "x$withval" in
        xno)
            AC_MSG_ERROR([jzmq requires the ZeroMQ library])
            ;;
        xyes|x)
            ;;
        *)
            CPPFLAGS="$CPPFLAGS -I${withval}/include"
            LDFLAGS="$LDFLAGS -L${withval}/lib"
            zeromq_prefix=${withval}
            ;;
    esac ]
)
if test "x$zeromq_prefix" = "xdetect"; then
    PKG_CHECK_MODULES(
        [ZeroMQ], [libzmq], [zeromq_prefix=pkgconfig], [zeromq_prefix=])
        if test "x$zeromq_prefix" = "xpkgconfig"; then
            CPPFLAGS="$CPPFLAGS ${ZeroMQ_CFLAGS}"
            LDFLAGS="$LDFLAGS ${ZeroMQ_LIBS}"
        fi
fi
AC_CHECK_HEADER([zmq.h], [], 
    [AC_MSG_ERROR([cannot find zmq.h])])
AC_CHECK_LIB([zmq], [zmq_init], [],
    [AC_MSG_ERROR([cannot link with -lzmq])])

# Check for JDK
if test "x$JAVA_HOME" = "x"; then
    AC_MSG_ERROR([the JAVA_HOME environment variable must be set to your JDK location.]);
fi
AC_PATH_PROG(JAVAC, javac, [no], [$PATH:$JAVA_HOME/bin])
if test "x$JAVAC" = "xno"; then
    AC_MSG_ERROR([cannot find javac.]);
fi
AC_PATH_PROG(JAVAH, javah, [no], [$PATH:$JAVA_HOME/bin])
if test "x$JAVAH" = "xno"; then
    AC_MSG_ERROR([cannot find javah.]);
fi
AC_PATH_PROG(JAR, jar, [no], [$PATH:$JAVA_HOME/bin])
if test "x$JAR" = "xno"; then
    AC_MSG_ERROR([cannot find jar.]);
fi

# Check for JNI headers
AC_MSG_CHECKING([for jni.h in $JAVA_HOME/include])
if test -f $JAVA_HOME/include/jni.h; then
    AC_MSG_RESULT([yes])
else
    AC_MSG_ERROR([cannot find jni.h in $JAVA_HOME/include.]);
fi
CPPFLAGS="$CPPFLAGS -I${JAVA_HOME}/include"

# Need host-specific path for jni_md.h on some systems, this is braindead
AC_CANONICAL_HOST
case "${host_os}" in
    *solaris*)
        CPPFLAGS="$CPPFLAGS -I${JAVA_HOME}/include/solaris"
    ;;
    *openbsd*)
        CPPFLAGS="$CPPFLAGS -I${JAVA_HOME}/include/openbsd"
    ;;
    *linux*)
        CPPFLAGS="$CPPFLAGS -I${JAVA_HOME}/include/linux"
    ;;
    *darwin*)
        # Darwin does not need an extra -I path
    ;;
    *)
        AC_MSG_ERROR([don't know how to find jni_md.h on this platform])
    ;;
esac

JAVAROOT=.
AC_SUBST(JAVAROOT)

AC_OUTPUT(Makefile src/Makefile perf/Makefile)

#  On Linux patch libtool to delete hardcoded paths (rpath).
case "${host_os}" in
    *linux*)
        sed < libtool > libtool-2 \
        's/^hardcode_libdir_flag_spec.*$'/'hardcode_libdir_flag_spec=" "/'
        mv libtool-2 libtool
        chmod 755 libtool
        ;;
esac

