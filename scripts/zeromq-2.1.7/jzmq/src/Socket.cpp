/*
    Copyright (c) 2007-2010 iMatix Corporation

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the Lesser GNU General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    Lesser GNU General Public License for more details.

    You should have received a copy of the Lesser GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <assert.h>
#include <string.h>

#include <zmq.h>

#include "jzmq.hpp"
#include "util.hpp"
#include "org_zeromq_ZMQ_Socket.h"

/** Handle to Java's Socket::socketHandle. */
static jfieldID socket_handle_fid = NULL;


static void ensure_socket (JNIEnv *env,
                           jobject obj);
static void *get_socket (JNIEnv *env,
                         jobject obj,
                         int do_assert);
static void put_socket (JNIEnv *env,
                        jobject obj,
                        void *s);
static void *fetch_context (JNIEnv *env,
                            jobject context);


/**
 * Called to construct a Java Socket object.
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_construct (JNIEnv *env,
                                                                  jobject obj,
                                                                  jobject context,
                                                                  jint type)
{
    void *s = get_socket (env, obj, 0);
    if (s)
        return;

    void *c = fetch_context (env, context);
    if (c == NULL) {
        raise_exception (env, EINVAL);
        return;
    }

    s = zmq_socket (c, type);
    int err = zmq_errno();
    put_socket(env, obj, s);

    if (s == NULL) {
        raise_exception (env, err);
        return;
    }
}

/**
 * Called to destroy a Java Socket object.
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_finalize (JNIEnv *env,
                                                                 jobject obj)
{
    void *s = get_socket (env, obj, 0);
    if (! s)
        return;

    int rc = zmq_close (s);
    int err = zmq_errno();
    s = NULL;
    put_socket (env, obj, s);

    if (rc != 0) {
        raise_exception (env, err);
        return;
    }
}

/**
 * Called by Java's Socket::getLongSockopt(int option).
 */
JNIEXPORT jlong JNICALL Java_org_zeromq_ZMQ_00024Socket_getLongSockopt (JNIEnv *env,
                                                                        jobject obj,
                                                                        jint option)
{
    switch (option) {
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(3,0,0)
    case ZMQ_RECONNECT_IVL:
    case ZMQ_RECONNECT_IVL_MAX:
    case ZMQ_BACKLOG:
    case ZMQ_MAXMSGSIZE:
    case ZMQ_SNDHWM:
    case ZMQ_RCVHWM:
#else
    case ZMQ_HWM:
    case ZMQ_SWAP:
    case ZMQ_MCAST_LOOP:
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(2,1,0)
    case ZMQ_TYPE:
    case ZMQ_FD:
    case ZMQ_EVENTS:
    case ZMQ_LINGER:
#endif
#endif
    case ZMQ_AFFINITY:
    case ZMQ_RATE:
    case ZMQ_RECOVERY_IVL:
    case ZMQ_SNDBUF:
    case ZMQ_RCVBUF:
    case ZMQ_RCVMORE:
        {
            void *s = get_socket (env, obj, 1);
            jlong ret = 0;
            int rc = 0;
            int err = 0;

            uint64_t optval = 0;
            size_t optvallen = sizeof(optval);
            rc = zmq_getsockopt (s, option, &optval, &optvallen);
            err = zmq_errno();
            ret = (jlong) optval;

            if (rc != 0) {
                raise_exception (env, err);
                return 0L;
            }
            return ret;
        }
    default:
        raise_exception (env, EINVAL);
        return 0L;
    }
}

/**
 * Called by Java's Socket::getBytesSockopt(int option).
 */
JNIEXPORT jbyteArray JNICALL Java_org_zeromq_ZMQ_00024Socket_getBytesSockopt (JNIEnv *env,
                                                                              jobject obj,
                                                                              jint option)
{
    switch (option) {
    case ZMQ_IDENTITY:
        {
            void *s = get_socket (env, obj, 1);

            // Warning: hard-coded limit here.
            char optval[1024];
            size_t optvallen = 1024;
            int rc = zmq_getsockopt (s, option, optval, &optvallen);
            int err = zmq_errno();
            if (rc != 0) {
                raise_exception (env, err);
                return env->NewByteArray (0);
            }

            jbyteArray array = env->NewByteArray (optvallen);
            if (array == NULL) {
                raise_exception (env, EINVAL);
                return env->NewByteArray(0);
            }

            env->SetByteArrayRegion (array, 0, optvallen, (const jbyte*) optval);
            return array;
        }
    default:
        raise_exception (env, EINVAL);
        return env->NewByteArray(0);
    }
}

/**
 * Called by Java's Socket::setLongSockopt(int option, long value).
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_setLongSockopt (JNIEnv *env,
                                                                       jobject obj,
                                                                       jint option,
                                                                       jlong value)
{
    switch (option) {
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(3,0,0)
    case ZMQ_RECONNECT_IVL:
    case ZMQ_RECONNECT_IVL_MAX:
    case ZMQ_BACKLOG:
    case ZMQ_MAXMSGSIZE:
    case ZMQ_SNDHWM:
    case ZMQ_RCVHWM:
#else
    case ZMQ_HWM:
    case ZMQ_SWAP:
    case ZMQ_MCAST_LOOP:
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(2,1,0)
    case ZMQ_LINGER:
#endif
#endif
    case ZMQ_AFFINITY:
    case ZMQ_RATE:
    case ZMQ_RECOVERY_IVL:
    case ZMQ_SNDBUF:
    case ZMQ_RCVBUF:
        {
            void *s = get_socket (env, obj, 1);
            int rc = 0;
            int err = 0;
            uint64_t optval = (uint64_t) value;
            
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(2,1,0)
            if (option == ZMQ_LINGER) {
                int ival = (int) optval;
                size_t optvallen = sizeof(ival);
                rc = zmq_setsockopt (s, option, &ival, optvallen);
            } else
#endif
            {
                size_t optvallen = sizeof(optval);
                rc = zmq_setsockopt (s, option, &optval, optvallen);
            }
            err = zmq_errno();

            if (rc != 0) {
                raise_exception (env, err);
            }
            return;
        }
    default:
        raise_exception (env, EINVAL);
        return;
    }
}

/**
 * Called by Java's Socket::setBytesSockopt(int option, byte[] value).
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_setBytesSockopt (JNIEnv *env,
                                                                        jobject obj,
                                                                        jint option,
                                                                        jbyteArray value)
{
    switch (option) {
    case ZMQ_IDENTITY:
    case ZMQ_SUBSCRIBE:
    case ZMQ_UNSUBSCRIBE:
        {
            if (value == NULL) {
                raise_exception (env, EINVAL);
                return;
            }

            void *s = get_socket (env, obj, 1);

            jbyte *optval = env->GetByteArrayElements (value, NULL);
            if (! optval) {
                raise_exception (env, EINVAL);
                return;
            }
            size_t optvallen = env->GetArrayLength (value);
            int rc = zmq_setsockopt (s, option, optval, optvallen);
            int err = zmq_errno();
            env->ReleaseByteArrayElements (value, optval, 0);
            if (rc != 0) {
                raise_exception (env, err);
            }

            return;
        }
    default:
        raise_exception (env, EINVAL);
        return;
    }
}

/**
 * Called by Java's Socket::bind(String addr).
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_bind (JNIEnv *env,
                                                             jobject obj,
                                                             jstring addr)
{
    void *s = get_socket (env, obj, 1);

    if (addr == NULL) {
        raise_exception (env, EINVAL);
        return;
    }

    const char *c_addr = env->GetStringUTFChars (addr, NULL);
    if (c_addr == NULL) {
        raise_exception (env, EINVAL);
        return;
    }

    int rc = zmq_bind (s, c_addr);
    int err = zmq_errno();
    env->ReleaseStringUTFChars (addr, c_addr);

    if (rc != 0) {
        raise_exception (env, err);
        return;
    }
}

/**
 * Called by Java's Socket::connect(String addr).
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Socket_connect (JNIEnv *env,
                                                                jobject obj,
                                                                jstring addr)
{
    void *s = get_socket (env, obj, 1);

    if (addr == NULL) {
        raise_exception (env, EINVAL);
        return;
    }

    const char *c_addr = env->GetStringUTFChars (addr, NULL);
    if (c_addr == NULL) {
        raise_exception (env, EINVAL);
        return;
    }

    int rc = zmq_connect (s, c_addr);
    int err = zmq_errno();
    env->ReleaseStringUTFChars (addr, c_addr);

    if (rc != 0) {
        raise_exception (env, err);
        return;
    }
}

/**
 * Called by Java's Socket::send(byte [] msg, int flags).
 */
JNIEXPORT jboolean JNICALL Java_org_zeromq_ZMQ_00024Socket_send (JNIEnv *env,
                                                                 jobject obj,
                                                                 jbyteArray msg,
                                                                 jint flags)
{
    void *s = get_socket (env, obj, 1);

    jsize size = env->GetArrayLength (msg); 
    zmq_msg_t message;
    int rc = zmq_msg_init_size (&message, size);
    int err = zmq_errno();
    if (rc != 0) {
        raise_exception (env, err);
        return JNI_FALSE;
    }

    jbyte *data = env->GetByteArrayElements (msg, 0);
    if (! data) {
        raise_exception (env, EINVAL);
        return JNI_FALSE;
    }

    memcpy (zmq_msg_data (&message), data, size);
    env->ReleaseByteArrayElements (msg, data, 0);
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(3,0,0)
    rc = zmq_sendmsg (s, &message, flags);
#else
    rc = zmq_send (s, &message, flags);
#endif
    err = zmq_errno();
        
    if (rc < 0 && err == EAGAIN) {
        rc = zmq_msg_close (&message);
        err = zmq_errno();
        if (rc != 0) {
            raise_exception (env, err);
            return JNI_FALSE;
        }
        return JNI_FALSE;
    }
    
    if (rc < 0) {
        raise_exception (env, err);
        rc = zmq_msg_close (&message);
        err = zmq_errno();
        if (rc != 0) {
            raise_exception (env, err);
            return JNI_FALSE;
        }
        return JNI_FALSE;
    }

    rc = zmq_msg_close (&message);
    err = zmq_errno();
    if (rc != 0) {
        raise_exception (env, err);
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/**
 * Called by Java's Socket::recv(int flags).
 */
JNIEXPORT jbyteArray JNICALL Java_org_zeromq_ZMQ_00024Socket_recv (JNIEnv *env,
                                                                   jobject obj,
                                                                   jint flags)
{
    void *s = get_socket (env, obj, 1);

    zmq_msg_t message;
    int rc = zmq_msg_init (&message);
    int err = zmq_errno();
    if (rc != 0) {
        raise_exception (env, err);
        return NULL;
    }

#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(3,0,0)
    rc = zmq_recvmsg (s, &message, flags);
#else
    rc = zmq_recv (s, &message, flags);
#endif
    err = zmq_errno();
    if (rc < 0 && err == EAGAIN) {
        rc = zmq_msg_close (&message);
        err = zmq_errno();
        if (rc != 0) {
            raise_exception (env, err);
            return NULL;
        }
        return NULL;
    }

    if (rc < 0) {
        raise_exception (env, err);
        rc = zmq_msg_close (&message);
        err = zmq_errno();
        if (rc != 0) {
            raise_exception (env, err);
            return NULL;
        }
        return NULL;
    }

    // No errors are defined for these two functions. Should they?
    int sz = zmq_msg_size (&message);
    void* pd = zmq_msg_data (&message);

    jbyteArray data = env->NewByteArray (sz);
    if (! data) {
        raise_exception (env, EINVAL);
        return NULL;
    }

    env->SetByteArrayRegion (data, 0, sz, (jbyte*) pd);

    rc = zmq_msg_close(&message);
    assert (rc == 0);

    return data;
}


/**
 * Make sure we have a valid pointer to Java's Socket::socketHandle.
 */
static void ensure_socket (JNIEnv *env,
                           jobject obj)
{
    if (socket_handle_fid == NULL) {
        jclass cls = env->GetObjectClass (obj);
        assert (cls);
        socket_handle_fid = env->GetFieldID (cls, "socketHandle", "J");
        assert (socket_handle_fid);
        env->DeleteLocalRef (cls);
    }
}

/**
 * Get the value of Java's Socket::socketHandle.
 */
static void *get_socket (JNIEnv *env,
                         jobject obj,
                         int do_assert)
{
    ensure_socket (env, obj);
    void *s = (void*) env->GetLongField (obj, socket_handle_fid);

    if (do_assert)
        assert (s);

    return s;
}

/**
 * Set the value of Java's Socket::socketHandle.
 */
static void put_socket (JNIEnv *env,
                        jobject obj,
                        void *s)
{
    ensure_socket (env, obj);
    env->SetLongField (obj, socket_handle_fid, (jlong) s);
}

/**
 * Get the value of contextHandle for the specified Java Context.
 */
static void *fetch_context (JNIEnv *env,
                            jobject context)
{
    static jmethodID get_context_handle_mid = NULL;

    if (get_context_handle_mid == NULL) {
        jclass cls = env->GetObjectClass (context);
        assert (cls);
        get_context_handle_mid = env->GetMethodID (cls,
            "getContextHandle", "()J");
        env->DeleteLocalRef (cls);
        assert (get_context_handle_mid);
    }

    void *c = (void*) env->CallLongMethod (context, get_context_handle_mid);
    if (env->ExceptionCheck ()) {
        c = NULL;
    }

    return c;
}
