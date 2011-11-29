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

#include <zmq.h>

#include "jzmq.hpp"
#include "util.hpp"
#include "org_zeromq_ZMQ_Context.h"


/** Handle to Java's Context::contextHandle. */
static jfieldID ctx_handle_fid = NULL;


static void ensure_context (JNIEnv *env,
                            jobject obj);
static void *get_context (JNIEnv *env,
                          jobject obj,
                          int do_assert);
static void put_context (JNIEnv *env,
                         jobject obj,
                         void *s);


/**
 * Called to construct a Java Context object.
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Context_construct (JNIEnv *env,
                                                                   jobject obj,
                                                                   jint io_threads)
{
    void *c = get_context (env, obj, 0);
    if (c)
        return;

    c = zmq_init (io_threads);
    int err = zmq_errno();
    put_context (env, obj, c);

    if (c == NULL) {
        raise_exception (env, err);
        return;
    }
}

/**
 * Called to destroy a Java Context object.
 */
JNIEXPORT void JNICALL Java_org_zeromq_ZMQ_00024Context_finalize (JNIEnv *env,
                                                                  jobject obj)
{
    void *c = get_context (env, obj, 0);
    if (! c)
        return;

    int rc = zmq_term (c);
    int err = zmq_errno();
    c = NULL;
    put_context (env, obj, c);

    if (rc != 0) {
        raise_exception (env, err);
        return;
    }
}


/**
 * Make sure we have a valid pointer to Java's Context::contextHandle.
 */
static void ensure_context (JNIEnv *env,
                            jobject obj)
{
    if (ctx_handle_fid == NULL) {
        jclass cls = env->GetObjectClass (obj);
        assert (cls);
        ctx_handle_fid = env->GetFieldID (cls, "contextHandle", "J");
        assert (ctx_handle_fid);
        env->DeleteLocalRef (cls);
    }
}

/**
 * Get the value of Java's Context::contextHandle.
 */
static void *get_context (JNIEnv *env,
                          jobject obj,
                          int do_assert)
{
    ensure_context (env, obj);
    void *s = (void*) env->GetLongField (obj, ctx_handle_fid);

    if (do_assert)
        assert (s);

    return s;
}

/**
 * Set the value of Java's Context::contextHandle.
 */
static void put_context (JNIEnv *env,
                         jobject obj,
                         void *s)
{
    ensure_context (env, obj);
    env->SetLongField (obj, ctx_handle_fid, (jlong) s);
}
