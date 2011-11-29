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

#include "util.hpp"

/**
 * Raise an exception that includes 0MQ's error message.
 */
void raise_exception (JNIEnv *env, int err)
{
    //  Get exception class.
    jclass exception_class = env->FindClass ("org/zeromq/ZMQException");
    assert (exception_class);

    //  Get exception class constructor
    jmethodID constructor_method = env->GetMethodID(exception_class,
    	"<init>", "(Ljava/lang/String;I)V");
    assert (constructor_method);

    //  Get text description of the exception.
    const char *err_desc = zmq_strerror (err);
    
    jstring err_str = env->NewStringUTF(err_desc);

    //  Create exception class instance
    jthrowable exception = static_cast<jthrowable>(env->NewObject(
    	exception_class, constructor_method, err_str, err));

    //  Raise the exception.
    int rc = env->Throw (exception);
    env->DeleteLocalRef (exception_class);
    env->DeleteLocalRef (err_str);

    assert (rc == 0);
}
