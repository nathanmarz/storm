/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.security.auth;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.InetAddress;
import com.google.common.annotations.VisibleForTesting;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import javax.security.auth.Subject;

/**
 * context request context includes info about:
 *
 *   1. remote address,
 *   2. remote subject and primary principal
 *   3. request ID
 */
public class ReqContext {
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    private Subject _subject;
    private InetAddress _remoteAddr;
    private Integer _reqID;
    private Map _storm_conf;
    private Principal realPrincipal;

    @Override
    public String toString() {
        return "ReqContext{" +
                "realPrincipal=" + ((realPrincipal != null) ? realPrincipal.getName() : "null") +
                ", _reqID=" + _reqID +
                ", _remoteAddr=" + _remoteAddr +
                ", _authZPrincipal=" + ((principal() != null) ? principal().getName() : "null") +
                ", ThreadId=" + Thread.currentThread().toString() +
                '}';
    }

    /**
     * @return a request context associated with current thread
     */
    public static ReqContext context() {
        return ctxt.get();
    }

    /**
     * Reset the context back to a default.  used for testing.
     */
    public static void reset() {
        ctxt.remove();
    }

    //each thread will have its own request context
    private static final ThreadLocal < ReqContext > ctxt = 
            new ThreadLocal < ReqContext > () {
        @Override 
        protected ReqContext initialValue() {
            return new ReqContext(AccessController.getContext());
        }
    };

    //private constructor
    @VisibleForTesting
    public ReqContext(AccessControlContext acl_ctxt) {
        _subject = Subject.getSubject(acl_ctxt);
        _reqID = uniqueId.incrementAndGet();
    }

    //private constructor
    @VisibleForTesting
    public ReqContext(Subject sub) {
        _subject = sub;
        _reqID = uniqueId.incrementAndGet();
    }


    /**
     * client address
     */
    public void setRemoteAddress(InetAddress addr) {
        _remoteAddr = addr;
    }

    public InetAddress remoteAddress() {
        return _remoteAddr;
    }

    /**
     * Set remote subject explicitly
     */
    public void setSubject(Subject subject) {
        _subject = subject;
    }

    /**
     * Retrieve client subject associated with this request context
     */
    public Subject subject() {
        return _subject;
    }

    /**
     * The primary principal associated current subject
     */
    public Principal principal() {
        if (_subject == null) return null;
        Set<Principal> princs = _subject.getPrincipals();
        if (princs.size()==0) return null;
        return (Principal) (princs.toArray()[0]);
    }

    public void setRealPrincipal(Principal realPrincipal) {
        this.realPrincipal = realPrincipal;
    }
    /**
     * The real principal associated with the subject.
     */
    public Principal realPrincipal() {
        return this.realPrincipal;
    }

    /**
     * @return true if this request is an impersonation request.
     */
    public boolean isImpersonating() {
        return this.realPrincipal != null && !this.realPrincipal.equals(this.principal());
    }
    
    /**
     * request ID of this request
     */
    public Integer requestID() {
        return _reqID;
    }

}
