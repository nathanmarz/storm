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
package org.apache.storm.security.auth.plain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.security.auth.AbstractSaslServerCallbackHandler;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SaslTransportPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

/**
 * SASL server side callback handler
 */
public class PlainServerCallbackHandler extends AbstractSaslServerCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PlainServerCallbackHandler.class);
    public static final String PASSWORD = "password";

    public PlainServerCallbackHandler() throws IOException {
        userName=null;
    }

    protected void handlePasswordCallback(PasswordCallback pc) {
        LOG.debug("handlePasswordCallback");
        pc.setPassword(PASSWORD.toCharArray());

    }

}
