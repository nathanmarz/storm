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
package org.apache.storm;

import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;

import java.util.Map;

import javax.security.auth.Subject;

/**
 * mock implementation of INimbusCredentialPlugin,IAutoCredentials and ICredentialsRenewer for testing only.
 */
public class MockAutoCred implements INimbusCredentialPlugin, IAutoCredentials, ICredentialsRenewer {
    public static final String NIMBUS_CRED_KEY = "nimbusCredTestKey";
    public static final String NIMBUS_CRED_VAL = "nimbusTestCred";
    public static final String NIMBUS_CRED_RENEW_VAL = "renewedNimbusTestCred";
    public static final String GATEWAY_CRED_KEY = "gatewayCredTestKey";
    public static final String GATEWAY_CRED_VAL = "gatewayTestCred";
    public static final String GATEWAY_CRED_RENEW_VAL = "renewedGatewayTestCred";

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        credentials.put(GATEWAY_CRED_KEY, GATEWAY_CRED_VAL);
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map conf) {
        credentials.put(NIMBUS_CRED_KEY, NIMBUS_CRED_VAL);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        subject.getPublicCredentials().add(credentials.get(NIMBUS_CRED_KEY));
        subject.getPublicCredentials().add(credentials.get(GATEWAY_CRED_KEY));
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        populateSubject(subject, credentials);
    }

    @Override
    public void renew(Map<String, String> credentials, Map topologyConf) {
        credentials.put(NIMBUS_CRED_KEY, NIMBUS_CRED_RENEW_VAL);
        credentials.put(GATEWAY_CRED_KEY, GATEWAY_CRED_RENEW_VAL);
    }

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void shutdown() {

    }
}
