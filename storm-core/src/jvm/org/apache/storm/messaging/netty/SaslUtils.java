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
package org.apache.storm.messaging.netty;

import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.Charsets;

import org.apache.storm.Config;

class SaslUtils {
    public static final String KERBEROS = "GSSAPI";
    public static final String AUTH_DIGEST_MD5 = "DIGEST-MD5";
    public static final String DEFAULT_REALM = "default";

    static Map<String, String> getSaslProps() {
        Map<String, String> props = new HashMap<>();
        props.put(Sasl.POLICY_NOPLAINTEXT, "true");
        return props;
    }

    /**
     * Encode a password as a base64-encoded char[] array.
     * 
     * @param password
     *            as a byte array.
     * @return password as a char array.
     */
    static char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password), Charsets.UTF_8)
                .toCharArray();
    }

    /**
     * Encode a identifier as a base64-encoded char[] array.
     * 
     * @param identifier
     *            as a byte array.
     * @return identifier as a char array.
     */
    static String encodeIdentifier(byte[] identifier) {
        return new String(Base64.encodeBase64(identifier), Charsets.UTF_8);
    }

    static String getSecretKey(Map conf) {
        return conf == null || conf.isEmpty() ? null : (String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
    }

}
