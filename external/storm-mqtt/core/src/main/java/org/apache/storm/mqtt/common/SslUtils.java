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
package org.apache.storm.mqtt.common;


import org.apache.storm.mqtt.ssl.KeyStoreLoader;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.security.KeyStore;

public class SslUtils {
    private SslUtils(){}

    public static void checkSslConfig(String url, KeyStoreLoader loader){
        URI uri = URI.create(url);
        String scheme = uri.getScheme().toLowerCase();
        if(!(scheme.equals("tcp") || scheme.startsWith("tls") || scheme.startsWith("ssl"))){
            throw new IllegalArgumentException("Unrecognized URI scheme: " + scheme);
        }
        if(!scheme.equalsIgnoreCase("tcp") && loader == null){
            throw new IllegalStateException("A TLS/SSL MQTT URL was specified, but no KeyStoreLoader configured. " +
                    "A KeyStoreLoader implementation is required when using TLS/SSL.");
        }
    }

    public static SSLContext sslContext(String scheme, KeyStoreLoader keyStoreLoader) throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(keyStoreLoader.keyStoreInputStream(), keyStoreLoader.keyStorePassword().toCharArray());

        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(keyStoreLoader.trustStoreInputStream(), keyStoreLoader.trustStorePassword().toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyStoreLoader.keyPassword().toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        SSLContext sc = SSLContext.getInstance(scheme.toUpperCase());
        TrustManager[] trustManagers = tmf.getTrustManagers();
        sc.init(kmf.getKeyManagers(), trustManagers, null);

        return sc;
    }
}
