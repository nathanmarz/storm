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
package org.apache.storm.mqtt.ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * KeyStoreLoader implementation that uses local files.
 */
public class DefaultKeyStoreLoader implements KeyStoreLoader {
    private String ksFile = null;
    private String tsFile = null;
    private String keyStorePassword = "";
    private String trustStorePassword = "";
    private String keyPassword = "";

    /**
     * Creates a DefaultKeystoreLoader that uses the same file
     * for both the keystore and truststore.
     *
     * @param keystore path to keystore file
     */
    public DefaultKeyStoreLoader(String keystore){
        this.ksFile = keystore;
    }

    /**
     * Creates a DefaultKeystoreLoader that uses separate files
     * for the keystore and truststore.
     *
     * @param keystore path to keystore file
     * @param truststore path to truststore file
     */
    public DefaultKeyStoreLoader(String keystore, String truststore){
        this.ksFile = keystore;
        this.tsFile = truststore;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    @Override
    public InputStream keyStoreInputStream() throws FileNotFoundException {
        return new FileInputStream(this.ksFile);
    }

    @Override
    public InputStream trustStoreInputStream() throws FileNotFoundException {
        // if no truststore file, assume the truststore is the keystore.
        if(this.tsFile == null){
            return new FileInputStream(this.ksFile);
        } else {
            return new FileInputStream(this.tsFile);
        }
    }

    @Override
    public String keyStorePassword() {
        return this.keyStorePassword;
    }

    @Override
    public String trustStorePassword() {
        return this.trustStorePassword;
    }

    @Override
    public String keyPassword() {
        return this.keyPassword;
    }
}
