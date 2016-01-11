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

import javax.security.auth.Subject;

/**
 * Provides a way to automatically push credentials to a topology and to
 * retreave them in the worker.
 */
public interface IAutoCredentials {

    public void prepare(Map conf);

    /**
     * Called to populate the credentials on the client side.
     * @param credentials the credentials to be populated.
     */
    public void populateCredentials(Map<String, String> credentials);

    /**
     * Called to initially populate the subject on the worker side with credentials passed in.
     * @param subject the subject to optionally put credentials in.
     * @param credentials the credentials to be used.
     */ 
    public void populateSubject(Subject subject, Map<String, String> credentials);


    /**
     * Called to update the subject on the worker side when new credentials are recieved.
     * This means that populateSubject has already been called on this subject.  
     * @param subject the subject to optionally put credentials in.
     * @param credentials the credentials to be used.
     */ 
    public void updateSubject(Subject subject, Map<String, String> credentials);

}
