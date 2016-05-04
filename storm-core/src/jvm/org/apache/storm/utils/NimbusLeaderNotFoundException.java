/*
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
package org.apache.storm.utils;

/**
 * This exception class is used to signify a problem with nimbus leader
 * identification.  It should not be used when connection failures happen, but
 * only when successful operations result in the absence of an identified
 * leader.
 */
public class NimbusLeaderNotFoundException extends RuntimeException {
    public NimbusLeaderNotFoundException() {
        super();
    }
    public NimbusLeaderNotFoundException(String msg) {
        super(msg);
    }

    public NimbusLeaderNotFoundException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public NimbusLeaderNotFoundException(Throwable cause) {
        super(cause);
    }
}
