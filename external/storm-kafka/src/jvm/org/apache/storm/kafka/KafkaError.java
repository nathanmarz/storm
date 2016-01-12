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
package org.apache.storm.kafka;

public enum KafkaError {
    NO_ERROR,
    OFFSET_OUT_OF_RANGE,
    INVALID_MESSAGE,
    UNKNOWN_TOPIC_OR_PARTITION,
    INVALID_FETCH_SIZE,
    LEADER_NOT_AVAILABLE,
    NOT_LEADER_FOR_PARTITION,
    REQUEST_TIMED_OUT,
    BROKER_NOT_AVAILABLE,
    REPLICA_NOT_AVAILABLE,
    MESSAGE_SIZE_TOO_LARGE,
    STALE_CONTROLLER_EPOCH,
    OFFSET_METADATA_TOO_LARGE,
    UNKNOWN;

    public static KafkaError getError(int errorCode) {
        if (errorCode < 0 || errorCode >= UNKNOWN.ordinal()) {
            return UNKNOWN;
        } else {
            return values()[errorCode];
        }
    }
}
