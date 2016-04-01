/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;

/**
 * Represents the stream and output fields used by a topic
 */
public class KafkaSpoutStream implements Serializable {
    private final Fields outputFields;
    private final String streamId;
    private final String topic;

    /** Represents the specified outputFields and topic with the default stream */
    KafkaSpoutStream(Fields outputFields, String topic) {
        this(outputFields, Utils.DEFAULT_STREAM_ID, topic);
    }

    /** Represents the specified outputFields and topic with the specified stream */
    KafkaSpoutStream(Fields outputFields, String streamId, String topic) {
        if (outputFields == null || streamId == null || topic == null) {
            throw new IllegalArgumentException(String.format("Constructor parameters cannot be null. " +
                    "[outputFields=%s, streamId=%s, topic=%s]", outputFields, streamId, topic));
        }
        this.outputFields = outputFields;
        this.streamId = streamId;
        this.topic = topic;
    }

    Fields getOutputFields() {
        return outputFields;
    }

    String getStreamId() {
        return streamId;
    }

    String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "KafkaSpoutStream{" +
                "outputFields=" + outputFields +
                ", streamId='" + streamId + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
