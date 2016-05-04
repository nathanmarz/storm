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
package org.apache.storm.redis.common.mapper;

import java.io.Serializable;

/**
 * RedisDataTypeDescription defines data type and additional key if needed for lookup / store tuples.
 */
public class RedisDataTypeDescription implements Serializable {
    public enum RedisDataType { STRING, HASH, LIST, SET, SORTED_SET, HYPER_LOG_LOG }

    private RedisDataType dataType;
    private String additionalKey;

    /**
     * Constructor
     * @param dataType data type
     */
    public RedisDataTypeDescription(RedisDataType dataType) {
        this(dataType, null);
    }

    /**
     * Constructor
     * @param dataType data type
     * @param additionalKey additional key for hash and sorted set
     */
    public RedisDataTypeDescription(RedisDataType dataType, String additionalKey) {
        this.dataType = dataType;
        this.additionalKey = additionalKey;

        if (dataType == RedisDataType.HASH || dataType == RedisDataType.SORTED_SET) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
            }
        }
    }

    /**
     * Returns defined data type.
     * @return data type
     */
    public RedisDataType getDataType() {
        return dataType;
    }

    /**
     * Returns defined additional key.
     * @return additional key
     */
    public String getAdditionalKey() {
        return additionalKey;
    }
}
