package org.apache.storm.redis.common.mapper;

import java.io.Serializable;

public class RedisDataTypeDescription implements Serializable {
    public enum RedisDataType { STRING, HASH, LIST, SET, SORTED_SET, HYPER_LOG_LOG }

    private RedisDataType dataType;
    private String additionalKey;

    public RedisDataTypeDescription(RedisDataType dataType) {
        this(dataType, null);
    }

    public RedisDataTypeDescription(RedisDataType dataType, String additionalKey) {
        this.dataType = dataType;
        this.additionalKey = additionalKey;

        if (dataType == RedisDataType.HASH || dataType == RedisDataType.SORTED_SET) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
            }
        }
    }

    public RedisDataType getDataType() {
        return dataType;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }
}
