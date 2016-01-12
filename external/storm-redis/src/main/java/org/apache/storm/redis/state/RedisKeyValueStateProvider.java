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
package org.apache.storm.redis.state;

import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.apache.storm.state.State;
import org.apache.storm.state.StateProvider;
import org.apache.storm.task.TopologyContext;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides {@link RedisKeyValueState}
 */
public class RedisKeyValueStateProvider implements StateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyValueStateProvider.class);

    @Override
    public State newState(String namespace, Map stormConf, TopologyContext context) {
        try {
            return getRedisKeyValueState(namespace, getStateConfig(stormConf));
        } catch (Exception ex) {
            LOG.error("Error loading config from storm conf {}", stormConf);
            throw new RuntimeException(ex);
        }
    }

    StateConfig getStateConfig(Map stormConf) throws Exception {
        StateConfig stateConfig = null;
        String providerConfig = null;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        if (stormConf.containsKey(org.apache.storm.Config.TOPOLOGY_STATE_PROVIDER_CONFIG)) {
            providerConfig = (String) stormConf.get(org.apache.storm.Config.TOPOLOGY_STATE_PROVIDER_CONFIG);
            stateConfig = mapper.readValue(providerConfig, StateConfig.class);
        } else {
            stateConfig = new StateConfig();
        }
        return stateConfig;
    }

    private RedisKeyValueState getRedisKeyValueState(String namespace, StateConfig config) throws Exception {
        return new RedisKeyValueState(namespace, getJedisPoolConfig(config), getKeySerializer(config), getValueSerializer(config));
    }

    private Serializer getKeySerializer(StateConfig config) throws Exception {
        Serializer serializer = null;
        if (config.keySerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.keySerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.keyClass != null) {
            serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.keyClass)));
        } else {
            serializer = new DefaultStateSerializer();
        }
        return serializer;
    }

    private Serializer getValueSerializer(StateConfig config) throws Exception {
        Serializer serializer = null;
        if (config.valueSerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.valueSerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.valueClass != null) {
            serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.valueClass)));
        } else {
            serializer = new DefaultStateSerializer();
        }
        return serializer;
    }

    private JedisPoolConfig getJedisPoolConfig(StateConfig config) {
        return config.jedisPoolConfig != null ? config.jedisPoolConfig : new JedisPoolConfig.Builder().build();
    }

    static class StateConfig {
        String keyClass;
        String valueClass;
        String keySerializerClass;
        String valueSerializerClass;
        JedisPoolConfig jedisPoolConfig;

        @Override
        public String toString() {
            return "StateConfig{" +
                    "keyClass='" + keyClass + '\'' +
                    ", valueClass='" + valueClass + '\'' +
                    ", keySerializerClass='" + keySerializerClass + '\'' +
                    ", valueSerializerClass='" + valueSerializerClass + '\'' +
                    ", jedisPoolConfig=" + jedisPoolConfig +
                    '}';
        }
    }

}
