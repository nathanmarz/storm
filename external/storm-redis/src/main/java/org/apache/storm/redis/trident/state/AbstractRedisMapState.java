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
package org.apache.storm.redis.trident.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AbstractRedisMapState is base class of any RedisMapState, which implements IBackingMap.
 * <p/>
 * Derived classes should provide<br/>
 * - which Serializer it uses<br/>
 * - which KeyFactory it uses<br/>
 * - how to retrieve values from Redis<br/>
 * - how to store values to Redis<br/>
 * and AbstractRedisMapState takes care of rest things.
 *
 * @param <T> value's type class
 */
public abstract class AbstractRedisMapState<T> implements IBackingMap<T> {
	public static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
			StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
			StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
			StateType.OPAQUE, new JSONOpaqueSerializer()
	));

	/**
	 * {@inheritDoc}
	 */
	@Override public List<T> multiGet(List<List<Object>> keys) {
		if (keys.size() == 0) {
			return Collections.emptyList();
		}

		List<String> stringKeys = buildKeys(keys);
		List<String> values = retrieveValuesFromRedis(stringKeys);

		return deserializeValues(keys, values);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		if (keys.size() == 0) {
			return;
		}

		Map<String, String> keyValues = new HashMap<String, String>();
		for (int i = 0; i < keys.size(); i++) {
			String val = new String(getSerializer().serialize(vals.get(i)));
			String redisKey = getKeyFactory().build(keys.get(i));
			keyValues.put(redisKey, val);
		}

		updateStatesToRedis(keyValues);
	}

	private List<String> buildKeys(List<List<Object>> keys) {
		List<String> stringKeys = new ArrayList<String>();

		for (List<Object> key : keys) {
			stringKeys.add(getKeyFactory().build(key));
		}

		return stringKeys;
	}

	private List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
		List<T> result = new ArrayList<T>(keys.size());
		for (String value : values) {
			if (value != null) {
				result.add((T) getSerializer().deserialize(value.getBytes()));
			} else {
				result.add(null);
			}
		}
		return result;
	}

	/**
	 * Returns Serializer which is used for serializing tuple value and deserializing Redis value.
	 *
	 * @return serializer
	 */
	protected abstract Serializer getSerializer();

	/**
	 * Returns KeyFactory which is used for converting state key -> Redis key.
	 * @return key factory
	 */
	protected abstract KeyFactory getKeyFactory();

	/**
	 * Retrieves values from Redis that each value is corresponding to each key.
	 *
	 * @param keys keys having state values
	 * @return values which are corresponding to keys
	 */
	protected abstract List<String> retrieveValuesFromRedis(List<String> keys);

	/**
	 * Updates (key, value) pairs to Redis.
	 *
	 * @param keyValues (key, value) pairs
	 */
	protected abstract void updateStatesToRedis(Map<String, String> keyValues);
}
