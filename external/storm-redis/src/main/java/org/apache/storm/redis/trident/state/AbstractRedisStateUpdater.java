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

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractRedisStateUpdater<T extends State> extends BaseStateUpdater<T> {
	private final RedisStoreMapper storeMapper;

	protected int expireIntervalSec = 0;
	protected final RedisDataTypeDescription.RedisDataType dataType;
	protected final String additionalKey;

	public AbstractRedisStateUpdater(RedisStoreMapper storeMapper) {
		this.storeMapper = storeMapper;
		RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
		this.dataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();
	}

	public void setExpireInterval(int expireIntervalSec) {
		if (expireIntervalSec > 0) {
			this.expireIntervalSec = expireIntervalSec;
		} else {
			this.expireIntervalSec = 0;
		}
	}

	@Override
	public void updateState(T state, List<TridentTuple> inputs,
			TridentCollector collector) {
		Map<String, String> keyToValue = new HashMap<String, String>();

		for (TridentTuple input : inputs) {
			String key = storeMapper.getKeyFromTuple(input);
			String value = storeMapper.getValueFromTuple(input);

			keyToValue.put(key, value);
		}

		updateStatesToRedis(state, keyToValue);
	}

	protected abstract void updateStatesToRedis(T state, Map<String, String> keyToValue);
}
