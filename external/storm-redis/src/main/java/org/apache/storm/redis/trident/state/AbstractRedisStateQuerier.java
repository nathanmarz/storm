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

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public abstract class AbstractRedisStateQuerier<T extends State> extends BaseQueryFunction<T, List<Values>> {
	private final RedisLookupMapper lookupMapper;
	protected final RedisDataTypeDescription.RedisDataType dataType;
	protected final String additionalKey;

	public AbstractRedisStateQuerier(RedisLookupMapper lookupMapper) {
		this.lookupMapper = lookupMapper;

		RedisDataTypeDescription dataTypeDescription = lookupMapper.getDataTypeDescription();
		this.dataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();
	}

	@Override
	public List<List<Values>> batchRetrieve(T state, List<TridentTuple> inputs) {
		List<List<Values>> values = Lists.newArrayList();

		List<String> keys = Lists.newArrayList();
		for (TridentTuple input : inputs) {
			keys.add(lookupMapper.getKeyFromTuple(input));
		}

		List<String> redisVals = retrieveValuesFromRedis(state, keys);
		for (int i = 0 ; i < redisVals.size() ; i++) {
			values.add(lookupMapper.toTuple(inputs.get(i), redisVals.get(i)));
		}

		return values;
	}

	@Override
	public void execute(TridentTuple tuple, List<Values> values, TridentCollector collector) {
		for (Values value : values) {
			collector.emit(value);
		}
	}

	protected abstract List<String> retrieveValuesFromRedis(T redisClusterState, List<String> keys);
}
