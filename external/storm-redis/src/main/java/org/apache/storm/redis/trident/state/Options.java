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
import org.apache.storm.trident.state.Serializer;

import java.io.Serializable;

/**
 * Options of State.<br/>
 * It's a data structure (whole things are public) and you can access and modify all fields.
 *
 * @param <T> value's type class
 */
public class Options<T> implements Serializable {
	private static final RedisDataTypeDescription DEFAULT_REDIS_DATATYPE = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING);

	public int localCacheSize = 1000;
	public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
	public KeyFactory keyFactory = null;
	public Serializer<T> serializer = null;
	public RedisDataTypeDescription dataTypeDescription = DEFAULT_REDIS_DATATYPE;
	public int expireIntervalSec = 0;
}
