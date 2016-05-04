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

import java.io.Serializable;
import java.util.List;

/**
 * KeyFactory defines conversion of state key (which could be compounded) -> Redis key.
 */
public interface KeyFactory extends Serializable {
    /**
     * Converts state key to Redis key.
     * @param key state key
     * @return Redis key
     */
	String build(List<Object> key);

    /**
     * Default Key Factory
     */
	class DefaultKeyFactory implements KeyFactory {
        /**
         * {@inheritDoc}
         * <p/>
         * Currently DefaultKeyFactory returns just first element of list.
         *
         * @param key state key
         * @return Redis key
         * @throws RuntimeException when key is compound key
         * @see KeyFactory#build(List)
         */
        @Override
		public String build(List<Object> key) {
			if (key.size() != 1)
				throw new RuntimeException("Default KeyFactory does not support compound keys");

			return (String) key.get(0);
		}
	}
}

