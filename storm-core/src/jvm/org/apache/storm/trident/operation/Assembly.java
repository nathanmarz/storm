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
package org.apache.storm.trident.operation;

import org.apache.storm.trident.Stream;


/**
 * The `Assembly` interface provides a means to encapsulate logic applied to a {@link org.apache.storm.trident.Stream}.
 *
 * Usage:
 *
 * ```java
 * Stream mystream = ...;
 * Stream assemblyStream = mystream.applyAssembly(myAssembly);
 * ```
 *
 * @see org.apache.storm.trident.Stream
 * @see org.apache.storm.trident.operation.builtin.FirstN
 *
 */
public interface Assembly {
    /**
     * Applies the `Assembly` to a given {@link org.apache.storm.trident.Stream}
     *
     * @param input
     * @return
     */
    Stream apply(Stream input);
}
