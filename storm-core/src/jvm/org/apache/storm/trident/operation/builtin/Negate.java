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
package org.apache.storm.trident.operation.builtin;

import java.util.Map;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * A `Filter` implementation that inverts another delegate `Filter`.
 *
 * The `Negate.isKeep()` method simply returns the opposite of the delegate's `isKeep()` method:
 *
 * ```java
 * public boolean isKeep(TridentTuple tuple) {
 *      return !this.delegate.isKeep(tuple);
 * }
 * ```
 *
 * The `Negate` filter is useful for dividing a Stream in two based on some boolean condition.
 *
 * Suppose we had a Stream named `userStream` containing information about users, and a custom `Filter` implementation,
 * `RegisteredUserFilter` that filtered out unregistered users. We could divide the `userStream` Stream into two
 * separate Streams -- one for registered users, and one for unregistered users -- by doing the following:
 *
 * ```java
 * Stream userStream = ...
 *
 * Filter registeredFilter = new ResisteredUserFilter();
 * Filter unregisteredFilter = new Negate(registeredFilter);
 *
 * Stream registeredUserStream = userStream.each(userStream.getOutputFields(), registeredFilter);
 * Stream unregisteredUserStream = userStream.each(userStream.getOutputFields(), unregisteredFilter);
 * ```
 *
 */
public class Negate implements Filter {
    
    Filter _delegate;
    
    public Negate(Filter delegate) {
        _delegate = delegate;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return !_delegate.isKeep(tuple);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _delegate.prepare(conf, context);
    }

    @Override
    public void cleanup() {
        _delegate.cleanup();
    }
    
}
