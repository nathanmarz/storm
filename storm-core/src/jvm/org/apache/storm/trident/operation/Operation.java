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

import java.io.Serializable;
import java.util.Map;

/**
 * Parent interface for Trident `Filter`s and `Function`s.
 *
 * `Operation` defines two lifecycle methods for Trident components. The `prepare()` method is called once when the
 * `Operation` is first initialized. The `cleanup()` method is called in local mode when the local cluster is
 * being shut down. In distributed mode, the `cleanup()` method is not guaranteed to be called in every situation, but
 * Storm will make a best effort call `cleanup()` whenever possible.
 */
public interface Operation extends Serializable {
    /**
     * Called when the `Operation` is first initialized.
     * @param conf the Storm configuration map
     * @param context the operation context which provides information such as the number of partitions in the stream,
     *                and the current partition index. It also provides methods for registering operation-specific
     *                metrics.
     * @see org.apache.storm.trident.operation.TridentOperationContext
     */
    void prepare(Map conf, TridentOperationContext context);

    /**
     * When running in local mode, called when the local cluster is being shut down.
     */
    void cleanup();
}
