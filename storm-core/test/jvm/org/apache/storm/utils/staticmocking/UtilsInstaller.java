/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance
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
package org.apache.storm.utils.staticmocking;

import org.apache.storm.utils.Utils;

public class UtilsInstaller implements AutoCloseable {

    private Utils _oldInstance;
    private Utils _curInstance;

    public UtilsInstaller(Utils instance) {
        _oldInstance = Utils.setInstance(instance);
        _curInstance = instance;
    }

    @Override
    public void close() throws Exception {
        if (Utils.setInstance(_oldInstance) != _curInstance) {
            throw new IllegalStateException(
                    "Instances of this resource must be closed in reverse order of opening.");
        }
    }
}