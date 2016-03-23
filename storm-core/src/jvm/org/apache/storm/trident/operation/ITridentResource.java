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

import java.util.Map;

/**
 * This interface is implemented by various Trident classes in order to
 * gather and propogate resources that have been set on them.
 * @see ResourceDeclarer
 */
public interface ITridentResource {
    /**
     * @return a name of resource name -> amount of that resource. *Return should never be null!*
     */
    Map<String, Number> getResources();
}
