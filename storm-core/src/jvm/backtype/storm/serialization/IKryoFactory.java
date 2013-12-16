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
package backtype.storm.serialization;

import com.esotericsoftware.kryo.Kryo;
import java.util.Map;

/**
 * An interface that controls the Kryo instance used by Storm for serialization.
 * The lifecycle is:
 * 
 * 1. The Kryo instance is constructed using getKryo
 * 2. Storm registers the default classes (e.g. arrays, lists, maps, etc.)
 * 3. Storm calls preRegister hook
 * 4. Storm registers all user-defined registrations through topology.kryo.register
 * 5. Storm calls postRegister hook
 * 6. Storm calls all user-defined decorators through topology.kryo.decorators
 * 7. Storm calls postDecorate hook
 */
public interface IKryoFactory {
    Kryo getKryo(Map conf);
    void preRegister(Kryo k, Map conf);
    void postRegister(Kryo k, Map conf);
    void postDecorate(Kryo k, Map conf);
}