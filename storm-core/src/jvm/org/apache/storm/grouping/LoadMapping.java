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
package org.apache.storm.grouping;

import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
import java.util.HashMap;

/**
 * Holds a list of the current loads
 */
public class LoadMapping {
    private static final Load NOT_CONNECTED = new Load(false, 1.0, 1.0);
    private final AtomicReference<Map<Integer,Load>> _local = new AtomicReference<Map<Integer,Load>>(new HashMap<Integer,Load>());
    private final AtomicReference<Map<Integer,Load>> _remote = new AtomicReference<Map<Integer,Load>>(new HashMap<Integer,Load>());

    public void setLocal(Map<Integer, Double> local) {
        Map<Integer, Load> newLocal = new HashMap<Integer, Load>();
        if (local != null) {
          for (Map.Entry<Integer, Double> entry: local.entrySet()) {
            newLocal.put(entry.getKey(), new Load(true, entry.getValue(), 0.0));
          }
        }
        _local.set(newLocal);
    }

    public void setRemote(Map<Integer, Load> remote) {
        if (remote != null) {
          _remote.set(new HashMap<Integer, Load>(remote));
        } else {
          _remote.set(new HashMap<Integer, Load>());
        }
    }

    public Load getLoad(int task) {
        Load ret = _local.get().get(task);
        if (ret == null) {
          ret = _remote.get().get(task);
        }
        if (ret == null) {
          ret = NOT_CONNECTED;
        }
        return ret;
    }

    public double get(int task) {
        return getLoad(task).getLoad();
    }
}
