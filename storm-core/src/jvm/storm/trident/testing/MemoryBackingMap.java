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
package storm.trident.testing;

import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryBackingMap implements IBackingMap<Object> {
    Map _vals = new HashMap();

    @Override
    public List<Object> multiGet(List<List<Object>> keys) {
        List ret = new ArrayList();
        for(List key: keys) {
            ret.add(_vals.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Object> vals) {
        for(int i=0; i<keys.size(); i++) {
            List key = keys.get(i);
            Object val = vals.get(i);
            _vals.put(key, val);
        }
    }
}
