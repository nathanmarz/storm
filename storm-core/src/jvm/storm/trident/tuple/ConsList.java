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
package storm.trident.tuple;

import java.util.AbstractList;
import java.util.List;

public class ConsList extends AbstractList<Object> {
    List<Object> _elems;
    Object _first;
    
    public ConsList(Object o, List<Object> elems) {
        _elems = elems;
        _first = o;
    }

    @Override
    public Object get(int i) {
        if(i==0) return _first;
        else {
            return _elems.get(i - 1);
        }
    }

    @Override
    public int size() {
        return _elems.size() + 1;
    }
}
