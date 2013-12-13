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
package storm.trident.operation.builtin;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;


public class Equals extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        for(int i=0; i<tuple.size()-1; i++) {
            Object o1 = tuple.getValue(i);
            Object o2 = tuple.getValue(i+1);
            if(o1==null && o2!=null || o1!=null && o2==null || !o1.equals(o2)) {
                return false;
            }
        }
        return true;
    }
    
}
