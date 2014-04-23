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

import java.io.Serializable;
import java.util.AbstractList;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;


public class ComboList extends AbstractList<Object> {    
    public static class Factory implements Serializable {
        Pointer[] index;
        int[] sizes;
        
        public Factory(int... sizes) {
            this.sizes = sizes;
            int total = 0;
            for(int size: sizes) {
                total+=size;
            }
            index = new Pointer[total];
            int i=0;
            int j=0;
            for(int size: sizes) {
                for(int z=0; z<size; z++) {
                    index[j] = new Pointer(i, z);
                    j++;
                }
                i++;
            }
        }
        
        public ComboList create(List[] delegates) {
            if(delegates.length!=sizes.length) {
                throw new RuntimeException("Expected " + sizes.length + " lists, but instead got " + delegates.length + " lists");
            }
            for(int i=0; i<delegates.length; i++) {
                List l = delegates[i];
                if(l==null || l.size() != sizes[i]) {
                    throw new RuntimeException("Got unexpected delegates to ComboList: " + ToStringBuilder.reflectionToString(delegates));
                }
            }
            return new ComboList(delegates, index);
        }
    }
    
    private static class Pointer implements Serializable {
        int listIndex;
        int subIndex;
        
        public Pointer(int listIndex, int subIndex) {
            this.listIndex = listIndex;
            this.subIndex = subIndex;
        }
        
    }
    
    Pointer[] _index;
    List[] _delegates;
    
    public ComboList(List[] delegates, Pointer[] index) {
        _index = index;
        _delegates = delegates;
    }
            
    @Override
    public Object get(int i) {
        Pointer ptr = _index[i];
        return _delegates[ptr.listIndex].get(ptr.subIndex);
    }

    @Override
    public int size() {
        return _index.length;
    }
}
