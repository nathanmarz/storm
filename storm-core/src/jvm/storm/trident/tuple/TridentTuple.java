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
import java.util.List;
import java.util.Map;

public interface TridentTuple extends List<Object> {
    public static interface Factory extends Serializable {
        Map<String, ValuePointer> getFieldIndex();
        List<String> getOutputFields();
        int numDelegates();
    }

    List<Object> getValues();
    
    Object getValue(int i);
    
    String getString(int i);
    
    Integer getInteger(int i);
    
    Long getLong(int i);
    
    Boolean getBoolean(int i);
    
    Short getShort(int i);
    
    Byte getByte(int i);
    
    Double getDouble(int i);
    
    Float getFloat(int i);
    
    byte[] getBinary(int i);    
    
    Object getValueByField(String field);
    
    String getStringByField(String field);
    
    Integer getIntegerByField(String field);
    
    Long getLongByField(String field);
    
    Boolean getBooleanByField(String field);
    
    Short getShortByField(String field);
    
    Byte getByteByField(String field);
    
    Double getDoubleByField(String field);
    
    Float getFloatByField(String field);
    
    byte[] getBinaryByField(String field);
}
