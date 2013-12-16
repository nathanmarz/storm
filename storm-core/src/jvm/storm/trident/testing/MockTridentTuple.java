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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

/**
 * A tuple intended for use in testing.
 */
public class MockTridentTuple extends ArrayList<Object> implements TridentTuple{
  private final Map<String, Integer> fieldMap;

  public MockTridentTuple(List<String> fieldNames, List<?> values) {
    super(values);
    fieldMap = setupFieldMap(fieldNames);
  }

  public MockTridentTuple(List<String> fieldName, Object... values) {
    super(Arrays.asList(values));
    fieldMap = setupFieldMap(fieldName);
  }

  private Map<String, Integer> setupFieldMap(List<String> fieldNames) {
    Map<String, Integer> newFieldMap = new HashMap<String, Integer>(fieldNames.size());

    int idx = 0;
    for (String fieldName : fieldNames) {
      newFieldMap.put(fieldName, idx++);
    }
    return newFieldMap;
  }

  private int getIndex(String fieldName) {
    Integer index = fieldMap.get(fieldName);
    if (index == null) {
      throw new IllegalArgumentException("Unknown field name: " + fieldName);
    }
    return index;
  }

  @Override
  public List<Object> getValues() {
    return this;
  }

  @Override
  public Object getValue(int i) {
    return get(i);
  }

  @Override
  public String getString(int i) {
    return (String)get(i);
  }

  @Override
  public Integer getInteger(int i) {
    return (Integer)get(i);
  }

  @Override
  public Long getLong(int i) {
    return (Long)get(i);
  }

  @Override
  public Boolean getBoolean(int i) {
    return (Boolean)get(i);
  }

  @Override
  public Short getShort(int i) {
    return (Short)get(i);
  }

  @Override
  public Byte getByte(int i) {
    return (Byte)get(i);
  }

  @Override
  public Double getDouble(int i) {
    return (Double)get(i);
  }

  @Override
  public Float getFloat(int i) {
    return (Float)get(i);
  }

  @Override
  public byte[] getBinary(int i) {
    return (byte[]) get(i);
  }

  @Override
  public Object getValueByField(String field) {
    return get(getIndex(field));
  }

  @Override
  public String getStringByField(String field) {
    return (String) getValueByField(field);
  }

  @Override
  public Integer getIntegerByField(String field) {
    return (Integer) getValueByField(field);
  }

  @Override
  public Long getLongByField(String field) {
    return (Long) getValueByField(field);
  }

  @Override
  public Boolean getBooleanByField(String field) {
    return (Boolean) getValueByField(field);
  }

  @Override
  public Short getShortByField(String field) {
    return (Short) getValueByField(field);
  }

  @Override
  public Byte getByteByField(String field) {
    return (Byte) getValueByField(field);
  }

  @Override
  public Double getDoubleByField(String field) {
    return (Double) getValueByField(field);
  }

  @Override
  public Float getFloatByField(String field) {
    return (Float) getValueByField(field);
  }

  @Override
  public byte[] getBinaryByField(String field) {
    return (byte[]) getValueByField(field);
  }
}
