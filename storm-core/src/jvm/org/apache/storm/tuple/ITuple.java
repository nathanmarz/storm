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
package org.apache.storm.tuple;

import java.util.List;

public interface ITuple {

    /**
     * Returns the number of fields in this tuple.
     */
    public int size();

    /**
     * Returns true if this tuple contains the specified name of the field.
     */
    public boolean contains(String field);

    /**
     * Gets the names of the fields in this tuple.
     */
    public Fields getFields();

    /**
    *  Returns the position of the specified field in this tuple.
    *  
    * @throws IllegalArgumentException - if field does not exist
    */
    public int fieldIndex(String field);

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    public List<Object> select(Fields selector);

    /**
     * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed. 
     *  
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Object getValue(int i);

    /**
     * Returns the String at position i in the tuple. 
     *  
     * @throws ClassCastException If that field is not a String 
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public String getString(int i);

    /**
     * Returns the Integer at position i in the tuple. 
     *  
    * @throws ClassCastException If that field is not a Integer 
    * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Integer getInteger(int i);

    /**
     * Returns the Long at position i in the tuple. 
     *  
    * @throws ClassCastException If that field is not a Long
    * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Long getLong(int i);

    /**
     * Returns the Boolean at position i in the tuple. 
     *  
    * @throws ClassCastException If that field is not a Boolean
    * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Boolean getBoolean(int i);

    /**
     * Returns the Short at position i in the tuple. 
     *  
     * @throws ClassCastException If that field is not a Short
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Short getShort(int i);

    /**
     * Returns the Byte at position i in the tuple. 
     *  
     * @throws ClassCastException If that field is not a Byte
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Byte getByte(int i);

    /**
     * Returns the Double at position i in the tuple. 
     *  
    * @throws ClassCastException If that field is not a Double
    * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Double getDouble(int i);

    /**
     * Returns the Float at position i in the tuple. 
     *  
    * @throws ClassCastException If that field is not a Float
    * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public Float getFloat(int i);

    /**
     * Returns the byte array at position i in the tuple. 
     *  
     * @throws ClassCastException If that field is not a byte array 
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    public byte[] getBinary(int i);

    /**
     * Gets the field with a specific name. Returns object since tuples are dynamically typed. 
     *  
     * @throws IllegalArgumentException - if field does not exist
     */
    public Object getValueByField(String field);

    /**
     * Gets the String field with a specific name.
     *  
     * @throws ClassCastException If that field is not a String 
     * @throws IllegalArgumentException - if field does not exist
     */
    public String getStringByField(String field);

    /**
     * Gets the Integer field with a specific name.
     *  
     * @throws ClassCastException If that field is not an Integer
     * @throws IllegalArgumentException - if field does not exist
     */
    public Integer getIntegerByField(String field);

    /**
     * Gets the Long field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Long
     * @throws IllegalArgumentException - if field does not exist
     */
    public Long getLongByField(String field);

    /**
     * Gets the Boolean field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Boolean
     * @throws IllegalArgumentException - if field does not exist
     */
    public Boolean getBooleanByField(String field);

    /**
     * Gets the Short field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Short
     * @throws IllegalArgumentException - if field does not exist
     */
    public Short getShortByField(String field);

    /**
     * Gets the Byte field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Byte
     * @throws IllegalArgumentException - if field does not exist
     */
    public Byte getByteByField(String field);

    /**
     * Gets the Double field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Double
     * @throws IllegalArgumentException - if field does not exist
     */
    public Double getDoubleByField(String field);

    /**
     * Gets the Float field with a specific name.
     *  
     * @throws ClassCastException If that field is not a Float
     * @throws IllegalArgumentException - if field does not exist
     */
    public Float getFloatByField(String field);

    /**
     * Gets the Byte array field with a specific name.
     *  
     * @throws ClassCastException If that field is not a byte array
     * @throws IllegalArgumentException - if field does not exist
     */
    public byte[] getBinaryByField(String field);

    /**
     * Gets all the values in this tuple.
     */
    public List<Object> getValues();

}
