/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.bolt;

import backtype.storm.tuple.Tuple;

/**
 * A default implementation of IEventDataFormat that converts the tuple
 * into a delimited string.
 */
public class DefaultEventDataFormat implements IEventDataFormat {
  private static final long serialVersionUID = 1L;
  private String delimiter = ",";
  
  public DefaultEventDataFormat withFieldDelimiter(String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  @Override
  public byte[] serialize(Tuple tuple) {
    StringBuilder sb = new StringBuilder();
    for(Object obj : tuple.getValues()) {
      if(sb.length() != 0) {
        sb.append(delimiter);
      }
      sb.append(obj.toString());
    }
    return sb.toString().getBytes();
  }

}
