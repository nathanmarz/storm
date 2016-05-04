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
package org.apache.storm.hbase.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private Utils(){}

    public static long toLong(Object obj){
        long l = 0;
        if(obj != null){
            if(obj instanceof Number){
                l = ((Number)obj).longValue();
            } else{
                LOG.warn("Could not coerce {} to Long", obj.getClass().getName());
            }
        }
        return l;
    }

    public static byte[] toBytes(Object obj) {
        if(obj == null) {
          return null;
        } else if(obj instanceof String){
            return ((String)obj).getBytes();
        } else if (obj instanceof Integer){
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof Long){
            return Bytes.toBytes((Long)obj);
        } else if (obj instanceof Short){
            return Bytes.toBytes((Short)obj);
        } else if (obj instanceof Float){
            return Bytes.toBytes((Float)obj);
        } else if (obj instanceof Double){
            return Bytes.toBytes((Double)obj);
        } else if (obj instanceof Boolean){
            return Bytes.toBytes((Boolean)obj);
        } else if (obj instanceof BigDecimal){
            return Bytes.toBytes((BigDecimal)obj);
        } else {
            LOG.error("Can't convert class to byte array: " + obj.getClass().getName());
            return new byte[0];
        }
    }
}
