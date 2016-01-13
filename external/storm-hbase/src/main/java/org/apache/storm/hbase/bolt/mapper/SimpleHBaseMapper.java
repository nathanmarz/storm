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
package org.apache.storm.hbase.bolt.mapper;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.storm.hbase.common.Utils.*;

/**
 *
 */
public class SimpleHBaseMapper implements HBaseMapper {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleHBaseMapper.class);
    
    private String rowKeyField;
//    private String timestampField;
    private byte[] columnFamily;
    private Fields columnFields;
    private Fields counterFields;

    public SimpleHBaseMapper(){
    }


    public SimpleHBaseMapper withRowKeyField(String rowKeyField){
        this.rowKeyField = rowKeyField;
        return this;
    }

    public SimpleHBaseMapper withColumnFields(Fields columnFields){
        this.columnFields = columnFields;
        return this;
    }

    public SimpleHBaseMapper withCounterFields(Fields counterFields){
        this.counterFields = counterFields;
        return this;
    }

    public SimpleHBaseMapper withColumnFamily(String columnFamily){
        this.columnFamily = columnFamily.getBytes();
        return this;
    }

//    public SimpleTridentHBaseMapper withTimestampField(String timestampField){
//        this.timestampField = timestampField;
//        return this;
//    }

    @Override
    public byte[] rowKey(Tuple tuple) {
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return toBytes(objVal);
    }

    @Override
    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        if(this.columnFields != null){
            // TODO timestamps
            for(String field : this.columnFields){
                cols.addColumn(this.columnFamily, field.getBytes(), toBytes(tuple.getValueByField(field)));
            }
        }
        if(this.counterFields != null){
            for(String field : this.counterFields){
                cols.addCounter(this.columnFamily, field.getBytes(), toLong(tuple.getValueByField(field)));
            }
        }
        return cols;
    }
}
