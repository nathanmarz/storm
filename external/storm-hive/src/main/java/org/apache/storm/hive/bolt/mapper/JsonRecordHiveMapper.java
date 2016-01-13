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
package org.apache.storm.hive.bolt.mapper;


import org.apache.storm.tuple.Fields;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.IOException;

public class JsonRecordHiveMapper implements HiveMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedRecordHiveMapper.class);
    private Fields columnFields;
    private Fields partitionFields;
    private String timeFormat;
    private SimpleDateFormat parseDate;

    public JsonRecordHiveMapper() {
    }

    public JsonRecordHiveMapper withColumnFields(Fields columnFields) {
        this.columnFields = columnFields;
        return this;
    }

    public JsonRecordHiveMapper withPartitionFields(Fields partitionFields) {
        this.partitionFields = partitionFields;
        return this;
    }

    public JsonRecordHiveMapper withTimeAsPartitionField(String timeFormat) {
        this.timeFormat = timeFormat;
        parseDate = new SimpleDateFormat(timeFormat);
        return this;
    }

    @Override
    public RecordWriter createRecordWriter(HiveEndPoint endPoint)
        throws StreamingException, IOException, ClassNotFoundException {
        return new StrictJsonWriter(endPoint);
    }

    @Override
    public void write(TransactionBatch txnBatch, Tuple tuple)
        throws StreamingException, IOException, InterruptedException {
        txnBatch.write(mapRecord(tuple));
    }

    @Override
    public List<String> mapPartitions(Tuple tuple) {
        List<String> partitionList = new ArrayList<String>();
        if(this.partitionFields != null) {
            for(String field: this.partitionFields) {
                partitionList.add(tuple.getStringByField(field));
            }
        }
        if (this.timeFormat != null) {
            partitionList.add(getPartitionsByTimeFormat());
        }
        return partitionList;
    }

    @Override
    public byte[] mapRecord(Tuple tuple) {
        JSONObject obj = new JSONObject();
        if(this.columnFields != null) {
            for(String field: this.columnFields) {
                obj.put(field,tuple.getValueByField(field));
            }
        }
        return obj.toJSONString().getBytes();
    }

    @Override
    public List<String> mapPartitions(TridentTuple tuple) {
        List<String> partitionList = new ArrayList<String>();
        if(this.partitionFields != null) {
            for(String field: this.partitionFields) {
                partitionList.add(tuple.getStringByField(field));
            }
        }
        if (this.timeFormat != null) {
            partitionList.add(getPartitionsByTimeFormat());
        }
        return partitionList;
    }

    @Override
    public byte[] mapRecord(TridentTuple tuple) {
        JSONObject obj = new JSONObject();
        if(this.columnFields != null) {
            for(String field: this.columnFields) {
                obj.put(field,tuple.getValueByField(field));
            }
        }
        return obj.toJSONString().getBytes();
    }

    private String getPartitionsByTimeFormat() {
        Date d = new Date();
        return parseDate.format(d.getTime());
    }
}
