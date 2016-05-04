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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.IOException;

public class DelimitedRecordHiveMapper implements HiveMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedRecordHiveMapper.class);
    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private Fields columnFields;
    private Fields partitionFields;
    private String[] columnNames;
    private String timeFormat;
    private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private SimpleDateFormat parseDate;

    public DelimitedRecordHiveMapper() {
    }

    public DelimitedRecordHiveMapper withColumnFields(Fields columnFields) {
        this.columnFields = columnFields;
        List<String> tempColumnNamesList = this.columnFields.toList();
        columnNames = new String[tempColumnNamesList.size()];
        tempColumnNamesList.toArray(columnNames);
        return this;
    }

    public DelimitedRecordHiveMapper withPartitionFields(Fields partitionFields) {
        this.partitionFields = partitionFields;
        return this;
    }

    public DelimitedRecordHiveMapper withFieldDelimiter(String delimiter){
        this.fieldDelimiter = delimiter;
        return this;
    }

    public DelimitedRecordHiveMapper withTimeAsPartitionField(String timeFormat) {
        this.timeFormat = timeFormat;
        parseDate = new SimpleDateFormat(timeFormat);
        return this;
    }

    @Override
    public RecordWriter createRecordWriter(HiveEndPoint endPoint)
        throws StreamingException, IOException, ClassNotFoundException {
        return new DelimitedInputWriter(columnNames, fieldDelimiter,endPoint);
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
        StringBuilder builder = new StringBuilder();
        if(this.columnFields != null) {
            for(String field: this.columnFields) {
                builder.append(tuple.getValueByField(field));
                builder.append(fieldDelimiter);
            }
        }
        return builder.toString().getBytes();
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
        StringBuilder builder = new StringBuilder();
        if(this.columnFields != null) {
            for(String field: this.columnFields) {
                builder.append(tuple.getValueByField(field));
                builder.append(fieldDelimiter);
            }
        }
        return builder.toString().getBytes();
    }

    private String getPartitionsByTimeFormat() {
        Date d = new Date();
        return parseDate.format(d.getTime());
    }
}
