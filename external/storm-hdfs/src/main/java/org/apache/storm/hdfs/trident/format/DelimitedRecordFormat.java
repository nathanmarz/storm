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
package org.apache.storm.hdfs.trident.format;

import org.apache.storm.tuple.Fields;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * RecordFormat implementation that uses field and record delimiters.
 * By default uses a comma (",") as the field delimiter and a
 * newline ("\n") as the record delimiter.
 *
 *
 */
public class DelimitedRecordFormat implements RecordFormat {
    public static final String DEFAULT_FIELD_DELIMITER = ",";
    public static final String DEFAULT_RECORD_DELIMITER = "\n";
    private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private String recordDelimiter = DEFAULT_RECORD_DELIMITER;
    private Fields fields = null;

    /**
     * Only output the specified fields.
     *
     * @param fields
     * @return
     */
    public DelimitedRecordFormat withFields(Fields fields){
        this.fields = fields;
        return this;
    }

    /**
     * Overrides the default field delimiter.
     *
     * @param delimiter
     * @return
     */
    public DelimitedRecordFormat withFieldDelimiter(String delimiter){
        this.fieldDelimiter = delimiter;
        return this;
    }

    /**
     * Overrides the default record delimiter.
     *
     * @param delimiter
     * @return
     */
    public DelimitedRecordFormat withRecordDelimiter(String delimiter){
        this.recordDelimiter = delimiter;
        return this;
    }

    @Override
    public byte[] format(TridentTuple tuple) {
        StringBuilder sb = new StringBuilder();
        int size = this.fields.size();
        for(int i = 0; i < size; i++){
            sb.append(tuple.getValueByField(fields.get(i)));
            if(i != size - 1){
                sb.append(this.fieldDelimiter);
            }
        }
        sb.append(this.recordDelimiter);
        return sb.toString().getBytes();
    }
}
