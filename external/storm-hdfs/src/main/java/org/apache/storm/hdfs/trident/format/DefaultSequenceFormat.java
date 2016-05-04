/*
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Basic <code>SequenceFormat</code> implementation that uses
 * <code>LongWritable</code> for keys and <code>Text</code> for values.
 *
 */
public class DefaultSequenceFormat implements SequenceFormat {
    private transient LongWritable key;
    private transient Text value;

    private String keyField;
    private String valueField;

    public DefaultSequenceFormat(String keyField, String valueField){
        this.keyField = keyField;
        this.valueField = valueField;
    }



    @Override
    public Class keyClass() {
        return LongWritable.class;
    }

    @Override
    public Class valueClass() {
        return Text.class;
    }

    @Override
    public Writable key(TridentTuple tuple) {
        if(this.key == null){
            this.key  = new LongWritable();
        }
        this.key.set(tuple.getLongByField(this.keyField));
        return this.key;
    }

    @Override
    public Writable value(TridentTuple tuple) {
        if(this.value == null){
            this.value = new Text();
        }
        this.value.set(tuple.getStringByField(this.valueField));
        return this.value;
    }
}
