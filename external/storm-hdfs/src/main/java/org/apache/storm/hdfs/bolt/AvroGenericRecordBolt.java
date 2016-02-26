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
package org.apache.storm.hdfs.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

public class AvroGenericRecordBolt extends AbstractHdfsBolt{

    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordBolt.class);

    private transient FSDataOutputStream out;
    private Schema schema;
    private String schemaAsString;
    private DataFileWriter<GenericRecord> avroWriter;

    public AvroGenericRecordBolt withSchemaAsString(String schemaAsString)
    {
        this.schemaAsString = schemaAsString;
        return this;
    }

    public AvroGenericRecordBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public AvroGenericRecordBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public AvroGenericRecordBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public AvroGenericRecordBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public AvroGenericRecordBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public AvroGenericRecordBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public AvroGenericRecordBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    @Override
    protected void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing AvroGenericRecord Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        Schema.Parser parser = new Schema.Parser();
        this.schema = parser.parse(this.schemaAsString);
    }

    @Override
    protected void writeTuple(Tuple tuple) throws IOException {
        GenericRecord avroRecord = (GenericRecord) tuple.getValue(0);
        avroWriter.append(avroRecord);
        offset = this.out.getPos();
    }

    @Override
    protected void syncTuples() throws IOException {
        avroWriter.flush();

        LOG.debug("Attempting to sync all data to filesystem");
        if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
        this.syncPolicy.reset();
    }

    @Override
    protected void closeOutputFile() throws IOException
    {
        avroWriter.close();
        this.out.close();
    }

    @Override
    protected Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);

        //Initialize writer
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        avroWriter = new DataFileWriter<>(datumWriter);
        avroWriter.create(this.schema, this.out);

        return path;
    }
}
