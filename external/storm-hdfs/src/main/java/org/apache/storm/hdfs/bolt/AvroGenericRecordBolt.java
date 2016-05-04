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

import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.hdfs.common.AvroGenericRecordHDFSWriter;
import org.apache.storm.hdfs.common.Partitioner;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class AvroGenericRecordBolt extends AbstractHdfsBolt{

    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordBolt.class);

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

    public AvroGenericRecordBolt withMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public AvroGenericRecordBolt withPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    @Override
    protected void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing AvroGenericRecord Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
    }

    /**
     * AvroGenericRecordBolt must override this method because messages with different schemas cannot be written to the
     * same file.  By treating the complete schema as the "key" AbstractHdfsBolt will associate a different writer for
     * every distinct schema.
     */
    @Override
    protected String getWriterKey(Tuple tuple) {
        Schema recordSchema = ((GenericRecord) tuple.getValue(0)).getSchema();
        return recordSchema.toString();
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple) throws IOException {
        Schema recordSchema = ((GenericRecord) tuple.getValue(0)).getSchema();
        return new AvroGenericRecordHDFSWriter(this.rotationPolicy, path, this.fs.create(path), recordSchema);
    }
}
