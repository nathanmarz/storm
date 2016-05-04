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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.hdfs.common.Partitioner;
import org.apache.storm.hdfs.common.SequenceFileWriter;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class SequenceFileBolt extends AbstractHdfsBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileBolt.class);

    private SequenceFormat format;
    private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
    private transient SequenceFile.Writer writer;

    private String compressionCodec = "default";
    private transient CompressionCodecFactory codecFactory;

    public SequenceFileBolt() {
    }

    public SequenceFileBolt withCompressionCodec(String codec){
        this.compressionCodec = codec;
        return this;
    }

    public SequenceFileBolt withFsUrl(String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public SequenceFileBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public SequenceFileBolt withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public SequenceFileBolt withSequenceFormat(SequenceFormat format) {
        this.format = format;
        return this;
    }

    public SequenceFileBolt withSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public SequenceFileBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public SequenceFileBolt withCompressionType(SequenceFile.CompressionType compressionType){
        this.compressionType = compressionType;
        return this;
    }

    public SequenceFileBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    public SequenceFileBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public SequenceFileBolt withRetryCount(int fileRetryCount) {
        this.fileRetryCount = fileRetryCount;
        return this;
    }

    public SequenceFileBolt withPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    public SequenceFileBolt withMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing Sequence File Bolt...");
        if (this.format == null) throw new IllegalStateException("SequenceFormat must be specified.");

        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        this.codecFactory = new CompressionCodecFactory(hdfsConfig);
    }

    @Override
    protected String getWriterKey(Tuple tuple) {
        return "CONSTANT";
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple) throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(
                this.hdfsConfig,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(this.format.keyClass()),
                SequenceFile.Writer.valueClass(this.format.valueClass()),
                SequenceFile.Writer.compression(this.compressionType, this.codecFactory.getCodecByName(this.compressionCodec))
        );

        return new SequenceFileWriter(this.rotationPolicy, path, writer, this.format);
    }
}
