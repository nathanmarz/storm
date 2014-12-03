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
package org.apache.storm.hdfs.trident;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.format.SequenceFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;

import org.apache.storm.hdfs.trident.rotation.TimedRotationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;

public class HdfsState implements State {

    public static abstract class Options implements Serializable {

        protected String fsUrl;
        protected String configKey;
        protected transient FileSystem fs;
        private Path currentFile;
        protected FileRotationPolicy rotationPolicy;
        protected FileNameFormat fileNameFormat;
        protected int rotation = 0;
        protected transient Configuration hdfsConfig;
        protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
        protected transient Object writeLock;
        protected transient Timer rotationTimer;

        abstract void closeOutputFile() throws IOException;

        abstract Path createOutputFile() throws IOException;

        abstract void execute(List<TridentTuple> tuples) throws IOException;

        abstract void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException;

        protected void rotateOutputFile() throws IOException {
            LOG.info("Rotating output file...");
            long start = System.currentTimeMillis();
            synchronized (this.writeLock) {
                closeOutputFile();
                this.rotation++;

                Path newFile = createOutputFile();
                LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
                for (RotationAction action : this.rotationActions) {
                    action.execute(this.fs, this.currentFile);
                }
                this.currentFile = newFile;
            }
            long time = System.currentTimeMillis() - start;
            LOG.info("File rotation took {} ms.", time);


        }

        void prepare(Map conf, int partitionIndex, int numPartitions){
            this.writeLock = new Object();
            if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
            if (this.fsUrl == null) {
                throw new IllegalStateException("File system URL must be specified.");
            }
            this.fileNameFormat.prepare(conf, partitionIndex, numPartitions);
            this.hdfsConfig = new Configuration();
            Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
            if(map != null){
                for(String key : map.keySet()){
                    this.hdfsConfig.set(key, String.valueOf(map.get(key)));
                }
            }
            try{
                HdfsSecurityUtil.login(conf, hdfsConfig);
                doPrepare(conf, partitionIndex, numPartitions);
                this.currentFile = createOutputFile();

            } catch (Exception e){
                throw new RuntimeException("Error preparing HdfsState: " + e.getMessage(), e);
            }

            if(this.rotationPolicy instanceof TimedRotationPolicy){
                long interval = ((TimedRotationPolicy)this.rotationPolicy).getInterval();
                this.rotationTimer = new Timer(true);
                TimerTask task = new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            rotateOutputFile();
                        } catch(IOException e){
                            LOG.warn("IOException during scheduled file rotation.", e);
                        }
                    }
                };
                this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
            }
        }

    }

    public static class HdfsFileOptions extends Options {

        private transient FSDataOutputStream out;
        protected RecordFormat format;
        private long offset = 0;

        public HdfsFileOptions withFsUrl(String fsUrl){
            this.fsUrl = fsUrl;
            return this;
        }

        public HdfsFileOptions withConfigKey(String configKey){
            this.configKey = configKey;
            return this;
        }

        public HdfsFileOptions withFileNameFormat(FileNameFormat fileNameFormat){
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public HdfsFileOptions withRecordFormat(RecordFormat format){
            this.format = format;
            return this;
        }

        public HdfsFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy){
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public HdfsFileOptions addRotationAction(RotationAction action){
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing HDFS Bolt...");
            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        }

        @Override
        void closeOutputFile() throws IOException {
            this.out.close();
        }

        @Override
        Path createOutputFile() throws IOException {
            Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
            this.out = this.fs.create(path);
            return path;
        }

        @Override
        public void execute(List<TridentTuple> tuples) throws IOException {
            boolean rotated = false;
            synchronized (this.writeLock) {
                for (TridentTuple tuple : tuples) {
                    byte[] bytes = this.format.format(tuple);
                    out.write(bytes);
                    this.offset += bytes.length;

                    if (this.rotationPolicy.mark(tuple, this.offset)) {
                        rotateOutputFile();
                        this.offset = 0;
                        this.rotationPolicy.reset();
                        rotated = true;
                    }
                }
                if (!rotated) {
                    if (this.out instanceof HdfsDataOutputStream) {
                        ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
                    } else {
                        this.out.hsync();
                    }
                }
            }
        }
    }

    public static class SequenceFileOptions extends Options {
        private SequenceFormat format;
        private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
        private transient SequenceFile.Writer writer;
        private String compressionCodec = "default";
        private transient CompressionCodecFactory codecFactory;

        public SequenceFileOptions withCompressionCodec(String codec){
            this.compressionCodec = codec;
            return this;
        }

        public SequenceFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public SequenceFileOptions withConfigKey(String configKey){
            this.configKey = configKey;
            return this;
        }

        public SequenceFileOptions withFileNameFormat(FileNameFormat fileNameFormat) {
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public SequenceFileOptions withSequenceFormat(SequenceFormat format) {
            this.format = format;
            return this;
        }

        public SequenceFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy) {
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public SequenceFileOptions withCompressionType(SequenceFile.CompressionType compressionType){
            this.compressionType = compressionType;
            return this;
        }

        public SequenceFileOptions addRotationAction(RotationAction action){
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing Sequence File State...");
            if (this.format == null) throw new IllegalStateException("SequenceFormat must be specified.");

            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            this.codecFactory = new CompressionCodecFactory(hdfsConfig);
        }

        @Override
        Path createOutputFile() throws IOException {
            Path p = new Path(this.fsUrl + this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
            this.writer = SequenceFile.createWriter(
                    this.hdfsConfig,
                    SequenceFile.Writer.file(p),
                    SequenceFile.Writer.keyClass(this.format.keyClass()),
                    SequenceFile.Writer.valueClass(this.format.valueClass()),
                    SequenceFile.Writer.compression(this.compressionType, this.codecFactory.getCodecByName(this.compressionCodec))
            );
            return p;
        }

        @Override
        void closeOutputFile() throws IOException {
            this.writer.close();
        }

        @Override
        public void execute(List<TridentTuple> tuples) throws IOException {
            long offset;
            for(TridentTuple tuple : tuples) {
                synchronized (this.writeLock) {
                    this.writer.append(this.format.key(tuple), this.format.value(tuple));
                    offset = this.writer.getLength();
                }

                if (this.rotationPolicy.mark(tuple, offset)) {
                    rotateOutputFile();
                    this.rotationPolicy.reset();
                }
            }
        }

    }

    public static final Logger LOG = LoggerFactory.getLogger(HdfsState.class);
    private Options options;

    HdfsState(Options options){
        this.options = options;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions){
        this.options.prepare(conf, partitionIndex, numPartitions);
    }

    @Override
    public void beginCommit(Long txId) {
    }

    @Override
    public void commit(Long txId) {
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector){
        try{
            this.options.execute(tuples);
        } catch (IOException e){
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }
}
