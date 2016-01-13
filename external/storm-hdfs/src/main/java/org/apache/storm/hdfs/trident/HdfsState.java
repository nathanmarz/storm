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

import org.apache.storm.Config;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.format.SequenceFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.TimedRotationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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


        abstract void closeOutputFile() throws IOException;

        abstract Path createOutputFile() throws IOException;

        abstract void execute(List<TridentTuple> tuples) throws IOException;

        abstract void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException;

        abstract long getCurrentOffset() throws  IOException;

        abstract void doCommit(Long txId) throws IOException;

        abstract void doRecover(Path srcPath, long nBytes) throws Exception;

        protected void rotateOutputFile(boolean doRotateAction) throws IOException {
            LOG.info("Rotating output file...");
            long start = System.currentTimeMillis();
            closeOutputFile();
            this.rotation++;
            Path newFile = createOutputFile();
            if (doRotateAction) {
                LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
                for (RotationAction action : this.rotationActions) {
                    action.execute(this.fs, this.currentFile);
                }
            }
            this.currentFile = newFile;
            long time = System.currentTimeMillis() - start;
            LOG.info("File rotation took {} ms.", time);
        }

        protected void rotateOutputFile() throws IOException {
            rotateOutputFile(true);
        }


        void prepare(Map conf, int partitionIndex, int numPartitions) {
            if (this.rotationPolicy == null) {
                throw new IllegalStateException("RotationPolicy must be specified.");
            } else if (this.rotationPolicy instanceof FileSizeRotationPolicy) {
                long rotationBytes = ((FileSizeRotationPolicy) rotationPolicy).getMaxBytes();
                LOG.warn("FileSizeRotationPolicy specified with {} bytes.", rotationBytes);
                LOG.warn("Recovery will fail if data files cannot be copied within topology.message.timeout.secs.");
                LOG.warn("Ensure that the data files does not grow too big with the FileSizeRotationPolicy.");
            } else if (this.rotationPolicy instanceof TimedRotationPolicy) {
                LOG.warn("TimedRotationPolicy specified with interval {} ms.", ((TimedRotationPolicy) rotationPolicy).getInterval());
                LOG.warn("Recovery will fail if data files cannot be copied within topology.message.timeout.secs.");
                LOG.warn("Ensure that the data files does not grow too big with the TimedRotationPolicy.");
            }
            if (this.fsUrl == null) {
                throw new IllegalStateException("File system URL must be specified.");
            }
            this.fileNameFormat.prepare(conf, partitionIndex, numPartitions);
            this.hdfsConfig = new Configuration();
            Map<String, Object> map = (Map<String, Object>) conf.get(this.configKey);
            if (map != null) {
                for (String key : map.keySet()) {
                    this.hdfsConfig.set(key, String.valueOf(map.get(key)));
                }
            }
            try {
                HdfsSecurityUtil.login(conf, hdfsConfig);
                doPrepare(conf, partitionIndex, numPartitions);
                this.currentFile = createOutputFile();

            } catch (Exception e) {
                throw new RuntimeException("Error preparing HdfsState: " + e.getMessage(), e);
            }

            rotationPolicy.start();
        }

        /**
         * Recovers nBytes from srcFile to the new file created
         * by calling rotateOutputFile and then deletes the srcFile.
         */
        private void recover(String srcFile, long nBytes) {
            try {
                Path srcPath = new Path(srcFile);
                rotateOutputFile(false);
                this.rotationPolicy.reset();
                if (nBytes > 0) {
                    doRecover(srcPath, nBytes);
                    LOG.info("Recovered {} bytes from {} to {}", nBytes, srcFile, currentFile);
                } else {
                    LOG.info("Nothing to recover from {}", srcFile);
                }
                fs.delete(srcPath, false);
                LOG.info("Deleted file {} that had partial commits.", srcFile);
            } catch (Exception e) {
                LOG.warn("Recovery failed.", e);
                throw new RuntimeException(e);
            }
        }

    }

    public static class HdfsFileOptions extends Options {

        private transient FSDataOutputStream out;
        protected RecordFormat format;
        private long offset = 0;
        private int bufferSize =  131072; // default 128 K

        public HdfsFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public HdfsFileOptions withConfigKey(String configKey) {
            this.configKey = configKey;
            return this;
        }

        public HdfsFileOptions withFileNameFormat(FileNameFormat fileNameFormat) {
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public HdfsFileOptions withRecordFormat(RecordFormat format) {
            this.format = format;
            return this;
        }

        public HdfsFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy) {
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        /**
         * <p>Set the size of the buffer used for hdfs file copy in case of recovery. The default
         * value is 131072.</p>
         *
         * <p> Note: The lower limit for the parameter is 4096, below which the
         * option is ignored. </p>
         *
         * @param sizeInBytes the buffer size in bytes
         * @return {@link HdfsFileOptions}
         */
        public HdfsFileOptions withBufferSize(int sizeInBytes) {
            this.bufferSize = Math.max(4096, sizeInBytes); // at least 4K
            return this;
        }

        @Deprecated
        public HdfsFileOptions addRotationAction(RotationAction action) {
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing HDFS File state...");
            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        }

        @Override
        public long getCurrentOffset() {
            return offset;
        }

        @Override
        public void doCommit(Long txId) throws IOException {
            if (this.rotationPolicy.mark(this.offset)) {
                rotateOutputFile();
                this.offset = 0;
                this.rotationPolicy.reset();
            } else {
                if (this.out instanceof HdfsDataOutputStream) {
                    ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
                } else {
                    this.out.hsync();
                }
            }
        }

        @Override
        void doRecover(Path srcPath, long nBytes) throws IOException {
            this.offset = 0;
            FSDataInputStream is = this.fs.open(srcPath);
            copyBytes(is, out, nBytes);
            this.offset = nBytes;
        }

        private void copyBytes(FSDataInputStream is, FSDataOutputStream out, long bytesToCopy) throws IOException {
            byte[] buf = new byte[bufferSize];
            int n;
            while ((n = is.read(buf)) != -1 && bytesToCopy > 0) {
                out.write(buf, 0, (int) Math.min(n, bytesToCopy));
                bytesToCopy -= n;
            }
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
            for (TridentTuple tuple : tuples) {
                byte[] bytes = this.format.format(tuple);
                out.write(bytes);
                this.offset += bytes.length;
            }
        }
    }

    public static class SequenceFileOptions extends Options {
        private SequenceFormat format;
        private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
        private transient SequenceFile.Writer writer;
        private String compressionCodec = "default";
        private transient CompressionCodecFactory codecFactory;

        public SequenceFileOptions withCompressionCodec(String codec) {
            this.compressionCodec = codec;
            return this;
        }

        public SequenceFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public SequenceFileOptions withConfigKey(String configKey) {
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

        public SequenceFileOptions withCompressionType(SequenceFile.CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public SequenceFileOptions addRotationAction(RotationAction action) {
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
        public long getCurrentOffset() throws IOException {
            return this.writer.getLength();
        }

        @Override
        public void doCommit(Long txId) throws IOException {
            if (this.rotationPolicy.mark(this.writer.getLength())) {
                rotateOutputFile();
                this.rotationPolicy.reset();
            } else {
                this.writer.hsync();
            }
        }


        @Override
        void doRecover(Path srcPath, long nBytes) throws Exception {
            SequenceFile.Reader reader = new SequenceFile.Reader(this.hdfsConfig,
                    SequenceFile.Reader.file(srcPath), SequenceFile.Reader.length(nBytes));

            Writable key = (Writable) this.format.keyClass().newInstance();
            Writable value = (Writable) this.format.valueClass().newInstance();
            while(reader.next(key, value)) {
                this.writer.append(key, value);
            }
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
            for (TridentTuple tuple : tuples) {
                this.writer.append(this.format.key(tuple), this.format.value(tuple));
            }
        }

    }

    /**
     * TxnRecord [txnid, data_file_path, data_file_offset]
     * <p>
     * This is written to the index file during beginCommit() and used for recovery.
     * </p>
     */
    private static class TxnRecord {
        private long txnid;
        private String dataFilePath;
        private long offset;

        private TxnRecord(long txnId, String dataFilePath, long offset) {
            this.txnid = txnId;
            this.dataFilePath = dataFilePath;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return Long.toString(txnid) + "," + dataFilePath + "," + Long.toString(offset);
        }
    }


    public static final Logger LOG = LoggerFactory.getLogger(HdfsState.class);
    private Options options;
    private volatile TxnRecord lastSeenTxn;
    private Path indexFilePath;

    HdfsState(Options options) {
        this.options = options;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        this.options.prepare(conf, partitionIndex, numPartitions);
        initLastTxn(conf, partitionIndex);
    }

    private TxnRecord readTxnRecord(Path path) throws IOException {
        FSDataInputStream inputStream = null;
        try {
            inputStream = this.options.fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            if ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                return new TxnRecord(Long.valueOf(fields[0]), fields[1], Long.valueOf(fields[2]));
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return new TxnRecord(0, options.currentFile.toString(), 0);
    }

    /**
     * Returns temp file path corresponding to a file name.
     */
    private Path tmpFilePath(String filename) {
        return new Path(filename + ".tmp");
    }
    /**
     * Reads the last txn record from index file if it exists, if not
     * from .tmp file if exists.
     *
     * @param indexFilePath the index file path
     * @return the txn record from the index file or a default initial record.
     * @throws IOException
     */
    private TxnRecord getTxnRecord(Path indexFilePath) throws IOException {
        Path tmpPath = tmpFilePath(indexFilePath.toString());
        if (this.options.fs.exists(indexFilePath)) {
            return readTxnRecord(indexFilePath);
        } else if (this.options.fs.exists(tmpPath)) {
            return readTxnRecord(tmpPath);
        }
        return new TxnRecord(0, options.currentFile.toString(), 0);
    }

    private void initLastTxn(Map conf, int partition) {
        // include partition id in the file name so that index for different partitions are independent.
        String indexFileName = String.format(".index.%s.%d", conf.get(Config.TOPOLOGY_NAME), partition);
        this.indexFilePath = new Path(options.fileNameFormat.getPath(), indexFileName);
        try {
            this.lastSeenTxn = getTxnRecord(indexFilePath);
            LOG.debug("initLastTxn updated lastSeenTxn to [{}]", this.lastSeenTxn);
        } catch (IOException e) {
            LOG.warn("initLastTxn failed due to IOException.", e);
            throw new RuntimeException(e);
        }
    }

    private void updateIndex(long txId) {
        FSDataOutputStream out = null;
        LOG.debug("Starting index update.");
        try {
            Path tmpPath = tmpFilePath(indexFilePath.toString());
            out = this.options.fs.create(tmpPath, true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            TxnRecord txnRecord = new TxnRecord(txId, options.currentFile.toString(), this.options.getCurrentOffset());
            bw.write(txnRecord.toString());
            bw.newLine();
            bw.flush();
            /*
             * Delete the current index file and rename the tmp file to atomically
             * replace the index file. Orphan .tmp files are handled in getTxnRecord.
             */
            options.fs.delete(this.indexFilePath, false);
            options.fs.rename(tmpPath, this.indexFilePath);
            lastSeenTxn = txnRecord;
            LOG.debug("updateIndex updated lastSeenTxn to [{}]", this.lastSeenTxn);
        } catch (IOException e) {
            LOG.warn("Begin commit failed due to IOException. Failing batch", e);
            throw new FailedException(e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.warn("Begin commit failed due to IOException. Failing batch", e);
                    throw new FailedException(e);
                }
            }
        }
    }

    @Override
    public void beginCommit(Long txId) {
        if (txId <= lastSeenTxn.txnid) {
            LOG.info("txID {} is already processed, lastSeenTxn {}. Triggering recovery.", txId, lastSeenTxn);
            long start = System.currentTimeMillis();
            options.recover(lastSeenTxn.dataFilePath, lastSeenTxn.offset);
            LOG.info("Recovery took {} ms.", System.currentTimeMillis() - start);
        }
        updateIndex(txId);
    }

    @Override
    public void commit(Long txId) {
        try {
            options.doCommit(txId);
        } catch (IOException e) {
            LOG.warn("Commit failed due to IOException. Failing the batch.", e);
            throw new FailedException(e);
        }
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector) {
        try {
            this.options.execute(tuples);
        } catch (IOException e) {
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }

    /**
     * for unit tests
     */
    void close() throws IOException {
        this.options.closeOutputFile();
    }
}
