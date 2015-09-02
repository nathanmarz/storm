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

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;

public class HdfsBolt extends AbstractHdfsBolt{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);
    private static final Integer DEFAULT_RETRY_COUNT = 3;

    private transient FSDataOutputStream out;
    private RecordFormat format;
    private long offset = 0;
    private List<Tuple> tupleBatch = new LinkedList<>();
    Integer tickTupleInterval = 0;
    Integer fileRetryCount = DEFAULT_RETRY_COUNT;

    public HdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBolt withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public HdfsBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    public HdfsBolt withRetryCount(int fileRetryCount) {
        this.fileRetryCount = fileRetryCount;
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);

        // If interval is non-zero then it has already been explicitly set and we should not default it
        if (conf.containsKey("topology.message.timeout.secs") && tickTupleInterval == 0)
        {
            Integer topologyTimeout = Integer.parseInt(conf.get("topology.message.timeout.secs").toString());
            tickTupleInterval = (int)(Math.floor(topologyTimeout / 2));
            LOG.debug("Setting tick tuple interval to [" + tickTupleInterval + "] based on topology timeout");
        }
    }

    @Override
    public void execute(Tuple tuple) {

        synchronized (this.writeLock) {
            boolean forceSync = false;
            if (TupleUtils.isTick(tuple)) {
                LOG.debug("TICK! forcing a file system flush");
                forceSync = true;
            }
            else {
                try {
                    writeAndAddTuple(tuple);
                } catch (IOException e) {
                    //If the write failed, try to sync anything already written
                    LOG.info("Tuple failed to write, forcing a flush of existing data.");
                    this.collector.reportError(e);
                    forceSync = true;
                    this.collector.fail(tuple);
                }
            }

            if (this.syncPolicy.mark(tuple, this.offset) || (forceSync && tupleBatch.size() > 0)) {
                int attempts = 0;
                boolean success = false;
                IOException lastException = null;
                // Make every attempt to sync the data we have.  If it can't be done then kill the bolt with
                // a runtime exception.  The filesystem is presumably in a very bad state.
                while (success == false && attempts < fileRetryCount)
                {
                    attempts += 1;
                    try {
                        syncAndAckTuples();
                        success = true;
                    } catch (IOException e) {
                        LOG.warn("Data could not be synced to filesystem on attempt [" + attempts + "]");
                        this.collector.reportError(e);
                        lastException = e;
                    }
                }

                // If unsuccesful fail the pending tuples
                if (success == false)
                {
                    LOG.warn("Data could not be synced to filesystem, failing this batch of tuples");
                    for (Tuple t : tupleBatch)
                        this.collector.fail(t);
                    tupleBatch.clear();

                    throw new RuntimeException("Sync failed [" + attempts + "] times.", lastException);
                }
            }

        }

        if(this.rotationPolicy.mark(tuple, this.offset)) {
            try {
                rotateAndReset();
            } catch (IOException e) {
                this.collector.reportError(e);
                LOG.warn("File could not be rotated");
                //At this point there is nothing to do.  In all likelihood any filesystem operations will fail.
                //The next tuple will almost certainly fail to write and/or sync, which force a rotation.  That
                //will give rotateAndReset() a chance to work which includes creating a fresh file handle.
            }
        }
    }

    private void rotateAndReset() throws IOException {
        rotateOutputFile(); // synchronized
        this.offset = 0;
        this.rotationPolicy.reset();
    }

    private void syncAndAckTuples() throws IOException {
        LOG.debug("Attempting to sync all data to filesystem");
        if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
        this.syncPolicy.reset();
        LOG.debug("Data synced to filesystem. Ack'ing [" + tupleBatch.size() +"] tuples");
        for (Tuple t : tupleBatch)
            this.collector.ack(t);
        tupleBatch.clear();
    }

    private void writeAndAddTuple(Tuple tuple) throws IOException {
        byte[] bytes = this.format.format(tuple);
        out.write(bytes);
        this.offset += bytes.length;
        tupleBatch.add(tuple);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = super.getComponentConfiguration();
        if (conf == null)
            conf = new Config();

        if (tickTupleInterval > 0) {
            LOG.info("Enabling tick tuple with interval [" + tickTupleInterval + "]");
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTupleInterval);
        }

        return conf;
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
}
