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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;


public class HdfsBolt extends BaseRichBolt{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

    private OutputCollector collector;
    private FileSystem fs;
    private FSDataOutputStream out;
    private RecordFormat format;
    private SyncPolicy syncPolicy;
    private FileRotationPolicy rotationPolicy;
    private FileNameFormat fileNameFormat;
    private int rotation = 0;
    private String fsUrl;
    private String path;

    public HdfsBolt(){
    }

    public HdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBolt withPath(String path){
        this.path = path;
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

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        LOG.info("Preparing HDFS Bolt...");
        if(this.format == null) throw new IllegalStateException("RecordFormat must be specified.");
        if(this.syncPolicy == null) throw new IllegalStateException("SyncPolicy must be specified.");
        if(this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");


        if(this.fsUrl ==  null || this.path == null){
            throw new IllegalStateException("File system URL and base path must be specified.");
        }
        this.collector = collector;
        this.fileNameFormat.prepare(conf, topologyContext);

        try{
            Configuration hdfsConfig = new Configuration();
            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            out = this.fs.create(new Path(this.path, this.fileNameFormat.getName(this.rotation, System.currentTimeMillis())));
        } catch (Exception e){
            throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] bytes = this.format.format(tuple);
            out.write(bytes);
            this.collector.ack(tuple);

            if(this.syncPolicy.mark(tuple, bytes)){
                long start = System.currentTimeMillis();
                this.out.hsync();
                this.syncPolicy.reset();
            }
            if(this.rotationPolicy.mark(tuple, bytes)){
                rotateOutputFile();
                this.rotationPolicy.reset();
            }
        } catch (IOException e) {
            LOG.warn("write/sync failed.", e);
            this.collector.fail(tuple);
        }
    }

    private void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        this.out.hsync();
        this.out.close();
        this.rotation++;
        this.out = this.fs.create(new Path(this.path, this.fileNameFormat.getName(this.rotation, System.currentTimeMillis())));
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
