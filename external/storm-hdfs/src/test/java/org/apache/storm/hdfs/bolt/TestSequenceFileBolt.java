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

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;
import org.apache.storm.hdfs.bolt.format.DefaultSequenceFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.junit.*;

import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TestSequenceFileBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TestSequenceFileBolt.class);

    private String hdfsURI;
    private DistributedFileSystem fs;
    private MiniDFSCluster hdfsCluster;
    private static final String testRoot = "/unittest";
    Tuple tuple1 = generateTestTuple(1l, "first tuple");
    Tuple tuple2 = generateTestTuple(2l, "second tuple");

    @Mock private OutputCollector collector;
    @Mock private TopologyContext topologyContext;
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Configuration conf = new Configuration();
        conf.set("fs.trash.interval", "10");
        conf.setBoolean("dfs.permissions", true);
        File baseDir = new File("./target/hdfs/").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        fs = hdfsCluster.getFileSystem();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
    }

    @After
    public void shutDown() throws IOException {
        fs.close();
        hdfsCluster.shutdown();
    }

    @Test
    public void testTwoTuplesTwoFiles() throws IOException {
        SequenceFileBolt bolt = makeSeqBolt(hdfsURI, 1, .00001f);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple2);

        verify(collector).ack(tuple1);
        verify(collector).ack(tuple2);

        Assert.assertEquals(2, countNonZeroLengthFiles(testRoot));
    }

    @Test
    public void testTwoTuplesOneFile() throws IOException
    {
        SequenceFileBolt bolt = makeSeqBolt(hdfsURI, 2, 10000f);
        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);

        verifyZeroInteractions(collector);

        bolt.execute(tuple2);
        verify(collector).ack(tuple1);
        verify(collector).ack(tuple2);

        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot));
    }

    @Test
    public void testFailedSync() throws IOException
    {
        SequenceFileBolt bolt = makeSeqBolt(hdfsURI, 2, 10000f);
        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);

        fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        // All writes/syncs will fail so this should cause a RuntimeException
        thrown.expect(RuntimeException.class);
        bolt.execute(tuple1);
    }

    private SequenceFileBolt makeSeqBolt(String nameNodeAddr, int countSync, float rotationSizeMB) {

        SyncPolicy fieldsSyncPolicy = new CountSyncPolicy(countSync);

        FileRotationPolicy fieldsRotationPolicy =
                new FileSizeRotationPolicy(rotationSizeMB, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fieldsFileNameFormat = new DefaultFileNameFormat().withPath(testRoot);

        SequenceFormat seqFormat = new DefaultSequenceFormat("key", "value");

        return new SequenceFileBolt()
                .withFsUrl(nameNodeAddr)
                .withFileNameFormat(fieldsFileNameFormat)
                .withRotationPolicy(fieldsRotationPolicy)
                .withSequenceFormat(seqFormat)
                .withSyncPolicy(fieldsSyncPolicy);
    }

    private Tuple generateTestTuple(Long key, String value) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("key", "value");
            }
        };
        return new TupleImpl(topologyContext, new Values(key, value), 1, "");
    }

    // Generally used to compare how files were actually written and compare to expectations based on total
    // amount of data written and rotation policies
    private int countNonZeroLengthFiles(String path) throws IOException {
        Path p = new Path(path);
        int nonZero = 0;

        for (FileStatus file : fs.listStatus(p)) {
            if (file.getLen() > 0) {
                nonZero++;
            }
        }

        return nonZero;
    }

}
