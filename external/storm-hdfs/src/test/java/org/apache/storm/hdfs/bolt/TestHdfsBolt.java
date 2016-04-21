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
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MockTupleHelpers;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.Partitioner;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;

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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TestHdfsBolt {

    private String hdfsURI;
    private DistributedFileSystem fs;
    private MiniDFSCluster hdfsCluster;
    private static final String testRoot = "/unittest";
    Tuple tuple1 = generateTestTuple(1, "First Tuple", "SFO", "CA");
    Tuple tuple2 = generateTestTuple(1, "Second Tuple", "SJO", "CA");

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
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 1, .00001f);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple2);

        verify(collector).ack(tuple1);
        verify(collector).ack(tuple2);

        Assert.assertEquals(2, countNonZeroLengthFiles(testRoot));
    }

    @Test
    public void testPartitionedOutput() throws IOException {
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 1, 1000f);

        Partitioner partitoner = new Partitioner() {
            @Override
            public String getPartitionPath(Tuple tuple) {
                return Path.SEPARATOR + tuple.getStringByField("city");
            }
        };

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.withPartitioner(partitoner);

        bolt.execute(tuple1);
        bolt.execute(tuple2);

        verify(collector).ack(tuple1);
        verify(collector).ack(tuple2);

        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot + "/SFO"));
        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot + "/SJO"));
    }

    @Test
    public void testTwoTuplesOneFile() throws IOException
    {
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 2, 10000f);
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
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 2, 10000f);
        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);

        fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

        // All writes/syncs will fail so this should cause a RuntimeException
        thrown.expect(RuntimeException.class);
        bolt.execute(tuple1);

    }

    // One tuple and one rotation should yield one file with data
    // The failed executions should not cause rotations and any new files
    @Test
    public void testFailureFilecount() throws IOException, InterruptedException
    {
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 1, .000001f);
        bolt.prepare(new Config(), topologyContext, collector);

        bolt.execute(tuple1);
        fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        try
        {
            bolt.execute(tuple2);
        } catch (RuntimeException e) {
            //
        }
        try
        {
            bolt.execute(tuple2);
        } catch (RuntimeException e) {
            //
        }
        try
        {
            bolt.execute(tuple2);
        } catch (RuntimeException e) {
            //
        }

        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot));
        Assert.assertEquals(0, countZeroLengthFiles(testRoot));
    }

    @Test
    public void testTickTuples() throws IOException
    {
        HdfsBolt bolt = makeHdfsBolt(hdfsURI, 10, 10000f);
        bolt.prepare(new Config(), topologyContext, collector);

        bolt.execute(tuple1);

        //Should not have flushed to file system yet
        Assert.assertEquals(0, countNonZeroLengthFiles(testRoot));

        bolt.execute(MockTupleHelpers.mockTickTuple());

        //Tick should have flushed it
        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot));
    }

    public void createBaseDirectory(FileSystem passedFs, String path) throws IOException {
        Path p = new Path(path);
        passedFs.mkdirs(p);
    }

    private HdfsBolt makeHdfsBolt(String nameNodeAddr, int countSync, float rotationSizeMB) {

        RecordFormat fieldsFormat = new DelimitedRecordFormat().withFieldDelimiter("|");

        SyncPolicy fieldsSyncPolicy = new CountSyncPolicy(countSync);

        FileRotationPolicy fieldsRotationPolicy =
                new FileSizeRotationPolicy(rotationSizeMB, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fieldsFileNameFormat = new DefaultFileNameFormat().withPath(testRoot);

        return new HdfsBolt()
                .withFsUrl(nameNodeAddr)
                .withFileNameFormat(fieldsFileNameFormat)
                .withRecordFormat(fieldsFormat)
                .withRotationPolicy(fieldsRotationPolicy)
                .withSyncPolicy(fieldsSyncPolicy);
    }

    private Tuple generateTestTuple(Object id, Object msg,Object city,Object state) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("id", "msg","city","state");
            }
        };
        return new TupleImpl(topologyContext, new Values(id, msg,city,state), 1, "");
    }

    private void printFiles(String path) throws IOException {
        Path p = new Path(path);
        FileStatus[] fileStatuses = fs.listStatus(p);
        for (FileStatus file : fileStatuses) {
            System.out.println("@@@ " + file.getPath() + " [" + file.getLen() + "]");
        }
    }

    // Generally used to compare how files were actually written and compare to expectations based on total
    // amount of data written and rotation policies
    private int countNonZeroLengthFiles(String path) throws IOException {
        Path p = new Path(path);
        int nonZero = 0;

        for (FileStatus file : fs.listStatus(p))
            if (file.getLen() > 0)
                nonZero++;

        return nonZero;
    }

    private int countZeroLengthFiles(String path) throws IOException {
        Path p = new Path(path);
        int zeroLength = 0;

        for (FileStatus file : fs.listStatus(p))
            if (file.getLen() == 0)
                zeroLength++;

        return zeroLength;
    }
}
