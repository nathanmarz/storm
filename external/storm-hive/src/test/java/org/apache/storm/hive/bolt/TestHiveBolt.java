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

package org.apache.storm.hive.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MockTupleHelpers;

import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.HashSet;
import java.text.SimpleDateFormat;


import org.apache.hive.hcatalog.streaming.*;

public class TestHiveBolt {
    final static String dbName = "testdb";
    final static String tblName = "test_table";
    final static String dbName1 = "testdb1";
    final static String tblName1 = "test_table1";
    final static String PART1_NAME = "city";
    final static String PART2_NAME = "state";
    final static String[] partNames = { PART1_NAME, PART2_NAME };
    final String partitionVals = "sunnyvale,ca";
    private static final String COL1 = "id";
    private static final String COL2 = "msg";
    final String[] colNames = {COL1,COL2};
    final String[] colNames1 = {COL2,COL1};
    private String[] colTypes = {serdeConstants.INT_TYPE_NAME, serdeConstants.STRING_TYPE_NAME};
    private final HiveConf conf;
    private final Driver driver;
    private final int port ;
    final String metaStoreURI;
    private String dbLocation;
    private Config config = new Config();
    private HiveBolt bolt;
    private final static boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Mock
    private OutputCollector collector;

    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);

    public TestHiveBolt() throws Exception {
        port=9083;
        dbLocation = new String();
        //metaStoreURI = "jdbc:derby:;databaseName="+System.getProperty("java.io.tmpdir") +"metastore_db;create=true";
        metaStoreURI = null;
        conf = HiveSetupUtil.getHiveConf();
        TxnDbUtil.setConfValues(conf);
        TxnDbUtil.cleanDb();
        TxnDbUtil.prepDb();
        SessionState.start(new CliSessionState(conf));
        driver = new Driver(conf);

        // driver.init();
    }

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        HiveSetupUtil.dropDB(conf, dbName);
        if(WINDOWS) {
            dbLocation = dbFolder.newFolder(dbName + ".db").getCanonicalPath();
        } else {
            dbLocation = "raw://" + dbFolder.newFolder(dbName + ".db").getCanonicalPath();
        }
        HiveSetupUtil.createDbAndTable(conf, dbName, tblName, Arrays.asList(partitionVals.split(",")),
                colNames, colTypes, partNames, dbLocation);
        System.out.println("done");
    }

    @Test
    public void testEndpointConnection() throws Exception {
        // 1) Basic
        HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
                                              , Arrays.asList(partitionVals.split(",")));
        StreamingConnection connection = endPt.newConnection(false, null); //shouldn't throw
        connection.close();
        // 2) Leave partition unspecified
        endPt = new HiveEndPoint(metaStoreURI, dbName, tblName, null);
        endPt.newConnection(false, null).close(); // should not throw
    }

    @Test
    public void testWithByteArrayIdandMessage()
        throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
            .withTxnsPerBatch(2)
            .withBatchSize(2);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config,null,collector);
        Integer id = 100;
        String msg = "test-123";
        String city = "sunnyvale";
        String state = "ca";
        checkRecordCountInTable(tblName, dbName, 0);

        Set<Tuple> tupleSet = new HashSet<Tuple>();
        for (int i=0; i < 4; i++) {
            Tuple tuple = generateTestTuple(id,msg,city,state);
            bolt.execute(tuple);
            tupleSet.add(tuple);
        }
        for (Tuple t : tupleSet)
            verify(collector).ack(t);
        checkRecordCountInTable(tblName, dbName, 4);
        bolt.cleanup();
    }


    @Test
    public void testWithoutPartitions()
        throws Exception {
        HiveSetupUtil.dropDB(conf,dbName1);
        HiveSetupUtil.createDbAndTable(conf, dbName1, tblName1,null,
                                       colNames,colTypes,null, dbLocation);
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName1,tblName1,mapper)
            .withTxnsPerBatch(2)
            .withBatchSize(2)
            .withAutoCreatePartitions(false);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config,null,collector);
        Integer id = 100;
        String msg = "test-123";
        String city = "sunnyvale";
        String state = "ca";
        checkRecordCountInTable(tblName1,dbName1,0);

        Set<Tuple> tupleSet = new HashSet<Tuple>();
        for (int i=0; i < 4; i++) {
            Tuple tuple = generateTestTuple(id,msg,city,state);
            bolt.execute(tuple);
            tupleSet.add(tuple);
        }
        for (Tuple t : tupleSet)
            verify(collector).ack(t);
        bolt.cleanup();
        checkRecordCountInTable(tblName1, dbName1, 4);
    }

    @Test
    public void testWithTimeformat()
        throws Exception {
        String[] partNames1 = {"dt"};
        String timeFormat = "yyyy/MM/dd";
        HiveSetupUtil.dropDB(conf,dbName1);
        HiveSetupUtil.createDbAndTable(conf, dbName1, tblName1, null,
                colNames, colTypes, partNames1, dbLocation);
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withTimeAsPartitionField(timeFormat);
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName1,tblName1,mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(1)
                .withMaxOpenConnections(1);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config,null,collector);
        Integer id = 100;
        String msg = "test-123";
        Date d = new Date();
        SimpleDateFormat parseDate = new SimpleDateFormat(timeFormat);
        String today=parseDate.format(d.getTime());
        checkRecordCountInTable(tblName1, dbName1, 0);

        Set<Tuple> tupleSet = new HashSet<Tuple>();
        for (int i=0; i < 2; i++) {
            Tuple tuple = generateTestTuple(id,msg,null,null);
            tupleSet.add(tuple);
            bolt.execute(tuple);
        }
        for (Tuple t : tupleSet)
            verify(collector).ack(t);
        checkDataWritten(tblName1, dbName1, "100,test-123," + today, "100,test-123," + today);
        bolt.cleanup();
    }

    @Test
    public void testData()
        throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
            .withTxnsPerBatch(2)
            .withBatchSize(1);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config, null, new OutputCollector(collector));
        Tuple tuple1 = generateTestTuple(1, "SJC", "Sunnyvale", "CA");
        //Tuple tuple2 = generateTestTuple(2,"SFO","San Jose","CA");
        bolt.execute(tuple1);
        verify(collector).ack(tuple1);
        //bolt.execute(tuple2);
        //verify(collector).ack(tuple2);
        checkDataWritten(tblName, dbName, "1,SJC,Sunnyvale,CA");
        bolt.cleanup();
    }

    @Test
    public void testJsonWriter()
        throws Exception {
        // json record doesn't need columns to be in the same order
        // as table in hive.
        JsonRecordHiveMapper mapper = new JsonRecordHiveMapper()
            .withColumnFields(new Fields(colNames1))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
            .withTxnsPerBatch(2)
            .withBatchSize(1);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config,null,collector);
        Tuple tuple1 = generateTestTuple(1,"SJC","Sunnyvale","CA");
        //Tuple tuple2 = generateTestTuple(2,"SFO","San Jose","CA");
        bolt.execute(tuple1);
        verify(collector).ack(tuple1);
        //bolt.execute(tuple2);
        //verify(collector).ack(tuple2);
        checkDataWritten(tblName, dbName, "1,SJC,Sunnyvale,CA");
        bolt.cleanup();
    }

    @Test
    public void testNoAcksUntilFlushed()
    {
        JsonRecordHiveMapper mapper = new JsonRecordHiveMapper()
                .withColumnFields(new Fields(colNames1))
                .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(2);

        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config, null, new OutputCollector(collector));

        Tuple tuple1 = generateTestTuple(1,"SJC","Sunnyvale","CA");
        Tuple tuple2 = generateTestTuple(2,"SFO","San Jose","CA");

        bolt.execute(tuple1);
        verifyZeroInteractions(collector);

        bolt.execute(tuple2);
        verify(collector).ack(tuple1);
        verify(collector).ack(tuple2);
        bolt.cleanup();
    }

    @Test
    public void testNoAcksIfFlushFails() throws Exception
    {
        JsonRecordHiveMapper mapper = new JsonRecordHiveMapper()
                .withColumnFields(new Fields(colNames1))
                .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(2);

        HiveBolt spyBolt = Mockito.spy(new HiveBolt(hiveOptions));

        //This forces a failure of all the flush attempts
        doThrow(new InterruptedException()).when(spyBolt).flushAllWriters(true);


        spyBolt.prepare(config, null, new OutputCollector(collector));

        Tuple tuple1 = generateTestTuple(1,"SJC","Sunnyvale","CA");
        Tuple tuple2 = generateTestTuple(2,"SFO","San Jose","CA");

        spyBolt.execute(tuple1);
        spyBolt.execute(tuple2);

        verify(collector, never()).ack(tuple1);
        verify(collector, never()).ack(tuple2);

        spyBolt.cleanup();
    }

    @Test
    public void testTickTuple()
    {
        JsonRecordHiveMapper mapper = new JsonRecordHiveMapper()
                .withColumnFields(new Fields(colNames1))
                .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(2);

        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config, null, new OutputCollector(collector));

        Tuple tuple1 = generateTestTuple(1, "SJC", "Sunnyvale", "CA");
        Tuple tuple2 = generateTestTuple(2, "SFO", "San Jose", "CA");


        bolt.execute(tuple1);

        //The tick should cause tuple1 to be ack'd
        Tuple mockTick = MockTupleHelpers.mockTickTuple();
        bolt.execute(mockTick);
        verify(collector).ack(tuple1);

        //The second tuple should NOT be ack'd because the batch should be cleared and this will be
        //the first transaction in the new batch
        bolt.execute(tuple2);
        verify(collector, never()).ack(tuple2);

        bolt.cleanup();
    }

    @Test
    public void testNoTickEmptyBatches() throws Exception
    {
        JsonRecordHiveMapper mapper = new JsonRecordHiveMapper()
                .withColumnFields(new Fields(colNames1))
                .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(2);

        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config, null, new OutputCollector(collector));

        //The tick should NOT cause any acks since the batch was empty except for acking itself
        Tuple mockTick = MockTupleHelpers.mockTickTuple();
        bolt.execute(mockTick);
        verifyZeroInteractions(collector);

        bolt.cleanup();
    }

    @Test
    public void testMultiPartitionTuples()
        throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
            .withTxnsPerBatch(10)
            .withBatchSize(10);
        bolt = new HiveBolt(hiveOptions);
        bolt.prepare(config,null,new OutputCollector(collector));
        Integer id = 1;
        String msg = "test";
        String city = "San Jose";
        String state = "CA";
        checkRecordCountInTable(tblName,dbName,0);

        Set<Tuple> tupleSet = new HashSet<Tuple>();
        for(int i=0; i < 100; i++) {
            Tuple tuple = generateTestTuple(id,msg,city,state);
            tupleSet.add(tuple);
            bolt.execute(tuple);
        }
        checkRecordCountInTable(tblName, dbName, 100);
        for (Tuple t : tupleSet)
            verify(collector).ack(t);
        bolt.cleanup();
    }

    private void checkRecordCountInTable(String tableName,String dbName,int expectedCount)
        throws CommandNeedRetryException, IOException {
        int count = listRecordsInTable(tableName,dbName).size();
        Assert.assertEquals(expectedCount, count);
    }

    private  ArrayList<String> listRecordsInTable(String tableName,String dbName)
        throws CommandNeedRetryException, IOException {
        driver.compile("select * from " + dbName + "." + tableName);
        ArrayList<String> res = new ArrayList<String>();
        driver.getResults(res);
        return res;
    }

    private void checkDataWritten(String tableName,String dbName,String... row)
        throws CommandNeedRetryException, IOException {
        ArrayList<String> results = listRecordsInTable(tableName,dbName);
        for(int i = 0; i < row.length && results.size() > 0; i++) {
            String resultRow = results.get(i).replace("\t",",");
            System.out.println(resultRow);
            assertEquals(row[i],resultRow);
        }
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

}
