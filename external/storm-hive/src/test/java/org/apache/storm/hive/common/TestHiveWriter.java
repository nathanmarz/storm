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

package org.apache.storm.hive.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import junit.framework.Assert;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.hive.bolt.HiveSetupUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashMap;

public class TestHiveWriter {
    final static String dbName = "testdb";
    final static String tblName = "test_table2";

    public static final String PART1_NAME = "city";
    public static final String PART2_NAME = "state";
    public static final String[] partNames = { PART1_NAME, PART2_NAME };
    final String[] partitionVals = {"sunnyvale","ca"};
    final String[] colNames = {"id","msg"};
    private String[] colTypes = { "int", "string" };
    private final int port;
    private final String metaStoreURI;
    private final HiveConf conf;
    private ExecutorService callTimeoutPool;
    private final Driver driver;
    int timeout = 10000; // msec
    UserGroupInformation ugi = null;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();


    public TestHiveWriter() throws Exception {
        port = 9083;
        metaStoreURI = null;
        int callTimeoutPoolSize = 1;
        callTimeoutPool = Executors.newFixedThreadPool(callTimeoutPoolSize,
                                                       new ThreadFactoryBuilder().setNameFormat("hiveWriterTest").build());

        // 1) Start metastore
        conf = HiveSetupUtil.getHiveConf();
        TxnDbUtil.setConfValues(conf);
        TxnDbUtil.cleanDb();
        TxnDbUtil.prepDb();

        if(metaStoreURI!=null) {
            conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);
        }
        SessionState.start(new CliSessionState(conf));
        driver = new Driver(conf);
        driver.init();
    }

    @Before
    public void setUp() throws Exception {
        // 1) Setup tables
        HiveSetupUtil.dropDB(conf, dbName);
        String dbLocation = dbFolder.newFolder(dbName).getCanonicalPath() + ".db";
        HiveSetupUtil.createDbAndTable(conf, dbName, tblName, Arrays.asList(partitionVals),
                                       colNames,colTypes, partNames, dbLocation);
    }

    @Test
    public void testInstantiate() throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        HiveWriter writer = new HiveWriter(endPoint, 10, true, timeout
                                           ,callTimeoutPool, mapper, ugi);
        writer.close();
    }

    @Test
    public void testWriteBasic() throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        HiveWriter writer = new HiveWriter(endPoint, 10, true, timeout
                                           , callTimeoutPool, mapper, ugi);
        writeTuples(writer,mapper,3);
        writer.flush(false);
        writer.close();
        checkRecordCountInTable(dbName,tblName,3);
    }

    @Test
    public void testWriteMultiFlush() throws Exception {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));

        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        HiveWriter writer = new HiveWriter(endPoint, 10, true, timeout
                                           , callTimeoutPool, mapper, ugi);
        Tuple tuple = generateTestTuple("1","abc");
        writer.write(mapper.mapRecord(tuple));
        tuple = generateTestTuple("2","def");
        writer.write(mapper.mapRecord(tuple));
        Assert.assertEquals(writer.getTotalRecords(), 2);
        checkRecordCountInTable(dbName,tblName,0);
        writer.flush(true);
        Assert.assertEquals(writer.getTotalRecords(), 0);

        tuple = generateTestTuple("3","ghi");
        writer.write(mapper.mapRecord(tuple));
        writer.flush(true);

        tuple = generateTestTuple("4","klm");
        writer.write(mapper.mapRecord(tuple));
        writer.flush(true);
        writer.close();
        checkRecordCountInTable(dbName,tblName,4);
    }

    private Tuple generateTestTuple(Object id, Object msg) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                                                              new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
                @Override
                public Fields getComponentOutputFields(String componentId, String streamId) {
                    return new Fields("id", "msg");
                }
            };
        return new TupleImpl(topologyContext, new Values(id, msg), 1, "");
    }

    private void writeTuples(HiveWriter writer, HiveMapper mapper, int count)
        throws HiveWriter.WriteFailure, InterruptedException, SerializationError {
        Integer id = 100;
        String msg = "test-123";
        for (int i = 1; i <= count; i++) {
            Tuple tuple = generateTestTuple(id,msg);
            writer.write(mapper.mapRecord(tuple));
        }
    }

    private void checkRecordCountInTable(String dbName,String tableName,int expectedCount)
        throws CommandNeedRetryException, IOException {
        int count = listRecordsInTable(dbName,tableName).size();
        Assert.assertEquals(expectedCount, count);
    }

    private  ArrayList<String> listRecordsInTable(String dbName,String tableName)
        throws CommandNeedRetryException, IOException {
        driver.compile("select * from " + dbName + "." + tableName);
        ArrayList<String> res = new ArrayList<String>();
        driver.getResults(res);
        return res;
    }

}
