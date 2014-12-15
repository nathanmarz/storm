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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.TException;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveSetupUtil {
    public static class RawFileSystem extends RawLocalFileSystem {
        private static final URI NAME;
        static {
            try {
                NAME = new URI("raw:///");
            } catch (URISyntaxException se) {
                throw new IllegalArgumentException("bad uri", se);
            }
        }

        @Override
        public URI getUri() {
            return NAME;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            File file = pathToFile(path);
            if (!file.exists()) {
                throw new FileNotFoundException("Can't find " + path);
            }
            // get close enough
            short mod = 0;
            if (file.canRead()) {
                mod |= 0444;
            }
            if (file.canWrite()) {
                mod |= 0200;
            }
            if (file.canExecute()) {
                mod |= 0111;
            }
            ShimLoader.getHadoopShims();
            return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
                                  file.lastModified(), file.lastModified(),
                                  FsPermission.createImmutable(mod), "owen", "users", path);
        }
    }

    private final static String txnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

    public static HiveConf getHiveConf() {
        HiveConf conf = new HiveConf();
        // String metastoreDBLocation = "jdbc:derby:databaseName=/tmp/metastore_db;create=true";
        // conf.set("javax.jdo.option.ConnectionDriverName","org.apache.derby.jdbc.EmbeddedDriver");
        // conf.set("javax.jdo.option.ConnectionURL",metastoreDBLocation);
        conf.set("fs.raw.impl", RawFileSystem.class.getName());
        conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, txnMgr);
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
        return conf;
    }

    public static void createDbAndTable(HiveConf conf, String databaseName,
                                        String tableName, List<String> partVals,
                                        String[] colNames, String[] colTypes,
                                        String[] partNames, String dbLocation)
        throws Exception {
        IMetaStoreClient client = new HiveMetaStoreClient(conf);
        try {
            Database db = new Database();
            db.setName(databaseName);
            db.setLocationUri(dbLocation);
            client.createDatabase(db);

            Table tbl = new Table();
            tbl.setDbName(databaseName);
            tbl.setTableName(tableName);
            tbl.setTableType(TableType.MANAGED_TABLE.toString());
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(getTableColumns(colNames, colTypes));
            sd.setNumBuckets(1);
            sd.setLocation(dbLocation + Path.SEPARATOR + tableName);
            if(partNames!=null && partNames.length!=0) {
                tbl.setPartitionKeys(getPartitionKeys(partNames));
            }

            tbl.setSd(sd);

            sd.setBucketCols(new ArrayList<String>(2));
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tbl.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");

            sd.getSerdeInfo().setSerializationLib(OrcSerde.class.getName());
            sd.setInputFormat(OrcInputFormat.class.getName());
            sd.setOutputFormat(OrcOutputFormat.class.getName());

            Map<String, String> tableParams = new HashMap<String, String>();
            tbl.setParameters(tableParams);
            client.createTable(tbl);
            try {
                if(partVals!=null && partVals.size() > 0) {
                    addPartition(client, tbl, partVals);
                }
            } catch(AlreadyExistsException e) {
            }
        } finally {
            client.close();
        }
    }

    // delete db and all tables in it
    public static void dropDB(HiveConf conf, String databaseName) throws HiveException, MetaException {
        IMetaStoreClient client = new HiveMetaStoreClient(conf);
        try {
            for (String table : client.listTableNamesByFilter(databaseName, "", (short) -1)) {
                client.dropTable(databaseName, table, true, true);
            }
            client.dropDatabase(databaseName);
        } catch (TException e) {
            client.close();
        }
    }

    private static void addPartition(IMetaStoreClient client, Table tbl
                                     , List<String> partValues)
        throws IOException, TException {
        Partition part = new Partition();
        part.setDbName(tbl.getDbName());
        part.setTableName(tbl.getTableName());
        StorageDescriptor sd = new StorageDescriptor(tbl.getSd());
        sd.setLocation(sd.getLocation() + Path.SEPARATOR + makePartPath(tbl.getPartitionKeys(), partValues));
        part.setSd(sd);
        part.setValues(partValues);
        client.add_partition(part);
    }

    private static String makePartPath(List<FieldSchema> partKeys, List<String> partVals) {
        if(partKeys.size()!=partVals.size()) {
            throw new IllegalArgumentException("Partition values:" + partVals +
                                               ", does not match the partition Keys in table :" + partKeys );
        }
        StringBuffer buff = new StringBuffer(partKeys.size()*20);
        int i=0;
        for(FieldSchema schema : partKeys) {
            buff.append(schema.getName());
            buff.append("=");
            buff.append(partVals.get(i));
            if(i!=partKeys.size()-1) {
                buff.append(Path.SEPARATOR);
            }
            ++i;
        }
        return buff.toString();
    }

    private static List<FieldSchema> getTableColumns(String[] colNames, String[] colTypes) {
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for (int i=0; i<colNames.length; ++i) {
            fields.add(new FieldSchema(colNames[i], colTypes[i], ""));
        }
        return fields;
    }

    private static List<FieldSchema> getPartitionKeys(String[] partNames) {
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for (int i=0; i < partNames.length; ++i) {
           fields.add(new FieldSchema(partNames[i], serdeConstants.STRING_TYPE_NAME, ""));
        }
        return fields;
    }

}
