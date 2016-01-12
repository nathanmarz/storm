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
package org.apache.storm.blobstore;

import org.apache.storm.Config;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  Unit tests for most of the testable utility methods
 *  and BlobSynchronizer class methods
 */
public class BlobSynchronizerTest {
  private URI base;
  private File baseFile;
  private static Map conf = new HashMap();
  private NIOServerCnxnFactory factory;

  @Before
  public void init() throws Exception {
    initializeConfigs();
    baseFile = new File("target/blob-store-test-"+ UUID.randomUUID());
    base = baseFile.toURI();
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(baseFile);
    if (factory != null) {
      factory.shutdown();
    }
  }

  // Method which initializes nimbus admin
  public static void initializeConfigs() {
    conf.put(Config.NIMBUS_ADMINS,"admin");
    conf.put(Config.NIMBUS_SUPERVISOR_USERS,"supervisor");
  }

  private LocalFsBlobStore initLocalFs() {
    LocalFsBlobStore store = new LocalFsBlobStore();
    Map conf = Utils.readStormConfig();
    conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"org.apache.storm.security.auth.DefaultPrincipalToLocal");
    this.conf = conf;
    store.prepare(conf, null, null);
    return store;
  }

  @Test
  public void testBlobSynchronizerForKeysToDownload() {
    BlobStore store = initLocalFs();
    BlobSynchronizer sync = new BlobSynchronizer(store, conf);
    // test for keylist to download
    Set<String> zkSet = new HashSet<String>();
    zkSet.add("key1");
    Set<String> blobStoreSet = new HashSet<String>();
    blobStoreSet.add("key1");
    Set<String> resultSet = sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue("Not Empty", resultSet.isEmpty());
    zkSet.add("key1");
    blobStoreSet.add("key2");
    resultSet =  sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue("Not Empty", resultSet.isEmpty());
    blobStoreSet.remove("key1");
    blobStoreSet.remove("key2");
    zkSet.add("key1");
    resultSet =  sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue("Unexpected keys to download", (resultSet.size() == 1) && (resultSet.contains("key1")));
  }

  @Test
  public void testGetLatestSequenceNumber() throws Exception {
    List<String> stateInfoList = new ArrayList<String>();
    stateInfoList.add("nimbus1:8000-2");
    stateInfoList.add("nimbus-1:8000-4");
    assertTrue("Failed to get the latest version", BlobStoreUtils.getLatestSequenceNumber(stateInfoList)==4);
  }

  @Test
  public void testNimbodesWithLatestVersionOfBlob() throws Exception {
    TestingServer server = new TestingServer();
    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
    // Creating nimbus hosts containing latest version of blob
    zkClient.create().creatingParentContainersIfNeeded().forPath("/blobstore/key1/nimbus1:7800-1");
    zkClient.create().creatingParentContainersIfNeeded().forPath("/blobstore/key1/nimbus2:7800-2");
    Set<NimbusInfo> set = BlobStoreUtils.getNimbodesWithLatestSequenceNumberOfBlob(zkClient, "key1");
    assertEquals("Failed to get the correct nimbus hosts with latest blob version", (set.iterator().next()).getHost(),"nimbus2");
    zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/key1/nimbus1:7800-1");
    zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/key1/nimbus2:7800-2");
    zkClient.close();
    server.close();
  }

  @Test
  public void testNormalizeVersionInfo () throws Exception {
    BlobKeySequenceInfo info1 = BlobStoreUtils.normalizeNimbusHostPortSequenceNumberInfo("nimbus1:7800-1");
    assertTrue(info1.getNimbusHostPort().equals("nimbus1:7800"));
    assertTrue(info1.getSequenceNumber().equals("1"));
    BlobKeySequenceInfo info2 = BlobStoreUtils.normalizeNimbusHostPortSequenceNumberInfo("nimbus-1:7800-1");
    assertTrue(info2.getNimbusHostPort().equals("nimbus-1:7800"));
    assertTrue(info2.getSequenceNumber().equals("1"));
  }
}
