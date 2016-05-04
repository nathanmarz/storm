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
package org.apache.storm.hdfs.blobstore;

import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.AccessControlType;

import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class BlobStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
  protected static MiniDFSCluster dfscluster = null;
  protected static Configuration hadoopConf = null;
  URI base;
  File baseFile;
  private static Map conf = new HashMap();
  public static final int READ = 0x01;
  public static final int WRITE = 0x02;
  public static final int ADMIN = 0x04;

  @Before
  public void init() {
    System.setProperty("test.build.data", "target/test/data");
    initializeConfigs();
    baseFile = new File("/tmp/blob-store-test-"+UUID.randomUUID());
    base = baseFile.toURI();
  }

  @After
  public void cleanup()
          throws IOException {
    FileUtils.deleteDirectory(baseFile);
  }

  @AfterClass
  public static void cleanupAfterClass() throws IOException {
    if (dfscluster != null) {
      dfscluster.shutdown();
    }
  }

  // Method which initializes nimbus admin
  public static void initializeConfigs() {
    conf.put(Config.NIMBUS_ADMINS,"admin");
    conf.put(Config.NIMBUS_SUPERVISOR_USERS,"supervisor");
  }

  //Gets Nimbus Subject with NimbusPrincipal set on it
  public static Subject getNimbusSubject() {
    Subject nimbus = new Subject();
    nimbus.getPrincipals().add(new NimbusPrincipal());
    return nimbus;
  }

  // Overloading the assertStoreHasExactly method accomodate Subject in order to check for authorization
  public static void assertStoreHasExactly(BlobStore store, Subject who, String ... keys)
          throws IOException, KeyNotFoundException, AuthorizationException {
    Set<String> expected = new HashSet<String>(Arrays.asList(keys));
    Set<String> found = new HashSet<String>();
    Iterator<String> c = store.listKeys();
    while (c.hasNext()) {
      String keyName = c.next();
      found.add(keyName);
    }
    Set<String> extra = new HashSet<String>(found);
    extra.removeAll(expected);
    assertTrue("Found extra keys in the blob store "+extra, extra.isEmpty());
    Set<String> missing = new HashSet<String>(expected);
    missing.removeAll(found);
    assertTrue("Found keys missing from the blob store "+missing, missing.isEmpty());
  }

  public static void assertStoreHasExactly(BlobStore store, String ... keys)
          throws IOException, KeyNotFoundException, AuthorizationException {
    assertStoreHasExactly(store, null, keys);
  }

  // Overloading the readInt method accomodate Subject in order to check for authorization (security turned on)
  public static int readInt(BlobStore store, Subject who, String key) throws IOException, KeyNotFoundException, AuthorizationException {
    InputStream in = store.getBlob(key, who);
    try {
      return in.read();
    } finally {
      in.close();
    }
  }

  public static int readInt(BlobStore store, String key)
          throws IOException, KeyNotFoundException, AuthorizationException {
    return readInt(store, null, key);
  }

  public static void readAssertEquals(BlobStore store, String key, int value)
          throws IOException, KeyNotFoundException, AuthorizationException {
    assertEquals(value, readInt(store, key));
  }

  // Checks for assertion when we turn on security
  public void readAssertEqualsWithAuth(BlobStore store, Subject who, String key, int value)
          throws IOException, KeyNotFoundException, AuthorizationException {
    assertEquals(value, readInt(store, who, key));
  }

  private HdfsBlobStore initHdfs(String dirName)
          throws Exception {
    if (hadoopConf == null) {
      hadoopConf = new Configuration();
    }
    try {
      if (dfscluster == null) {
        dfscluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(3).build();
        dfscluster.waitActive();
      }
    } catch (IOException e) {
      LOG.error("error creating MiniDFSCluster");
    }
    Map conf = new HashMap();
    conf.put(Config.BLOBSTORE_DIR, dirName);
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"org.apache.storm.security.auth.DefaultPrincipalToLocal");
    conf.put(Config.STORM_BLOBSTORE_REPLICATION_FACTOR, 3);
    HdfsBlobStore store = new HdfsBlobStore();
    store.prepareInternal(conf, null, dfscluster.getConfiguration(0));
    return store;
  }

  @Test
  public void testHdfsReplication()
          throws Exception {
    BlobStore store = initHdfs("/storm/blobstoreReplication");
    testReplication("/storm/blobstoreReplication/test", store);
  }

  @Test
  public void testBasicHdfs()
          throws Exception {
    testBasic(initHdfs("/storm/blobstore1"));
  }

  @Test
  public void testMultipleHdfs()
          throws Exception {
    // use different blobstore dir so it doesn't conflict with other test
    testMultiple(initHdfs("/storm/blobstore2"));
  }

  @Test
  public void testHdfsWithAuth()
          throws Exception {
    // use different blobstore dir so it doesn't conflict with other tests
    testWithAuthentication(initHdfs("/storm/blobstore3"));
  }

  // Test for replication.
  public void testReplication(String path, BlobStore store)
          throws Exception {
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    metadata.set_replication_factor(4);
    AtomicOutputStream out = store.createBlob("test", metadata, null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", null), 4);
    store.deleteBlob("test", null);

    //Test for replication with NIMBUS as user
    Subject admin = getSubject("admin");
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    metadata.set_replication_factor(4);
    out = store.createBlob("test", metadata, admin);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", admin), 4);
    store.updateBlobReplication("test", 5, admin);
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", admin), 5);
    store.deleteBlob("test", admin);

    //Test for replication using SUPERVISOR access
    Subject supervisor = getSubject("supervisor");
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    metadata.set_replication_factor(4);
    out = store.createBlob("test", metadata, supervisor);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", supervisor), 4);
    store.updateBlobReplication("test", 5, supervisor);
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", supervisor), 5);
    store.deleteBlob("test", supervisor);

    //Test for a user having read or write or admin access to read replication for a blob
    String createSubject = "createSubject";
    String writeSubject = "writeSubject";
    String adminSubject = "adminSubject";
    Subject who = getSubject(createSubject);
    AccessControl writeAccess = new AccessControl(AccessControlType.USER, READ);
    AccessControl adminAccess = new AccessControl(AccessControlType.USER, ADMIN);
    writeAccess.set_name(writeSubject);
    adminAccess.set_name(adminSubject);
    List<AccessControl> acl = Arrays.asList(writeAccess, adminAccess);
    metadata = new SettableBlobMeta(acl);
    metadata.set_replication_factor(4);
    out = store.createBlob("test", metadata, who);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    who = getSubject(writeSubject);
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", who), 4);

    //Test for a user having WRITE or ADMIN privileges to change replication of a blob
    who = getSubject(adminSubject);
    store.updateBlobReplication("test", 5, who);
    assertEquals("Blobstore replication not matching", store.getBlobReplication("test", who), 5);
    store.deleteBlob("test", getSubject(createSubject));
  }

  public Subject getSubject(String name) {
    Subject subject = new Subject();
    SingleUserPrincipal user = new SingleUserPrincipal(name);
    subject.getPrincipals().add(user);
    return subject;
  }

  // Check for Blobstore with authentication
  public void testWithAuthentication(BlobStore store)
          throws Exception {
    //Test for Nimbus Admin
    Subject admin = getSubject("admin");
    assertStoreHasExactly(store);
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    AtomicOutputStream out = store.createBlob("test", metadata, admin);
    assertStoreHasExactly(store, "test");
    out.write(1);
    out.close();
    store.deleteBlob("test", admin);

    //Test for Supervisor Admin
    Subject supervisor = getSubject("supervisor");
    assertStoreHasExactly(store);
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    out = store.createBlob("test", metadata, supervisor);
    assertStoreHasExactly(store, "test");
    out.write(1);
    out.close();
    store.deleteBlob("test", supervisor);

    //Test for Nimbus itself as a user
    Subject nimbus = getNimbusSubject();
    assertStoreHasExactly(store);
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    out = store.createBlob("test", metadata, nimbus);
    assertStoreHasExactly(store, "test");
    out.write(1);
    out.close();
    store.deleteBlob("test", nimbus);

    // Test with a dummy test_subject for cases where subject !=null (security turned on)
    Subject who = getSubject("test_subject");
    assertStoreHasExactly(store);

    // Tests for case when subject != null (security turned on) and
    // acls for the blob are set to WORLD_EVERYTHING
    metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    out = store.createBlob("test", metadata, who);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test", 1);

    LOG.info("Deleting test");
    store.deleteBlob("test", who);
    assertStoreHasExactly(store);

    // Tests for case when subject != null (security turned on) and
    // acls are not set for the blob (DEFAULT)
    LOG.info("Creating test again");
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    out = store.createBlob("test", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING. Here the acl should not contain WORLD_EVERYTHING because
    // the subject is neither null nor empty. The ACL should however contain USER_EVERYTHING as user needs to have
    // complete access to the blob
    assertTrue("ACL does not contain WORLD_EVERYTHING", !metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test", 2);

    LOG.info("Updating test");
    out = store.updateBlob("test", who);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEqualsWithAuth(store, who, "test", 3);

    LOG.info("Updating test again");
    out = store.updateBlob("test", who);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);
    assertStoreHasExactly(store, "test");
    readAssertEqualsWithAuth(store, who, "test", 3);

    //Test for subject with no principals and acls set to WORLD_EVERYTHING
    who = new Subject();
    metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    LOG.info("Creating test");
    out = store.createBlob("test-empty-subject-WE", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test-empty-subject-WE", "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test-empty-subject-WE", 2);

    //Test for subject with no principals and acls set to DEFAULT
    who = new Subject();
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    LOG.info("Creating other");
    out = store.createBlob("test-empty-subject-DEF", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test-empty-subject-DEF", "test", "test-empty-subject-WE");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test-empty-subject-DEF", 2);

    if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }

  public void testBasic(BlobStore store)
          throws Exception {
    assertStoreHasExactly(store);
    LOG.info("Creating test");
    // Tests for case when subject == null (security turned off) and
    // acls for the blob are set to WORLD_EVERYTHING
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler
            .WORLD_EVERYTHING);
    AtomicOutputStream out = store.createBlob("test", metadata, null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEquals(store, "test", 1);

    LOG.info("Deleting test");
    store.deleteBlob("test", null);
    assertStoreHasExactly(store);

    // The following tests are run for both hdfs and local store to test the
    // update blob interface
    metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    LOG.info("Creating test again");
    out = store.createBlob("test", metadata, null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 2);
    LOG.info("Updating test");
    out = store.updateBlob("test", null);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);

    LOG.info("Updating test again");
    out = store.updateBlob("test", null);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);

    if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }


  public void testMultiple(BlobStore store)
          throws Exception {
    assertStoreHasExactly(store);
    LOG.info("Creating test");
    AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler
            .WORLD_EVERYTHING), null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 1);

    LOG.info("Creating other");
    out = store.createBlob("other", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
            null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 1);
    readAssertEquals(store, "other", 2);

    LOG.info("Updating other");
    out = store.updateBlob("other", null);
    out.write(5);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 1);
    readAssertEquals(store, "other", 5);

    LOG.info("Deleting test");
    store.deleteBlob("test", null);
    assertStoreHasExactly(store, "other");
    readAssertEquals(store, "other", 5);

    LOG.info("Creating test again");
    out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
            null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 2);
    readAssertEquals(store, "other", 5);

    LOG.info("Updating test");
    out = store.updateBlob("test", null);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 3);
    readAssertEquals(store, "other", 5);

    LOG.info("Deleting other");
    store.deleteBlob("other", null);
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);

    LOG.info("Updating test again");
    out = store.updateBlob("test", null);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);

    if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }
}
