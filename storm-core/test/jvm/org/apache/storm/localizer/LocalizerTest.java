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
package org.apache.storm.localizer;

import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import com.google.common.base.Joiner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class LocalizerTest {

  private File baseDir;

  private final String user1 = "user1";
  private final String user2 = "user2";
  private final String user3 = "user3";

  private ClientBlobStore mockblobstore = mock(ClientBlobStore.class);


  class TestLocalizer extends Localizer {

    TestLocalizer(Map conf, String baseDir) {
      super(conf, baseDir);
    }

    @Override
    protected ClientBlobStore getClientBlobStore() {
      return mockblobstore;
    }
  }

  class TestInputStreamWithMeta extends InputStreamWithMeta {
    private InputStream iostream;

    public TestInputStreamWithMeta() {
      iostream = IOUtils.toInputStream("some test data for my input stream");
    }

    public TestInputStreamWithMeta(InputStream istream) {
       iostream = istream;
    }

    @Override
    public long getVersion() throws IOException {
      return 1;
    }

    @Override
    public synchronized int read() {
      return 0;
    }

    @Override
    public synchronized int read(byte[] b)
    throws IOException {
      int length = iostream.read(b);
      if (length == 0) {
        return -1;
      }
      return length;
    }

    @Override
    public long getFileLength() {
        return 0;
    }
  };

  @Before
  public void setUp() throws Exception {
    baseDir = new File(System.getProperty("java.io.tmpdir") + "/blob-store-localizer-test-"+ UUID.randomUUID());
    if (!baseDir.mkdir()) {
      throw new IOException("failed to create base directory");
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      FileUtils.deleteDirectory(baseDir);
    } catch (IOException ignore) {}
  }

  protected String joinPath(String... pathList) {
      return Joiner.on(File.separator).join(pathList);
  }

  public String constructUserCacheDir(String base, String user) {
    return joinPath(base, Localizer.USERCACHE, user);
  }

  public String constructExpectedFilesDir(String base, String user) {
    return joinPath(constructUserCacheDir(base, user), Localizer.FILECACHE, Localizer.FILESDIR);
  }

  public String constructExpectedArchivesDir(String base, String user) {
    return joinPath(constructUserCacheDir(base, user), Localizer.FILECACHE, Localizer.ARCHIVESDIR);
  }

  @Test
  public void testDirPaths() throws Exception {
    Map conf = new HashMap();
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());

    String expectedDir = constructUserCacheDir(baseDir.toString(), user1);
    assertEquals("get local user dir doesn't return right value",
        expectedDir, localizer.getLocalUserDir(user1).toString());

    String expectedFileDir = joinPath(expectedDir, Localizer.FILECACHE);
    assertEquals("get local user file dir doesn't return right value",
        expectedFileDir, localizer.getLocalUserFileCacheDir(user1).toString());
  }

  @Test
  public void testReconstruct() throws Exception {
    Map conf = new HashMap();

    String expectedFileDir1 = constructExpectedFilesDir(baseDir.toString(), user1);
    String expectedArchiveDir1 = constructExpectedArchivesDir(baseDir.toString(), user1);
    String expectedFileDir2 = constructExpectedFilesDir(baseDir.toString(), user2);
    String expectedArchiveDir2 = constructExpectedArchivesDir(baseDir.toString(), user2);

    String key1 = "testfile1.txt";
    String key2 = "testfile2.txt";
    String key3 = "testfile3.txt";
    String key4 = "testfile4.txt";

    String archive1 = "archive1";
    String archive2 = "archive2";

    File user1file1 = new File(expectedFileDir1, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File user1file2 = new File(expectedFileDir1, key2 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File user2file3 = new File(expectedFileDir2, key3 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File user2file4 = new File(expectedFileDir2, key4 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);

    File user1archive1 = new File(expectedArchiveDir1, archive1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File user2archive2 = new File(expectedArchiveDir2, archive2 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File user1archive1file = new File(user1archive1, "file1");
    File user2archive2file = new File(user2archive2, "file2");

    // setup some files/dirs to emulate supervisor restart
    assertTrue("Failed setup filecache dir1", new File(expectedFileDir1).mkdirs());
    assertTrue("Failed setup filecache dir2", new File(expectedFileDir2).mkdirs());
    assertTrue("Failed setup file1", user1file1.createNewFile());
    assertTrue("Failed setup file2", user1file2.createNewFile());
    assertTrue("Failed setup file3", user2file3.createNewFile());
    assertTrue("Failed setup file4", user2file4.createNewFile());
    assertTrue("Failed setup archive dir1", user1archive1.mkdirs());
    assertTrue("Failed setup archive dir2", user2archive2.mkdirs());
    assertTrue("Failed setup file in archivedir1", user1archive1file.createNewFile());
    assertTrue("Failed setup file in archivedir2", user2archive2file.createNewFile());

    Localizer localizer = new TestLocalizer(conf, baseDir.toString());

    ArrayList<LocalResource> arrUser1Keys = new ArrayList<LocalResource>();
    arrUser1Keys.add(new LocalResource(key1, false));
    arrUser1Keys.add(new LocalResource(archive1, true));
    localizer.addReferences(arrUser1Keys, user1, "topo1");

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 3, lrsrcSet.getSize());
    assertEquals("user doesn't match", user1, lrsrcSet.getUser());
    LocalizedResource key1rsrc = lrsrcSet.get(key1, false);
    assertNotNull("Local resource doesn't exist but should", key1rsrc);
    assertEquals("key doesn't match", key1, key1rsrc.getKey());
    assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
    LocalizedResource key2rsrc = lrsrcSet.get(key2, false);
    assertNotNull("Local resource doesn't exist but should", key2rsrc);
    assertEquals("key doesn't match", key2, key2rsrc.getKey());
    assertEquals("refcount doesn't match", 0, key2rsrc.getRefCount());
    LocalizedResource archive1rsrc = lrsrcSet.get(archive1, true);
    assertNotNull("Local resource doesn't exist but should", archive1rsrc);
    assertEquals("key doesn't match", archive1, archive1rsrc.getKey());
    assertEquals("refcount doesn't match", 1, archive1rsrc.getRefCount());

    LocalizedResourceSet lrsrcSet2 = localizer.getUserResources().get(user2);
    assertEquals("local resource set size wrong", 3, lrsrcSet2.getSize());
    assertEquals("user doesn't match", user2, lrsrcSet2.getUser());
    LocalizedResource key3rsrc = lrsrcSet2.get(key3, false);
    assertNotNull("Local resource doesn't exist but should", key3rsrc);
    assertEquals("key doesn't match", key3, key3rsrc.getKey());
    assertEquals("refcount doesn't match", 0, key3rsrc.getRefCount());
    LocalizedResource key4rsrc = lrsrcSet2.get(key4, false);
    assertNotNull("Local resource doesn't exist but should", key4rsrc);
    assertEquals("key doesn't match", key4, key4rsrc.getKey());
    assertEquals("refcount doesn't match", 0, key4rsrc.getRefCount());
    LocalizedResource archive2rsrc = lrsrcSet2.get(archive2, true);
    assertNotNull("Local resource doesn't exist but should", archive2rsrc);
    assertEquals("key doesn't match", archive2, archive2rsrc.getKey());
    assertEquals("refcount doesn't match", 0, archive2rsrc.getRefCount());
  }

  @Test
  public void testArchivesTgz() throws Exception {
    testArchives(joinPath("test", "jvm", "org", "apache", "storm", "localizer", "localtestwithsymlink.tgz"), true, 21344);
  }

  @Test
  public void testArchivesZip() throws Exception {
    testArchives(joinPath("test", "jvm", "org", "apache", "storm", "localizer", "localtest.zip"), false, 21348);
  }

  @Test
  public void testArchivesTarGz() throws Exception {
    testArchives(joinPath("test", "jvm", "org", "apache", "storm", "localizer", "localtestwithsymlink.tar.gz"), true, 21344);
  }

  @Test
  public void testArchivesTar() throws Exception {
    testArchives(joinPath("test", "jvm", "org", "apache", "storm", "localizer", "localtestwithsymlink.tar"), true, 21344);
  }

  @Test
  public void testArchivesJar() throws Exception {
    testArchives(joinPath("test", "jvm", "org", "apache", "storm", "localizer", "localtestwithsymlink.jar"), false, 21416);
  }

  // archive passed in must contain symlink named tmptestsymlink if not a zip file
  public void testArchives(String archivePath, boolean supportSymlinks, int size) throws Exception {
    if (Utils.isOnWindows()) {
      // Windows should set this to false cause symlink in compressed file doesn't work properly.
      supportSymlinks = false;
    }

    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

    String key1 = new File(archivePath).getName();
    String topo1 = "topo1";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());
    // set really small so will do cleanup
    localizer.setTargetCacheSize(1);

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING));
    when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);

    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta(new
        FileInputStream(archivePath)));

    long timeBefore = System.nanoTime();
    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());
    LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, true), user1, topo1,
        user1Dir);
    long timeAfter = System.nanoTime();

    String expectedUserDir = joinPath(baseDir.toString(), Localizer.USERCACHE, user1);
    String expectedFileDir = joinPath(expectedUserDir, Localizer.FILECACHE, Localizer.ARCHIVESDIR);
    assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
    File keyFile = new File(expectedFileDir, key1 + ".0");
    assertTrue("blob not created", keyFile.exists());
    assertTrue("blob is not uncompressed", keyFile.isDirectory());
    File symlinkFile = new File(keyFile, "tmptestsymlink");

    if (supportSymlinks) {
      assertTrue("blob uncompressed doesn't contain symlink", Files.isSymbolicLink(
          symlinkFile.toPath()));
    } else {
      assertTrue("blob symlink file doesn't exist", symlinkFile.exists());
    }

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    assertEquals("user doesn't match", user1, lrsrcSet.getUser());
    LocalizedResource key1rsrc = lrsrcSet.get(key1, true);
    assertNotNull("Local resource doesn't exist but should", key1rsrc);
    assertEquals("key doesn't match", key1, key1rsrc.getKey());
    assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
    assertEquals("file path doesn't match", keyFile.toString(), key1rsrc.getFilePathWithVersion());
    assertEquals("size doesn't match", size, key1rsrc.getSize());
    assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
        .getLastAccessTime() <= timeAfter));

    timeBefore = System.nanoTime();
    localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, true);
    timeAfter = System.nanoTime();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    key1rsrc = lrsrcSet.get(key1, true);
    assertNotNull("Local resource doesn't exist but should", key1rsrc);
    assertEquals("refcount doesn't match", 0, key1rsrc.getRefCount());
    assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
        .getLastAccessTime() <= timeAfter));

    // should remove the blob since cache size set really small
    localizer.handleCacheCleanup();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertFalse("blob contents not deleted", symlinkFile.exists());
    assertFalse("blob not deleted", keyFile.exists());
    assertFalse("blob file dir not deleted", new File(expectedFileDir).exists());
    assertFalse("blob dir not deleted", new File(expectedUserDir).exists());
    assertNull("user set should be null", lrsrcSet);

  }

  @Test
  public void testBasic() throws Exception {
    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

    String key1 = "key1";
    String topo1 = "topo1";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());
    // set really small so will do cleanup
    localizer.setTargetCacheSize(1);

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING));
    when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);

    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());

    long timeBefore = System.nanoTime();
    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());
    LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
        user1Dir);
    long timeAfter = System.nanoTime();

    String expectedUserDir = joinPath(baseDir.toString(), Localizer.USERCACHE, user1);
    String expectedFileDir = joinPath(expectedUserDir, Localizer.FILECACHE, Localizer.FILESDIR);
    assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
    File keyFile = new File(expectedFileDir, key1);
    File keyFileCurrentSymlink = new File(expectedFileDir, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);

    assertTrue("blob not created", keyFileCurrentSymlink.exists());

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    assertEquals("user doesn't match", user1, lrsrcSet.getUser());
    LocalizedResource key1rsrc = lrsrcSet.get(key1, false);
    assertNotNull("Local resource doesn't exist but should", key1rsrc);
    assertEquals("key doesn't match", key1, key1rsrc.getKey());
    assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
    assertEquals("file path doesn't match", keyFile.toString(), key1rsrc.getFilePath());
    assertEquals("size doesn't match", 34, key1rsrc.getSize());
    assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
        .getLastAccessTime() <= timeAfter));

    timeBefore = System.nanoTime();
    localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
    timeAfter = System.nanoTime();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    key1rsrc = lrsrcSet.get(key1, false);
    assertNotNull("Local resource doesn't exist but should", key1rsrc);
    assertEquals("refcount doesn't match", 0, key1rsrc.getRefCount());
    assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
        .getLastAccessTime() <= timeAfter));

    // should remove the blob since cache size set really small
    localizer.handleCacheCleanup();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertNull("user set should be null", lrsrcSet);
    assertFalse("blob not deleted", keyFile.exists());
    assertFalse("blob dir not deleted", new File(expectedFileDir).exists());
    assertFalse("blob dir not deleted", new File(expectedUserDir).exists());
  }

  @Test
  public void testMultipleKeysOneUser() throws Exception {
    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

    String key1 = "key1";
    String topo1 = "topo1";
    String key2 = "key2";
    String key3 = "key3";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());
    // set to keep 2 blobs (each of size 34)
    localizer.setTargetCacheSize(68);

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING));
    when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
    when(mockblobstore.getBlob(key2)).thenReturn(new TestInputStreamWithMeta());
    when(mockblobstore.getBlob(key3)).thenReturn(new TestInputStreamWithMeta());

    List<LocalResource> keys = Arrays.asList(new LocalResource[]{new LocalResource(key1, false),
        new LocalResource(key2, false), new LocalResource(key3, false)});
    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());

    List<LocalizedResource> lrsrcs = localizer.getBlobs(keys, user1, topo1, user1Dir);
    LocalizedResource lrsrc = lrsrcs.get(0);
    LocalizedResource lrsrc2 = lrsrcs.get(1);
    LocalizedResource lrsrc3 = lrsrcs.get(2);

    String expectedFileDir = joinPath(baseDir.toString(), Localizer.USERCACHE, user1,
        Localizer.FILECACHE, Localizer.FILESDIR);
    assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
    File keyFile = new File(expectedFileDir, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File keyFile2 = new File(expectedFileDir, key2 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File keyFile3 = new File(expectedFileDir, key3 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);

    assertTrue("blob not created", keyFile.exists());
    assertTrue("blob not created", keyFile2.exists());
    assertTrue("blob not created", keyFile3.exists());
    assertEquals("size doesn't match", 34, keyFile.length());
    assertEquals("size doesn't match", 34, keyFile2.length());
    assertEquals("size doesn't match", 34, keyFile3.length());
    assertEquals("size doesn't match", 34, lrsrc.getSize());
    assertEquals("size doesn't match", 34, lrsrc3.getSize());
    assertEquals("size doesn't match", 34, lrsrc2.getSize());

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 3, lrsrcSet.getSize());
    assertEquals("user doesn't match", user1, lrsrcSet.getUser());

    long timeBefore = System.nanoTime();
    localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
    localizer.removeBlobReference(lrsrc2.getKey(), user1, topo1, false);
    localizer.removeBlobReference(lrsrc3.getKey(), user1, topo1, false);
    long timeAfter = System.nanoTime();

    // add reference to one and then remove reference again so it has newer timestamp
    lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1, user1Dir);
    assertTrue("timestamp not within range", (lrsrc.getLastAccessTime() >= timeBefore && lrsrc
        .getLastAccessTime() <= timeAfter));
    localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);

    // should remove the second blob first
    localizer.handleCacheCleanup();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 2, lrsrcSet.getSize());
    long end = System.currentTimeMillis() + 100;
    while ((end - System.currentTimeMillis()) >= 0 && keyFile2.exists()) {
      Thread.sleep(1);
    }
    assertFalse("blob not deleted", keyFile2.exists());
    assertTrue("blob deleted", keyFile.exists());
    assertTrue("blob deleted", keyFile3.exists());

    // set size to cleanup another one
    localizer.setTargetCacheSize(34);

    // should remove the third blob
    localizer.handleCacheCleanup();

    lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    assertTrue("blob deleted", keyFile.exists());
    assertFalse("blob not deleted", keyFile3.exists());
  }

  @Test(expected = AuthorizationException.class)
  public void testFailAcls() throws Exception {
    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60 * 60 * 1000);

    String topo1 = "topo1";
    String key1 = "key1";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    // set acl so user doesn't have read access
    AccessControl acl = new AccessControl(AccessControlType.USER, BlobStoreAclHandler.ADMIN);
    acl.set_name(user1);
    rbm.set_settable(new SettableBlobMeta(Arrays.asList(acl)));
    when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());

    // This should throw AuthorizationException because auth failed
    localizer.getBlob(new LocalResource(key1, false), user1, topo1, user1Dir);
  }

  @Test(expected = KeyNotFoundException.class)
  public void testKeyNotFoundException() throws Exception {
    Map conf = Utils.readStormConfig();
    String key1 = "key1";
    conf.put(Config.STORM_LOCAL_DIR, "target");
    LocalFsBlobStore bs = new LocalFsBlobStore();
    LocalFsBlobStore spy = spy(bs);
    Mockito.doReturn(true).when(spy).checkForBlobOrDownload(key1);
    Mockito.doNothing().when(spy).checkForBlobUpdate(key1);
    spy.prepare(conf,null,null);
    spy.getBlob(key1, null);
  }

    @Test
  public void testMultipleUsers() throws Exception {
    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

    String topo1 = "topo1";
    String topo2 = "topo2";
    String topo3 = "topo3";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());
    // set to keep 2 blobs (each of size 34)
    localizer.setTargetCacheSize(68);

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING));
    when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
    when(mockblobstore.getBlob(key2)).thenReturn(new TestInputStreamWithMeta());
    when(mockblobstore.getBlob(key3)).thenReturn(new TestInputStreamWithMeta());

    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());
    File user2Dir = localizer.getLocalUserFileCacheDir(user2);
    assertTrue("failed to create user dir", user2Dir.mkdirs());
    File user3Dir = localizer.getLocalUserFileCacheDir(user3);
    assertTrue("failed to create user dir", user3Dir.mkdirs());

    LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
        user1Dir);
    LocalizedResource lrsrc2 = localizer.getBlob(new LocalResource(key2, false), user2, topo2,
        user2Dir);
    LocalizedResource lrsrc3 = localizer.getBlob(new LocalResource(key3, false), user3, topo3,
        user3Dir);
    // make sure we support different user reading same blob
    LocalizedResource lrsrc1_user3 = localizer.getBlob(new LocalResource(key1, false), user3,
        topo3, user3Dir);

    String expectedUserDir1 = joinPath(baseDir.toString(), Localizer.USERCACHE, user1);
    String expectedFileDirUser1 = joinPath(expectedUserDir1, Localizer.FILECACHE, Localizer.FILESDIR);
    String expectedFileDirUser2 = joinPath(baseDir.toString(), Localizer.USERCACHE, user2,
        Localizer.FILECACHE, Localizer.FILESDIR);
    String expectedFileDirUser3 = joinPath(baseDir.toString(), Localizer.USERCACHE, user3,
        Localizer.FILECACHE, Localizer.FILESDIR);
    assertTrue("user filecache dir user1 not created", new File(expectedFileDirUser1).exists());
    assertTrue("user filecache dir user2 not created", new File(expectedFileDirUser2).exists());
    assertTrue("user filecache dir user3 not created", new File(expectedFileDirUser3).exists());

    File keyFile = new File(expectedFileDirUser1, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File keyFile2 = new File(expectedFileDirUser2, key2 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File keyFile3 = new File(expectedFileDirUser3, key3 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    File keyFile1user3 = new File(expectedFileDirUser3, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);

    assertTrue("blob not created", keyFile.exists());
    assertTrue("blob not created", keyFile2.exists());
    assertTrue("blob not created", keyFile3.exists());
    assertTrue("blob not created", keyFile1user3.exists());

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
    LocalizedResourceSet lrsrcSet2 = localizer.getUserResources().get(user2);
    assertEquals("local resource set size wrong", 1, lrsrcSet2.getSize());
    LocalizedResourceSet lrsrcSet3 = localizer.getUserResources().get(user3);
    assertEquals("local resource set size wrong", 2, lrsrcSet3.getSize());

    localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
    // should remove key1
    localizer.handleCacheCleanup();

    lrsrcSet = localizer.getUserResources().get(user1);
    lrsrcSet3 = localizer.getUserResources().get(user3);
    assertNull("user set should be null", lrsrcSet);
    assertFalse("blob dir not deleted", new File(expectedFileDirUser1).exists());
    assertFalse("blob dir not deleted", new File(expectedUserDir1).exists());
    assertEquals("local resource set size wrong", 2, lrsrcSet3.getSize());

    assertTrue("blob deleted", keyFile2.exists());
    assertFalse("blob not deleted", keyFile.exists());
    assertTrue("blob deleted", keyFile3.exists());
    assertTrue("blob deleted", keyFile1user3.exists());
  }

  @Test
  public void testUpdate() throws Exception {
    Map conf = new HashMap();
    // set clean time really high so doesn't kick in
    conf.put(Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

    String key1 = "key1";
    String topo1 = "topo1";
    String topo2 = "topo2";
    Localizer localizer = new TestLocalizer(conf, baseDir.toString());

    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_version(1);
    rbm.set_settable(new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING));
    when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);
    when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());

    File user1Dir = localizer.getLocalUserFileCacheDir(user1);
    assertTrue("failed to create user dir", user1Dir.mkdirs());
    LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
        user1Dir);

    String expectedUserDir = joinPath(baseDir.toString(), Localizer.USERCACHE, user1);
    String expectedFileDir = joinPath(expectedUserDir, Localizer.FILECACHE, Localizer.FILESDIR);
    assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
    File keyFile = new File(expectedFileDir, key1);
    File keyFileCurrentSymlink = new File(expectedFileDir, key1 + Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
    assertTrue("blob not created", keyFileCurrentSymlink.exists());
    File versionFile = new File(expectedFileDir, key1 + Utils.DEFAULT_BLOB_VERSION_SUFFIX);
    assertTrue("blob version file not created", versionFile.exists());
    assertEquals("blob version not correct", 1, Utils.localVersionOfBlob(keyFile.toString()));

    LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
    assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());

    // test another topology getting blob with updated version - it should update version now
    rbm.set_version(2);

    localizer.getBlob(new LocalResource(key1, false), user1, topo2, user1Dir);
    assertTrue("blob version file not created", versionFile.exists());
    assertEquals("blob version not correct", 2, Utils.localVersionOfBlob(keyFile.toString()));
    assertTrue("blob file with version 2 not created", new File(keyFile + ".2").exists());

    // now test regular updateBlob
    rbm.set_version(3);

    ArrayList<LocalResource> arr = new ArrayList<LocalResource>();
    arr.add(new LocalResource(key1, false));
    localizer.updateBlobs(arr, user1);
    assertTrue("blob version file not created", versionFile.exists());
    assertEquals("blob version not correct", 3, Utils.localVersionOfBlob(keyFile.toString()));
    assertTrue("blob file with version 3 not created", new File(keyFile + ".3").exists());
  }
}
