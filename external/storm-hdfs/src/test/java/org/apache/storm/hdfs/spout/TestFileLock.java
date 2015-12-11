/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.spout;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestFileLock {

  static MiniDFSCluster.Builder builder;
  static MiniDFSCluster hdfsCluster;
  static FileSystem fs;
  static String hdfsURI;
  static HdfsConfiguration conf = new  HdfsConfiguration();

  private Path filesDir = new Path("/tmp/lockdir");
  private Path locksDir = new Path("/tmp/lockdir");

  @BeforeClass
  public static void setupClass() throws IOException {
    conf.set(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,"5000");
    builder = new MiniDFSCluster.Builder(new Configuration());
    hdfsCluster = builder.build();
    fs  = hdfsCluster.getFileSystem();
    hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    fs.close();
    hdfsCluster.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    assert fs.mkdirs(filesDir) ;
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(filesDir, true);
  }

  @Test
  public void testBasic() throws Exception {
  // create empty files in filesDir
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
    fs.create(file1).close();
    fs.create(file2).close(); // create empty file

    // acquire lock on file1 and verify if worked
    FileLock lock1a = FileLock.tryLock(fs, file1, locksDir, "spout1");
    Assert.assertNotNull(lock1a);
    Assert.assertTrue(fs.exists(lock1a.getLockFile()));
    Assert.assertEquals(lock1a.getLockFile().getParent(), locksDir); // verify lock file location
    Assert.assertEquals(lock1a.getLockFile().getName(), file1.getName()); // very lock filename

    // acquire another lock on file1 and verify it failed
    FileLock lock1b = FileLock.tryLock(fs, file1, locksDir, "spout1");
    Assert.assertNull(lock1b);

    // release lock on file1 and check
    lock1a.release();
    Assert.assertFalse(fs.exists(lock1a.getLockFile()));

    // Retry locking and verify
    FileLock lock1c = FileLock.tryLock(fs, file1, locksDir, "spout1");
    Assert.assertNotNull(lock1c);
    Assert.assertTrue(fs.exists(lock1c.getLockFile()));
    Assert.assertEquals(lock1c.getLockFile().getParent(), locksDir); // verify lock file location
    Assert.assertEquals(lock1c.getLockFile().getName(), file1.getName()); // very lock filename

    // try locking another file2 at the same time
    FileLock lock2a = FileLock.tryLock(fs, file2, locksDir, "spout1");
    Assert.assertNotNull(lock2a);
    Assert.assertTrue(fs.exists(lock2a.getLockFile()));
    Assert.assertEquals(lock2a.getLockFile().getParent(), locksDir); // verify lock file location
    Assert.assertEquals(lock2a.getLockFile().getName(), file1.getName()); // very lock filename

    // release both locks
    lock2a.release();
    Assert.assertFalse(fs.exists(lock2a.getLockFile()));
    lock1c.release();
    Assert.assertFalse(fs.exists(lock1c.getLockFile()));
  }


}
