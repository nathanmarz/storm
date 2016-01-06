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

public class TestDirLock {

  static MiniDFSCluster.Builder builder;
  static MiniDFSCluster hdfsCluster;
  static FileSystem fs;
  static String hdfsURI;
  static HdfsConfiguration conf = new  HdfsConfiguration();
  static final int LOCK_EXPIRY_SEC = 1;
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
    assert fs.mkdirs(locksDir) ;
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(locksDir, true);
  }


  @Test
  public void testBasicLocking() throws Exception {
    // 1 grab lock
    DirLock lock = DirLock.tryLock(fs, locksDir);
    Assert.assertTrue(fs.exists(lock.getLockFile()));

    // 2 try to grab another lock while dir is locked
    DirLock lock2 = DirLock.tryLock(fs, locksDir); // should fail
    Assert.assertNull(lock2);

    // 3 let go first lock
    lock.release();
    Assert.assertFalse(fs.exists(lock.getLockFile()));

    // 4 try locking again
    lock2  = DirLock.tryLock(fs, locksDir);
    Assert.assertTrue(fs.exists(lock2.getLockFile()));
    lock2.release();
    Assert.assertFalse(fs.exists(lock.getLockFile()));
    lock2.release();  // should be throw
  }


  @Test
  public void testConcurrentLocking() throws Exception {
    DirLockingThread[] thds = startThreads(100, locksDir);
    for (DirLockingThread thd : thds) {
      thd.join();
      if( !thd.cleanExit ) {
        System.err.println(thd.getName() + " did not exit cleanly");
      }
      Assert.assertTrue(thd.cleanExit);
    }

    Path lockFile = new Path(locksDir + Path.SEPARATOR + DirLock.DIR_LOCK_FILE);
    Assert.assertFalse(fs.exists(lockFile));
  }

  private DirLockingThread[] startThreads(int thdCount, Path dir)
          throws IOException {
    DirLockingThread[] result = new DirLockingThread[thdCount];
    for (int i = 0; i < thdCount; i++) {
      result[i] = new DirLockingThread(i, fs, dir);
    }

    for (DirLockingThread thd : result) {
      thd.start();
    }
    return result;
  }

  @Test
  public void testLockRecovery() throws Exception {
    DirLock lock1 = DirLock.tryLock(fs, locksDir);   // should pass
    Assert.assertNotNull(lock1);

    DirLock lock2 = DirLock.takeOwnershipIfStale(fs, locksDir, LOCK_EXPIRY_SEC); // should fail
    Assert.assertNull(lock2);

    Thread.sleep(LOCK_EXPIRY_SEC*1000 + 500); // wait for lock to expire
    Assert.assertTrue(fs.exists(lock1.getLockFile()));

    DirLock lock3 = DirLock.takeOwnershipIfStale(fs, locksDir, LOCK_EXPIRY_SEC); // should pass now
    Assert.assertNotNull(lock3);
    Assert.assertTrue(fs.exists(lock3.getLockFile()));
    lock3.release();
    Assert.assertFalse(fs.exists(lock3.getLockFile()));
    lock1.release(); // should not throw
  }

  class DirLockingThread extends Thread {

    private int thdNum;
    private final FileSystem fs;
    private final Path dir;
    public boolean cleanExit = false;

    public DirLockingThread(int thdNum,FileSystem fs, Path dir)
            throws IOException {
      this.thdNum = thdNum;
      this.fs = fs;
      this.dir = dir;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("DirLockingThread-" + thdNum);
      DirLock lock = null;
      try {
        do {
          System.err.println("Trying lock " + getName());
          lock = DirLock.tryLock(fs, dir);
          System.err.println("Acquired lock " + getName());
          if(lock==null) {
            System.out.println("Retrying lock - " + getName());
          }
        } while (lock==null);
        cleanExit= true;
      } catch (Exception e) {
        e.printStackTrace();
      }
      finally {
          try {
            if(lock!=null) {
              lock.release();
              System.err.println("Released lock " + getName());
            }
          } catch (IOException e) {
            e.printStackTrace(System.err);
          }
      }
      System.err.println("Thread exiting " + getName());
    } // run()

  } // class DirLockingThread
}
