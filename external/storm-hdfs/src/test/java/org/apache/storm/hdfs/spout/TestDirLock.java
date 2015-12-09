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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class TestDirLock {


  static MiniDFSCluster.Builder builder;
  static MiniDFSCluster hdfsCluster;
  static FileSystem fs;
  static String hdfsURI;
  static Configuration conf = new  HdfsConfiguration();


  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private Path lockDir = new Path("/tmp/lockdir");


  @BeforeClass
  public static void setupClass() throws IOException {
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
    assert fs.mkdirs(lockDir) ;
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(lockDir, true);
  }

  @Test
  public void testConcurrentLocking() throws Exception {
//    -Dlog4j.configuration=config
    Logger.getRootLogger().setLevel(Level.ERROR);
    DirLockingThread[] thds = startThreads(10, lockDir );
    for (DirLockingThread thd : thds) {
      thd.start();
    }
    System.err.println("Thread creation complete");
    Thread.sleep(5000);
    for (DirLockingThread thd : thds) {
      thd.join(1000);
      if(thd.isAlive() && thd.cleanExit)
        System.err.println(thd.getName() + " did not exit cleanly");
      Assert.assertTrue(thd.cleanExit);
    }

    Path lockFile = new Path(lockDir + Path.SEPARATOR + DirLock.DIR_LOCK_FILE);
    Assert.assertFalse(fs.exists(lockFile));
  }



  private DirLockingThread[] startThreads(int thdCount, Path dir)
          throws IOException {
    DirLockingThread[] result = new DirLockingThread[thdCount];
    for (int i = 0; i < thdCount; i++) {
      result[i] = new DirLockingThread(i, fs, dir);
    }
    return result;
  }


  class DirLockingThread extends Thread {

    private final FileSystem fs;
    private final Path dir;
    public boolean cleanExit = false;

    public DirLockingThread(int thdNum,FileSystem fs, Path dir) throws IOException {
      this.fs = fs;
      this.dir = dir;
      Thread.currentThread().setName("DirLockingThread-" + thdNum);
    }

    @Override
    public void run() {
      try {
        DirLock lock;
        do {
          lock = DirLock.tryLock(fs, dir);
          if(lock==null) {
            System.out.println("Retrying lock - " + Thread.currentThread().getId());
          }
        } while (lock==null);
        lock.release();
        cleanExit= true;
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

  }
}
