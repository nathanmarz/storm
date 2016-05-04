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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class TestFileLock {

  static MiniDFSCluster.Builder builder;
  static MiniDFSCluster hdfsCluster;
  static FileSystem fs;
  static String hdfsURI;
  static HdfsConfiguration conf = new  HdfsConfiguration();

  private Path filesDir = new Path("/tmp/filesdir");
  private Path locksDir = new Path("/tmp/locskdir");

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
    assert fs.mkdirs(locksDir) ;
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(filesDir, true);
    fs.delete(locksDir, true);
  }

  @Test
  public void testBasicLocking() throws Exception {
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
    Assert.assertEquals(lock1a.getLockFile().getName(), file1.getName()); // verify lock filename

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
    Assert.assertEquals(lock1c.getLockFile().getName(), file1.getName()); // verify lock filename

    // try locking another file2 at the same time
    FileLock lock2a = FileLock.tryLock(fs, file2, locksDir, "spout1");
    Assert.assertNotNull(lock2a);
    Assert.assertTrue(fs.exists(lock2a.getLockFile()));
    Assert.assertEquals(lock2a.getLockFile().getParent(), locksDir); // verify lock file location
    Assert.assertEquals(lock2a.getLockFile().getName(), file2.getName()); // verify lock filename

    // release both locks
    lock2a.release();
    Assert.assertFalse(fs.exists(lock2a.getLockFile()));
    lock1c.release();
    Assert.assertFalse(fs.exists(lock1c.getLockFile()));
  }

  @Test
  public void testHeartbeat() throws Exception {
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    fs.create(file1).close();

    // acquire lock on file1
    FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
    Assert.assertNotNull(lock1);
    Assert.assertTrue(fs.exists(lock1.getLockFile()));

    ArrayList<String> lines = readTextFile(lock1.getLockFile());
    Assert.assertEquals("heartbeats appear to be missing", 1, lines.size());

    // hearbeat upon it
    lock1.heartbeat("1");
    lock1.heartbeat("2");
    lock1.heartbeat("3");

    lines = readTextFile(lock1.getLockFile());
    Assert.assertEquals("heartbeats appear to be missing", 4, lines.size());

    lock1.heartbeat("4");
    lock1.heartbeat("5");
    lock1.heartbeat("6");

    lines = readTextFile(lock1.getLockFile());
    Assert.assertEquals("heartbeats appear to be missing", 7,  lines.size());

    lock1.release();
    lines = readTextFile(lock1.getLockFile());
    Assert.assertNull(lines);
    Assert.assertFalse(fs.exists(lock1.getLockFile()));
  }

  @Test
  public void testConcurrentLocking() throws IOException, InterruptedException {
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    fs.create(file1).close();

    FileLockingThread[] thds = startThreads(100, file1, locksDir);
    for (FileLockingThread thd : thds) {
      thd.join();
      if( !thd.cleanExit) {
        System.err.println(thd.getName() + " did not exit cleanly");
      }
      Assert.assertTrue(thd.cleanExit);
    }

    Path lockFile = new Path(locksDir + Path.SEPARATOR + file1.getName());
    Assert.assertFalse(fs.exists(lockFile));
  }

  private FileLockingThread[] startThreads(int thdCount, Path fileToLock, Path locksDir)
          throws IOException {
    FileLockingThread[] result = new FileLockingThread[thdCount];
    for (int i = 0; i < thdCount; i++) {
      result[i] = new FileLockingThread(i, fs, fileToLock, locksDir, "spout" + Integer.toString(i));
    }

    for (FileLockingThread thd : result) {
      thd.start();
    }
    return result;
  }


  @Test
  public void testStaleLockDetection_SingleLock() throws Exception {
    final int LOCK_EXPIRY_SEC = 1;
    final int WAIT_MSEC = 1500;
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    fs.create(file1).close();
    FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
    try {
      // acquire lock on file1
      Assert.assertNotNull(lock1);
      Assert.assertTrue(fs.exists(lock1.getLockFile()));
      Thread.sleep(WAIT_MSEC);   // wait for lock to expire
      HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
      Assert.assertNotNull(expired);

      // heartbeat, ensure its no longer stale and read back the heartbeat data
      lock1.heartbeat("1");
      expired = FileLock.locateOldestExpiredLock(fs, locksDir, 1);
      Assert.assertNull(expired);

      FileLock.LogEntry lastEntry = lock1.getLastLogEntry();
      Assert.assertNotNull(lastEntry);
      Assert.assertEquals("1", lastEntry.fileOffset);

      // wait and check for expiry again
      Thread.sleep(WAIT_MSEC);
      expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
      Assert.assertNotNull(expired);
    } finally {
      lock1.release();
      fs.delete(file1, false);
    }
  }

  @Test
  public void testStaleLockDetection_MultipleLocks() throws Exception {
    final int LOCK_EXPIRY_SEC = 1;
    final int WAIT_MSEC = 1500;
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
    Path file3 = new Path(filesDir + Path.SEPARATOR + "file3");

    fs.create(file1).close();
    fs.create(file2).close();
    fs.create(file3).close();

    // 1) acquire locks on file1,file2,file3
    FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
    FileLock lock2 = FileLock.tryLock(fs, file2, locksDir, "spout2");
    FileLock lock3 = FileLock.tryLock(fs, file3, locksDir, "spout3");
    Assert.assertNotNull(lock1);
    Assert.assertNotNull(lock2);
    Assert.assertNotNull(lock3);

    try {
      HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
      Assert.assertNull(expired);

      // 2) wait for all 3 locks to expire then heart beat on 2 locks and verify stale lock
      Thread.sleep(WAIT_MSEC);
      lock1.heartbeat("1");
      lock2.heartbeat("1");

      expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
      Assert.assertNotNull(expired);
      Assert.assertEquals("spout3", expired.getValue().componentID);
    } finally {
      lock1.release();
      lock2.release();
      lock3.release();
      fs.delete(file1, false);
      fs.delete(file2, false);
      fs.delete(file3, false);
    }
  }

  @Test
  public void testLockRecovery() throws Exception {
    final int LOCK_EXPIRY_SEC = 1;
    final int WAIT_MSEC = LOCK_EXPIRY_SEC*1000 + 500;
    Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
    Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
    Path file3 = new Path(filesDir + Path.SEPARATOR + "file3");

    fs.create(file1).close();
    fs.create(file2).close();
    fs.create(file3).close();

    // 1) acquire locks on file1,file2,file3
    FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
    FileLock lock2 = FileLock.tryLock(fs, file2, locksDir, "spout2");
    FileLock lock3 = FileLock.tryLock(fs, file3, locksDir, "spout3");
    Assert.assertNotNull(lock1);
    Assert.assertNotNull(lock2);
    Assert.assertNotNull(lock3);

    try {
      HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
      Assert.assertNull(expired);

      // 1) Simulate lock file lease expiring and getting closed by HDFS
      closeUnderlyingLockFile(lock3);

      // 2) wait for all 3 locks to expire then heart beat on 2 locks
      Thread.sleep(WAIT_MSEC*2); // wait for locks to expire
      lock1.heartbeat("1");
      lock2.heartbeat("1");

      // 3) Take ownership of stale lock
      FileLock lock3b = FileLock.acquireOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC, "spout1");
      Assert.assertNotNull(lock3b);
      Assert.assertEquals("Expected lock3 file", Path.getPathWithoutSchemeAndAuthority(lock3b.getLockFile()), lock3.getLockFile());
    } finally {
      lock1.release();
      lock2.release();
      lock3.release();
      fs.delete(file1, false);
      fs.delete(file2, false);
      try {
        fs.delete(file3, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void closeUnderlyingLockFile(FileLock lock) throws ReflectiveOperationException {
    Method m = FileLock.class.getDeclaredMethod("forceCloseLockFile");
    m.setAccessible(true);
    m.invoke(lock);
  }

  /** return null if file not found */
  private ArrayList<String> readTextFile(Path file) throws IOException {
    FSDataInputStream os = null;
    try {
      os = fs.open(file);
      if (os == null) {
        return null;
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(os));
      ArrayList<String> lines = new ArrayList<>();
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        lines.add(line);
      }
      return lines;
    } catch( FileNotFoundException e) {
      return null;
    } finally {
      if(os!=null) {
        os.close();
      }
    }
  }

  class FileLockingThread extends Thread {

    private int thdNum;
    private final FileSystem fs;
    public boolean cleanExit = false;
    private Path fileToLock;
    private Path locksDir;
    private String spoutId;

    public FileLockingThread(int thdNum, FileSystem fs, Path fileToLock, Path locksDir, String spoutId)
            throws IOException {
      this.thdNum = thdNum;
      this.fs = fs;
      this.fileToLock = fileToLock;
      this.locksDir = locksDir;
      this.spoutId = spoutId;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("FileLockingThread-" + thdNum);
      FileLock lock = null;
      try {
        do {
          System.err.println("Trying lock - " + getName());
          lock = FileLock.tryLock(fs, this.fileToLock, this.locksDir, spoutId);
          System.err.println("Acquired lock - " + getName());
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
            System.err.println("Released lock - " + getName());
          }
        } catch (IOException e) {
          e.printStackTrace(System.err);
        }
      }
      System.err.println("Thread exiting - " + getName());
    } // run()

  } // class FileLockingThread
}
