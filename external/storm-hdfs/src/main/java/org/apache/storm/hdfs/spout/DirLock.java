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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Facility to synchronize access to HDFS directory. The lock itself is represented
 * as a file in the same directory. Relies on atomic file creation.
 */
public class DirLock {
  private FileSystem fs;
  private final Path lockFile;
  public static final String DIR_LOCK_FILE = "DIRLOCK";
  private static final Logger LOG = LoggerFactory.getLogger(DirLock.class);
  private DirLock(FileSystem fs, Path lockFile) throws IOException {
    if( fs.isDirectory(lockFile) ) {
      throw new IllegalArgumentException(lockFile.toString() + " is not a directory");
    }
    this.fs = fs;
    this.lockFile = lockFile;
  }

  /** Get a lock on file if not already locked
   *
   * @param fs
   * @param dir  the dir on which to get a lock
   * @return The lock object if it the lock was acquired. Returns null if the dir is already locked.
   * @throws IOException if there were errors
   */
  public static DirLock tryLock(FileSystem fs, Path dir) throws IOException {
    Path lockFile = getDirLockFile(dir);

    try {
      FSDataOutputStream ostream = HdfsUtils.tryCreateFile(fs, lockFile);
      if (ostream!=null) {
        LOG.debug("Thread ({}) Acquired lock on dir {}", threadInfo(), dir);
        ostream.close();
        return new DirLock(fs, lockFile);
      } else {
        LOG.debug("Thread ({}) cannot lock dir {} as its already locked.", threadInfo(), dir);
        return null;
      }
    } catch (IOException e) {
        LOG.error("Error when acquiring lock on dir " + dir, e);
        throw e;
    }
  }

  private static Path getDirLockFile(Path dir) {
    return new Path(dir.toString() + Path.SEPARATOR_CHAR + DIR_LOCK_FILE );
  }

  private static String threadInfo () {
    return "ThdId=" + Thread.currentThread().getId() + ", ThdName="
            + Thread.currentThread().getName();
  }

  /** Release lock on dir by deleting the lock file */
  public void release() throws IOException {
    if(!fs.delete(lockFile, false)) {
      LOG.error("Thread {} could not delete dir lock {} ", threadInfo(), lockFile);
    }
    else {
      LOG.debug("Thread {} Released dir lock {} ", threadInfo(), lockFile);
    }
  }

  /** if the lock on the directory is stale, take ownership */
  public static DirLock takeOwnershipIfStale(FileSystem fs, Path dirToLock, int lockTimeoutSec) {
    Path dirLockFile = getDirLockFile(dirToLock);

    long now =  System.currentTimeMillis();
    long expiryTime = now - (lockTimeoutSec*1000);

    try {
      long modTime = fs.getFileStatus(dirLockFile).getModificationTime();
      if(modTime <= expiryTime) {
        return takeOwnership(fs, dirLockFile);
      }
      return null;
    } catch (IOException e)  {
      return  null;
    }
  }

  private static DirLock takeOwnership(FileSystem fs, Path dirLockFile) throws IOException {
    if(fs instanceof DistributedFileSystem) {
      if (!((DistributedFileSystem) fs).recoverLease(dirLockFile)) {
        LOG.warn("Unable to recover lease on dir lock file " + dirLockFile + " right now. Cannot transfer ownership. Will need to try later.");
        return null;
      }
    }

    // delete and recreate lock file
    if( fs.delete(dirLockFile, false) ) { // returns false if somebody else already deleted it (to take ownership)
      FSDataOutputStream ostream = HdfsUtils.tryCreateFile(fs, dirLockFile);
      if(ostream!=null) {
        ostream.close();
      }
      return new DirLock(fs, dirLockFile);
    }
    return null;
  }

  public Path getLockFile() {
    return lockFile;
  }
}
