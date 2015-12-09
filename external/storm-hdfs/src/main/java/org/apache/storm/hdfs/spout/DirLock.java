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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DirLock {
  private FileSystem fs;
  private final Path lockFile;
  public static final String DIR_LOCK_FILE = "DIRLOCK";
  private static final Logger log = LoggerFactory.getLogger(DirLock.class);
  private DirLock(FileSystem fs, Path lockFile) throws IOException {
    if( fs.isDirectory(lockFile) )
      throw new IllegalArgumentException(lockFile.toString() + " is not a directory");
    this.fs = fs;
    this.lockFile = lockFile;
  }

  /** Returns null if somebody else has a lock
   *
   * @param fs
   * @param dir  the dir on which to get a lock
   * @return lock object
   * @throws IOException if there were errors
   */
  public static DirLock tryLock(FileSystem fs, Path dir) throws IOException {
    Path lockFile = new Path(dir.toString() + Path.SEPARATOR_CHAR + DIR_LOCK_FILE );
    try {
      FSDataOutputStream os = fs.create(lockFile, false);
      if(log.isInfoEnabled()) {
        log.info("Thread acquired dir lock  " + threadInfo() + " - lockfile " + lockFile);
      }
      os.close();
      return new DirLock(fs, lockFile);
    } catch (FileAlreadyExistsException e) {
      return null;
    }
  }

  private static String threadInfo () {
    return "ThdId=" + Thread.currentThread().getId() + ", ThdName=" + Thread.currentThread().getName();
  }
  public void release() throws IOException {
    fs.delete(lockFile, false);
    log.info("Thread {} released dir lock {} ", threadInfo(), lockFile);
  }

  public Path getLockFile() {
    return lockFile;
  }
}
