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


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;

/**
 * Facility to synchronize access to HDFS files. Thread gains exclusive access to a file by acquiring
 * a FileLock object. The lock itself is represented as file on HDFS. Relies on atomic file creation.
 * Owning thread must heartbeat periodically on the lock to prevent the lock from being deemed as
 * stale (i.e. lock whose owning thread have died).
 */
public class FileLock {

  private final FileSystem fs;
  private final String componentID;
  private final Path lockFile;
  private final FSDataOutputStream lockFileStream;
  private LogEntry lastEntry;

  private static final Logger LOG = LoggerFactory.getLogger(FileLock.class);

  private FileLock(FileSystem fs, Path lockFile, FSDataOutputStream lockFileStream, String spoutId)
          throws IOException {
    this.fs = fs;
    this.lockFile = lockFile;
    this.lockFileStream = lockFileStream;
    this.componentID = spoutId;
    logProgress("0", false);
  }

  private FileLock(FileSystem fs, Path lockFile, String spoutId, LogEntry entry)
          throws IOException {
    this.fs = fs;
    this.lockFile = lockFile;
    this.lockFileStream =  fs.append(lockFile);
    this.componentID = spoutId;
    LOG.info("Acquired abandoned lockFile {}, Spout {}", lockFile, spoutId);
    logProgress(entry.fileOffset, true);
  }

  public void heartbeat(String fileOffset) throws IOException {
    logProgress(fileOffset, true);
  }

  // new line is at beginning of each line (instead of end) for better recovery from
  // partial writes of prior lines
  private void logProgress(String fileOffset, boolean prefixNewLine)
          throws IOException {
    long now = System.currentTimeMillis();
    LogEntry entry = new LogEntry(now, componentID, fileOffset);
    String line = entry.toString();
    if(prefixNewLine) {
      lockFileStream.writeBytes(System.lineSeparator() + line);
    }
    else {
      lockFileStream.writeBytes(line);
    }
    lockFileStream.hflush();

    lastEntry = entry; // update this only after writing to hdfs
  }

  /** Release lock by deleting file
   * @throws IOException if lock file could not be deleted
   */
  public void release() throws IOException {
    lockFileStream.close();
    if(!fs.delete(lockFile, false)) {
      LOG.warn("Unable to delete lock file, Spout = {}", componentID);
      throw new IOException("Unable to delete lock file");
    }
    LOG.debug("Released lock file {}. Spout {}", lockFile, componentID);
  }

  // For testing only.. invoked via reflection
  private void forceCloseLockFile() throws IOException {
    lockFileStream.close();
  }

  /** returns lock on file or null if file is already locked. throws if unexpected problem */
  public static FileLock tryLock(FileSystem fs, Path fileToLock, Path lockDirPath, String spoutId)
          throws IOException {
    Path lockFile = new Path(lockDirPath, fileToLock.getName());

    try {
      FSDataOutputStream ostream = HdfsUtils.tryCreateFile(fs, lockFile);
      if (ostream != null) {
        LOG.debug("Acquired lock on file {}. LockFile= {}, Spout = {}", fileToLock, lockFile, spoutId);
        return new FileLock(fs, lockFile, ostream, spoutId);
      } else {
        LOG.debug("Cannot lock file {} as its already locked. Spout = {}", fileToLock, spoutId);
        return null;
      }
    } catch (IOException e) {
      LOG.error("Error when acquiring lock on file " + fileToLock + " Spout = " + spoutId, e);
      throw e;
    }
  }

  /**
   * checks if lockFile is older than 'olderThan' UTC time by examining the modification time
   * on file and (if necessary) the timestamp in last log entry in the file. If its stale, then
   * returns the last log entry, else returns null.
   * @param fs
   * @param lockFile
   * @param olderThan  time (millis) in UTC.
   * @return the last entry in the file if its too old. null if last entry is not too old
   * @throws IOException
   */
  public static LogEntry getLastEntryIfStale(FileSystem fs, Path lockFile, long olderThan)
          throws IOException {
    long modifiedTime = fs.getFileStatus(lockFile).getModificationTime();
    if( modifiedTime <= olderThan ) { // look
      //Impt: HDFS timestamp may not reflect recent appends, so we double check the
      // timestamp in last line of file to see when the last update was made
      LogEntry lastEntry =  getLastEntry(fs, lockFile);
      if(lastEntry==null) {
        LOG.warn("Empty lock file found. Deleting it. {}", lockFile);
        try {
          if(!fs.delete(lockFile, false))
            throw new IOException("Empty lock file deletion failed");
        } catch (Exception e) {
          LOG.error("Unable to delete empty lock file " + lockFile, e);
        }
      }
      if( lastEntry.eventTime <= olderThan )
        return lastEntry;
    }
    return null;
  }

  /**
   * returns the last log entry
   * @param fs
   * @param lockFile
   * @return
   * @throws IOException
   */
  public static LogEntry getLastEntry(FileSystem fs, Path lockFile)
          throws IOException {
    FSDataInputStream in = fs.open(lockFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String lastLine = null;
    for(String line = reader.readLine(); line!=null; line = reader.readLine() ) {
      lastLine=line;
    }
    return LogEntry.deserialize(lastLine);
  }

  /**
   * Takes ownership of the lock file if possible.
   * @param lockFile
   * @param lastEntry   last entry in the lock file. this param is an optimization.
   *                    we dont scan the lock file again to find its last entry here since
   *                    its already been done once by the logic used to check if the lock
   *                    file is stale. so this value comes from that earlier scan.
   * @param spoutId     spout id
   * @throws IOException if unable to acquire
   * @return null if lock File is not recoverable
   */
  public static FileLock takeOwnership(FileSystem fs, Path lockFile, LogEntry lastEntry, String spoutId)
          throws IOException {
    try {
      if(fs instanceof DistributedFileSystem ) {
        if( !((DistributedFileSystem) fs).recoverLease(lockFile) ) {
          LOG.warn("Unable to recover lease on lock file {} right now. Cannot transfer ownership. Will need to try later. Spout = {}", lockFile, spoutId);
          return null;
        }
      }
      return new FileLock(fs, lockFile, spoutId, lastEntry);
    } catch (IOException e) {
      if (e instanceof RemoteException &&
              ((RemoteException) e).unwrapRemoteException() instanceof AlreadyBeingCreatedException) {
        LOG.warn("Lock file " + lockFile + "is currently open. Cannot transfer ownership now. Will need to try later. Spout= " + spoutId, e);
        return null;
      } else { // unexpected error
        LOG.warn("Cannot transfer ownership now for lock file " + lockFile + ". Will need to try later. Spout =" + spoutId, e);
        throw e;
      }
    }
  }

  /**
   * Finds a oldest expired lock file (using modification timestamp), then takes
   * ownership of the lock file
   * Impt: Assumes access to lockFilesDir has been externally synchronized such that
   *       only one thread accessing the same thread
   * @param fs
   * @param lockFilesDir
   * @param locktimeoutSec
   * @return
   */
  public static FileLock acquireOldestExpiredLock(FileSystem fs, Path lockFilesDir, int locktimeoutSec, String spoutId)
          throws IOException {
    // list files
    long now = System.currentTimeMillis();
    long olderThan = now - (locktimeoutSec*1000);
    Collection<Path> listing = HdfsUtils.listFilesByModificationTime(fs, lockFilesDir, olderThan);

    // locate expired lock files (if any). Try to take ownership (oldest lock first)
    for (Path file : listing) {
      if(file.getName().equalsIgnoreCase( DirLock.DIR_LOCK_FILE) ) {
        continue;
      }
      LogEntry lastEntry = getLastEntryIfStale(fs, file, olderThan);
      if(lastEntry!=null) {
        FileLock lock = FileLock.takeOwnership(fs, file, lastEntry, spoutId);
        if(lock!=null) {
          return lock;
        }
      }
    }
    if(listing.isEmpty()) {
      LOG.debug("No abandoned lock files found by Spout {}", spoutId);
    }
    return null;
  }


  /**
   * Finds oldest expired lock file (using modification timestamp), then takes
   * ownership of the lock file
   * Impt: Assumes access to lockFilesDir has been externally synchronized such that
   *       only one thread accessing the same thread
   * @param fs
   * @param lockFilesDir
   * @param locktimeoutSec
   * @return a Pair<lock file path, last entry in lock file> .. if expired lock file found
   * @throws IOException
   */
  public static HdfsUtils.Pair<Path,LogEntry> locateOldestExpiredLock(FileSystem fs, Path lockFilesDir, int locktimeoutSec)
          throws IOException {
    // list files
    long now =  System.currentTimeMillis();
    long olderThan = now - (locktimeoutSec*1000);
    Collection<Path> listing = HdfsUtils.listFilesByModificationTime(fs, lockFilesDir, olderThan);

    // locate oldest expired lock file (if any) and take ownership
    for (Path file : listing) {
      if(file.getName().equalsIgnoreCase( DirLock.DIR_LOCK_FILE) ) {
        continue;
      }
      LogEntry lastEntry = getLastEntryIfStale(fs, file, olderThan);
      if(lastEntry!=null) {
        return new HdfsUtils.Pair<>(file, lastEntry);
      }
    }
    LOG.debug("No abandoned files found");
    return null;
  }

  public LogEntry getLastLogEntry() {
    return lastEntry;
  }

  public Path getLockFile() {
    return lockFile;
  }

  public static class LogEntry {
    private static final int NUM_FIELDS = 3;
    public final long eventTime;
    public final String componentID;
    public final String fileOffset;

    public LogEntry(long eventtime, String componentID, String fileOffset) {
      this.eventTime = eventtime;
      this.componentID = componentID;
      this.fileOffset = fileOffset;
    }

    public String toString() {
      return eventTime + "," + componentID + "," + fileOffset;
    }
    public static LogEntry deserialize(String line) {
      String[] fields = line.split(",", NUM_FIELDS);
      return new LogEntry(Long.parseLong(fields[0]), fields[1], fields[2]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (!(o instanceof LogEntry)) { return false; }

      LogEntry logEntry = (LogEntry) o;

      if (eventTime != logEntry.eventTime) { return false; }
      if (!componentID.equals(logEntry.componentID)) { return false; }
      return fileOffset.equals(logEntry.fileOffset);

    }

    @Override
    public int hashCode() {
      int result = (int) (eventTime ^ (eventTime >>> 32));
      result = 31 * result + componentID.hashCode();
      result = 31 * result + fileOffset.hashCode();
      return result;
    }
  }
}
