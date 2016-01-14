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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

public class HdfsSpout extends BaseRichSpout {

  // user configurable
  private String hdfsUri;            // required
  private String readerType;         // required
  private Fields outputFields;       // required
  private Path sourceDirPath;        // required
  private Path archiveDirPath;       // required
  private Path badFilesDirPath;      // required
  private Path lockDirPath;

  private int commitFrequencyCount = Configs.DEFAULT_COMMIT_FREQ_COUNT;
  private int commitFrequencySec = Configs.DEFAULT_COMMIT_FREQ_SEC;
  private int maxOutstanding = Configs.DEFAULT_MAX_OUTSTANDING;
  private int lockTimeoutSec = Configs.DEFAULT_LOCK_TIMEOUT;
  private boolean clocksInSync = true;

  private String inprogress_suffix = ".inprogress";
  private String ignoreSuffix = ".ignore";

  // other members
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSpout.class);

  private ProgressTracker tracker = null;

  private FileSystem hdfs;
  private FileReader reader;

  private SpoutOutputCollector collector;
  HashMap<MessageId, List<Object> > inflight = new HashMap<>();
  LinkedBlockingQueue<HdfsUtils.Pair<MessageId, List<Object>>> retryList = new LinkedBlockingQueue<>();

  private Configuration hdfsConfig;

  private Map conf = null;
  private FileLock lock;
  private String spoutId = null;

  HdfsUtils.Pair<Path,FileLock.LogEntry> lastExpiredLock = null;
  private long lastExpiredLockTime = 0;

  private long tupleCounter = 0;
  private boolean ackEnabled = false;
  private int acksSinceLastCommit = 0 ;
  private final AtomicBoolean commitTimeElapsed = new AtomicBoolean(false);
  private Timer commitTimer;
  private boolean fileReadCompletely = true;

  private String configKey = Configs.DEFAULT_HDFS_CONFIG_KEY; // key for hdfs Kerberos configs

  public HdfsSpout() {
  }
  /** Name of the output field names. Number of fields depends upon the reader type */
  public HdfsSpout withOutputFields(String... fields) {
    outputFields = new Fields(fields);
    return this;
  }

  /** set key name under which HDFS options are placed. (similar to HDFS bolt).
   * default key name is 'hdfs.config' */
  public HdfsSpout withConfigKey(String configKey) {
    this.configKey = configKey;
    return this;
  }

  public Path getLockDirPath() {
    return lockDirPath;
  }

  public SpoutOutputCollector getCollector() {
    return collector;
  }

  public void nextTuple() {
    LOG.trace("Next Tuple {}", spoutId);
    // 1) First re-emit any previously failed tuples (from retryList)
    if (!retryList.isEmpty()) {
      LOG.debug("Sending tuple from retry list");
      HdfsUtils.Pair<MessageId, List<Object>> pair = retryList.remove();
      emitData(pair.getValue(), pair.getKey());
      return;
    }

    if( ackEnabled  &&  tracker.size()>= maxOutstanding) {
      LOG.warn("Waiting for more ACKs before generating new tuples. " +
              "Progress tracker size has reached limit {}, SpoutID {}"
              , maxOutstanding, spoutId);
      // Don't emit anything .. allow configured spout wait strategy to kick in
      return;
    }

    // 2) If no failed tuples to be retried, then send tuples from hdfs
    while (true) {
      try {
        // 3) Select a new file if one is not open already
        if (reader == null) {
          reader = pickNextFile();
          if (reader == null) {
            LOG.debug("Currently no new files to process under : " + sourceDirPath);
            return;
          } else {
            fileReadCompletely=false;
          }
        }
        if( fileReadCompletely ) { // wait for more ACKs before proceeding
          return;
        }
        // 4) Read record from file, emit to collector and record progress
        List<Object> tuple = reader.next();
        if (tuple != null) {
          fileReadCompletely= false;
          ++tupleCounter;
          MessageId msgId = new MessageId(tupleCounter, reader.getFilePath(), reader.getFileOffset());
          emitData(tuple, msgId);

          if(!ackEnabled) {
            ++acksSinceLastCommit; // assume message is immediately ACKed in non-ack mode
            commitProgress(reader.getFileOffset());
          } else {
            commitProgress(tracker.getCommitPosition());
          }
          return;
        } else {
          fileReadCompletely = true;
          if(!ackEnabled) {
            markFileAsDone(reader.getFilePath());
          }
        }
      } catch (IOException e) {
        LOG.error("I/O Error processing at file location " + getFileProgress(reader), e);
        // don't emit anything .. allow configured spout wait strategy to kick in
        return;
      } catch (ParseException e) {
        LOG.error("Parsing error when processing at file location " + getFileProgress(reader) +
                ". Skipping remainder of file.", e);
        markFileAsBad(reader.getFilePath());
        // Note: We don't return from this method on ParseException to avoid triggering the
        // spout wait strategy (due to no emits). Instead we go back into the loop and
        // generate a tuple from next file
      }
    } // while
  }

  // will commit progress into lock file if commit threshold is reached
  private void commitProgress(FileOffset position) {
    if(position==null) {
      return;
    }
    if ( lock!=null && canCommitNow() ) {
      try {
        String pos = position.toString();
        lock.heartbeat(pos);
        LOG.debug("{} Committed progress. {}", spoutId, pos);
        acksSinceLastCommit = 0;
        commitTimeElapsed.set(false);
        setupCommitElapseTimer();
      } catch (IOException e) {
        LOG.error("Unable to commit progress Will retry later. Spout ID = " + spoutId, e);
      }
    }
  }

  private void setupCommitElapseTimer() {
    if(commitFrequencySec<=0) {
      return;
    }
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        commitTimeElapsed.set(true);
      }
    };
    commitTimer.schedule(timerTask, commitFrequencySec * 1000);
  }

  private static String getFileProgress(FileReader reader) {
    return reader.getFilePath() + " " + reader.getFileOffset();
  }

  private void markFileAsDone(Path filePath) {
    try {
      Path newFile = renameCompletedFile(reader.getFilePath());
      LOG.info("Completed processing {}. Spout Id = {}", newFile, spoutId);
    } catch (IOException e) {
      LOG.error("Unable to archive completed file" + filePath + " Spout ID " + spoutId, e);
    }
    closeReaderAndResetTrackers();
  }

  private void markFileAsBad(Path file) {
    String fileName = file.toString();
    String fileNameMinusSuffix = fileName.substring(0, fileName.indexOf(inprogress_suffix));
    String originalName = new Path(fileNameMinusSuffix).getName();
    Path  newFile = new Path( badFilesDirPath + Path.SEPARATOR + originalName);

    LOG.info("Moving bad file {} to {}. Processed it till offset {}. SpoutID= {}", originalName, newFile, tracker.getCommitPosition(), spoutId);
    try {
      if (!hdfs.rename(file, newFile) ) { // seems this can fail by returning false or throwing exception
        throw new IOException("Move failed for bad file: " + file); // convert false ret value to exception
      }
    } catch (IOException e) {
      LOG.warn("Error moving bad file: " + file + " to destination " + newFile + " SpoutId =" + spoutId, e);
    }
    closeReaderAndResetTrackers();
  }

  private void closeReaderAndResetTrackers() {
    inflight.clear();
    tracker.offsets.clear();
    retryList.clear();

    reader.close();
    reader = null;
    releaseLockAndLog(lock, spoutId);
    lock = null;
  }

  private static void releaseLockAndLog(FileLock fLock, String spoutId) {
    try {
      if(fLock!=null) {
        fLock.release();
        LOG.debug("Spout {} released FileLock. SpoutId = {}", fLock.getLockFile(), spoutId);
      }
    } catch (IOException e) {
      LOG.error("Unable to delete lock file : " +fLock.getLockFile() + " SpoutId =" + spoutId, e);
    }
  }

  protected void emitData(List<Object> tuple, MessageId id) {
    LOG.trace("Emitting - {}", id);
    this.collector.emit(tuple, id);
    inflight.put(id, tuple);
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    LOG.info("Opening HDFS Spout");
    this.conf = conf;
    this.commitTimer = new Timer();
    this.tracker = new ProgressTracker();
    this.hdfsConfig = new Configuration();

    this.collector = collector;
    this.hdfsConfig = new Configuration();
    this.tupleCounter = 0;

    // Hdfs related settings
    if( conf.containsKey(Configs.HDFS_URI)) {
      this.hdfsUri = conf.get(Configs.HDFS_URI).toString();
    } else {
      throw new RuntimeException(Configs.HDFS_URI + " setting is required");
    }

    try {
      this.hdfs = FileSystem.get(URI.create(hdfsUri), hdfsConfig);
    } catch (IOException e) {
      LOG.error("Unable to instantiate file system", e);
      throw new RuntimeException("Unable to instantiate file system", e);
    }


    if ( conf.containsKey(configKey) ) {
      Map<String, Object> map = (Map<String, Object>)conf.get(configKey);
        if(map != null) {
          for(String keyName : map.keySet()){
            LOG.info("HDFS Config override : {} = {} ", keyName, String.valueOf(map.get(keyName)));
            this.hdfsConfig.set(keyName, String.valueOf(map.get(keyName)));
          }
          try {
            HdfsSecurityUtil.login(conf, hdfsConfig);
          } catch (IOException e) {
            LOG.error("HDFS Login failed ", e);
            throw new RuntimeException(e);
          }
        } // if(map != null)
      }

    // Reader type config
    if( conf.containsKey(Configs.READER_TYPE) ) {
      readerType = conf.get(Configs.READER_TYPE).toString();
      checkValidReader(readerType);
    }

    // -- source dir config
    if ( !conf.containsKey(Configs.SOURCE_DIR) ) {
      LOG.error(Configs.SOURCE_DIR + " setting is required");
      throw new RuntimeException(Configs.SOURCE_DIR + " setting is required");
    }
    this.sourceDirPath = new Path( conf.get(Configs.SOURCE_DIR).toString() );

    // -- archive dir config
    if ( !conf.containsKey(Configs.ARCHIVE_DIR) ) {
      LOG.error(Configs.ARCHIVE_DIR + " setting is required");
      throw new RuntimeException(Configs.ARCHIVE_DIR + " setting is required");
    }
    this.archiveDirPath = new Path( conf.get(Configs.ARCHIVE_DIR).toString() );
    validateOrMakeDir(hdfs, archiveDirPath, "Archive");

    // -- bad files dir config
    if ( !conf.containsKey(Configs.BAD_DIR) ) {
      LOG.error(Configs.BAD_DIR + " setting is required");
      throw new RuntimeException(Configs.BAD_DIR + " setting is required");
    }

    this.badFilesDirPath = new Path(conf.get(Configs.BAD_DIR).toString());
    validateOrMakeDir(hdfs, badFilesDirPath, "bad files");

    // -- ignore file names config
    if ( conf.containsKey(Configs.IGNORE_SUFFIX) ) {
      this.ignoreSuffix = conf.get(Configs.IGNORE_SUFFIX).toString();
    }

    // -- lock dir config
    String lockDir = !conf.containsKey(Configs.LOCK_DIR) ? getDefaultLockDir(sourceDirPath) : conf.get(Configs.LOCK_DIR).toString() ;
    this.lockDirPath = new Path(lockDir);
    validateOrMakeDir(hdfs,lockDirPath,"locks");

    // -- lock timeout
    if( conf.get(Configs.LOCK_TIMEOUT) !=null ) {
      this.lockTimeoutSec = Integer.parseInt(conf.get(Configs.LOCK_TIMEOUT).toString());
    }

    // -- enable/disable ACKing
    Object ackers = conf.get(Config.TOPOLOGY_ACKER_EXECUTORS);
    if( ackers!=null ) {
      int ackerCount = Integer.parseInt(ackers.toString());
      this.ackEnabled = (ackerCount>0);
      LOG.debug("ACKer count = {}", ackerCount);
    }
    else { // ackers==null when ackerCount not explicitly set on the topology
      this.ackEnabled = true;
      LOG.debug("ACK count not explicitly set on topology.");
    }

    LOG.info("ACK mode is {}", ackEnabled ? "enabled" : "disabled");

    // -- commit frequency - count
    if( conf.get(Configs.COMMIT_FREQ_COUNT) != null ) {
      commitFrequencyCount = Integer.parseInt(conf.get(Configs.COMMIT_FREQ_COUNT).toString());
    }

    // -- commit frequency - seconds
    if( conf.get(Configs.COMMIT_FREQ_SEC) != null ) {
      commitFrequencySec = Integer.parseInt(conf.get(Configs.COMMIT_FREQ_SEC).toString());
      if(commitFrequencySec<=0) {
        throw new RuntimeException(Configs.COMMIT_FREQ_SEC + " setting must be greater than 0");
      }
    }

    // -- max outstanding tuples
    if( conf.get(Configs.MAX_OUTSTANDING) !=null ) {
      maxOutstanding = Integer.parseInt(conf.get(Configs.MAX_OUTSTANDING).toString());
    }

    // -- clocks in sync
    if( conf.get(Configs.CLOCKS_INSYNC) !=null ) {
      clocksInSync = Boolean.parseBoolean(conf.get(Configs.CLOCKS_INSYNC).toString());
    }

    // -- spout id
    spoutId = context.getThisComponentId();

    // setup timer for commit elapse time tracking
    setupCommitElapseTimer();
  }

  private static void validateOrMakeDir(FileSystem fs, Path dir, String dirDescription) {
    try {
      if(fs.exists(dir)) {
        if(! fs.isDirectory(dir) ) {
          LOG.error(dirDescription + " directory is a file, not a dir. " + dir);
          throw new RuntimeException(dirDescription + " directory is a file, not a dir. " + dir);
        }
      } else if(! fs.mkdirs(dir) ) {
        LOG.error("Unable to create " + dirDescription + " directory " + dir);
        throw new RuntimeException("Unable to create " + dirDescription + " directory " + dir);
      }
    } catch (IOException e) {
      LOG.error("Unable to create " + dirDescription + " directory " + dir, e);
      throw new RuntimeException("Unable to create " + dirDescription + " directory " + dir, e);
    }
  }

  private String getDefaultLockDir(Path sourceDirPath) {
    return sourceDirPath.toString() + Path.SEPARATOR + Configs.DEFAULT_LOCK_DIR;
  }

  private static void checkValidReader(String readerType) {
    if(readerType.equalsIgnoreCase(Configs.TEXT)  || readerType.equalsIgnoreCase(Configs.SEQ) )
      return;
    try {
      Class<?> classType = Class.forName(readerType);
      classType.getConstructor(FileSystem.class, Path.class, Map.class);
      return;
    } catch (ClassNotFoundException e) {
      LOG.error(readerType + " not found in classpath.", e);
      throw new IllegalArgumentException(readerType + " not found in classpath.", e);
    } catch (NoSuchMethodException e) {
      LOG.error(readerType + " is missing the expected constructor for Readers.", e);
      throw new IllegalArgumentException(readerType + " is missing the expected constuctor for Readers.");
    }
  }

  @Override
  public void ack(Object msgId) {
    LOG.trace("Ack received for msg {} on spout {}", msgId, spoutId);
    if(!ackEnabled) {
      return;
    }
    MessageId id = (MessageId) msgId;
    inflight.remove(id);
    ++acksSinceLastCommit;
    tracker.recordAckedOffset(id.offset);
    commitProgress(tracker.getCommitPosition());
    if(fileReadCompletely && inflight.isEmpty()) {
      markFileAsDone(reader.getFilePath());
      reader = null;
    }
    super.ack(msgId);
  }

  private boolean canCommitNow() {

    if( commitFrequencyCount>0 &&  acksSinceLastCommit >= commitFrequencyCount ) {
      return true;
    }
    return commitTimeElapsed.get();
  }

  @Override
  public void fail(Object msgId) {
    LOG.trace("Fail received for msg id {} on spout {}", msgId, spoutId);
    super.fail(msgId);
    if(ackEnabled) {
      HdfsUtils.Pair<MessageId, List<Object>> item = HdfsUtils.Pair.of(msgId, inflight.remove(msgId));
      retryList.add(item);
    }
  }

  private FileReader pickNextFile() {
    try {
      // 1) If there are any abandoned files, pick oldest one
      lock = getOldestExpiredLock();
      if (lock != null) {
        LOG.debug("Spout {} now took over ownership of abandoned FileLock {}", spoutId, lock.getLockFile());
        Path file = getFileForLockFile(lock.getLockFile(), sourceDirPath);
        String resumeFromOffset = lock.getLastLogEntry().fileOffset;
        LOG.info("Resuming processing of abandoned file : {}", file);
        return createFileReader(file, resumeFromOffset);
      }

      // 2) If no abandoned files, then pick oldest file in sourceDirPath, lock it and rename it
      Collection<Path> listing = HdfsUtils.listFilesByModificationTime(hdfs, sourceDirPath, 0);

      for (Path file : listing) {
        if (file.getName().endsWith(inprogress_suffix)) {
          continue;
        }
        if (file.getName().endsWith(ignoreSuffix)) {
          continue;
        }
        lock = FileLock.tryLock(hdfs, file, lockDirPath, spoutId);
        if (lock == null) {
          LOG.debug("Unable to get FileLock for {}, so skipping it.", file);
          continue; // could not lock, so try another file.
        }
        try {
          Path newFile = renameToInProgressFile(file);
          FileReader result = createFileReader(newFile);
          LOG.info("Processing : {} ", file);
          return result;
        } catch (Exception e) {
          LOG.error("Skipping file " + file, e);
          releaseLockAndLog(lock, spoutId);
          continue;
        }
      }

      return null;
    } catch (IOException e) {
      LOG.error("Unable to select next file for consumption " + sourceDirPath, e);
      return null;
    }
  }

  /**
   * If clocks in sync, then acquires the oldest expired lock
   * Else, on first call, just remembers the oldest expired lock, on next call check if the lock is updated. if not updated then acquires the lock
   * @return a lock object
   * @throws IOException
   */
  private FileLock getOldestExpiredLock() throws IOException {
    // 1 - acquire lock on dir
    DirLock dirlock = DirLock.tryLock(hdfs, lockDirPath);
    if (dirlock == null) {
      dirlock = DirLock.takeOwnershipIfStale(hdfs, lockDirPath, lockTimeoutSec);
      if (dirlock == null) {
        LOG.debug("Spout {} could not take over ownership of DirLock for {}", spoutId, lockDirPath);
        return null;
      }
      LOG.debug("Spout {} now took over ownership of abandoned DirLock for {}", spoutId, lockDirPath);
    } else {
      LOG.debug("Spout {} now owns DirLock for {}", spoutId, lockDirPath);
    }

    try {
      // 2 - if clocks are in sync then simply take ownership of the oldest expired lock
      if (clocksInSync) {
        return FileLock.acquireOldestExpiredLock(hdfs, lockDirPath, lockTimeoutSec, spoutId);
      }

      // 3 - if clocks are not in sync ..
      if( lastExpiredLock == null ) {
        // just make a note of the oldest expired lock now and check if its still unmodified after lockTimeoutSec
        lastExpiredLock = FileLock.locateOldestExpiredLock(hdfs, lockDirPath, lockTimeoutSec);
        lastExpiredLockTime = System.currentTimeMillis();
        return null;
      }
      // see if lockTimeoutSec time has elapsed since we last selected the lock file
      if( hasExpired(lastExpiredLockTime) ) {
        return null;
      }

      // If lock file has expired, then own it
      FileLock.LogEntry lastEntry = FileLock.getLastEntry(hdfs, lastExpiredLock.getKey());
      if( lastEntry.equals(lastExpiredLock.getValue()) ) {
        FileLock result = FileLock.takeOwnership(hdfs, lastExpiredLock.getKey(), lastEntry, spoutId);
        lastExpiredLock = null;
        return  result;
      } else {
        // if lock file has been updated since last time, then leave this lock file alone
        lastExpiredLock = null;
        return null;
      }
    } finally {
      dirlock.release();
      LOG.debug("Released DirLock {}, SpoutID {} ", dirlock.getLockFile(), spoutId);
    }
  }

  private boolean hasExpired(long lastModifyTime) {
    return (System.currentTimeMillis() - lastModifyTime ) < lockTimeoutSec*1000;
  }

  /**
   * Creates a reader that reads from beginning of file
   * @param file file to read
   * @return
   * @throws IOException
   */
  private FileReader createFileReader(Path file)
          throws IOException {
    if(readerType.equalsIgnoreCase(Configs.SEQ)) {
      return new SequenceFileReader(this.hdfs, file, conf);
    }
    if(readerType.equalsIgnoreCase(Configs.TEXT)) {
      return new TextFileReader(this.hdfs, file, conf);
    }
    try {
      Class<?> clsType = Class.forName(readerType);
      Constructor<?> constructor = clsType.getConstructor(FileSystem.class, Path.class, Map.class);
      return (FileReader) constructor.newInstance(this.hdfs, file, conf);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException("Unable to instantiate " + readerType + " reader", e);
    }
  }


  /**
   * Creates a reader that starts reading from 'offset'
   * @param file the file to read
   * @param offset the offset string should be understandable by the reader type being used
   * @return
   * @throws IOException
   */
  private FileReader createFileReader(Path file, String offset)
          throws IOException {
    if(readerType.equalsIgnoreCase(Configs.SEQ)) {
      return new SequenceFileReader(this.hdfs, file, conf, offset);
    }
    if(readerType.equalsIgnoreCase(Configs.TEXT)) {
      return new TextFileReader(this.hdfs, file, conf, offset);
    }

    try {
      Class<?> clsType = Class.forName(readerType);
      Constructor<?> constructor = clsType.getConstructor(FileSystem.class, Path.class, Map.class, String.class);
      return (FileReader) constructor.newInstance(this.hdfs, file, conf, offset);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException("Unable to instantiate " + readerType, e);
    }
  }

  /**
   * Renames files with .inprogress suffix
   * @return path of renamed file
   * @throws if operation fails
   */
  private Path renameToInProgressFile(Path file)
          throws IOException {
    Path newFile =  new Path( file.toString() + inprogress_suffix );
    try {
      if (hdfs.rename(file, newFile)) {
        return newFile;
      }
      throw new RenameException(file, newFile);
    } catch (IOException e){
      throw new RenameException(file, newFile, e);
    }
  }

  /** Returns the corresponding input file in the 'sourceDirPath' for the specified lock file.
   *  If no such file is found then returns null
   */
  private Path getFileForLockFile(Path lockFile, Path sourceDirPath)
          throws IOException {
    String lockFileName = lockFile.getName();
    Path dataFile = new Path(sourceDirPath + Path.SEPARATOR + lockFileName + inprogress_suffix);
    if( hdfs.exists(dataFile) ) {
      return dataFile;
    }
    dataFile = new Path(sourceDirPath + Path.SEPARATOR +  lockFileName);
    if(hdfs.exists(dataFile)) {
      return dataFile;
    }
    return null;
  }


  // renames files and returns the new file path
  private Path renameCompletedFile(Path file) throws IOException {
    String fileName = file.toString();
    String fileNameMinusSuffix = fileName.substring(0, fileName.indexOf(inprogress_suffix));
    String newName = new Path(fileNameMinusSuffix).getName();

    Path  newFile = new Path( archiveDirPath + Path.SEPARATOR + newName );
    LOG.info("Completed consuming file {}", fileNameMinusSuffix);
    if (!hdfs.rename(file, newFile) ) {
      throw new IOException("Rename failed for file: " + file);
    }
    LOG.debug("Renamed file {} to {} ", file, newFile);
    return newFile;
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(outputFields);
  }

  static class MessageId implements  Comparable<MessageId> {
    public long msgNumber; // tracks order in which msg came in
    public String fullPath;
    public FileOffset offset;

    public MessageId(long msgNumber, Path fullPath, FileOffset offset) {
      this.msgNumber = msgNumber;
      this.fullPath = fullPath.toString();
      this.offset = offset;
    }

    @Override
    public String toString() {
      return "{'" +  fullPath + "':" + offset + "}";
    }

    @Override
    public int compareTo(MessageId rhs) {
      if (msgNumber<rhs.msgNumber) {
        return -1;
      }
      if(msgNumber>rhs.msgNumber) {
        return 1;
      }
      return 0;
    }
  }

  private static class RenameException extends IOException {
    public final Path oldFile;
    public final Path newFile;

    public RenameException(Path oldFile, Path newFile) {
      super("Rename of " + oldFile + " to " + newFile + " failed");
      this.oldFile = oldFile;
      this.newFile = newFile;
    }

    public RenameException(Path oldFile, Path newFile, IOException cause) {
      super("Rename of " + oldFile + " to " + newFile + " failed", cause);
      this.oldFile = oldFile;
      this.newFile = newFile;
    }
  }
}
