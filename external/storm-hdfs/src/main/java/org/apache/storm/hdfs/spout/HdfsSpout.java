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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class HdfsSpout extends BaseRichSpout {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsSpout.class);

  private Path sourceDirPath;
  private Path archiveDirPath;
  private Path badFilesDirPath;
  private Path lockDirPath;

  private int commitFrequencyCount = Configs.DEFAULT_COMMIT_FREQ_COUNT;
  private int commitFrequencySec = Configs.DEFAULT_COMMIT_FREQ_SEC;
  private int maxDuplicates = Configs.DEFAULT_MAX_DUPLICATES;
  private int lockTimeoutSec = Configs.DEFAULT_LOCK_TIMEOUT;
  private boolean clocksInSync = true;

  private ProgressTracker tracker = new ProgressTracker();

  private FileSystem hdfs;
  private FileReader reader;

  private SpoutOutputCollector collector;
  HashMap<MessageId, List<Object> > inflight = new HashMap<>();
  LinkedBlockingQueue<HdfsUtils.Pair<MessageId, List<Object>>> retryList = new LinkedBlockingQueue<>();

  private String inprogress_suffix = ".inprogress";
  private String ignoreSuffix = ".ignore";

  private Configuration hdfsConfig;
  private String readerType;

  private Map conf = null;
  private FileLock lock;
  private String spoutId = null;

  HdfsUtils.Pair<Path,FileLock.LogEntry> lastExpiredLock = null;
  private long lastExpiredLockTime = 0;

  private long tupleCounter = 0;
  private boolean ackEnabled = false;
  private int acksSinceLastCommit = 0 ;
  private final AtomicBoolean commitTimeElapsed = new AtomicBoolean(false);
  private final Timer commitTimer = new Timer();
  private boolean fileReadCompletely = false;

  private String configKey = Configs.DEFAULT_HDFS_CONFIG_KEY; // key for hdfs kerberos configs

  public HdfsSpout() {
  }

  public Path getLockDirPath() {
    return lockDirPath;
  }

  public SpoutOutputCollector getCollector() {
    return collector;
  }

  public HdfsSpout withConfigKey(String configKey){
    this.configKey = configKey;
    return this;
  }

  public void nextTuple() {
    LOG.debug("Next Tuple");
    // 1) First re-emit any previously failed tuples (from retryList)
    if (!retryList.isEmpty()) {
      LOG.debug("Sending from retry list");
      HdfsUtils.Pair<MessageId, List<Object>> pair = retryList.remove();
      emitData(pair.getValue(), pair.getKey());
      return;
    }

    if( ackEnabled  &&  tracker.size()>=maxDuplicates ) {
      LOG.warn("Waiting for more ACKs before generating new tuples. " +
               "Progress tracker size has reached limit {}"
              , maxDuplicates);
      // Don't emit anything .. allow configured spout wait strategy to kick in
      return;
    }

    // 2) If no failed tuples, then send tuples from hdfs
    while (true) {
      try {
        // 3) Select a new file if one is not open already
        if (reader == null) {
          reader = pickNextFile();
          if (reader == null) {
            LOG.info("Currently no new files to process under : " + sourceDirPath);
            return;
          }
        }

        // 4) Read record from file, emit to collector and record progress
        List<Object> tuple = reader.next();
        if (tuple != null) {
          fileReadCompletely= false;
          ++tupleCounter;
          MessageId msgId = new MessageId(tupleCounter, reader.getFilePath(), reader.getFileOffset());
          emitData(tuple, msgId);

          if(!ackEnabled) {
            ++acksSinceLastCommit; // assume message is immediately acked in non-ack mode
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
        // note: Unfortunately not emitting anything here due to parse error
        // will trigger the configured spout wait strategy which is unnecessary
      }
    }

  }

  // will commit progress into lock file if commit threshold is reached
  private void commitProgress(FileOffset position) {
    if ( lock!=null && canCommitNow() ) {
      try {
        lock.heartbeat(position.toString());
        acksSinceLastCommit = 0;
        commitTimeElapsed.set(false);
        setupCommitElapseTimer();
      } catch (IOException e) {
        LOG.error("Unable to commit progress Will retry later.", e);
      }
    }
  }

  private void setupCommitElapseTimer() {
    if(commitFrequencySec<=0)
      return;
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        commitTimeElapsed.set(false);
      }
    };
    commitTimer.schedule(timerTask, commitFrequencySec * 1000);
  }


  private static String getFileProgress(FileReader reader) {
    return reader.getFilePath() + " " + reader.getFileOffset();
  }

  private void markFileAsDone(Path filePath) {
    fileReadCompletely = false;
    try {
      renameCompletedFile(reader.getFilePath());
    } catch (IOException e) {
      LOG.error("Unable to archive completed file" + filePath, e);
    }
    unlockAndCloseReader();

  }

  private void markFileAsBad(Path file) {
    String fileName = file.toString();
    String fileNameMinusSuffix = fileName.substring(0, fileName.indexOf(inprogress_suffix));
    String originalName = new Path(fileNameMinusSuffix).getName();
    Path  newFile = new Path( badFilesDirPath + Path.SEPARATOR + originalName);

    LOG.info("Moving bad file to " + newFile);
    try {
      if (!hdfs.rename(file, newFile) ) { // seems this can fail by returning false or throwing exception
        throw new IOException("Move failed for bad file: " + file); // convert false ret value to exception
      }
    } catch (IOException e) {
      LOG.warn("Error moving bad file: " + file + ". to destination :  " + newFile);
    }

    unlockAndCloseReader();
  }

  private void unlockAndCloseReader() {
    reader.close();
    reader = null;
    try {
      lock.release();
    } catch (IOException e) {
      LOG.error("Unable to delete lock file : " + this.lock.getLockFile(), e);
    }
    lock = null;
  }



  protected void emitData(List<Object> tuple, MessageId id) {
    LOG.debug("Emitting - {}", id);
    this.collector.emit(tuple, id);
    inflight.put(id, tuple);
  }

  public void open(Map conf, TopologyContext context,  SpoutOutputCollector collector) {
    this.conf = conf;
    final String FILE_SYSTEM = "filesystem";
    LOG.info("Opening");
    this.collector = collector;
    this.hdfsConfig = new Configuration();
    this.tupleCounter = 0;

    for( Object k : conf.keySet() ) {
      String key = k.toString();
      if( ! FILE_SYSTEM.equalsIgnoreCase( key ) ) { // to support unit test only
        String val = conf.get(key).toString();
        LOG.info("Config setting : " + key + " = " + val);
        this.hdfsConfig.set(key, val);
      }
      else
        this.hdfs = (FileSystem) conf.get(key);

      if(key.equalsIgnoreCase(Configs.READER_TYPE)) {
        readerType = conf.get(key).toString();
        checkValidReader(readerType);
      }
    }

    // - Hdfs configs
    this.hdfsConfig = new Configuration();
    Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
    if(map != null){
      for(String key : map.keySet()){
        this.hdfsConfig.set(key, String.valueOf(map.get(key)));
      }
    }

    try {
      HdfsSecurityUtil.login(conf, hdfsConfig);
    } catch (IOException e) {
      LOG.error("Failed to open " + sourceDirPath);
      throw new RuntimeException(e);
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

    try {
      if(hdfs.exists(archiveDirPath)) {
        if(! hdfs.isDirectory(archiveDirPath) ) {
          LOG.error("Archive directory is a file. " + archiveDirPath);
          throw new RuntimeException("Archive directory is a file. " + archiveDirPath);
        }
      } else if(! hdfs.mkdirs(archiveDirPath) ) {
        LOG.error("Unable to create archive directory. " + archiveDirPath);
        throw new RuntimeException("Unable to create archive directory " + archiveDirPath);
      }
    } catch (IOException e) {
      LOG.error("Unable to create archive dir ", e);
      throw new RuntimeException("Unable to create archive directory ", e);
    }

    // -- bad files dir config
    if ( !conf.containsKey(Configs.BAD_DIR) ) {
      LOG.error(Configs.BAD_DIR + " setting is required");
      throw new RuntimeException(Configs.BAD_DIR + " setting is required");
    }

    this.badFilesDirPath = new Path(conf.get(Configs.BAD_DIR).toString());

    try {
      if(hdfs.exists(badFilesDirPath)) {
        if(! hdfs.isDirectory(badFilesDirPath) ) {
          LOG.error("Bad files directory is a file: " + badFilesDirPath);
          throw new RuntimeException("Bad files directory is a file: " + badFilesDirPath);
        }
      } else if(! hdfs.mkdirs(badFilesDirPath) ) {
        LOG.error("Unable to create directory for bad files: " + badFilesDirPath);
        throw new RuntimeException("Unable to create a directory for bad files: " + badFilesDirPath);
      }
    } catch (IOException e) {
      LOG.error("Unable to create archive dir ", e);
      throw new RuntimeException(e.getMessage(), e);
    }

    // -- ignore filename suffix
    if ( conf.containsKey(Configs.IGNORE_SUFFIX) ) {
      this.ignoreSuffix = conf.get(Configs.IGNORE_SUFFIX).toString();
    }

    // -- lock dir config
    String lockDir = !conf.containsKey(Configs.LOCK_DIR) ? getDefaultLockDir(sourceDirPath) : conf.get(Configs.LOCK_DIR).toString() ;
    this.lockDirPath = new Path(lockDir);

    try {
      if(hdfs.exists(lockDirPath)) {
        if(! hdfs.isDirectory(lockDirPath) ) {
          LOG.error("Lock directory is a file: " + lockDirPath);
          throw new RuntimeException("Lock directory is a file: " + lockDirPath);
        }
      } else if(! hdfs.mkdirs(lockDirPath) ) {
        LOG.error("Unable to create lock directory: " + lockDirPath);
        throw new RuntimeException("Unable to create lock directory: " + lockDirPath);
      }
    } catch (IOException e) {
      LOG.error("Unable to create lock dir: " + lockDirPath, e);
      throw new RuntimeException(e.getMessage(), e);
    }

    // -- lock timeout
    if( conf.get(Configs.LOCK_TIMEOUT) !=null )
      this.lockTimeoutSec =  Integer.parseInt(conf.get(Configs.LOCK_TIMEOUT).toString());

    // -- enable/disable ACKing
    Object ackers = conf.get(Config.TOPOLOGY_ACKER_EXECUTORS);
    if( ackers!=null )
      this.ackEnabled = ( Integer.parseInt( ackers.toString() ) > 0 );
    else
      this.ackEnabled = false;

    // -- commit frequency - count
    if( conf.get(Configs.COMMIT_FREQ_COUNT) != null )
      commitFrequencyCount = Integer.parseInt( conf.get(Configs.COMMIT_FREQ_COUNT).toString() );

    // -- commit frequency - seconds
    if( conf.get(Configs.COMMIT_FREQ_SEC) != null )
      commitFrequencySec = Integer.parseInt( conf.get(Configs.COMMIT_FREQ_SEC).toString() );

    // -- max duplicate
    if( conf.get(Configs.MAX_DUPLICATE) !=null )
      maxDuplicates = Integer.parseInt( conf.get(Configs.MAX_DUPLICATE).toString() );

    // -- clocks in sync
    if( conf.get(Configs.CLOCKS_INSYNC) !=null )
      clocksInSync = Boolean.parseBoolean(conf.get(Configs.CLOCKS_INSYNC).toString());

    // -- spout id
    spoutId = context.getThisComponentId();

    // setup timer for commit elapse time tracking
    setupCommitElapseTimer();
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
    MessageId id = (MessageId) msgId;
    inflight.remove(id);
    ++acksSinceLastCommit;
    tracker.recordAckedOffset(id.offset);
    commitProgress(tracker.getCommitPosition());
    if(fileReadCompletely) {
      markFileAsDone(reader.getFilePath());
      reader = null;
    }
    super.ack(msgId);
  }

  private boolean canCommitNow() {
    if( acksSinceLastCommit >= commitFrequencyCount )
      return true;
    return commitTimeElapsed.get();
  }

  @Override
  public void fail(Object msgId) {
    super.fail(msgId);
    HdfsUtils.Pair<MessageId, List<Object>> item = HdfsUtils.Pair.of(msgId, inflight.remove(msgId));
    retryList.add(item);
  }

  private FileReader pickNextFile()  {
    try {
      // 1) If there are any abandoned files, pick oldest one
      lock = getOldestExpiredLock();
      if (lock != null) {
        Path file = getFileForLockFile(lock.getLockFile(), sourceDirPath);
        String resumeFromOffset = lock.getLastLogEntry().fileOffset;
        LOG.info("Processing abandoned file : {}", file);
        return createFileReader(file, resumeFromOffset);
      }

      // 2) If no abandoned files, then pick oldest file in sourceDirPath, lock it and rename it
      Collection<Path> listing = HdfsUtils.listFilesByModificationTime(hdfs, sourceDirPath, 0);

      for (Path file : listing) {
        if( file.getName().endsWith(inprogress_suffix) )
          continue;
        if( file.getName().endsWith(ignoreSuffix) )
          continue;

        LOG.info("Processing : {} ", file);
        lock = FileLock.tryLock(hdfs, file, lockDirPath, spoutId);
        if( lock==null ) {
          LOG.info("Unable to get lock, so skipping file: {}", file);
          continue; // could not lock, so try another file.
        }
        Path newFile = renameSelectedFile(file);
        return createFileReader(newFile);
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
   * @return
   * @throws IOException
   */
  private FileLock getOldestExpiredLock() throws IOException {
    // 1 - acquire lock on dir
    DirLock dirlock = DirLock.tryLock(hdfs, lockDirPath);
    if (dirlock == null)
      return null;
    try {
      // 2 - if clocks are in sync then simply take ownership of the oldest expired lock
      if (clocksInSync)
        return FileLock.acquireOldestExpiredLock(hdfs, lockDirPath, lockTimeoutSec, spoutId);

      // 3 - if clocks are not in sync ..
      if( lastExpiredLock == null ) {
        // just make a note of the oldest expired lock now and check if its still unmodified after lockTimeoutSec
        lastExpiredLock = FileLock.locateOldestExpiredLock(hdfs, lockDirPath, lockTimeoutSec);
        lastExpiredLockTime = System.currentTimeMillis();
        return null;
      }
      // see if lockTimeoutSec time has elapsed since we last selected the lock file
      if( hasExpired(lastExpiredLockTime) )
        return null;

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
    if(readerType.equalsIgnoreCase(Configs.SEQ))
      return new SequenceFileReader(this.hdfs, file, conf);
    if(readerType.equalsIgnoreCase(Configs.TEXT))
      return new TextFileReader(this.hdfs, file, conf);

    try {
      Class<?> clsType = Class.forName(readerType);
      Constructor<?> constructor = clsType.getConstructor(FileSystem.class, Path.class, Map.class);
      return (FileReader) constructor.newInstance(this.hdfs, file, conf);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException("Unable to instantiate " + readerType, e);
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
    if(readerType.equalsIgnoreCase(Configs.SEQ))
      return new SequenceFileReader(this.hdfs, file, conf, offset);
    if(readerType.equalsIgnoreCase(Configs.TEXT))
      return new TextFileReader(this.hdfs, file, conf, offset);

    try {
      Class<?> clsType = Class.forName(readerType);
      Constructor<?> constructor = clsType.getConstructor(FileSystem.class, Path.class, Map.class, String.class);
      return (FileReader) constructor.newInstance(this.hdfs, file, conf, offset);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException("Unable to instantiate " + readerType, e);
    }
  }

  // returns new path of renamed file
  private Path renameSelectedFile(Path file)
          throws IOException {
    Path newFile =  new Path( file.toString() + inprogress_suffix );
    if( ! hdfs.rename(file, newFile) ) {
      throw new IOException("Rename failed for file: " + file);
    }
    return newFile;
  }

  /** Returns the corresponding input file in the 'sourceDirPath' for the specified lock file.
   *  If no such file is found then returns null
   */
  private Path getFileForLockFile(Path lockFile, Path sourceDirPath)
          throws IOException {
    String lockFileName = lockFile.getName();
    Path dataFile = new Path(sourceDirPath + lockFileName + inprogress_suffix);
    if( hdfs.exists(dataFile) )
      return dataFile;
    dataFile = new Path(sourceDirPath + lockFileName);
    if(hdfs.exists(dataFile))
      return dataFile;
    return null;
  }


  private Path renameCompletedFile(Path file) throws IOException {
    String fileName = file.toString();
    String fileNameMinusSuffix = fileName.substring(0, fileName.indexOf(inprogress_suffix));
    String newName = new Path(fileNameMinusSuffix).getName();

    Path  newFile = new Path( archiveDirPath + Path.SEPARATOR + newName );
    LOG.debug("Renaming complete file to " + newFile);
    LOG.info("Completed file " + fileNameMinusSuffix );
    if (!hdfs.rename(file, newFile) ) {
      throw new IOException("Rename failed for file: " + file);
    }
    return newFile;
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    Fields fields = reader.getOutputFields();
    declarer.declare(fields);
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
      if (msgNumber<rhs.msgNumber)
        return -1;
      if(msgNumber>rhs.msgNumber)
        return 1;
      return 0;
    }
  }

}
