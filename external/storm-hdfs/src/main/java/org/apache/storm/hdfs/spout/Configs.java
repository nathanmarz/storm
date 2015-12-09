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

public class Configs {
  public static final String READER_TYPE = "hdfsspout.reader.type";
  public static final String TEXT = "text";
  public static final String SEQ = "seq";

  public static final String SOURCE_DIR = "hdfsspout.source.dir";           // dir from which to read files
  public static final String ARCHIVE_DIR = "hdfsspout.archive.dir";         // completed files will be moved here
  public static final String BAD_DIR = "hdfsspout.badfiles.dir";            // unpraseable files will be moved here
  public static final String LOCK_DIR = "hdfsspout.lock.dir";               // dir in which lock files will be created
  public static final String COMMIT_FREQ_COUNT = "hdfsspout.commit.count";  // commit after N records
  public static final String COMMIT_FREQ_SEC = "hdfsspout.commit.sec";      // commit after N secs
  public static final String MAX_DUPLICATE = "hdfsspout.max.duplicate";
  public static final String LOCK_TIMEOUT = "hdfsspout.lock.timeout.sec";   // inactivity duration after which locks are considered candidates for being reassigned to another spout
  public static final String CLOCKS_INSYNC = "hdfsspout.clocks.insync";     // if clocks on machines in the Storm cluster are in sync
  public static final String IGNORE_SUFFIX = "hdfsspout.ignore.suffix";     // filenames with this suffix will be ignored by the Spout

  public static final String DEFAULT_LOCK_DIR = ".lock";
  public static final int DEFAULT_COMMIT_FREQ_COUNT = 10000;
  public static final int DEFAULT_COMMIT_FREQ_SEC = 10;
  public static final int DEFAULT_MAX_DUPLICATES = 100;
  public static final int DEFAULT_LOCK_TIMEOUT = 5 * 60; // 5 min
  public static final String DEFAULT_HDFS_CONFIG_KEY = "hdfs.config";


} // class Configs
