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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class TestProgressTracker {

  private FileSystem fs;
  private Configuration conf = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  public File baseFolder;

  @Before
  public void setUp() throws Exception {
    fs = FileSystem.getLocal(conf);
  }

  @Test
  public void testBasic() throws Exception {
    ProgressTracker tracker = new ProgressTracker();
    baseFolder = tempFolder.newFolder("trackertest");

    Path file = new Path( baseFolder.toString() + Path.SEPARATOR + "testHeadTrimming.txt" );
    createTextFile(file, 10);

    // create reader and do some checks
    TextFileReader reader = new TextFileReader(fs, file, null);
    FileOffset pos0 = tracker.getCommitPosition();
    Assert.assertNull(pos0);

    TextFileReader.Offset currOffset = reader.getFileOffset();
    Assert.assertNotNull(currOffset);
    Assert.assertEquals(0, currOffset.charOffset);

    // read 1st line and ack
    Assert.assertNotNull(reader.next());
    TextFileReader.Offset pos1 = reader.getFileOffset();
    tracker.recordAckedOffset(pos1);

    TextFileReader.Offset pos1b = (TextFileReader.Offset) tracker.getCommitPosition();
    Assert.assertEquals(pos1, pos1b);

    // read 2nd line and ACK
    Assert.assertNotNull(reader.next());
    TextFileReader.Offset pos2 = reader.getFileOffset();
    tracker.recordAckedOffset(pos2);

    tracker.dumpState(System.err);
    TextFileReader.Offset pos2b = (TextFileReader.Offset) tracker.getCommitPosition();
    Assert.assertEquals(pos2, pos2b);


    // read lines 3..7, don't ACK .. commit pos should remain same
    Assert.assertNotNull(reader.next());//3
    TextFileReader.Offset pos3 = reader.getFileOffset();
    Assert.assertNotNull(reader.next());//4
    TextFileReader.Offset pos4 = reader.getFileOffset();
    Assert.assertNotNull(reader.next());//5
    TextFileReader.Offset pos5 = reader.getFileOffset();
    Assert.assertNotNull(reader.next());//6
    TextFileReader.Offset pos6 = reader.getFileOffset();
    Assert.assertNotNull(reader.next());//7
    TextFileReader.Offset pos7 = reader.getFileOffset();

    // now ack msg 5 and check
    tracker.recordAckedOffset(pos5);
    Assert.assertEquals(pos2, tracker.getCommitPosition()); // should remain unchanged @ 2
    tracker.recordAckedOffset(pos4);
    Assert.assertEquals(pos2, tracker.getCommitPosition()); // should remain unchanged @ 2
    tracker.recordAckedOffset(pos3);
    Assert.assertEquals(pos5, tracker.getCommitPosition()); // should be at 5

    tracker.recordAckedOffset(pos6);
    Assert.assertEquals(pos6, tracker.getCommitPosition()); // should be at 6
    tracker.recordAckedOffset(pos6);                        // double ack on same msg
    Assert.assertEquals(pos6, tracker.getCommitPosition()); // should still be at 6

    tracker.recordAckedOffset(pos7);
    Assert.assertEquals(pos7, tracker.getCommitPosition()); // should be at 7

    tracker.dumpState(System.err);
  }



  private void createTextFile(Path file, int lineCount) throws IOException {
    FSDataOutputStream os = fs.create(file);
    for (int i = 0; i < lineCount; i++) {
      os.writeBytes("line " + i + System.lineSeparator());
    }
    os.close();
  }

}
