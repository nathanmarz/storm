/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.hdfs.common.HdfsUtils.Pair;


public class TestHdfsSpout {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  public File baseFolder;
  private Path source;
  private Path archive;
  private Path badfiles;


  public TestHdfsSpout() {
  }

  static MiniDFSCluster.Builder builder;
  static MiniDFSCluster hdfsCluster;
  static FileSystem fs;
  static String hdfsURI;
  static Configuration conf = new Configuration();

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
  public void setup() throws Exception {
    baseFolder = tempFolder.newFolder("hdfsspout");
    source = new Path(baseFolder.toString() + "/source");
    fs.mkdirs(source);
    archive = new Path(baseFolder.toString() + "/archive");
    fs.mkdirs(archive);
    badfiles = new Path(baseFolder.toString() + "/bad");
    fs.mkdirs(badfiles);

  }

  @After
  public void shutDown() throws IOException {
    fs.delete(new Path(baseFolder.toString()),true);
  }

  @Test
  public void testSimpleText() throws IOException {
    Path file1 = new Path(source.toString() + "/file1.txt");
    createTextFile(file1, 5);

    Path file2 = new Path(source.toString() + "/file2.txt");
    createTextFile(file2, 5);

    listDir(source);

    Map conf = getDefaultConfig();
    conf.put(Configs.COMMIT_FREQ_COUNT, "1");
    conf.put(Configs.COMMIT_FREQ_SEC, "1");
    HdfsSpout spout = makeSpout(0, conf, Configs.TEXT);

    List<String> res = runSpout(spout,"r11", "a0", "a1", "a2", "a3", "a4");
    for (String re : res) {
      System.err.println(re);
    }

    listCompletedDir();
    Path arc1 = new Path(archive.toString() + "/file1.txt");
    Path arc2 = new Path(archive.toString() + "/file2.txt");
    checkCollectorOutput_txt((MockCollector) spout.getCollector(), arc1, arc2);
  }


  private void checkCollectorOutput_txt(MockCollector collector, Path... txtFiles) throws IOException {
    ArrayList<String> expected = new ArrayList<>();
    for (Path txtFile : txtFiles) {
      List<String> lines= getTextFileContents(fs, txtFile);
      expected.addAll(lines);
    }

    List<String> actual = new ArrayList<>();
    for (Pair<HdfsSpout.MessageId, List<Object>> item : collector.items) {
      actual.add(item.getValue().get(0).toString());
    }
    Assert.assertEquals(expected, actual);
  }

  private List<String> getTextFileContents(FileSystem fs, Path txtFile) throws IOException {
    ArrayList<String> result = new ArrayList<>();
    FSDataInputStream istream = fs.open(txtFile);
    InputStreamReader isreader = new InputStreamReader(istream,"UTF-8");
    BufferedReader reader = new BufferedReader(isreader);

    for( String line = reader.readLine(); line!=null; line = reader.readLine() ) {
      result.add(line);
    }
    isreader.close();
    return result;
  }

  private void checkCollectorOutput_seq(MockCollector collector, Path... seqFiles) throws IOException {
    ArrayList<String> expected = new ArrayList<>();
    for (Path seqFile : seqFiles) {
      List<String> lines= getSeqFileContents(fs, seqFile);
      expected.addAll(lines);
    }
    Assert.assertTrue(expected.equals(collector.lines));
  }

  private List<String> getSeqFileContents(FileSystem fs, Path... seqFiles) throws IOException {
    ArrayList<String> result = new ArrayList<>();

    for (Path seqFile : seqFiles) {
      Path file = new Path(fs.getUri().toString() + seqFile.toString());
      SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
      try {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        while (reader.next(key, value)) {
          String keyValStr = Arrays.asList(key, value).toString();
          result.add(keyValStr);
        }
      } finally {
        reader.close();
      }
    }// for
    return result;
  }

  private void listCompletedDir() throws IOException {
    listDir(source);
    listDir(archive);
  }

  private List<String> listBadDir() throws IOException {
    return listDir(badfiles);
  }

  private List<String> listDir(Path p) throws IOException {
    ArrayList<String> result = new ArrayList<>();
    System.err.println("*** Listing " + p);
    RemoteIterator<LocatedFileStatus> fileNames =  fs.listFiles(p, false);
    while ( fileNames.hasNext() ) {
      LocatedFileStatus fileStatus = fileNames.next();
      System.err.println(fileStatus.getPath());
      result.add(fileStatus.getPath().toString());
    }
    return result;
  }


  @Test
  public void testSimpleSequenceFile() throws IOException {

    source = new Path("/tmp/hdfsspout/source");
    fs.mkdirs(source);
    archive = new Path("/tmp/hdfsspout/archive");
    fs.mkdirs(archive);

    Path file1 = new Path(source + "/file1.seq");
    createSeqFile(fs, file1);

    Path file2 = new Path(source + "/file2.seq");
    createSeqFile(fs, file2);

    Map conf = getDefaultConfig();
    HdfsSpout spout = makeSpout(0, conf, Configs.SEQ);

    List<String> res = runSpout(spout, "r11", "a0", "a1", "a2", "a3", "a4");
    for (String re : res) {
      System.err.println(re);
    }

    listDir(archive);


    Path f1 = new Path(archive + "/file1.seq");
    Path f2 = new Path(archive + "/file2.seq");

    checkCollectorOutput_seq((MockCollector) spout.getCollector(), f1, f2);
  }

// - TODO: this test needs the spout to fail with an exception
  @Test
  public void testFailure() throws Exception {

    Path file1 = new Path(source.toString() + "/file1.txt");
    createTextFile(file1, 5);

    listDir(source);

    Map conf = getDefaultConfig();
//    conf.put(HdfsSpout.Configs.BACKOFF_SEC, "2");
    HdfsSpout spout = makeSpout(0, conf, MockTextFailingReader.class.getName());
    List<String> res = runSpout(spout, "r3");
    for (String re : res) {
      System.err.println(re);
    }

    listCompletedDir();
    List<String> badFiles = listBadDir();
    Assert.assertEquals( badFiles.size(), 1);
    Assert.assertEquals(((MockCollector) spout.getCollector()).lines.size(), 1);
  }

  // @Test
  public void testLocking() throws Exception {
    Path file1 = new Path(source.toString() + "/file1.txt");
    createTextFile(file1, 5);

    listDir(source);

    Map conf = getDefaultConfig();
    conf.put(Configs.COMMIT_FREQ_COUNT, "1");
    conf.put(Configs.COMMIT_FREQ_SEC, "1");
    HdfsSpout spout = makeSpout(0, conf, Configs.TEXT);
    List<String> res = runSpout(spout,"r4");
    for (String re : res) {
      System.err.println(re);
    }
    List<String> lockFiles = listDir(spout.getLockDirPath());
    Assert.assertEquals(1, lockFiles.size());
    runSpout(spout, "r3");
    List<String> lines = readTextFile(fs, lockFiles.get(0));
    System.err.println(lines);
    Assert.assertEquals(6, lines.size());
  }

  private static List<String> readTextFile(FileSystem fs, String f) throws IOException {
    Path file = new Path(f);
    FSDataInputStream x = fs.open(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(x));
    String line = null;
    ArrayList<String> result = new ArrayList<>();
    while( (line = reader.readLine()) !=null )
      result.add( line );
    return result;
  }

  private Map getDefaultConfig() {
    Map conf = new HashMap();
    conf.put(Configs.SOURCE_DIR, source.toString());
    conf.put(Configs.ARCHIVE_DIR, archive.toString());
    conf.put(Configs.BAD_DIR, badfiles.toString());
    conf.put("filesystem", fs);
    return conf;
  }


  private static HdfsSpout makeSpout(int spoutId, Map conf, String readerType) {
    HdfsSpout spout = new HdfsSpout();
    MockCollector collector = new MockCollector();
    conf.put(Configs.READER_TYPE, readerType);
    spout.open(conf, new MockTopologyContext(spoutId), collector);
    return spout;
  }

  /**
   * Execute a sequence of calls to EventHubSpout.
   *
   * @param cmds: set of commands to run,
   * e.g. "r,r,r,r,a1,f2,...". The commands are:
   * r[N] -  receive() called N times
   * aN - ack, item number: N
   * fN - fail, item number: N
   */

  private List<String> runSpout(HdfsSpout spout,  String...  cmds) {
    MockCollector collector = (MockCollector) spout.getCollector();
      for(String cmd : cmds) {
        if(cmd.startsWith("r")) {
          int count = 1;
          if(cmd.length() > 1) {
            count = Integer.parseInt(cmd.substring(1));
          }
          for(int i=0; i<count; ++i) {
            spout.nextTuple();
          }
        }
        else if(cmd.startsWith("a")) {
          int n = Integer.parseInt(cmd.substring(1));
          Pair<HdfsSpout.MessageId, List<Object>> item = collector.items.get(n);
          spout.ack(item.getKey());
        }
        else if(cmd.startsWith("f")) {
          int n = Integer.parseInt(cmd.substring(1));
          Pair<HdfsSpout.MessageId, List<Object>> item = collector.items.get(n);
          spout.fail(item.getKey());
        }
      }
      return collector.lines;
    }

  private void createTextFile(Path file, int lineCount) throws IOException {
    FSDataOutputStream os = fs.create(file);
    for (int i = 0; i < lineCount; i++) {
      os.writeBytes("line " + i + System.lineSeparator());
    }
    os.close();
  }



  private static void createSeqFile(FileSystem fs, Path file) throws IOException {

    Configuration conf = new Configuration();
    try {
      if(fs.exists(file)) {
        fs.delete(file, false);
      }

      SequenceFile.Writer w = SequenceFile.createWriter(fs, conf, file, IntWritable.class, Text.class );
      for (int i = 0; i < 5; i++) {
        w.append(new IntWritable(i), new Text("line " + i));
      }
      w.close();
      System.out.println("done");
    } catch (IOException e) {
      e.printStackTrace();

    }
  }



  static class MockCollector extends SpoutOutputCollector {
    //comma separated offsets
    public ArrayList<String> lines;
    public ArrayList<Pair<HdfsSpout.MessageId, List<Object> > > items;

    public MockCollector() {
      super(null);
      lines = new ArrayList<>();
      items = new ArrayList<>();
    }



    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
//      HdfsSpout.MessageId id = (HdfsSpout.MessageId) messageId;
//      lines.add(id.toString() + ' ' + tuple.toString());
      lines.add(tuple.toString());
      items.add(HdfsUtils.Pair.of(messageId, tuple));
      return null;
    }

    @Override
    public void emitDirect(int arg0, String arg1, List<Object> arg2, Object arg3) {
      throw new NotImplementedException();
    }

    @Override
    public void reportError(Throwable arg0) {
      throw new NotImplementedException();
    }

    @Override
    public long getPendingCount() {
      return 0;
    }
  } // class MockCollector



  // Throws exceptions for 2nd and 3rd line read attempt
  static class MockTextFailingReader extends TextFileReader {
    int readAttempts = 0;

    public MockTextFailingReader(FileSystem fs, Path file, Map conf) throws IOException {
      super(fs, file, conf);
    }

    @Override
    public List<Object> next() throws IOException, ParseException {
      readAttempts++;
      if (readAttempts == 2) {
        throw new IOException("mock test exception");
      } else if (readAttempts >= 3) {
        throw new ParseException("mock test exception", null);
      }
      return super.next();
    }
  }

  static class MockTopologyContext extends TopologyContext {
    private final int componentId;

    public MockTopologyContext(int componentId) {
      // StormTopology topology, Map stormConf, Map<Integer, String> taskToComponent, Map<String, List<Integer>> componentToSortedTasks, Map<String, Map<String, Fields>> componentToStreamToFields, String stormId, String codeDir, String pidDir, Integer taskId, Integer workerPort, List<Integer> workerTasks, Map<String, Object> defaultResources, Map<String, Object> userResources, Map<String, Object> executorData, Map<Integer, Map<Integer, Map<String, IMetric>>> registeredMetrics, Atom openOrPrepareWasCalled
      super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
      this.componentId = componentId;
    }

    public String getThisComponentId() {
      return Integer.toString( componentId );
    }

  }

}
