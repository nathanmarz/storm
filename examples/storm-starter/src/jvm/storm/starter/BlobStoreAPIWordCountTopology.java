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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.LocalCluster;
import backtype.storm.blobstore.AtomicOutputStream;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.blobstore.NimbusBlobStore;

import backtype.storm.generated.AccessControl;
import backtype.storm.generated.AccessControlType;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.blobstore.BlobStoreAclHandler;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

public class BlobStoreAPIWordCountTopology {
    private static ClientBlobStore store; // Client API to invoke blob store API functionality
    private static String key = "key";
    private static String fileName = "blacklist.txt";
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreAPIWordCountTopology.class);

    public static void prepare() {
        Config conf = new Config();
        conf.putAll(Utils.readStormConfig());
        store = Utils.getClientBlobStore(conf);
    }

    // Spout implementation
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            _collector.emit(new Values(getRandomSentence()));
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

    }

    // Bolt implementation
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class FilterWords extends BaseBasicBolt {
        String key = "key";
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            try {
                ClientBlobStore store = Utils.getClientBlobStore(Utils.readStormConfig());
                String word = tuple.getString(0);
                Set<String> wordSet = parseBlobContent(getBlobContent(key, store).toString());
                if (!wordSet.contains(word)) {
                    collector.emit(new Values(word));
                }
            } catch (AuthorizationException | KeyNotFoundException | IOException exp) {
                throw new RuntimeException(exp);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public void buildAndLaunchWordCountTopology(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 5);
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("filter", new FilterWords(), 6).shuffleGrouping("split");

        Config conf = new Config();
        conf.setDebug(true);
        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());
                Thread.sleep(10000);
                cluster.shutdown();
            }
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException | InterruptedException exp) {
            throw new RuntimeException(exp);
        }
    }

    // Equivalent create command on command line
    // storm blobstore create --file blacklist.txt --acl o::rwa key
    private static void createBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, File file)
            throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
        // For more on how to set acls while creating the blob please look at distcache-blobstore.md documentation
        String stringBlobACL = "o::rwa";
        AccessControl blobACL = BlobStoreAclHandler.parseAccessControl(stringBlobACL);
        List<AccessControl> acls = new LinkedList<AccessControl>();
        acls.add(blobACL); // more ACLs can be added here
        SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
        AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey,settableBlobMeta);
        blobStream.write(readFile(file).toString().getBytes());
        blobStream.close();
    }

    // Equivalent read command on command line
    // storm blobstore cat --file blacklist.txt key
    private static StringBuilder getBlobContent(String blobKey, ClientBlobStore clientBlobStore)
            throws AuthorizationException, KeyNotFoundException, IOException {
        InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
        BufferedReader br = new BufferedReader(new InputStreamReader(blobInputStream));
        StringBuilder blobContent = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            blobContent.append(line);
            blobContent.append("\n");
        }
        return blobContent;
    }

    // Equivalent update command on command line
    // storm blobstore update --file blacklist.txt key
    private static void updateBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, File file)
            throws KeyNotFoundException, AuthorizationException, IOException {
        AtomicOutputStream blobOutputStream = clientBlobStore.updateBlob(blobKey);
        blobOutputStream.write(readFile(file).toString().getBytes());
        blobOutputStream.close();
    }

    private static String getRandomSentence() {
        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String sentence = sentences[new Random().nextInt(sentences.length)];
        return sentence;
    }

    private static Set<String> getRandomWordSet() {
        Set<String> randomWordSet = new HashSet<>();
        Random random = new Random();
        String[] words = new String[]{ "cow", "jumped", "over", "the", "moon", "apple", "day", "doctor", "away",
                "four", "seven", "ago", "snow", "white", "seven", "dwarfs", "nature", "two" };
        // Choosing atmost 5 words to update the blacklist file for filtering
        for (int i=0; i<5; i++) {
            randomWordSet.add(words[random.nextInt(words.length)]);
        }
        return randomWordSet;
    }

    private static Set<String> parseBlobContent(String content) {
        StringTokenizer tokens = new StringTokenizer(content, "\r\n");
        Set<String> wordSet = new HashSet<>();
        while (tokens.hasMoreElements()) {
            wordSet.add(tokens.nextToken());
        }
        return wordSet;
    }

    private static StringBuilder readFile(File file) throws IOException {
        String line;
        StringBuilder fileContent = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(file.getCanonicalFile()));
        while ((line = br.readLine()) != null) {
            fileContent.append(line);
            fileContent.append("\n");
        }
        return fileContent;
    }

    // Creating a blacklist file to read from the disk
    public static File createFile(String fileName) throws IOException {
        File file = null;
        file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        writeToFile(file, getRandomWordSet());
        LOG.info(readFile(file).toString());
        return file;
    }

    // Updating a blacklist file periodically with random words
    public static File updateFile(File file) throws IOException {
        writeToFile(file, getRandomWordSet());
        return file;
    }

    // Writing random words to be blacklisted
    public static void writeToFile(File file, Set<String> content) throws IOException{
        FileWriter fw = new FileWriter(file.getCanonicalPath(), false);
        BufferedWriter bw = new BufferedWriter(fw);
        Iterator<String> iter = content.iterator();
        while(iter.hasNext()) {
            bw.write(iter.next());
        }
        bw.close();
    }

    public static void main(String[] args) {
        prepare();
        BlobStoreAPIWordCountTopology wc = new BlobStoreAPIWordCountTopology();
        try {
            File file = createFile(fileName);
            // Creating blob again before launching topology
            createBlobWithContent(key, store, file);
            // Launching word count topology with blob updates
            wc.buildAndLaunchWordCountTopology(args);
            // Updating file few times every 5 seconds
            for(int i=0; i<10; i++) {
                updateBlobWithContent(key, store, updateFile(file));
                Utils.sleep(5000);
            }
        } catch (KeyAlreadyExistsException kae) {
            LOG.info("Key already exists {}", kae);
        } catch (AuthorizationException | KeyNotFoundException | IOException exp) {
            throw new RuntimeException(exp);
        }
    }
}


