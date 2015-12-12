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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BlobStoreAPIWordCountTopology {
    private static NimbusBlobStore store = new NimbusBlobStore(); // Client API to invoke blob store API functionality
    private static String key = "key1";
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreAPIWordCountTopology.class);
    private static final List<AccessControl> WORLD_EVERYTHING = Arrays.asList(new AccessControl(AccessControlType.OTHER,
            BlobStoreAclHandler.READ | BlobStoreAclHandler.WRITE | BlobStoreAclHandler.ADMIN));

    // Spout implementation
    public static class BlobStoreSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        BlobStoreAPIWordCountTopology wc;
        String key;
        NimbusBlobStore store;


        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            wc = new BlobStoreAPIWordCountTopology();
            key = "key1";
            store = new NimbusBlobStore();
            store.prepare(Utils.readStormConfig());
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            try {
                 _collector.emit(new Values(wc.getBlobContent(key, store)));
            } catch (AuthorizationException | KeyNotFoundException | IOException exp) {
                throw new RuntimeException(exp);
            }
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

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public void buildAndLaunchWordCountTopology(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new BlobStoreSpout(), 5);

        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

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

    private static void createBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, SettableBlobMeta settableBlobMeta)
            throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
        AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey,settableBlobMeta);
        blobStream.write(getRandomSentence().getBytes());
        blobStream.close();
    }

    private static String getBlobContent(String blobKey, ClientBlobStore clientBlobStore)
            throws AuthorizationException, KeyNotFoundException, IOException {
        InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
        BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
        return r.readLine();
    }

    private static void updateBlobWithContent(String blobKey, ClientBlobStore clientBlobStore)
            throws KeyNotFoundException, AuthorizationException, IOException {
        AtomicOutputStream blobOutputStream = clientBlobStore.updateBlob(blobKey);
        blobOutputStream.write(getRandomSentence().getBytes());
        blobOutputStream.close();
    }

    private static String getRandomSentence() {
        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String sentence = sentences[new Random().nextInt(sentences.length)];
        return sentence;
    }

    private void prepareAndPerformBasicOperations()
            throws KeyAlreadyExistsException, AuthorizationException, KeyNotFoundException, IOException{
        Map conf = Utils.readStormConfig();
        store.prepare(conf);
        // create blob with content
        createBlobWithContent(key, store, new SettableBlobMeta(WORLD_EVERYTHING));
        // read blob content
        LOG.info(getBlobContent(key, store));
        // update blob
        updateBlobWithContent(key, store);
        LOG.info(getBlobContent(key, store));
        // delete blob through API
        store.deleteBlob(key);
    }

    public static void main(String[] args) {
        // Basic blob store API calls create, read, update and delete
        BlobStoreAPIWordCountTopology wc = new BlobStoreAPIWordCountTopology();
        try {
            wc.prepareAndPerformBasicOperations();
            // Creating blob again before launching topology
            createBlobWithContent(key, store, new SettableBlobMeta(WORLD_EVERYTHING));
            LOG.info(getBlobContent(key, store));
            // Launching word count topology with blob updates
            wc.buildAndLaunchWordCountTopology(args);
            // Updating blobs few times
            for (int i = 0; i < 10; i++) {
                updateBlobWithContent(key, store);
                Utils.sleep(100);
            }
        } catch (KeyAlreadyExistsException kae) {
            LOG.info("Key already exists {}", kae);
        } catch (AuthorizationException | KeyNotFoundException | IOException exp) {
            throw new RuntimeException(exp);
        }
    }
}


