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

package org.apache.storm.solr.trident;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.solr.spout.SolrJsonSpout;
import org.apache.storm.solr.topology.SolrJsonTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;

import java.io.IOException;

public class SolrJsonTridentTopology extends SolrJsonTopology {
    public static void main(String[] args) throws Exception {
        SolrJsonTridentTopology solrJsonTridentTopology = new SolrJsonTridentTopology();
        solrJsonTridentTopology.run(args);
    }

    protected StormTopology getTopology() throws IOException {
        final TridentTopology topology = new TridentTopology();
        final SolrJsonSpout spout = new SolrJsonSpout();
        final Stream stream = topology.newStream("SolrJsonSpout", spout);
        final StateFactory solrStateFactory = new SolrStateFactory(getSolrConfig(), getSolrMapper());
        stream.partitionPersist(solrStateFactory, spout.getOutputFields(),  new SolrUpdater(), new Fields());
        return topology.build();
    }
}
