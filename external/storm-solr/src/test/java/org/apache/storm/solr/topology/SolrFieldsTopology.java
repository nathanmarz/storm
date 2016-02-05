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

package org.apache.storm.solr.topology;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.solr.bolt.SolrUpdateBolt;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.mapper.SolrFieldsMapper;
import org.apache.storm.solr.mapper.SolrMapper;
import org.apache.storm.solr.schema.builder.RestJsonSchemaBuilder;
import org.apache.storm.solr.spout.SolrFieldsSpout;

import java.io.IOException;

public class SolrFieldsTopology extends SolrTopology {
        public static void main(String[] args) throws Exception {
            SolrFieldsTopology solrFieldsTopology = new SolrFieldsTopology();
            solrFieldsTopology.run(args);
        }

    protected SolrMapper getSolrMapper() throws IOException {
        return new SolrFieldsMapper.Builder(
                new RestJsonSchemaBuilder("localhost", "8983", COLLECTION), COLLECTION)
                    .setMultiValueFieldToken("%").build();
    }

    protected SolrCommitStrategy getSolrCommitStgy() {
        return new CountBasedCommit(2);         // To Commit to Solr and Ack according to the commit strategy
    }

    protected StormTopology getTopology() throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SolrFieldsSpout", new SolrFieldsSpout());
        builder.setBolt("SolrUpdateBolt", new SolrUpdateBolt(getSolrConfig(), getSolrMapper(), getSolrCommitStgy()))
                .shuffleGrouping("SolrFieldsSpout");
        return builder.createTopology();
    }
}
