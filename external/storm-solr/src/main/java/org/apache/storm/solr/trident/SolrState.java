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

import org.apache.storm.topology.FailedException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

public class SolrState implements State {
    private static final Logger logger = LoggerFactory.getLogger(SolrState.class);

    private final SolrConfig solrConfig;
    private final SolrMapper solrMapper;
    private SolrClient solrClient;

    public SolrState(SolrConfig solrConfig, SolrMapper solrMapper) {
        this.solrConfig = solrConfig;
        this.solrMapper = solrMapper;
    }

    protected void prepare() {
        solrClient = new CloudSolrClient(solrConfig.getZkHostString());
    }

    @Override
    public void beginCommit(Long aLong){ }

    @Override
    public void commit(Long aLong) { }

    public void updateState(List<TridentTuple> tuples) {
        try {
            SolrRequest solrRequest = solrMapper.toSolrRequest(tuples);
            solrClient.request(solrRequest, solrMapper.getCollection());
            solrClient.commit(solrMapper.getCollection());
        } catch (Exception e) {
            final String msg = String.format("%s", tuples);
            logger.warn(msg);
            throw new FailedException(msg, e);
        }
    }
}
