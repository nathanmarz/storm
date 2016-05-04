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
package org.apache.storm.trident.spout;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.trident.topology.state.RotatingTransactionalState;
import org.apache.storm.trident.topology.state.TransactionalState;


public class TridentSpoutCoordinator implements IBasicBolt {
    public static final Logger LOG = LoggerFactory.getLogger(TridentSpoutCoordinator.class);
    private static final String META_DIR = "meta";

    ITridentSpout<Object> _spout;
    ITridentSpout.BatchCoordinator<Object> _coord;
    RotatingTransactionalState _state;
    TransactionalState _underlyingState;
    String _id;

    
    public TridentSpoutCoordinator(String id, ITridentSpout<Object> spout) {
        _spout = spout;
        _id = id;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        _coord = _spout.getCoordinator(_id, conf, context);
        _underlyingState = TransactionalState.newCoordinatorState(conf, _id);
        _state = new RotatingTransactionalState(_underlyingState, META_DIR);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        TransactionAttempt attempt = (TransactionAttempt) tuple.getValue(0);

        if(tuple.getSourceStreamId().equals(MasterBatchCoordinator.SUCCESS_STREAM_ID)) {
            _state.cleanupBefore(attempt.getTransactionId());
            _coord.success(attempt.getTransactionId());
        } else {
            long txid = attempt.getTransactionId();
            Object prevMeta = _state.getPreviousState(txid);
            Object meta = _coord.initializeTransaction(txid, prevMeta, _state.getState(txid));
            _state.overrideState(txid, meta);
            collector.emit(MasterBatchCoordinator.BATCH_STREAM_ID, new Values(attempt, meta));
        }
                
    }

    @Override
    public void cleanup() {
        _coord.close();
        _underlyingState.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(MasterBatchCoordinator.BATCH_STREAM_ID, new Fields("tx", "metadata"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }   
}
