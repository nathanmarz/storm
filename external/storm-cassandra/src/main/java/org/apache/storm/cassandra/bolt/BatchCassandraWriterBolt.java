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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import com.datastax.driver.core.Statement;
import org.apache.storm.cassandra.executor.AsyncResultHandler;
import org.apache.storm.cassandra.executor.impl.BatchAsyncResultHandler;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BatchCassandraWriterBolt extends BaseCassandraBolt<List<Tuple>> {

    private final static Logger LOG = LoggerFactory.getLogger(BatchCassandraWriterBolt.class);

    public static final int DEFAULT_EMIT_FREQUENCY = 2;

    private LinkedBlockingQueue<Tuple> queue;
    
    private int tickFrequencyInSeconds;
    
    private long lastModifiedTimesMillis;

    private int batchMaxSize = 1000;

    private String componentID;

    private AsyncResultHandler<List<Tuple>> asyncResultHandler;

    /**
     * Creates a new {@link CassandraWriterBolt} instance.
     *
     * @param tupleMapper
     */
    public BatchCassandraWriterBolt(CQLStatementTupleMapper tupleMapper) {
        this(tupleMapper, DEFAULT_EMIT_FREQUENCY);
    }

    /**
     * Creates a new {@link CassandraWriterBolt} instance.
     *
     * @param tupleMapper
     */
    public BatchCassandraWriterBolt(CQLStatementTupleMapper tupleMapper, int tickFrequencyInSeconds) {
        super(tupleMapper);
        this.tickFrequencyInSeconds = tickFrequencyInSeconds;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map stormConfig, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(stormConfig, topologyContext, outputCollector);
        this.componentID = topologyContext.getThisComponentId();
        this.queue = new LinkedBlockingQueue<>(batchMaxSize);
        this.lastModifiedTimesMillis = now();
    }

    @Override
    protected AsyncResultHandler<List<Tuple>> getAsyncHandler() {
        if( asyncResultHandler == null) {
            asyncResultHandler = new BatchAsyncResultHandler(getResultHandler());
        }
        return asyncResultHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void process(Tuple input) {
        if( ! queue.offer(input) ) {
            LOG.info(logPrefix() + "The message queue is full, preparing batch statement...");
            prepareAndExecuteStatement();
            queue.add(input);
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void onTickTuple() {
        prepareAndExecuteStatement();
    }

    public void prepareAndExecuteStatement() {
        int size = queue.size();
        if( size > 0 ) {
            List<Tuple> inputs = new ArrayList<>(size);
            queue.drainTo(inputs);
            try {
                List<PairStatementTuple> psl = buildStatement(inputs);

                int sinceLastModified = updateAndGetSecondsSinceLastModified();
                LOG.debug(logPrefix() + "Execute cql batches with {} statements after {} seconds", size, sinceLastModified);

                checkTimeElapsedSinceLastExec(sinceLastModified);

                GroupingBatchBuilder batchBuilder = new GroupingBatchBuilder(cassandraConfConfig.getBatchSizeRows(), psl);

                int batchSize = 0;
                for (PairBatchStatementTuples batch : batchBuilder) {
                    LOG.debug(logPrefix() + "Writing data to {} in batches of {} rows.", cassandraConfConfig.getKeyspace(), batch.getInputs().size());
                    getAsyncExecutor().execAsync(batch.getStatement(), batch.getInputs());
                    batchSize++;
                }

                int pending = getAsyncExecutor().getPendingTasksSize();
                if (pending > batchSize) {
                    LOG.warn( logPrefix() + "Currently pending tasks is superior to the number of submit batches({}) : {}", batchSize, pending);
                }
                
            } catch (Throwable r) {
                LOG.error(logPrefix() + "Error(s) occurred while preparing batch statements");
                getAsyncHandler().failure(r, inputs);
            }
        }
    }

    private List<PairStatementTuple> buildStatement(List<Tuple> inputs) {
        List<PairStatementTuple> stmts = new ArrayList<>(inputs.size());

        for(Tuple t : inputs) {
            List<Statement> sl = getMapper().map(stormConfig, session, t);
            for(Statement s : sl)
                stmts.add(new PairStatementTuple(t, s) );
        }
        return stmts;
    }

    private void checkTimeElapsedSinceLastExec(int sinceLastModified) {
        if(sinceLastModified > tickFrequencyInSeconds)
            LOG.warn( logPrefix() + "The time elapsed since last execution exceeded tick tuple frequency - {} > {} seconds", sinceLastModified, tickFrequencyInSeconds);
    }

    private String logPrefix() {
        return componentID + " - ";
    }

    public BatchCassandraWriterBolt withTickFrequency(long time, TimeUnit unit) {
        this.tickFrequencyInSeconds = (int)unit.toSeconds(time);
        return this;
    }

    /**
     * Maximum number of tuple kept in memory before inserting batches to cassandra.
     * @param size the max queue size.
     * @return <code>this</code>
     */
    public BatchCassandraWriterBolt withQueueSize(int size) {
        this.batchMaxSize = size;
        return this;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }
    
    private int updateAndGetSecondsSinceLastModified() {
        long now = now();
        int seconds = (int) (now - lastModifiedTimesMillis) / 1000;
        lastModifiedTimesMillis = now;
        return seconds;
    }

    private long now() {
        return Time.currentTimeMillis();
    }
}
