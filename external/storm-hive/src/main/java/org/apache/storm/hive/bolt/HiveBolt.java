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

package org.apache.storm.hive.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.hive.common.HiveWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hive.hcatalog.streaming.*;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.common.HiveUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;

public class HiveBolt extends  BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);
    private OutputCollector collector;
    private HiveOptions options;
    private Integer currentBatchSize;
    private ExecutorService callTimeoutPool;
    private transient Timer heartBeatTimer;
    private Boolean kerberosEnabled = false;
    private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);
    private UserGroupInformation ugi = null;
    HashMap<HiveEndPoint, HiveWriter> allWriters;

    public HiveBolt(HiveOptions options) {
        this.options = options;
        this.currentBatchSize = 0;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector)  {
        try {
            if(options.getKerberosPrincipal() == null && options.getKerberosKeytab() == null) {
                kerberosEnabled = false;
            } else if(options.getKerberosPrincipal() != null && options.getKerberosKeytab() != null) {
                kerberosEnabled = true;
            } else {
                throw new IllegalArgumentException("To enable Kerberos, need to set both KerberosPrincipal " +
                                                   " & KerberosKeytab");
            }

            if (kerberosEnabled) {
                try {
                    ugi = HiveUtils.authenticate(options.getKerberosKeytab(), options.getKerberosPrincipal());
                } catch(HiveUtils.AuthenticationFailed ex) {
                    LOG.error("Hive Kerberos authentication failed " + ex.getMessage(), ex);
                    throw new IllegalArgumentException(ex);
                }
            }
            this.collector = collector;
            allWriters = new HashMap<HiveEndPoint,HiveWriter>();
            String timeoutName = "hive-bolt-%d";
            this.callTimeoutPool = Executors.newFixedThreadPool(1,
                                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
            heartBeatTimer = new Timer();
            setupHeartBeatTimer();
        } catch(Exception e) {
            LOG.warn("unable to make connection to hive ",e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            List<String> partitionVals = options.getMapper().mapPartitions(tuple);
            HiveEndPoint endPoint = HiveUtils.makeEndPoint(partitionVals, options);
            HiveWriter writer = getOrCreateWriter(endPoint);
            if(timeToSendHeartBeat.compareAndSet(true, false)) {
                enableHeartBeatOnAllWriters();
            }
            writer.write(options.getMapper().mapRecord(tuple));
            currentBatchSize++;
            if(currentBatchSize >= options.getBatchSize()) {
                flushAllWriters();
                currentBatchSize = 0;
            }
            collector.ack(tuple);
        } catch(Exception e) {
            this.collector.reportError(e);
            collector.fail(tuple);
            flushAndCloseWriters();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                HiveWriter w = entry.getValue();
                LOG.info("Flushing writer to {}", w);
                w.flush(false);
                LOG.info("Closing writer to {}", w);
                w.close();
            } catch (Exception ex) {
                LOG.warn("Error while closing writer to " + entry.getKey() +
                         ". Exception follows.", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        ExecutorService toShutdown[] = {callTimeoutPool};
        for (ExecutorService execService : toShutdown) {
            execService.shutdown();
            try {
                while (!execService.isTerminated()) {
                    execService.awaitTermination(
                                 options.getCallTimeOut(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                LOG.warn("shutdown interrupted on " + execService, ex);
            }
        }
        callTimeoutPool = null;
        super.cleanup();
        LOG.info("Hive Bolt stopped");
    }


    private void setupHeartBeatTimer() {
        if(options.getHeartBeatInterval()>0) {
            heartBeatTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        timeToSendHeartBeat.set(true);
                        setupHeartBeatTimer();
                    }
                }, options.getHeartBeatInterval() * 1000);
        }
    }

    private void flushAllWriters()
        throws HiveWriter.CommitFailure, HiveWriter.TxnBatchFailure, HiveWriter.TxnFailure, InterruptedException {
        for(HiveWriter writer: allWriters.values()) {
            writer.flush(true);
        }
    }

    /**
     * Closes all writers and remove them from cache
     * @return number of writers retired
     */
    private void closeAllWriters() {
        try {
            //1) Retire writers
            for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
                entry.getValue().close();
            }
            //2) Clear cache
            allWriters.clear();
        } catch(Exception e) {
            LOG.warn("unable to close writers. ", e);
        }
    }

    private void flushAndCloseWriters() {
        try {
            flushAllWriters();
        } catch(Exception e) {
            LOG.warn("unable to flush hive writers. ", e);
        } finally {
            closeAllWriters();
        }
    }

    private void enableHeartBeatOnAllWriters() {
        for (HiveWriter writer : allWriters.values()) {
            writer.setHeartBeatNeeded();
        }
    }

    private HiveWriter getOrCreateWriter(HiveEndPoint endPoint)
        throws HiveWriter.ConnectFailure, InterruptedException {
        try {
            HiveWriter writer = allWriters.get( endPoint );
            if( writer == null ) {
                LOG.debug("Creating Writer to Hive end point : " + endPoint);
                writer = HiveUtils.makeHiveWriter(endPoint, callTimeoutPool, ugi, options);
                if(allWriters.size() > options.getMaxOpenConnections()){
                    int retired = retireIdleWriters();
                    if(retired==0) {
                        retireEldestWriter();
                    }
                }
                allWriters.put(endPoint, writer);
            }
            return writer;
        } catch (HiveWriter.ConnectFailure e) {
            LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
            throw e;
        }
    }

    /**
     * Locate writer that has not been used for longest time and retire it
     */
    private void retireEldestWriter() {
        long oldestTimeStamp = System.currentTimeMillis();
        HiveEndPoint eldest = null;
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if(entry.getValue().getLastUsed() < oldestTimeStamp) {
                eldest = entry.getKey();
                oldestTimeStamp = entry.getValue().getLastUsed();
            }
        }
        try {
            LOG.info("Closing least used Writer to Hive end point : " + eldest);
            allWriters.remove(eldest).close();
        } catch (IOException e) {
            LOG.warn("Failed to close writer for end point: " + eldest, e);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Locate all writers past idle timeout and retire them
     * @return number of writers retired
     */
    private int retireIdleWriters() {
        int count = 0;
        long now = System.currentTimeMillis();
        ArrayList<HiveEndPoint> retirees = new ArrayList<HiveEndPoint>();

        //1) Find retirement candidates
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if(now - entry.getValue().getLastUsed() > options.getIdleTimeout()) {
                ++count;
                retirees.add(entry.getKey());
            }
        }
        //2) Retire them
        for(HiveEndPoint ep : retirees) {
            try {
                LOG.info("Closing idle Writer to Hive end point : {}", ep);
                allWriters.remove(ep).close();
            } catch (IOException e) {
                LOG.warn("Failed to close writer for end point: {}. Error: "+ ep, e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when attempting to close writer for end point: " + ep, e);
                Thread.currentThread().interrupt();
            }
        }
        return count;
    }

}
