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
package org.apache.storm.task;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.rpc.IShellMetric;
import org.apache.storm.multilang.BoltMsg;
import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ShellBoltMessageQueue;
import org.apache.storm.utils.ShellProcess;
import clojure.lang.RT;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A bolt that shells out to another process to process tuples. ShellBolt
 * communicates with that process over stdio using a special protocol. An ~100
 * line library is required to implement that protocol, and adapter libraries
 * currently exist for Ruby and Python.
 *
 * To run a ShellBolt on a cluster, the scripts that are shelled out to must be
 * in the resources directory within the jar submitted to the master.
 * During development/testing on a local machine, that resources directory just
 * needs to be on the classpath.
 *
 * When creating topologies using the Java API, subclass this bolt and implement
 * the IRichBolt interface to create components for the topology that use other languages. For example:
 *
 *
 * ```java
 * public class MyBolt extends ShellBolt implements IRichBolt {
 *      public MyBolt() {
 *          super("python", "mybolt.py");
 *      }
 *
 *      public void declareOutputFields(OutputFieldsDeclarer declarer) {
 *          declarer.declare(new Fields("field1", "field2"));
 *      }
 * }
 * ```
 */
public class ShellBolt implements IBolt {
    public static final String HEARTBEAT_STREAM_ID = "__heartbeat";
    public static final Logger LOG = LoggerFactory.getLogger(ShellBolt.class);
    OutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<>();

    private String[] _command;
    private Map<String, String> env = new HashMap<>();
    private ShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private ShellBoltMessageQueue _pendingWrites = new ShellBoltMessageQueue();
    private Random _rand;

    private Thread _readerThread;
    private Thread _writerThread;
    
    private TopologyContext _context;

    private int workerTimeoutMills;
    private ScheduledExecutorService heartBeatExecutorService;
    private AtomicLong lastHeartbeatTimestamp = new AtomicLong();
    private AtomicBoolean sendHeartbeatFlag = new AtomicBoolean(false);

    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        _command = command;
    }

    public ShellBolt setEnv(Map<String, String> env) {
        this.env = env;
        return this;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        final OutputCollector collector) {
        Object maxPending = stormConf.get(Config.TOPOLOGY_SHELLBOLT_MAX_PENDING);
        if (maxPending != null) {
            this._pendingWrites = new ShellBoltMessageQueue(((Number)maxPending).intValue());
        }

        _rand = new Random();
        _collector = collector;

        _context = context;

        if (stormConf.containsKey(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS)) {
            workerTimeoutMills = 1000 * RT.intCast(stormConf.get(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS));
        } else {
            workerTimeoutMills = 1000 * RT.intCast(stormConf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
        }

        _process = new ShellProcess(_command);
        if (!env.isEmpty()) {
            _process.setEnv(env);
        }

        //subprocesses must send their pid first thing
        Number subpid = _process.launch(stormConf, context);
        LOG.info("Launched subprocess with pid " + subpid);

        // reader
        _readerThread = new Thread(new BoltReaderRunnable());
        _readerThread.start();

        _writerThread = new Thread(new BoltWriterRunnable());
        _writerThread.start();

        heartBeatExecutorService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
        heartBeatExecutorService.scheduleAtFixedRate(new BoltHeartbeatTimerTask(this), 1, 1, TimeUnit.SECONDS);

        LOG.info("Start checking heartbeat...");
        setHeartbeat();
    }

    public void execute(Tuple input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        //just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        try {
            BoltMsg boltMsg = createBoltMessage(input, genId);

            _pendingWrites.putBoltMsg(boltMsg);
        } catch(InterruptedException e) {
            String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
            throw new RuntimeException("Error during multilang processing " + processInfo, e);
        }
    }

    private BoltMsg createBoltMessage(Tuple input, String genId) {
        BoltMsg boltMsg = new BoltMsg();
        boltMsg.setId(genId);
        boltMsg.setComp(input.getSourceComponent());
        boltMsg.setStream(input.getSourceStreamId());
        boltMsg.setTask(input.getSourceTask());
        boltMsg.setTuple(input.getValues());
        return boltMsg;
    }

    public void cleanup() {
        _running = false;
        heartBeatExecutorService.shutdownNow();
        _writerThread.interrupt();
        _readerThread.interrupt();
        _process.destroy();
        _inputs.clear();
    }

    private void handleAck(Object id) {
        Tuple acked = _inputs.remove(id);
        if(acked==null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(Object id) {
        Tuple failed = _inputs.remove(id);
        if(failed==null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleError(String msg) {
        _collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    private void handleEmit(ShellMsg shellMsg) throws InterruptedException {
        List<Tuple> anchors = new ArrayList<>();
        List<String> recvAnchors = shellMsg.getAnchors();
        if (recvAnchors != null) {
            for (String anchor : recvAnchors) {
                Tuple t = _inputs.get(anchor);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + anchor + " after ack/fail");
                }
                anchors.add(t);
            }
        }

        if(shellMsg.getTask() == 0) {
            List<Integer> outtasks = _collector.emit(shellMsg.getStream(), anchors, shellMsg.getTuple());
            if (shellMsg.areTaskIdsNeeded()) {
                _pendingWrites.putTaskIds(outtasks);
            }
        } else {
            _collector.emitDirect((int) shellMsg.getTask(),
                    shellMsg.getStream(), anchors, shellMsg.getTuple());
        }
    }

    private void handleLog(ShellMsg shellMsg) {
        String msg = shellMsg.getMsg();
        msg = "ShellLog " + _process.getProcessInfoString() + " " + msg;
        ShellMsg.ShellLogLevel logLevel = shellMsg.getLogLevel();

        switch (logLevel) {
            case TRACE:
                LOG.trace(msg);
                break;
            case DEBUG:
                LOG.debug(msg);
                break;
            case INFO:
                LOG.info(msg);
                break;
            case WARN:
                LOG.warn(msg);
                break;
            case ERROR:
                LOG.error(msg);
                _collector.reportError(new ReportedFailedException(msg));
                break;
            default:
                LOG.info(msg);
                break;
        }
    }

    private void handleMetrics(ShellMsg shellMsg) {
        //get metric name
        String name = shellMsg.getMetricName();
        if (name.isEmpty()) {
            throw new RuntimeException("Receive Metrics name is empty");
        }
        
        //get metric by name
        IMetric iMetric = _context.getRegisteredMetricByName(name);
        if (iMetric == null) {
            throw new RuntimeException("Could not find metric by name["+name+"] ");
        }
        if ( !(iMetric instanceof IShellMetric)) {
            throw new RuntimeException("Metric["+name+"] is not IShellMetric, can not call by RPC");
        }
        IShellMetric iShellMetric = (IShellMetric)iMetric;
        
        //call updateMetricFromRPC with params
        Object paramsObj = shellMsg.getMetricParams();
        try {
            iShellMetric.updateMetricFromRPC(paramsObj);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }       
    }

    private void setHeartbeat() {
        lastHeartbeatTimestamp.set(System.currentTimeMillis());
    }

    private long getLastHeartbeat() {
        return lastHeartbeatTimestamp.get();
    }

    private void die(Throwable exception) {
        String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
        _exception = new RuntimeException(processInfo, exception);
        String message = String.format("Halting process: ShellBolt died. Command: %s, ProcessInfo %s",
                Arrays.toString(_command),
                processInfo);
        LOG.error(message, exception);
        _collector.reportError(exception);
        if (_running || (exception instanceof Error)) { //don't exit if not running, unless it is an Error
            System.exit(11);
        }
    }

    private class BoltHeartbeatTimerTask extends TimerTask {
        private ShellBolt bolt;

        public BoltHeartbeatTimerTask(ShellBolt bolt) {
            this.bolt = bolt;
        }

        @Override
        public void run() {
            long currentTimeMillis = System.currentTimeMillis();
            long lastHeartbeat = getLastHeartbeat();

            LOG.debug("BOLT - current time : {}, last heartbeat : {}, worker timeout (ms) : {}",
                    currentTimeMillis, lastHeartbeat, workerTimeoutMills);

            if (currentTimeMillis - lastHeartbeat > workerTimeoutMills) {
                bolt.die(new RuntimeException("subprocess heartbeat timeout"));
            }

            sendHeartbeatFlag.compareAndSet(false, true);
        }
    }

    private class BoltReaderRunnable implements Runnable {
        public void run() {
            while (_running) {
                try {
                    ShellMsg shellMsg = _process.readShellMsg();

                    String command = shellMsg.getCommand();
                    if (command == null) {
                        throw new IllegalArgumentException("Command not found in bolt message: " + shellMsg);
                    }

                    setHeartbeat();

                    // We don't need to take care of sync, cause we're always updating heartbeat
                    switch (command) {
                        case "ack":
                            handleAck(shellMsg.getId());
                            break;
                        case "fail":
                            handleFail(shellMsg.getId());
                            break;
                        case "error":
                            handleError(shellMsg.getMsg());
                            break;
                        case "log":
                            handleLog(shellMsg);
                            break;
                        case "emit":
                            handleEmit(shellMsg);
                            break;
                        case "metrics":
                            handleMetrics(shellMsg);
                            break;
                    }
                } catch (InterruptedException e) {
                } catch (Throwable t) {
                    die(t);
                }
            }
        }
    }

    private class BoltWriterRunnable implements Runnable {
        public void run() {
            while (_running) {
                try {
                    if (sendHeartbeatFlag.get()) {
                        LOG.debug("BOLT - sending heartbeat request to subprocess");

                        String genId = Long.toString(_rand.nextLong());
                        _process.writeBoltMsg(createHeartbeatBoltMessage(genId));
                        sendHeartbeatFlag.compareAndSet(true, false);
                    }

                    Object write = _pendingWrites.poll(1, SECONDS);
                    if (write instanceof BoltMsg) {
                        _process.writeBoltMsg((BoltMsg) write);
                    } else if (write instanceof List<?>) {
                        _process.writeTaskIds((List<Integer>)write);
                    } else if (write != null) {
                        throw new RuntimeException("Unknown class type to write: " + write.getClass().getName());
                    }
                } catch (Throwable t) {
                    die(t);
                }
            }
        }

        private BoltMsg createHeartbeatBoltMessage(String genId) {
            BoltMsg msg = new BoltMsg();
            msg.setId(genId);
            msg.setTask(Constants.SYSTEM_TASK_ID);
            msg.setStream(HEARTBEAT_STREAM_ID);
            msg.setTuple(new ArrayList<>());
            return msg;
        }
    }
}
