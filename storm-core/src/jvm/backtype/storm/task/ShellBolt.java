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
package backtype.storm.task;

import backtype.storm.Config;
import backtype.storm.generated.ShellComponent;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.rpc.IShellMetric;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.ShellProcess;
import backtype.storm.multilang.BoltMsg;
import backtype.storm.multilang.ShellMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that shells out to another process to process tuples. ShellBolt
 * communicates with that process over stdio using a special protocol. An ~100
 * line library is required to implement that protocol, and adapter libraries
 * currently exist for Ruby and Python.
 *
 * <p>To run a ShellBolt on a cluster, the scripts that are shelled out to must be
 * in the resources directory within the jar submitted to the master.
 * During development/testing on a local machine, that resources directory just
 * needs to be on the classpath.</p>
 *
 * <p>When creating topologies using the Java API, subclass this bolt and implement
 * the IRichBolt interface to create components for the topology that use other languages. For example:
 * </p>
 *
 * <pre>
 * public class MyBolt extends ShellBolt implements IRichBolt {
 *      public MyBolt() {
 *          super("python", "mybolt.py");
 *      }
 *
 *      public void declareOutputFields(OutputFieldsDeclarer declarer) {
 *          declarer.declare(new Fields("field1", "field2"));
 *      }
 * }
 * </pre>
 */
public class ShellBolt implements IBolt {
    public static Logger LOG = LoggerFactory.getLogger(ShellBolt.class);
    Process _subprocess;
    OutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<String, Tuple>();

    private String[] _command;
    private ShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private LinkedBlockingQueue _pendingWrites = new LinkedBlockingQueue();
    private Random _rand;

    private Thread _readerThread;
    private Thread _writerThread;
    
    private TopologyContext _context;

    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        _command = command;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        final OutputCollector collector) {
        Object maxPending = stormConf.get(Config.TOPOLOGY_SHELLBOLT_MAX_PENDING);
        if (maxPending != null) {
           this._pendingWrites = new LinkedBlockingQueue(((Number)maxPending).intValue());
        }
        _rand = new Random();
        _collector = collector;

        _context = context;

        _process = new ShellProcess(_command);

        //subprocesses must send their pid first thing
        Number subpid = _process.launch(stormConf, context);
        LOG.info("Launched subprocess with pid " + subpid);

        // reader
        _readerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        ShellMsg shellMsg = _process.readShellMsg();

                        String command = shellMsg.getCommand();
                        if(command.equals("ack")) {
                            handleAck(shellMsg.getId());
                        } else if (command.equals("fail")) {
                            handleFail(shellMsg.getId());
                        } else if (command.equals("error")) {
                            handleError(shellMsg.getMsg());
                        } else if (command.equals("log")) {
                            handleLog(shellMsg);
                        } else if (command.equals("emit")) {
                            handleEmit(shellMsg);
                        } else if (command.equals("metrics")) {
                            handleMetrics(shellMsg);
                        }
                    } catch (InterruptedException e) {
                    } catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });

        _readerThread.start();

        _writerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        Object write = _pendingWrites.poll(1, SECONDS);
                        if (write instanceof BoltMsg) {
                            _process.writeBoltMsg((BoltMsg)write);
                        } else if (write instanceof List<?>) {
                            _process.writeTaskIds((List<Integer>)write);
                        } else if (write != null) {
                            throw new RuntimeException("Unknown class type to write: " + write.getClass().getName());
                        }
                    } catch (InterruptedException e) {
                    } catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });

        _writerThread.start();
    }

    public void execute(Tuple input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        //just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        try {
            BoltMsg boltMsg = new BoltMsg();
            boltMsg.setId(genId);
            boltMsg.setComp(input.getSourceComponent());
            boltMsg.setStream(input.getSourceStreamId());
            boltMsg.setTask(input.getSourceTask());
            boltMsg.setTuple(input.getValues());

            _pendingWrites.put(boltMsg);
        } catch(InterruptedException e) {
            String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
            throw new RuntimeException("Error during multilang processing " + processInfo, e);
        }
    }

    public void cleanup() {
        _running = false;
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
        List<Tuple> anchors = new ArrayList<Tuple>();
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
                _pendingWrites.put(outtasks);
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

    private void die(Throwable exception) {
        String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
        _exception = new RuntimeException(processInfo, exception);
    }

}
