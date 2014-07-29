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
package backtype.storm.spout;

import backtype.storm.generated.ShellComponent;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.rpc.IShellMetric;
import backtype.storm.multilang.ShellMsg;
import backtype.storm.multilang.SpoutMsg;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.ShellProcess;
import java.util.Map;
import java.util.List;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ShellSpout implements ISpout {
    public static Logger LOG = LoggerFactory.getLogger(ShellSpout.class);

    private SpoutOutputCollector _collector;
    private String[] _command;
    private ShellProcess _process;
    
    private TopologyContext _context;
    
    private SpoutMsg _spoutMsg;

    public ShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellSpout(String... command) {
        _command = command;
    }

    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        _context = context;

        _process = new ShellProcess(_command);

        Number subpid = _process.launch(stormConf, context);
        LOG.info("Launched subprocess with pid " + subpid);
    }

    public void close() {
        _process.destroy();
    }

    public void nextTuple() {
        if (_spoutMsg == null) {
            _spoutMsg = new SpoutMsg();
        }
        _spoutMsg.setCommand("next");
        _spoutMsg.setId("");
        querySubprocess();
    }

    public void ack(Object msgId) {
        if (_spoutMsg == null) {
            _spoutMsg = new SpoutMsg();
        }
        _spoutMsg.setCommand("ack");
        _spoutMsg.setId(msgId);
        querySubprocess();
    }

    public void fail(Object msgId) {
        if (_spoutMsg == null) {
            _spoutMsg = new SpoutMsg();
        }
        _spoutMsg.setCommand("fail");
        _spoutMsg.setId(msgId);
        querySubprocess();
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

    private void querySubprocess() {
        try {
            _process.writeSpoutMsg(_spoutMsg);

            while (true) {
                ShellMsg shellMsg = _process.readShellMsg();
                String command = shellMsg.getCommand();
                if (command.equals("sync")) {
                    return;
                } else if (command.equals("log")) {
                    handleLog(shellMsg);
                } else if (command.equals("emit")) {
                    String stream = shellMsg.getStream();
                    Long task = shellMsg.getTask();
                    List<Object> tuple = shellMsg.getTuple();
                    Object messageId = shellMsg.getId();
                    if (task == 0) {
                        List<Integer> outtasks = _collector.emit(stream, tuple, messageId);
                        if (shellMsg.areTaskIdsNeeded()) {
                            _process.writeTaskIds(outtasks);
                        }
                    } else {
                        _collector.emitDirect((int) task.longValue(), stream, tuple, messageId);
                    }
                } else if (command.equals("metrics")) {
                    handleMetrics(shellMsg);
                } else {
                    throw new RuntimeException("Unknown command received: " + command);
                }
            }
        } catch (Exception e) {
            String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
            throw new RuntimeException(processInfo, e);
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

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
}
