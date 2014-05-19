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

    private void querySubprocess() {
        try {
            _process.writeSpoutMsg(_spoutMsg);

            while (true) {
                ShellMsg shellMsg = _process.readShellMsg();
                String command = shellMsg.getCommand();
                if (command.equals("sync")) {
                    return;
                } else if (command.equals("log")) {
                    String msg = shellMsg.getMsg();
                    LOG.info("Shell msg: " + msg);
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
                } else {
                    throw new RuntimeException("Unknown command received: " + command);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
}
