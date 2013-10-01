package backtype.storm.multilang;

import backtype.storm.generated.ShellComponent;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.multilang.ShellProcess;
import backtype.storm.utils.Utils;
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
    private SpoutMsg spoutMsg;

    public ShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
        _process = new ShellProcess(new JsonSerializer(), _command);
    }

    public ShellSpout(String... command) {
        _command = command;
        _process = new ShellProcess(new JsonSerializer(), _command);
    }

    public ShellSpout(ISerializer serializer, String... command) {
        _command = command;
        _process = new ShellProcess(serializer, _command);
    }

    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;

        Number subpid = _process.launch(stormConf, context);
        LOG.info("Launched subprocess with pid " + subpid);
    }

    public void close() {
        _process.destroy();
    }

    public void nextTuple() {
        if (spoutMsg == null) {
            spoutMsg = new SpoutMsg();
        }
        spoutMsg.setCommand("next");
        spoutMsg.setId("");
        querySubprocess();
    }

    public void ack(Object msgId) {
    	if (spoutMsg == null) {
            spoutMsg = new SpoutMsg();
        }
        spoutMsg.setCommand("ack");
        spoutMsg.setId(msgId.toString());
        querySubprocess();
    }

    public void fail(Object msgId) {
    	if (spoutMsg == null) {
            spoutMsg = new SpoutMsg();
        }
        spoutMsg.setCommand("fail");
        spoutMsg.setId(msgId.toString());
        querySubprocess();
    }

    private void querySubprocess() {
        try {
            _process.writeSpoutMsg(spoutMsg);

            while (true) {
                Emission emission = _process.readEmission();
                String command = emission.getCommand();
                if (command.equals("sync")) {
                    return;
                } else if (command.equals("log")) {
                    String msg = emission.getMsg();
                    LOG.info("Shell msg: " + msg);
                } else if (command.equals("emit")) {
                    String stream = emission.getStream();
                    Long task = emission.getTask();
                    List<Object> tuple = emission.getTuple();
                    Object messageId = emission.getId();
                    if (task == 0) {
                        List<Integer> outtasks = _collector.emit(stream, tuple, messageId);
                        _process.writeTaskIds(outtasks);
                    } else {
                        _collector.emitDirect((int)task.longValue(), stream, tuple, messageId);
                    }
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
