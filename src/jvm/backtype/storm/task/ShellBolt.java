package backtype.storm.task;

import backtype.storm.generated.ShellComponent;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ShellProcess;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

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
    public static Logger LOG = Logger.getLogger(ShellBolt.class);
    Process _subprocess;
    OutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<String, Tuple>();

    private String[] _command;
    private ShellProcess _process;
    private volatile boolean _running = true;
    private LinkedBlockingQueue _pendingWrites = new LinkedBlockingQueue();

    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        _command = command;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        final OutputCollector collector) {
        _process = new ShellProcess(_command);
        _collector = collector;

        try {
            //subprocesses must send their pid first thing
            String subpid = _process.launch(stormConf, context);
            LOG.info("Launched subprocess with pid " + subpid);
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess", e);
        }

        // reader
        new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        Map action = _process.readMap();
                        if (action == null) {
                            // ignore sync
                        }

                        String command = (String) action.get("command");
                        if(command.equals("ack")) {
                            handleAck(action);
                        } else if (command.equals("fail")) {
                            handleFail(action);
                        } else if (command.equals("log")) {
                            String msg = (String) action.get("msg");
                            LOG.info("Shell msg: " + msg);
                        } else if (command.equals("emit")) {
                            handleEmit(action);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }).start();

        // writer
        new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        Object write = _pendingWrites.poll(1, SECONDS);
                        if (write != null) {
                            _process.writeObject(write);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }).start();
    }

    public void execute(Tuple input) {
        //just need an id
        String genId = Long.toString(MessageId.generateId());
        _inputs.put(genId, input);
        try {
            JSONObject obj = new JSONObject();
            obj.put("id", genId);
            obj.put("comp", input.getSourceComponent());
            obj.put("stream", input.getSourceStreamId());
            obj.put("task", input.getSourceTask());
            obj.put("tuple", input.getValues());
            _pendingWrites.put(obj);
        } catch(InterruptedException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void cleanup() {
        _process.destroy();
        _inputs.clear();
        _running = false;
    }

    private void handleAck(Map action) {
        String id = (String) action.get("id");
        Tuple acked = _inputs.remove(id);
        if(acked==null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(Map action) {
        String id = (String) action.get("id");
        Tuple failed = _inputs.remove(id);
        if(failed==null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleEmit(Map action) throws InterruptedException {
        String stream = (String) action.get("stream");
        if(stream==null) stream = Utils.DEFAULT_STREAM_ID;
        Long task = (Long) action.get("task");
        List<Object> tuple = (List) action.get("tuple");
        List<Tuple> anchors = new ArrayList<Tuple>();
        Object anchorObj = action.get("anchors");
        if(anchorObj!=null) {
            if(anchorObj instanceof String) {
                anchorObj = Arrays.asList(anchorObj);
            }
            for(Object o: (List) anchorObj) {
                Tuple t = _inputs.get((String) o);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + o + " after ack/fail");
                }
                anchors.add(t);
            }
        }
        if(task==null) {
            List<Integer> outtasks = _collector.emit(stream, anchors, tuple);
            _pendingWrites.put(outtasks);
        } else {
            _collector.emitDirect((int)task.longValue(), stream, anchors, tuple);
        }
    }
}
