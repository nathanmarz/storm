package backtype.storm.task;

import backtype.storm.generated.ShellComponent;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

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
    DataOutputStream _processin;
    BufferedReader _processout;
    OutputCollector _collector;
    Map<Long, Tuple> _inputs = new HashMap<Long, Tuple>();
    String[] command;
    
    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        this.command = command;
    }

    private String initializeSubprocess(TopologyContext context) {
        //can change this to launchSubprocess and have it return the pid (that the subprcess returns)
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));
        try {
            _subprocess = builder.start();
            _processin = new DataOutputStream(_subprocess.getOutputStream());
            _processout = new BufferedReader(new InputStreamReader(_subprocess.getInputStream()));
            sendToSubprocess(context.getPIDDir());
            //subprocesses must send their pid first thing
            String subpid = _processout.readLine();
            LOG.info("Launched subprocess with pid " + subpid);
            return subpid;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            initializeSubprocess(context);
            _collector = collector;

            sendToSubprocess(JSONValue.toJSONString(stormConf));
            sendToSubprocess(context.toJSONString());
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess", e);
        }
    }

    public void execute(Tuple input) {
        //just need an id
        long genId = MessageId.generateId();
        _inputs.put(genId, input);
        try {
            JSONObject obj = new JSONObject();
            obj.put("id", genId);
            obj.put("comp", input.getSourceComponent());
            obj.put("stream", input.getSourceStreamId());
            obj.put("task", input.getSourceTask());
            obj.put("tuple", input.getValues());
            sendToSubprocess(obj.toString());
            while(true) {
              String line = "";
              while(true) {
                  String subline = _processout.readLine();
                  if(subline==null)
                      throw new RuntimeException("Pipe to subprocess seems to be broken!");
                  if(subline.equals("sync")) {
                      line = subline;
                      break;
                  }
                  if(subline.equals("end")) {
                      break;
                  }
                  if(line.length()!=0) {
                      line+="\n";
                  }
                  line+=subline;
              }
              if(line.equals("sync")) {
                  break;
              } else {
                  Map action = (Map) JSONValue.parse(line);
                  String command = (String) action.get("command");
                  if(command.equals("ack")) {
                    Long id = (Long) action.get("id");
                    Tuple acked = _inputs.remove(id);
                    if(acked==null) {
                        throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
                    }
                    _collector.ack(acked);
                  } else if (command.equals("fail")) {
                    Long id = (Long) action.get("id");
                    Tuple failed = _inputs.remove(id);
                    if(failed==null) {
                        throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
                    }
                    _collector.fail(failed);
                  } else if (command.equals("log")) {
                    String msg = (String) action.get("msg");
                    LOG.info("Shell msg: " + msg);
                  } else if(command.equals("emit")) {
                    String stream = (String) action.get("stream");
                    if(stream==null) stream = Utils.DEFAULT_STREAM_ID;
                    Long task = (Long) action.get("task");
                    List<Object> tuple = (List) action.get("tuple");
                    List<Tuple> anchors = new ArrayList<Tuple>();
                    Object anchorObj = action.get("anchors");
                    if(anchorObj!=null) {
                        if(anchorObj instanceof Long) {
                            anchorObj = Arrays.asList(anchorObj);
                        }
                        for(Object o: (List) anchorObj) {
                            anchors.add(_inputs.get((Long) o));
                        }
                    }
                    if(task==null) {
                       List<Integer> outtasks = _collector.emit(stream, anchors, tuple);
                       sendToSubprocess(JSONValue.toJSONString(outtasks));
                    } else {
                        _collector.emitDirect((int)task.longValue(), stream, anchors, tuple);
                    }
                  }
              }
            }
        } catch(IOException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void cleanup() {
        _subprocess.destroy();
        _inputs.clear();
        _processin = null;
        _processout = null;
        _collector = null;
    }

    private void sendToSubprocess(String str) throws IOException {
        _processin.writeBytes(str + "\n");
        _processin.writeBytes("end\n");
        _processin.flush();
    }

}
