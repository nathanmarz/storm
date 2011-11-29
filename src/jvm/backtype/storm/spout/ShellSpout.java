package backtype.storm.spout;

import backtype.storm.generated.ShellComponent;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class ShellSpout implements ISpout {
	
    public static Logger LOG = Logger.getLogger(ShellSpout.class);

	String _shellCommand;
    String _codeResource;

    Process _subprocess;
	DataOutputStream _processin;
    BufferedReader _processout;

    SpoutOutputCollector _collector;
	
    public ShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }
    
    public ShellSpout(String shellCommand, String codeResource) {
        _shellCommand = shellCommand;
        _codeResource = codeResource;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
            initializeSubprocess(context);
            _collector = collector;

            sendToSubprocess(JSONValue.toJSONString(conf));
            sendToSubprocess(context.toJSONString());
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess", e);
        }		
    }

    public void close() {
        _subprocess.destroy();
        _processin = null;
        _processout = null;
        _collector = null;
    }

    public void nextTuple() {
    	try
		{
			//send the sub process the nextTuple command, then wait for a response
    		sendLineToSubprocess("next");
			
			String line = "";
			while (true)
			{
               	String subline = _processout.readLine();
				if(subline == null) {
					throw new RuntimeException("Pipe to subprocess seems to be broken!");
				}					

				if (subline.equals("end")) {
					break;
				}

				if (line.length() != 0) {
					line += "\n";
				}
				
				line += subline;
			}
			
			if (line.length() > 0)
			{		
				Map action = (Map) JSONValue.parse(line);
				String command = (String) action.get("command");
				
				if (command.equals("emit")) {
					Long id = (Long) action.get("id");
                    Long stream = (Long) action.get("stream");
                    Long task = (Long) action.get("task");
                    List<Object> tuple = (List) action.get("tuple");
					List<Tuple> anchors = new ArrayList<Tuple>();
                    Object anchorObj = action.get("anchors");

                    if (stream == null) {
						stream = (long) Utils.DEFAULT_STREAM_ID;
					}

					if (task == null) {
                       List<Integer> outtasks = _collector.emit((int)stream.longValue(), tuple, id);
                    } else {
                        _collector.emitDirect((int)task.longValue(), (int)stream.longValue(), tuple, id);
                    }
				}
				else if (command.equals("log")) {
                    String msg = (String) action.get("msg");
                    LOG.info("Shell msg: " + msg);
				}
			}
		}
		catch(IOException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void ack(Object msgId) {
        try
		{
			JSONObject obj = new JSONObject();
            obj.put("id", (Long) msgId);
			obj.put("command", "ack");
			
            sendToSubprocess(obj.toString());
		}
		catch(IOException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void fail(Object msgId) {
        try
		{
			JSONObject obj = new JSONObject();
            obj.put("id", (Long) msgId);
			obj.put("command", "fail");
			
            sendToSubprocess(obj.toString());
		}
		catch(IOException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

	private String initializeSubprocess(TopologyContext context) {
        //can change this to launchSubprocess and have it return the pid (that the subprocess returns)
        ProcessBuilder builder = new ProcessBuilder(_shellCommand, _codeResource);
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

	private void sendToSubprocess(String str) throws IOException {
        _processin.writeBytes(str + "\n");
        _processin.writeBytes("end\n");
        _processin.flush();
    }	
	
	private void sendLineToSubprocess(String str) throws IOException {
		_processin.writeBytes(str + "\n");
		_processin.flush();
	}
}
