package backtype.storm.utils;

import backtype.storm.task.TopologyContext;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class ShellProcess {
    private DataOutputStream processIn;
    private BufferedReader processOut;
    private Process _subprocess;
    private String[] command;

    public ShellProcess(String[] command) {
        this.command = command;
    }

    public String launch(Map conf, TopologyContext context) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));
        _subprocess = builder.start();

        processIn = new DataOutputStream(_subprocess.getOutputStream());
        processOut = new BufferedReader(new InputStreamReader(_subprocess.getInputStream()));

        writeString(context.getPIDDir());
        String pid = readString();
        writeObject(conf);
        writeObject(context);
        return pid;
    }

    public void destroy() {
        _subprocess.destroy();
    }

    public void writeObject(Object obj) throws IOException {
        writeString(JSONValue.toJSONString(obj));
    }

    public void writeString(String string) throws IOException {
        // don't need synchronization for now
        //synchronized (processIn) {
            processIn.writeBytes(string + "\nend\n");
            processIn.flush();
            //}
    }

    // returns null for sync. odd?
    public Map readMap() throws IOException {
        String string = readString();
        if (string.equals("sync")) {
            return null; // previously had: return readMap();
        } else {
            Map map = (Map)JSONValue.parse(string);
            if (map != null) {
                return map;
            } else {
                throw new IOException("unable to parse: " + string);
            }
        }
    }

    public String readString() throws IOException {
        StringBuilder line = new StringBuilder();

        //synchronized (processOut) {
            while (true) {
                String subline = processOut.readLine();
                if(subline==null)
                    throw new RuntimeException("Pipe to subprocess seems to be broken!");
                if(subline.equals("sync")) {
                    return subline;
                }
                if(subline.equals("end")) {
                    break;
                }
                if(line.length()!=0) {
                    line.append("\n");
                }
                line.append(subline);
            }
            //}

        return line.toString();
    }
}
