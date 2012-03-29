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

    public Number launch(Map conf, TopologyContext context) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));
        _subprocess = builder.start();

        processIn = new DataOutputStream(_subprocess.getOutputStream());
        processOut = new BufferedReader(new InputStreamReader(_subprocess.getInputStream()));

        JSONObject pidDir = new JSONObject();
        pidDir.put("pidDir", context.getPIDDir());
        writeMessage(pidDir);

        Number pid = (Number)readMessage().get("pid");

        JSONObject setupInfo = new JSONObject();
        setupInfo.put("conf", conf);
        setupInfo.put("context", context);
        writeMessage(setupInfo);

        return pid;
    }

    public void destroy() {
        _subprocess.destroy();
    }

    public void writeMessage(Object msg) throws IOException {
        writeString(JSONValue.toJSONString(msg));
    }

    private void writeString(String str) throws IOException {
        byte[] strBytes = str.getBytes("UTF-8");
        processIn.write(strBytes, 0, strBytes.length);
        processIn.writeBytes("\nend\n");
        processIn.flush();
    }

    public JSONObject readMessage() throws IOException {
        String string = readString();
        JSONObject msg = (JSONObject)JSONValue.parse(string);
        if (msg != null) {
            return msg;
        } else {
            throw new IOException("unable to parse: " + string);
        }
    }

    private String readString() throws IOException {
        StringBuilder line = new StringBuilder();

        //synchronized (processOut) {
            while (true) {
                String subline = processOut.readLine();
                if(subline==null)
                    throw new RuntimeException("Pipe to subprocess seems to be broken!");
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
