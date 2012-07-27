package backtype.storm.utils;

import backtype.storm.task.TopologyContext;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class ShellProcess {
    private DataOutputStream processIn;
    private BufferedReader processOut;
    private InputStream processErrorStream;
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
        processErrorStream = _subprocess.getErrorStream();

        JSONObject setupInfo = new JSONObject();
        setupInfo.put("pidDir", context.getPIDDir());
        setupInfo.put("conf", conf);
        setupInfo.put("context", context);
        writeMessage(setupInfo);

        return (Number)readMessage().get("pid");
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

    public String getErrorsString() {
        if(processErrorStream!=null) {
            try {
                return IOUtils.toString(processErrorStream);
            } catch(IOException e) {
                return "(Unable to capture error stream)";
            }
        } else {
            return "";
        }
    }

    private String readString() throws IOException {
        StringBuilder line = new StringBuilder();

        //synchronized (processOut) {
            while (true) {
                String subline = processOut.readLine();
                if(subline==null) {
                    StringBuilder errorMessage = new StringBuilder();
                    errorMessage.append("Pipe to subprocess seems to be broken!");
                    if (line.length() == 0) {
                        errorMessage.append(" No output read.\n");
                    }
                    else {
                        errorMessage.append(" Currently read output: " + line.toString() + "\n");
                    }
                    errorMessage.append("Shell Process Exception:\n");
                    errorMessage.append(getErrorsString() + "\n");
                    throw new RuntimeException(errorMessage.toString());
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
