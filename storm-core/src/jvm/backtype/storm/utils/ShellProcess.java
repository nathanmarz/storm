package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.multilang.ISerializer;
import backtype.storm.multilang.BoltMsg;
import backtype.storm.multilang.NoOutputException;
import backtype.storm.multilang.ShellMsg;
import backtype.storm.multilang.SpoutMsg;
import backtype.storm.task.TopologyContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class ShellProcess implements Serializable {
    public static Logger LOG = Logger.getLogger(ShellProcess.class);
    private Process      _subprocess;
    private InputStream  processErrorStream;
    private String[]     command;
    public ISerializer   serializer;

    public ShellProcess(String[] command) {
        this.command = command;
    }

    public Number launch(Map conf, TopologyContext context) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));

        this.serializer = getSerializer(conf);

        Number pid;
        try {
            _subprocess = builder.start();
            processErrorStream = _subprocess.getErrorStream();
            serializer.initialize(_subprocess.getOutputStream(),
                    _subprocess.getInputStream());
            pid = serializer.connect(conf, context);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error when launching multilang subprocess\n"
                            + getErrorsString(), e);
        } catch (NoOutputException e) {
            throw new RuntimeException(e + getErrorsString() + "\n");
        }
        return pid;
    }

    private ISerializer getSerializer(Map conf) {
        //get factory class name
        String serializer_className = (String)conf.get(Config.STORM_MULTILANG_SERIALIZER);
        LOG.info("Storm multilang serializer:" + serializer_className);

        ISerializer serializer = null;
        try {
            //create a factory class
            Class klass = Class.forName(serializer_className);
            //obtain a serializer object
            Object obj = klass.newInstance();
            serializer = (ISerializer)obj;
        } catch(Exception e) {
            throw new RuntimeException("Failed to construct multilang serializer from serializer " + serializer_className, e);
        }
        return serializer;
    }

    public void destroy() {
        _subprocess.destroy();
    }

    public ShellMsg readShellMsg() throws IOException {
        try {
            return serializer.readShellMsg();
        } catch (NoOutputException e) {
            throw new RuntimeException(e + getErrorsString() + "\n");
        }
    }

    public void writeBoltMsg(BoltMsg msg) throws IOException {
        serializer.writeBoltMsg(msg);
        // drain the error stream to avoid dead lock because of full error
        // stream buffer
        drainErrorStream();
    }

    public void writeSpoutMsg(SpoutMsg msg) throws IOException {
        serializer.writeSpoutMsg(msg);
        // drain the error stream to avoid dead lock because of full error
        // stream buffer
        drainErrorStream();
    }

    public void writeTaskIds(List<Integer> taskIds) throws IOException {
        serializer.writeTaskIds(taskIds);
        // drain the error stream to avoid dead lock because of full error
        // stream buffer
        drainErrorStream();
    }

    public void drainErrorStream() {
        try {
            while (processErrorStream.available() > 0) {
                int bufferSize = processErrorStream.available();
                byte[] errorReadingBuffer = new byte[bufferSize];

                processErrorStream.read(errorReadingBuffer, 0, bufferSize);

                LOG.info("Got error from shell process: "
                        + new String(errorReadingBuffer));
            }
        } catch (Exception e) {
        }
    }

    public String getErrorsString() {
        if (processErrorStream != null) {
            try {
                return IOUtils.toString(processErrorStream);
            } catch (IOException e) {
                return "(Unable to capture error stream)";
            }
        } else {
            return "";
        }
    }
}
