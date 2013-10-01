package backtype.storm.multilang;

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
    private Process _subprocess;
    private InputStream processErrorStream;
    private String[] command;
    public ISerializer serializer;

    public ShellProcess(ISerializer serializer, String[] command) {
        this.command = command;
        this.serializer = serializer;
    }

    public Number launch(Map conf, TopologyContext context) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));

        Number pid;
        try {
	        _subprocess = builder.start();
	        processErrorStream = _subprocess.getErrorStream();
	        serializer.initialize(_subprocess.getOutputStream(), _subprocess.getInputStream());
	        pid = serializer.connect(conf, context);
        } catch (IOException e) {
        	throw new RuntimeException("Error when launching multilang subprocess\n" + getErrorsString(), e);
        } catch (NoOutputException e) {
            throw new RuntimeException(e + getErrorsString() + "\n");
    	}
        return pid;
    }

    public void destroy() {
        _subprocess.destroy();
    }

    public Emission readEmission() throws IOException {
    	try  {
    		return serializer.readEmission();
    	} catch (NoOutputException e) {
            throw new RuntimeException(e + getErrorsString() + "\n");
    	}
    }

    public void writeImmission(Immission msg) throws IOException {
    	serializer.writeImmission(msg);
        // drain the error stream to avoid dead lock because of full error stream buffer
        drainErrorStream();
    }

    public void writeSpoutMsg(SpoutMsg msg) throws IOException {
    	serializer.writeSpoutMsg(msg);
        // drain the error stream to avoid dead lock because of full error stream buffer
        drainErrorStream();
    }

    public void writeTaskIds(List<Integer> taskIds) throws IOException {
    	serializer.writeTaskIds(taskIds);
        // drain the error stream to avoid dead lock because of full error stream buffer
        drainErrorStream();
    }

    public void drainErrorStream()
    {
        try {
            while (processErrorStream.available() > 0)
            {
                int bufferSize = processErrorStream.available();
                byte[] errorReadingBuffer =  new byte[bufferSize];

                processErrorStream.read(errorReadingBuffer, 0, bufferSize);

                LOG.info("Got error from shell process: " + new String(errorReadingBuffer));
            }
        } catch(Exception e) {
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
}
