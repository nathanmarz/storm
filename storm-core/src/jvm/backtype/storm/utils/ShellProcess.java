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
    public static Logger ShellLogger;
    private Process      _subprocess;
    private InputStream  processErrorStream;
    private String[]     command;
    public ISerializer   serializer;
    public Number pid;
    public String componentName;

    public ShellProcess(String[] command) {
        this.command = command;
    }

    public Number launch(Map conf, TopologyContext context) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));

        ShellLogger = Logger.getLogger(context.getThisComponentId());

        this.componentName = context.getThisComponentId();
        this.serializer = getSerializer(conf);

        try {
            _subprocess = builder.start();
            processErrorStream = _subprocess.getErrorStream();
            serializer.initialize(_subprocess.getOutputStream(), _subprocess.getInputStream());
            this.pid = serializer.connect(conf, context);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error when launching multilang subprocess\n"
                            + getErrorsString(), e);
        } catch (NoOutputException e) {
            throw new RuntimeException(e + getErrorsString() + "\n");
        }
        return this.pid;
    }

    private ISerializer getSerializer(Map conf) {
        //get factory class name
        String serializer_className = (String)conf.get(Config.TOPOLOGY_MULTILANG_SERIALIZER);
        LOG.info("Storm multilang serializer: " + serializer_className);

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
        // Log any info sent on the error stream
        logErrorStream();
    }

    public void writeSpoutMsg(SpoutMsg msg) throws IOException {
        serializer.writeSpoutMsg(msg);
        // Log any info sent on the error stream
        logErrorStream();
    }

    public void writeTaskIds(List<Integer> taskIds) throws IOException {
        serializer.writeTaskIds(taskIds);
        // Log any info sent on the error stream
        logErrorStream();
    }

    public void logErrorStream() {
        try {
            while (processErrorStream.available() > 0) {
                int bufferSize = processErrorStream.available();
                byte[] errorReadingBuffer = new byte[bufferSize];
                processErrorStream.read(errorReadingBuffer, 0, bufferSize);
                ShellLogger.info(new String(errorReadingBuffer));
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

    /**
     *
     * @return pid, if the process has been launched, null otherwise.
     */
    public Number getPid() {
        return this.pid;
    }

    /**
     *
     * @return the name of component.
     */
    public String getComponentName() {
        return this.componentName;
    }

    /**
     *
     * @return exit code of the process if process is terminated, -1 if process is not started or terminated.
     */
    public int getExitCode() {
        try {
            return this._subprocess != null ? this._subprocess.exitValue() : -1;
        } catch(IllegalThreadStateException e) {
            return -1;
        }
    }

    public String getProcessInfoString() {
        return String.format("pid:%s, name:%s", pid, componentName);
    }

    public String getProcessTerminationInfoString() {
        return String.format(" exitCode:%s, errorString:%s ", getExitCode(), getErrorsString());
    }
}