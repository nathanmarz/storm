package backtype.storm.multilang;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;

/**
 * The ISerializer interface describes the methods that an object should
 * implement to provide serialization and de-serialization capabilities to
 * non-JVM language components.
 */
public interface ISerializer extends Serializable {

    /**
     * This method sets the input and output streams of the serializer
     *
     * @param processIn output stream to non-JVM component
     * @param processOut input stream from non-JVM component
     */
    void initialize(OutputStream processIn, InputStream processOut);

    /**
     * This method transmits the Storm config to the non-JVM process and
     * receives its pid.
     *
     * @param conf storm configuration
     * @param context topology context
     * @return process pid
     */
    Number connect(Map conf, TopologyContext context) throws IOException,
            NoOutputException;

    /**
     * This method receives a shell message from the non-JVM process
     *
     * @return shell message
     */
    ShellMsg readShellMsg() throws IOException, NoOutputException;

    /**
     * This method sends a bolt message to a non-JVM bolt process
     *
     * @param msg bolt message
     */
    void writeBoltMsg(BoltMsg msg) throws IOException;

    /**
     * This method sends a spout message to a non-JVM spout process
     *
     * @param msg spout message
     */
    void writeSpoutMsg(SpoutMsg msg) throws IOException;

    /**
     * This method sends a list of task IDs to a non-JVM bolt process
     *
     * @param taskIds list of task IDs
     */
    void writeTaskIds(List<Integer> taskIds) throws IOException;
}
