package backtype.storm.messaging.zmq;

import java.nio.ByteBuffer;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Connection implements IConnection {
    public static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    private Socket socket;
    
    Connection(Socket socket) {
        this.socket = socket;
    }
    
    public void close() {
       LOG.debug("zmq.Connection:close()");
       if (socket != null) {
           socket.close();
           socket = null;
       }
    }

    public TaskMessage recv(int flags) {
        LOG.debug("zmq.Connection:recv()");
        TaskMessage message = new TaskMessage(0, null);
        message.deserialize(ByteBuffer.wrap(socket.recv(flags)));
        return message;
    }

    public void send(int taskId, byte[] payload) {
        LOG.debug("zmq.Connection:send()");
        ByteBuffer buffer = new TaskMessage(taskId, payload).serialize();
        socket.send(buffer.array(),  ZMQ.NOBLOCK);
    }    
}
