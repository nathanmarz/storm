package backtype.storm.messaging.zmq;

import java.nio.ByteBuffer;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.TransportFactory;

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
        byte[] packet = socket.recv(flags);
        return parsePacket(packet);
    }

    public void send(int taskId, byte[] payload) {
        LOG.debug("zmq.Connection:send()");
        byte[] packet = mkPacket(new TaskMessage(taskId, payload));
        socket.send(packet,  ZMQ.NOBLOCK);
    }
    
    private byte[] mkPacket(TaskMessage message) {
        byte[] payload = message.message();
        ByteBuffer bb = ByteBuffer.allocate(payload.length+2);
        bb.putShort((short)message.task());
        bb.put(payload);
        return bb.array();
    }
    
    private TaskMessage parsePacket(byte[] packet) {
        if (packet==null) return null;
        ByteBuffer bb = ByteBuffer.wrap(packet);
        int task = bb.getShort();
        byte[] payload = new byte[packet.length-2];
        bb.get(payload);
        return new TaskMessage(task,payload);
    }
}
