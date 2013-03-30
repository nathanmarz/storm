package backtype.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
    private int _task;
    private byte[] _message;
    
    public TaskMessage(int task, byte[] message) {
        _task = task;
        _message = message;
    }
    
    public int task() {
        return _task;
    }

    public byte[] message() {
        return _message;
    }
    
    public byte[] serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length+2);
        bb.putShort((short)_task);
        bb.put(_message);
        return bb.array();
    }
    
    public void deserialize(byte[] packet) {
        if (packet==null) return;
        ByteBuffer bb = ByteBuffer.wrap(packet);
        _task = bb.getShort();
        _message = new byte[packet.length-2];
        bb.get(_message);
    }

}
