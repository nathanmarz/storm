package backtype.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
    final int SHORT_SIZE = 2;
    final int INT_SIZE = 4;    
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
    
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length+SHORT_SIZE+INT_SIZE);
        bb.putShort((short)_task);
        if (_message==null) 
            bb.putInt(0);
        else {
            bb.putInt(_message.length);
            bb.put(_message);
        }
        return bb;
    }
    
    public void deserialize(ByteBuffer packet) {
        if (packet==null) return;
        _task = packet.getShort();
        int len = packet.getInt();
        if (len ==0) 
            _message = null; 
        else {
            _message = new byte[len];
            packet.get(_message);
        }
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("task:");
        buf.append(_task);
        buf.append(" message size:");
        if (_message!=null) buf.append(_message.length);
        else buf.append(0);
        return buf.toString();
    }
}
