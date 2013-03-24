package backtype.storm.messaging;

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
}
