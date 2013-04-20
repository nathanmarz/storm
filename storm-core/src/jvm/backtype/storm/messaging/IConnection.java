package backtype.storm.messaging;

public interface IConnection {   
    /**
     * receive a message (consists taskId and payload)
     * @param flags 0: block, 1: non-block
     * @return
     */
    public TaskMessage recv(int flags);
    /**
     * send a message with taskId and payload
     * @param taskId task ID
     * @param payload
     */
    public void send(int taskId,  byte[] payload);
    
    /**
     * close this connection
     */
    public void close();
}
