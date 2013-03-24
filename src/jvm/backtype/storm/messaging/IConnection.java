package backtype.storm.messaging;

import clojure.lang.PersistentVector;

public interface IConnection {    
    public TaskMessage recv();
    public TaskMessage recv_with_flags(int flags);
    public void send(int task,  byte[] message);
    public void close();
}
