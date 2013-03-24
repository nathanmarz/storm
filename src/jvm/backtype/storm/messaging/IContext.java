package backtype.storm.messaging;

import java.util.Map;

public interface IContext {
    public void prepare(Map storm_conf);
    
    public IConnection bind(String storm_id, int port);
    public IConnection connect(String storm_id, String host, int port);
    public void term();
};
