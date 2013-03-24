package backtype.storm.messaging;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

public class TransportFactory {
    public static final Logger LOG = LoggerFactory.getLogger(TransportFactory.class);

    public static IContext makeContext(Map storm_conf) {
        
        IContext transport = null;
        try {
            //get factory class name
            String transport_plugin_klassName = (String)storm_conf.get(Config.STORM_MESSAGING_TRANSPORT);
            LOG.debug("Storm peer transport plugin:"+transport_plugin_klassName);
            //create a factory class
            Class klass = Class.forName(transport_plugin_klassName);
            //obtain a factory object
            ITransport factory = (ITransport)klass.newInstance();
            //create a context 
            transport = factory.newContext();
            //initialize with storm configuration
            transport.prepare(storm_conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        } 
        return transport;
    }
}
