package backtype.storm.messaging;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

public class TransportFactory {
    public static final Logger LOG = LoggerFactory.getLogger(TransportFactory.class);

    public static IContext makeContext(Map storm_conf) {
        
        //get factory class name
        String transport_plugin_klassName = (String)storm_conf.get(Config.STORM_MESSAGING_TRANSPORT);
        LOG.info("Storm peer transport plugin:"+transport_plugin_klassName);

        IContext transport = null;
        try {
            //create a factory class
            Class klass = Class.forName(transport_plugin_klassName);
            //obtain a context object
            transport = (IContext)klass.newInstance();
            //initialize with storm configuration
            transport.prepare(storm_conf);
        } catch(Exception e) {
            throw new RuntimeException("Fail to construct messaging plugin from plugin "+transport_plugin_klassName, e);
        } 
        return transport;
    }
}
