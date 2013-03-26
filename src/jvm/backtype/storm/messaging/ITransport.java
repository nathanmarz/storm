package backtype.storm.messaging;

import java.util.Map;

/**
 * This interface needs to be implemented for messaging plugin. 
 * 
 * Messging plugin should be specified via Storm config parameter, storm.messaging.transport.
 * 
 * A messaging plugin needs to implements a simple interface, ITransport, 
 * and should have a default constructor and implements, newContext(), to return an IContext.
 * Immedaitely, we will invoke IContext::prepare(storm_conf) to enable context to be configured
 * according to storm configuration. 
 */
public interface ITransport {
    public IContext newContext();
}
