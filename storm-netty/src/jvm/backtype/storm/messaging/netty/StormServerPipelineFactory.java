package backtype.storm.messaging.netty;

import java.util.Map;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;


class StormServerPipelineFactory implements  ChannelPipelineFactory {
    private Server server;
    @SuppressWarnings("rawtypes")
    private Map conf;
    
    @SuppressWarnings("rawtypes")
    StormServerPipelineFactory(Server server, Map conf) {
        this.server = server;        
        this.conf = conf;
    }
    
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder());
        // Encoder
        pipeline.addLast("encoder", new MessageEncoder(conf));
        // business logic.
        pipeline.addLast("handler", new StormServerHandler(server));

        return pipeline;
    }
}
